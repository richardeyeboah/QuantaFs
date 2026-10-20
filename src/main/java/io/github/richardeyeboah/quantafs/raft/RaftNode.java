package io.github.richardeyeboah.quantafs.raft;

import io.github.richardeyeboah.quantafs.raft.ProposalOutcome.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;

/**
 * Deterministic Raft core for one voter: leader election (milestone 2A) plus log replication,
 * commitment, and ordered application (milestone 2B).
 *
 * <p><b>Inputs and outputs.</b> {@link #tick()} advances logical time by one tick,
 * {@link #receive(RaftMessage)} delivers a message, and {@link #propose(Command)} submits a client
 * command. Each returns the messages to send. Final results of proposals are collected with
 * {@link #drainOutcomes()}. The node owns no threads, reads no clock, and does no network I/O.
 *
 * <p><b>Durability ordering.</b> Every change to {@code currentTerm}, {@code votedFor}, or the log is
 * written to {@link RaftStorage} in one atomic write that completes <em>before</em> any message that
 * depends on it is returned. Committed entries are applied to the {@link StateMachine} strictly in
 * index order.
 *
 * <p><b>Fail closed.</b> If storage or the state machine fails, the node halts: that input yields no
 * messages, every later input is ignored, and pending proposals are reported {@code UNKNOWN}.
 * Restarting (constructing a new node over the same storage) resumes from what is durable.
 *
 * <p><b>Restart.</b> {@code lastApplied} comes from the state machine's durable marker, and
 * {@code commitIndex} starts equal to it: applied entries are known to be committed, while a log
 * entry's mere presence proves nothing. The rest is relearned from the leader.
 *
 * <p>Rule references are to the Raft paper (Ongaro and Ousterhout, 2014), mainly Figure 2. Not
 * implemented: snapshots, membership changes, PreVote, leader leases or read index. Not thread-safe.
 */
public final class RaftNode {

    /** Upper bound on entries carried by one AppendEntries message. */
    public static final int MAX_ENTRIES_PER_APPEND = 16;

    private final NodeId id;
    private final ClusterConfig cluster;
    private final List<NodeId> peers;
    private final ElectionTiming timing;
    private final RandomSource random;
    private final RaftStorage storage;
    private final StateMachine stateMachine;

    // Mirrors of durable state, updated only after the storage write succeeds.
    private long currentTerm;
    private Optional<NodeId> votedFor;

    // Volatile state, rebuilt on restart.
    private Role role = Role.FOLLOWER;
    private NodeId leaderId;
    private final Set<NodeId> votesGranted = new TreeSet<>();
    private final Set<NodeId> votesRejected = new TreeSet<>();
    private int electionElapsedTicks;
    private int electionTimeoutTicks;
    private int heartbeatElapsedTicks;
    private RuntimeException haltCause;
    private long ignoredMessages;
    private long commitIndex;
    private long lastApplied;

    // Leader-only volatile state.
    private final Map<NodeId, Long> nextIndex = new TreeMap<>();
    private final Map<NodeId, Long> matchIndex = new TreeMap<>();
    private final NavigableMap<Long, Long> pendingProposals = new TreeMap<>(); // index -> term
    private final List<ProposalOutcome> outcomes = new ArrayList<>();

    /**
     * Creates a follower from durable state.
     *
     * @throws PersistenceException if storage and state machine are inconsistent; the node refuses
     *     to start rather than guess
     */
    public RaftNode(
            NodeId id,
            ClusterConfig cluster,
            ElectionTiming timing,
            RandomSource random,
            RaftStorage storage,
            StateMachine stateMachine) {
        this.id = Objects.requireNonNull(id, "id");
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.timing = Objects.requireNonNull(timing, "timing");
        this.random = Objects.requireNonNull(random, "random");
        this.storage = Objects.requireNonNull(storage, "storage");
        this.stateMachine = Objects.requireNonNull(stateMachine, "stateMachine");
        this.peers = cluster.peersOf(id);

        TermAndVote durable = storage.termAndVote();
        LogPosition last = storage.lastPosition();
        if (last.term() > durable.currentTerm()) {
            throw new PersistenceException("refusing to start: last log term " + last.term()
                    + " exceeds persisted term " + durable.currentTerm());
        }
        LogPosition applied = stateMachine.lastApplied();
        if (applied.index() > last.index()) {
            throw new PersistenceException("refusing to start: state machine applied index " + applied.index()
                    + " but the log ends at " + last.index() + "; applied log entries are missing");
        }
        if (applied.index() > 0 && storage.termAt(applied.index()) != applied.term()) {
            throw new PersistenceException("refusing to start: applied entry " + applied
                    + " differs from log term " + storage.termAt(applied.index()) + " at that index");
        }
        this.currentTerm = durable.currentTerm();
        this.votedFor = durable.votedFor();
        this.lastApplied = applied.index();
        this.commitIndex = applied.index();
        resetElectionTimer();
    }

    // ================================================================ inputs

    /** Advances logical time by one tick. */
    public List<RaftMessage> tick() {
        return guarded(this::onTick);
    }

    /** Handles one message delivered by the network. */
    public List<RaftMessage> receive(RaftMessage message) {
        Objects.requireNonNull(message, "message");
        return guarded(() -> onReceive(message));
    }

    /**
     * Submits a client command. Only the leader accepts; it appends the command durably and starts
     * replicating it. Success is reported later through {@link #drainOutcomes()}.
     */
    public ProposeResult propose(Command command) {
        Objects.requireNonNull(command, "command");
        if (command instanceof Command.NoOp) {
            throw new IllegalArgumentException("NO_OP entries are internal to the leader");
        }
        if (isHalted()) {
            return new ProposeResult.Unavailable("node halted: " + haltCause.getMessage());
        }
        if (role != Role.LEADER) {
            return new ProposeResult.NotLeader(Optional.ofNullable(leaderId));
        }
        LogEntry entry = new LogEntry(storage.lastIndex() + 1, currentTerm, command);
        try {
            storage.write(StorageWrite.append(List.of(entry)));
            pendingProposals.put(entry.index(), entry.term());
            advanceLeaderCommit(); // a single-voter cluster commits immediately
            return new ProposeResult.Accepted(entry.index(), entry.term(), broadcastAppendEntries());
        } catch (PersistenceException | StateMachineException e) {
            pendingProposals.remove(entry.index());
            halt(e);
            return new ProposeResult.OutcomeUnknown(e.getMessage());
        }
    }

    /** Proposal outcomes produced since the last call, in the order they were decided. */
    public List<ProposalOutcome> drainOutcomes() {
        List<ProposalOutcome> drained = List.copyOf(outcomes);
        outcomes.clear();
        return drained;
    }

    private List<RaftMessage> guarded(Supplier<List<RaftMessage>> step) {
        if (isHalted()) {
            return List.of();
        }
        try {
            return step.get();
        } catch (PersistenceException | StateMachineException e) {
            halt(e);
            return List.of();
        }
    }

    private void halt(RuntimeException cause) {
        haltCause = cause;
        abandonPendingProposals();
    }

    private List<RaftMessage> onTick() {
        if (role == Role.LEADER) {
            // Figure 2, Leaders: repeat AppendEntries during idle periods. Every heartbeat is a real
            // AppendEntries for that follower's nextIndex, so it also retries replication.
            heartbeatElapsedTicks++;
            if (heartbeatElapsedTicks >= timing.heartbeatIntervalTicks()) {
                heartbeatElapsedTicks = 0;
                return broadcastAppendEntries();
            }
            return List.of();
        }
        electionElapsedTicks++;
        if (electionElapsedTicks >= electionTimeoutTicks) {
            return startElection();
        }
        return List.of();
    }

    private List<RaftMessage> onReceive(RaftMessage message) {
        // Membership is checked before the message can influence any state, including its term.
        if (!message.to().equals(id) || !cluster.contains(message.from())) {
            ignoredMessages++;
            return List.of();
        }
        return switch (message) {
            case RequestVoteRequest request -> onRequestVote(request);
            case RequestVoteResponse response -> onRequestVoteResponse(response);
            case AppendEntriesRequest request -> onAppendEntries(request);
            case AppendEntriesResponse response -> onAppendEntriesResponse(response);
        };
    }

    // ================================================================ elections (2A)

    private List<RaftMessage> startElection() {
        // Candidates: increment currentTerm and vote for self, durably, before any RequestVote.
        persist(new TermAndVote(currentTerm + 1, Optional.of(id)));
        role = Role.CANDIDATE;
        leaderId = null;
        votesGranted.clear();
        votesRejected.clear();
        votesGranted.add(id);
        resetElectionTimer();
        if (votesGranted.size() >= cluster.quorum()) {
            return becomeLeader(); // single-voter cluster
        }
        LogPosition lastLog = storage.lastPosition();
        return peers.stream()
                .<RaftMessage>map(peer -> new RequestVoteRequest(id, peer, currentTerm, lastLog))
                .toList();
    }

    private List<RaftMessage> onRequestVote(RequestVoteRequest request) {
        // RequestVote receiver rule 1: reply false if term < currentTerm.
        if (request.term() < currentTerm) {
            return List.of(new RequestVoteResponse(id, request.from(), currentTerm, false));
        }
        boolean newTerm = request.term() > currentTerm;
        Optional<NodeId> vote = newTerm ? Optional.empty() : votedFor;

        // Receiver rule 2: grant if not yet voted for someone else and the candidate's log is at
        // least as up to date as this node's durable log (section 5.4.1).
        boolean mayVote = vote.isEmpty() || vote.get().equals(request.from());
        boolean logOk = request.lastLog().isAtLeastAsUpToDateAs(storage.lastPosition());
        boolean grant = mayVote && logOk;
        if (grant) {
            vote = Optional.of(request.from());
        }
        persist(new TermAndVote(request.term(), vote)); // term and vote in one atomic write
        if (newTerm) {
            stepDownAfterTermChange();
        }
        if (grant) {
            resetElectionTimer(); // Followers: granting a vote restarts the election timer
        }
        return List.of(new RequestVoteResponse(id, request.from(), currentTerm, grant));
    }

    private List<RaftMessage> onRequestVoteResponse(RequestVoteResponse response) {
        if (response.term() > currentTerm) {
            return adoptHigherTerm(response.term());
        }
        if (response.term() < currentTerm || role != Role.CANDIDATE) {
            return List.of();
        }
        if (response.voteGranted()) {
            votesGranted.add(response.from()); // a set counts each voter once
            votesRejected.remove(response.from());
            if (votesGranted.size() >= cluster.quorum()) {
                return becomeLeader();
            }
        } else if (!votesGranted.contains(response.from())) {
            votesRejected.add(response.from());
        }
        return List.of();
    }

    private List<RaftMessage> becomeLeader() {
        role = Role.LEADER;
        leaderId = id;
        heartbeatElapsedTicks = 0;
        long next = storage.lastIndex() + 1;
        for (NodeId peer : peers) {
            nextIndex.put(peer, next);
            matchIndex.put(peer, 0L);
        }
        // Section 8: a new leader appends a no-op from its own term. Only current-term entries can be
        // committed by counting replicas (section 5.4.2), so this is what lets it commit, and thereby
        // discover, the entries it inherited from earlier terms.
        storage.write(StorageWrite.append(List.of(new LogEntry(next, currentTerm, Command.noOp()))));
        advanceLeaderCommit();
        return broadcastAppendEntries();
    }

    /** All servers: if a message carries term T > currentTerm, adopt T and become a follower. */
    private List<RaftMessage> adoptHigherTerm(long term) {
        persist(new TermAndVote(term, Optional.empty()));
        stepDownAfterTermChange();
        return List.of();
    }

    /**
     * Called after a higher term is durable. A leader or candidate becomes a follower with a fresh
     * election timer; a follower keeps its running timer. A leader's unfinished proposals become
     * {@code UNKNOWN}: a later leader may still commit them.
     */
    private void stepDownAfterTermChange() {
        leaderId = null;
        votesGranted.clear();
        votesRejected.clear();
        if (role == Role.LEADER) {
            abandonPendingProposals();
            nextIndex.clear();
            matchIndex.clear();
        }
        if (role != Role.FOLLOWER) {
            role = Role.FOLLOWER;
            resetElectionTimer();
        }
    }

    // ================================================================ replication (2B), follower side

    private List<RaftMessage> onAppendEntries(AppendEntriesRequest request) {
        // Receiver rule 1: reply false if term < currentTerm, without resetting the timer.
        if (request.term() < currentTerm) {
            return List.of(failure(request));
        }
        boolean newTerm = request.term() > currentTerm;
        if (!newTerm) {
            if (role == Role.LEADER) {
                throw new IllegalStateException("Election Safety violated: " + id + " and " + request.from()
                        + " both lead term " + currentTerm);
            }
            if (leaderId != null && !leaderId.equals(request.from())) {
                throw new IllegalStateException("Election Safety violated: " + leaderId + " and "
                        + request.from() + " both lead term " + currentTerm);
            }
        }

        // Receiver rule 2: the log must contain an entry at prevLogIndex whose term is prevLogTerm.
        long lastIndex = storage.lastIndex();
        boolean consistent = request.prevLogIndex() <= lastIndex
                && storage.termAt(request.prevLogIndex()) == request.prevLogTerm();

        // Rules 3 and 4: skip entries already present with the same term; at the first index whose
        // term differs, delete that entry and everything after it; append whatever is new. An
        // identical, duplicated, or older (shorter) request therefore changes nothing: a matching
        // request never truncates.
        long truncateFrom = 0;
        List<LogEntry> toAppend = List.of();
        if (consistent) {
            List<LogEntry> entries = request.entries();
            int firstNew = entries.size();
            for (int i = 0; i < entries.size(); i++) {
                LogEntry entry = entries.get(i);
                if (entry.index() > lastIndex) {
                    firstNew = i;
                    break;
                }
                if (storage.termAt(entry.index()) != entry.term()) {
                    if (entry.index() <= commitIndex) {
                        throw new IllegalStateException("leader " + request.from() + " (term " + request.term()
                                + ") conflicts with committed entry " + entry.index() + " on " + id);
                    }
                    truncateFrom = entry.index();
                    firstNew = i;
                    break;
                }
            }
            toAppend = entries.subList(firstNew, entries.size());
        }

        // A newer term and the log changes go to disk in one atomic write, before the reply exists.
        Optional<TermAndVote> termChange =
                newTerm ? Optional.of(new TermAndVote(request.term(), Optional.empty())) : Optional.empty();
        StorageWrite write = new StorageWrite(termChange, truncateFrom, toAppend);
        if (!write.isEmpty()) {
            storage.write(write);
        }
        if (newTerm) {
            currentTerm = request.term();
            votedFor = Optional.empty();
            stepDownAfterTermChange();
        }
        if (role == Role.CANDIDATE) {
            role = Role.FOLLOWER; // Candidates: AppendEntries from the new leader => follower
            votesGranted.clear();
            votesRejected.clear();
        }
        leaderId = request.from();
        resetElectionTimer(); // Followers: AppendEntries from the current leader restarts the timer

        if (!consistent) {
            return List.of(failure(request));
        }
        // Rule 5: commitIndex = min(leaderCommit, index of last new entry). Only the prefix this
        // request verified counts; anything beyond it in the local log is unverified.
        long match = request.lastCoveredIndex();
        long newCommit = Math.min(request.leaderCommit(), match);
        if (newCommit > commitIndex) {
            commitIndex = newCommit;
            applyCommitted();
        }
        return List.of(new AppendEntriesResponse(
                id, request.from(), currentTerm, request.term(), true, request.prevLogIndex(), match,
                storage.lastIndex()));
    }

    private AppendEntriesResponse failure(AppendEntriesRequest request) {
        return new AppendEntriesResponse(
                id, request.from(), currentTerm, request.term(), false, request.prevLogIndex(), 0, storage.lastIndex());
    }

    // ================================================================ replication (2B), leader side

    private List<RaftMessage> onAppendEntriesResponse(AppendEntriesResponse response) {
        if (response.term() > currentTerm) {
            return adoptHigherTerm(response.term());
        }
        // Only answers to this leader's current-term requests are meaningful.
        if (role != Role.LEADER || response.term() != currentTerm || response.requestTerm() != currentTerm) {
            return List.of();
        }
        NodeId follower = response.from();
        long match = matchIndex.get(follower);
        long next = nextIndex.get(follower);

        if (response.success()) {
            if (response.matchIndex() > storage.lastIndex()) {
                throw new IllegalStateException(follower + " acknowledged index " + response.matchIndex()
                        + " but leader " + id + " ends at " + storage.lastIndex());
            }
            // matchIndex only grows: a delayed or duplicated success cannot move it back, and any
            // success proves the follower durably held that prefix in this term.
            boolean advanced = response.matchIndex() > match;
            if (advanced) {
                match = response.matchIndex();
                matchIndex.put(follower, match);
                nextIndex.put(follower, Math.max(next, match + 1));
                advanceLeaderCommit();
            }
            return advanced && nextIndex.get(follower) <= storage.lastIndex()
                    ? List.of(appendEntriesTo(follower))
                    : List.of();
        }
        // A failure is acted on only if it answers the probe the leader would send now; older or
        // duplicated failures are ignored, so they cannot push nextIndex back repeatedly.
        if (response.requestPrevLogIndex() != next - 1) {
            return List.of();
        }
        long retry = Math.max(match + 1, Math.min(response.requestPrevLogIndex(), response.followerLastIndex() + 1));
        nextIndex.put(follower, retry);
        return List.of(appendEntriesTo(follower));
    }

    /**
     * Leaders (Figure 2): commit the highest N with a majority of matchIndex >= N <em>and</em>
     * log[N].term == currentTerm. Entries from older terms are never committed by counting replicas
     * (section 5.4.2, Figure 8); they become committed as the prefix of a current-term entry.
     */
    private void advanceLeaderCommit() {
        for (long n = storage.lastIndex(); n > commitIndex; n--) {
            if (storage.termAt(n) != currentTerm) {
                return; // terms never decrease along the log, so every earlier entry is older too
            }
            long index = n;
            long replicas = 1 + matchIndex.values().stream().filter(m -> m >= index).count();
            if (replicas >= cluster.quorum()) {
                commitIndex = n;
                applyCommitted();
                return;
            }
        }
    }

    private List<RaftMessage> broadcastAppendEntries() {
        return peers.stream().<RaftMessage>map(this::appendEntriesTo).toList();
    }

    private AppendEntriesRequest appendEntriesTo(NodeId peer) {
        long next = nextIndex.get(peer);
        long prev = next - 1;
        long last = storage.lastIndex();
        List<LogEntry> entries = storage.entries(next, Math.min(last, next + MAX_ENTRIES_PER_APPEND - 1));
        return new AppendEntriesRequest(id, peer, currentTerm, prev, storage.termAt(prev), entries, commitIndex);
    }

    // ================================================================ application

    /** Applies committed entries strictly in index order; completes the leader's proposals. */
    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            LogEntry entry = storage.entry(lastApplied + 1);
            stateMachine.apply(entry); // atomic with its lastApplied marker
            lastApplied = entry.index();
            Long proposedTerm = pendingProposals.remove(entry.index());
            if (proposedTerm != null) {
                Status status = proposedTerm == entry.term() ? Status.COMMITTED : Status.UNKNOWN;
                outcomes.add(new ProposalOutcome(entry.index(), proposedTerm, status));
            }
        }
    }

    private void abandonPendingProposals() {
        pendingProposals.forEach((index, term) -> outcomes.add(new ProposalOutcome(index, term, Status.UNKNOWN)));
        pendingProposals.clear();
    }

    // ================================================================ helpers

    private void resetElectionTimer() {
        electionElapsedTicks = 0;
        electionTimeoutTicks = random.nextInt(timing.electionTimeoutMinTicks(), timing.electionTimeoutMaxTicks());
    }

    /** Writes term and vote durably, then mirrors them in memory. Throws on failure. */
    private void persist(TermAndVote next) {
        if (next.currentTerm() == currentTerm && next.votedFor().equals(votedFor)) {
            return;
        }
        storage.write(StorageWrite.termAndVote(next));
        currentTerm = next.currentTerm();
        votedFor = next.votedFor();
    }

    // ================================================================ observation

    public NodeId id() {
        return id;
    }

    public ClusterConfig cluster() {
        return cluster;
    }

    public Role role() {
        return role;
    }

    public long currentTerm() {
        return currentTerm;
    }

    public Optional<NodeId> votedFor() {
        return votedFor;
    }

    /** The leader of the current term as known to this node (itself, if it is the leader). */
    public Optional<NodeId> leaderId() {
        return Optional.ofNullable(leaderId);
    }

    public Set<NodeId> votesGranted() {
        return Set.copyOf(votesGranted);
    }

    public Set<NodeId> votesRejected() {
        return Set.copyOf(votesRejected);
    }

    public long commitIndex() {
        return commitIndex;
    }

    public long lastApplied() {
        return lastApplied;
    }

    /** Leader only: next index to send to {@code peer}. */
    public Optional<Long> nextIndex(NodeId peer) {
        return Optional.ofNullable(nextIndex.get(peer));
    }

    /** Leader only: highest index known to be replicated on {@code peer}. */
    public Optional<Long> matchIndex(NodeId peer) {
        return Optional.ofNullable(matchIndex.get(peer));
    }

    public int pendingProposalCount() {
        return pendingProposals.size();
    }

    public boolean isHalted() {
        return haltCause != null;
    }

    public Optional<RuntimeException> haltCause() {
        return Optional.ofNullable(haltCause);
    }

    public int electionTimeoutTicks() {
        return electionTimeoutTicks;
    }

    public int electionElapsedTicks() {
        return electionElapsedTicks;
    }

    public long ignoredMessageCount() {
        return ignoredMessages;
    }
}
