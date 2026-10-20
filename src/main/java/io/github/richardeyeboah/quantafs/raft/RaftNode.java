package io.github.richardeyeboah.quantafs.raft;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Deterministic Raft leader-election core for one voter (milestone 2A).
 *
 * <p>The node has exactly two inputs, {@link #tick()} (one unit of logical time) and
 * {@link #receive(RaftMessage)}, and one output: the messages it wants sent. It owns no threads,
 * reads no clock, and performs no network I/O. Its only side effect is writing {@link TermAndVote}
 * to the injected {@link ElectionStateStore}, which always completes <em>before</em> any message
 * that depends on it is returned. Randomness comes from the injected {@link RandomSource}.
 *
 * <p>Rule references are to the Raft paper (Ongaro and Ousterhout, 2014), mainly Figure 2.
 *
 * <p><b>Fail closed:</b> if persistence fails, the node halts. It returns no messages for that
 * input and ignores all later input. A restart (constructing a new node over the same store)
 * resumes from whatever state actually reached stable storage.
 *
 * <p>Not implemented in 2A: log replication, commitment, PreVote, leadership transfer, and
 * membership changes. Not thread-safe; drive it from a single thread.
 */
public final class RaftNode {

    private final NodeId id;
    private final ClusterConfig cluster;
    private final List<NodeId> peers;
    private final ElectionTiming timing;
    private final RandomSource random;
    private final ElectionStateStore store;
    private final LogMetadata log;

    // Durable state, mirrored in memory only after the store accepts it.
    private long currentTerm;
    private Optional<NodeId> votedFor;

    // Volatile state, rebuilt from scratch on restart.
    private Role role = Role.FOLLOWER;
    private NodeId leaderId; // leader of currentTerm, if known
    private final Set<NodeId> votesGranted = new TreeSet<>();
    private final Set<NodeId> votesRejected = new TreeSet<>();
    private int electionElapsedTicks;
    private int electionTimeoutTicks;
    private int heartbeatElapsedTicks;
    private RuntimeException haltCause;
    private long ignoredMessages;

    /**
     * Creates a node that starts as a follower with the term and vote loaded from {@code store}.
     *
     * @throws PersistenceException if the stored state cannot be read
     */
    public RaftNode(
            NodeId id,
            ClusterConfig cluster,
            ElectionTiming timing,
            RandomSource random,
            ElectionStateStore store,
            LogMetadata log) {
        this.id = Objects.requireNonNull(id, "id");
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.timing = Objects.requireNonNull(timing, "timing");
        this.random = Objects.requireNonNull(random, "random");
        this.store = Objects.requireNonNull(store, "store");
        this.log = Objects.requireNonNull(log, "log");
        this.peers = cluster.peersOf(id);

        TermAndVote durable = store.load();
        if (log.lastEntry().term() > durable.currentTerm()) {
            throw new IllegalStateException(
                    "last log term " + log.lastEntry().term() + " exceeds persisted term "
                            + durable.currentTerm() + "; a server never holds entries from a future term");
        }
        this.currentTerm = durable.currentTerm();
        this.votedFor = durable.votedFor();
        resetElectionTimer();
    }

    /** Advances logical time by one tick. */
    public List<RaftMessage> tick() {
        if (isHalted()) {
            return List.of();
        }
        if (role == Role.LEADER) {
            // Figure 2, Leaders: repeat heartbeats during idle periods to prevent election timeouts.
            heartbeatElapsedTicks++;
            if (heartbeatElapsedTicks >= timing.heartbeatIntervalTicks()) {
                heartbeatElapsedTicks = 0;
                return heartbeats();
            }
            return List.of();
        }
        // Figure 2, Followers and Candidates: if the election timeout elapses, start an election.
        electionElapsedTicks++;
        if (electionElapsedTicks >= electionTimeoutTicks) {
            return startElection();
        }
        return List.of();
    }

    /** Handles one message delivered by the network. */
    public List<RaftMessage> receive(RaftMessage message) {
        Objects.requireNonNull(message, "message");
        if (isHalted()) {
            return List.of();
        }
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

    private List<RaftMessage> startElection() {
        // Figure 2, Candidates: increment currentTerm and vote for self, durably, before any
        // RequestVote is sent.
        if (!persist(currentTerm + 1, Optional.of(id))) {
            return List.of();
        }
        role = Role.CANDIDATE;
        leaderId = null;
        votesGranted.clear();
        votesRejected.clear();
        votesGranted.add(id);
        resetElectionTimer();
        if (votesGranted.size() >= cluster.quorum()) {
            return becomeLeader(); // single-voter cluster
        }
        LogPosition lastLog = log.lastEntry();
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
        // All servers: a higher term resets votedFor for the new term.
        Optional<NodeId> vote = newTerm ? Optional.empty() : votedFor;

        // Receiver rule 2: grant if votedFor is null or the candidate, and the candidate's log is
        // at least as up to date as ours (section 5.4.1).
        boolean mayVote = vote.isEmpty() || vote.get().equals(request.from());
        boolean logOk = request.lastLog().isAtLeastAsUpToDateAs(log.lastEntry());
        boolean grant = mayVote && logOk;
        if (grant) {
            vote = Optional.of(request.from());
        }

        // The new term and the vote are written in one atomic save before the reply exists.
        if (!persist(request.term(), vote)) {
            return List.of();
        }
        if (newTerm) {
            stepDownAfterTermChange();
        }
        if (grant) {
            // Figure 2, Followers: granting a vote restarts the election timer.
            resetElectionTimer();
        }
        return List.of(new RequestVoteResponse(id, request.from(), currentTerm, grant));
    }

    private List<RaftMessage> onRequestVoteResponse(RequestVoteResponse response) {
        if (response.term() > currentTerm) {
            return adoptHigherTerm(response.term());
        }
        if (response.term() < currentTerm || role != Role.CANDIDATE) {
            return List.of(); // stale, or the election is already decided
        }
        if (response.voteGranted()) {
            // A set counts each voter once, so duplicated responses cannot inflate the tally.
            votesGranted.add(response.from());
            votesRejected.remove(response.from());
            if (votesGranted.size() >= cluster.quorum()) {
                return becomeLeader();
            }
        } else if (!votesGranted.contains(response.from())) {
            votesRejected.add(response.from());
        }
        return List.of();
    }

    private List<RaftMessage> onAppendEntries(AppendEntriesRequest request) {
        // AppendEntries receiver rule 1: reply false if term < currentTerm. No timer reset: a
        // deposed leader must not keep followers from electing a new one.
        if (request.term() < currentTerm) {
            return List.of(new AppendEntriesResponse(id, request.from(), currentTerm, false));
        }
        if (request.term() > currentTerm) {
            if (!persist(request.term(), Optional.empty())) {
                return List.of();
            }
            stepDownAfterTermChange();
        }
        if (role == Role.LEADER) {
            throw new IllegalStateException(
                    "Election Safety violated: " + id + " and " + request.from() + " both lead term " + currentTerm);
        }
        if (leaderId != null && !leaderId.equals(request.from())) {
            throw new IllegalStateException(
                    "Election Safety violated: " + leaderId + " and " + request.from() + " both lead term " + currentTerm);
        }
        // Figure 2, Candidates: if AppendEntries is received from the new leader, convert to follower.
        if (role == Role.CANDIDATE) {
            role = Role.FOLLOWER;
            votesGranted.clear();
            votesRejected.clear();
        }
        leaderId = request.from();
        // Figure 2, Followers: AppendEntries from the current leader restarts the election timer.
        resetElectionTimer();
        return List.of(new AppendEntriesResponse(id, request.from(), currentTerm, true));
    }

    private List<RaftMessage> onAppendEntriesResponse(AppendEntriesResponse response) {
        if (response.term() > currentTerm) {
            return adoptHigherTerm(response.term());
        }
        return List.of(); // 2A heartbeats carry no entries, so there is nothing to track
    }

    /** All servers: if a message carries term T > currentTerm, set currentTerm = T and become a follower. */
    private List<RaftMessage> adoptHigherTerm(long term) {
        if (persist(term, Optional.empty())) {
            stepDownAfterTermChange();
        }
        return List.of();
    }

    /**
     * Called after a higher term is durable. A leader or candidate becomes a follower with a fresh
     * election timer. A node that was already a follower keeps its running timer: only a valid
     * heartbeat or a granted vote may restart it.
     */
    private void stepDownAfterTermChange() {
        leaderId = null;
        votesGranted.clear();
        votesRejected.clear();
        if (role != Role.FOLLOWER) {
            role = Role.FOLLOWER;
            resetElectionTimer();
        }
    }

    private List<RaftMessage> becomeLeader() {
        role = Role.LEADER;
        leaderId = id;
        heartbeatElapsedTicks = 0;
        // Figure 2, Leaders: upon election, send initial empty AppendEntries (heartbeats) at once.
        return heartbeats();
    }

    private List<RaftMessage> heartbeats() {
        return peers.stream().<RaftMessage>map(peer -> new AppendEntriesRequest(id, peer, currentTerm)).toList();
    }

    private void resetElectionTimer() {
        electionElapsedTicks = 0;
        electionTimeoutTicks =
                random.nextInt(timing.electionTimeoutMinTicks(), timing.electionTimeoutMaxTicks());
    }

    /**
     * Writes (term, vote) to stable storage, then mirrors it in memory. On failure the node halts
     * and memory keeps the last state known to be durable.
     */
    private boolean persist(long term, Optional<NodeId> vote) {
        if (term == currentTerm && vote.equals(votedFor)) {
            return true;
        }
        try {
            store.save(new TermAndVote(term, vote));
        } catch (RuntimeException e) {
            haltCause = e;
            return false;
        }
        currentTerm = term;
        votedFor = vote;
        return true;
    }

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

    /** Voters that granted this node's candidacy in the current term, including itself. */
    public Set<NodeId> votesGranted() {
        return Set.copyOf(votesGranted);
    }

    /** Voters that explicitly refused this node's candidacy in the current term. */
    public Set<NodeId> votesRejected() {
        return Set.copyOf(votesRejected);
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

    /** Messages dropped because they were addressed elsewhere or came from a non-member. */
    public long ignoredMessageCount() {
        return ignoredMessages;
    }
}
