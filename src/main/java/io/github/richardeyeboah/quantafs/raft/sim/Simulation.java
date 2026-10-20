package io.github.richardeyeboah.quantafs.raft.sim;

import io.github.richardeyeboah.quantafs.raft.AppendEntriesRequest;
import io.github.richardeyeboah.quantafs.raft.Bytes;
import io.github.richardeyeboah.quantafs.raft.ClusterConfig;
import io.github.richardeyeboah.quantafs.raft.Command;
import io.github.richardeyeboah.quantafs.raft.ElectionTiming;
import io.github.richardeyeboah.quantafs.raft.FailureMode;
import io.github.richardeyeboah.quantafs.raft.InMemoryRaftStorage;
import io.github.richardeyeboah.quantafs.raft.InMemoryStateMachine;
import io.github.richardeyeboah.quantafs.raft.LogEntry;
import io.github.richardeyeboah.quantafs.raft.NodeId;
import io.github.richardeyeboah.quantafs.raft.ProposalOutcome;
import io.github.richardeyeboah.quantafs.raft.ProposeResult;
import io.github.richardeyeboah.quantafs.raft.RaftMessage;
import io.github.richardeyeboah.quantafs.raft.RaftNode;
import io.github.richardeyeboah.quantafs.raft.RaftStorage;
import io.github.richardeyeboah.quantafs.raft.RandomSource;
import io.github.richardeyeboah.quantafs.raft.Role;
import io.github.richardeyeboah.quantafs.raft.SeededRandomSource;
import io.github.richardeyeboah.quantafs.raft.StateMachine;
import io.github.richardeyeboah.quantafs.raft.TermAndVote;
import io.github.richardeyeboah.quantafs.raft.storage.NodeStorage;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Deterministic, single-threaded simulation of a cluster of {@link RaftNode}s.
 *
 * <p>The simulation owns logical time and a model network. Each {@link #step()} advances time by one
 * tick, delivers every message due at that tick, then ticks every running node. One seeded random
 * generator decides message delays, drops, and duplicates and seeds every node's election timeouts,
 * so the same seed and the same calls always produce the same history. No threads, sleeps, or
 * wall-clock time are involved.
 *
 * <p><b>Storage.</b> By default each node gets {@link InMemoryRaftStorage} and an
 * {@link InMemoryStateMachine}: nothing touches the disk, and a crash keeps those objects, standing in
 * for a disk that survives. With {@link Builder#rocksDb(Path)} each node instead gets real RocksDB
 * files in its own directory ({@link NodeStorage}); a crash closes them and a restart reopens them.
 *
 * <p><b>Faults:</b> per-message delay (which reorders messages), drops, duplicates, partitions,
 * crashes, restarts, and (in-memory only) injected storage or apply failures.
 *
 * <p><b>Invariants</b>, checked after every node step; a violation throws
 * {@link IllegalStateException} naming the seed and tick:
 *
 * <ul>
 *   <li>Election Safety: at most one leader per term. Terms never decrease, a node never votes for
 *       two candidates in one term, and memory never runs ahead of storage.
 *   <li>Log Matching: if two logs contain an entry with the same index and term, they are identical up
 *       to that index.
 *   <li>Committed agreement (Leader Completeness / State Machine Safety): every node that considers
 *       index i committed has the same entry there.
 *   <li>{@code commitIndex} never decreases within an incarnation; {@code lastApplied} never decreases
 *       even across restarts; only committed entries are applied, in index order.
 *   <li>Every proposal reported {@code COMMITTED} is in the committed history at its index.
 *   <li>No node halts unless a failure was injected into it.
 * </ul>
 *
 * <p>This is a test and demonstration harness. It is not a network transport, and it does not make
 * the simulated nodes a usable database.
 */
public final class Simulation implements AutoCloseable {

    public enum EventKind {
        ELECTION,
        REPLICATION,
        CLIENT,
        FAULT
    }

    /** Something observable happened at logical time {@code tick}. */
    public record SimEvent(long tick, EventKind kind, String text) {
        @Override
        public String toString() {
            return String.format("[tick %4d] %s", tick, text);
        }
    }

    /** A message a node emitted at {@code tick}, before any network fault was applied. */
    public record SentMessage(long tick, RaftMessage message) {}

    /** A command proposed through the simulation, as a client would see it. */
    public static final class ClientRequest {
        private final long id;
        private final String node;
        private final Command command;
        private final long proposedAtTick;
        private final ProposeResult result;
        private ProposalOutcome.Status outcome;
        private long completedAtTick = -1;

        private ClientRequest(long id, String node, Command command, long proposedAtTick, ProposeResult result) {
            this.id = id;
            this.node = node;
            this.command = command;
            this.proposedAtTick = proposedAtTick;
            this.result = result;
        }

        public long id() {
            return id;
        }

        public String node() {
            return node;
        }

        public Command command() {
            return command;
        }

        public long proposedAtTick() {
            return proposedAtTick;
        }

        public ProposeResult result() {
            return result;
        }

        /** Final status, or empty while still pending (or if the proposal was never accepted). */
        public Optional<ProposalOutcome.Status> outcome() {
            return Optional.ofNullable(outcome);
        }

        public long completedAtTick() {
            return completedAtTick;
        }

        /** True only when the leader reported commitment and local application. */
        public boolean acknowledged() {
            return outcome == ProposalOutcome.Status.COMMITTED;
        }

        public Optional<ProposeResult.Accepted> accepted() {
            return result instanceof ProposeResult.Accepted a ? Optional.of(a) : Optional.empty();
        }
    }

    private record InFlight(long deliverAt, long sequence, RaftMessage message) {}

    private record IndexTerm(long index, long term) {}

    private record Snapshot(
            Role role,
            long term,
            Optional<NodeId> votedFor,
            Optional<NodeId> leaderId,
            Set<NodeId> granted,
            Set<NodeId> rejected,
            boolean halted,
            int electionTimeout,
            long lastIndex,
            long lastTerm,
            long commitIndex,
            long lastApplied) {}

    private static final class SimNode {
        final NodeId id;
        final Path directory; // null for in-memory nodes
        InMemoryRaftStorage memoryStorage;
        InMemoryStateMachine memoryMachine;
        NodeStorage nodeStorage;
        RandomSource firstRandom;
        RaftNode node; // null while crashed
        boolean failureInjected;
        long knownCommitted;
        long highestApplied;
        int appliedChecked;
        long checkedWrites = -1;

        SimNode(NodeId id, Path directory) {
            this.id = id;
            this.directory = directory;
        }

        RaftStorage storage() {
            return directory == null ? memoryStorage : nodeStorage.raftStorage();
        }

        StateMachine stateMachine() {
            return directory == null ? memoryMachine : nodeStorage.stateMachine();
        }
    }

    private final long seed;
    private final ClusterConfig cluster;
    private final ElectionTiming timing;
    private NetworkConfig network;
    private final SplittableRandom networkRandom;
    private final SplittableRandom nodeSeeds;
    private final Consumer<SimEvent> listener;
    private final boolean recordEvents;
    private final Map<NodeId, SimNode> nodes = new TreeMap<>();
    private final PriorityQueue<InFlight> inFlight =
            new PriorityQueue<>(Comparator.comparingLong(InFlight::deliverAt).thenComparingLong(InFlight::sequence));
    private final List<SimEvent> events = new ArrayList<>();
    private final List<LeaderRecord> leaderHistory = new ArrayList<>();
    private final Map<Long, NodeId> leaderByTerm = new HashMap<>();
    private final List<SentMessage> sent = new ArrayList<>();
    private final Map<NodeId, Long> highestTerm = new HashMap<>();
    private final Map<NodeId, Map<Long, NodeId>> votesCast = new HashMap<>();
    private final TreeMap<Long, LogEntry> committed = new TreeMap<>();
    private final Map<IndexTerm, Integer> prefixHashes = new HashMap<>();
    private final List<ClientRequest> requests = new ArrayList<>();
    private final Map<String, ClientRequest> pendingRequests = new HashMap<>();
    private List<Set<NodeId>> partition; // null means fully connected
    private long now;
    private long sequence;
    private long delivered;
    private long dropped;
    private long duplicated;

    private Simulation(Builder builder) {
        this.seed = builder.seed;
        this.cluster = ClusterConfig.of(builder.voters);
        this.timing = builder.timing;
        this.network = builder.network;
        SplittableRandom root = new SplittableRandom(builder.seed);
        this.networkRandom = root.split();
        this.nodeSeeds = root.split();
        this.listener = builder.listener;
        this.recordEvents = builder.recordEvents;
        for (NodeId id : cluster.voters()) {
            SimNode simNode = new SimNode(id, builder.rocksDbBase == null ? null : builder.rocksDbBase.resolve(id.value()));
            if (simNode.directory == null) {
                TermAndVote initialState = builder.initialStates.getOrDefault(id, TermAndVote.INITIAL);
                List<LogEntry> initialLog = builder.initialLogs.getOrDefault(id, List.of());
                simNode.memoryStorage = new InMemoryRaftStorage(initialState, initialLog);
                simNode.memoryMachine = new InMemoryStateMachine();
            } else {
                if (builder.initialStates.containsKey(id) || builder.initialLogs.containsKey(id)) {
                    throw new IllegalArgumentException("initial fixtures are supported for in-memory nodes only");
                }
                simNode.nodeStorage = NodeStorage.open(simNode.directory);
            }
            simNode.firstRandom = builder.randomOverrides.get(id);
            nodes.put(id, simNode);
            startNode(simNode);
        }
    }

    public static Builder builder(String... voterIds) {
        return new Builder(Arrays.stream(voterIds).map(NodeId::of).toList());
    }

    // ================================================================ time

    /** Advances logical time by one tick: deliver due messages, then tick every running node. */
    public void step() {
        now++;
        while (!inFlight.isEmpty() && inFlight.peek().deliverAt() <= now) {
            deliver(inFlight.poll().message());
        }
        for (SimNode simNode : nodes.values()) {
            if (simNode.node != null) {
                Snapshot before = snapshot(simNode);
                List<RaftMessage> out = simNode.node.tick();
                observe(simNode, before, null);
                send(out);
            }
        }
    }

    public void run(int ticks) {
        for (int i = 0; i < ticks; i++) {
            step();
        }
    }

    /** Steps until {@code condition} holds; returns false if it did not within {@code maxTicks}. */
    public boolean runUntil(Predicate<Simulation> condition, int maxTicks) {
        for (int i = 0; i < maxTicks; i++) {
            if (condition.test(this)) {
                return true;
            }
            step();
        }
        return condition.test(this);
    }

    // ================================================================ clients

    /**
     * Proposes {@code command} to node {@code id}, as a client connected to that node would. The
     * outcome is filled in later, when (and if) the node reports it.
     */
    public ClientRequest propose(String id, Command command) {
        SimNode simNode = requireNode(id);
        if (simNode.node == null) {
            throw new IllegalStateException(id + " is down");
        }
        Snapshot before = snapshot(simNode);
        ProposeResult result = simNode.node.propose(command);
        ClientRequest request = new ClientRequest(requests.size() + 1, id, command, now, result);
        requests.add(request);
        switch (result) {
            case ProposeResult.Accepted accepted -> {
                pendingRequests.put(requestKey(simNode.id, accepted.index(), accepted.term()), request);
                emit(EventKind.CLIENT, "client -> " + id + ": " + command + " accepted at index " + accepted.index()
                        + " (term " + accepted.term() + "); waiting for commit");
            }
            case ProposeResult.NotLeader notLeader -> emit(EventKind.CLIENT, "client -> " + id + ": " + command
                    + " REJECTED: " + id + " is not the leader (hint: "
                    + notLeader.leaderHint().map(NodeId::value).orElse("unknown") + ")");
            case ProposeResult.Unavailable unavailable ->
                    emit(EventKind.CLIENT, "client -> " + id + ": " + command + " REJECTED: " + unavailable.reason());
            case ProposeResult.OutcomeUnknown unknown -> {
                request.outcome = ProposalOutcome.Status.UNKNOWN;
                request.completedAtTick = now;
                emit(EventKind.CLIENT, "client -> " + id + ": " + command + " outcome UNKNOWN: " + unknown.reason());
            }
        }
        observe(simNode, before, null);
        if (result instanceof ProposeResult.Accepted accepted) {
            send(accepted.messages());
        }
        return request;
    }

    public List<ClientRequest> clientRequests() {
        return List.copyOf(requests);
    }

    // ================================================================ faults

    public void crash(String id) {
        SimNode simNode = requireNode(id);
        if (simNode.node == null) {
            throw new IllegalStateException(id + " is already down");
        }
        RaftNode node = simNode.node;
        simNode.node = null;
        if (simNode.directory == null) {
            emit(EventKind.FAULT, "CRASH " + id + " (was " + node.role() + " in term " + node.currentTerm()
                    + "): volatile state discarded; its storage is kept (in memory here, standing in for a disk)");
        } else {
            simNode.nodeStorage.close();
            simNode.nodeStorage = null;
            emit(EventKind.FAULT, "CRASH " + id + " (was " + node.role() + " in term " + node.currentTerm()
                    + "): volatile state discarded; its RocksDB files are closed and stay on disk");
        }
        // Clients waiting on this node never hear back: from their side the outcome is unknown.
        pendingRequests.entrySet().removeIf(entry -> {
            ClientRequest request = entry.getValue();
            if (!request.node.equals(id)) {
                return false;
            }
            request.outcome = ProposalOutcome.Status.UNKNOWN;
            request.completedAtTick = now;
            emit(EventKind.CLIENT, id + " -/-> client: lost connection; outcome of " + request.command + " is UNKNOWN");
            return true;
        });
    }

    public void restart(String id) {
        SimNode simNode = requireNode(id);
        if (simNode.node != null) {
            throw new IllegalStateException(id + " is already running");
        }
        if (simNode.directory != null) {
            simNode.nodeStorage = NodeStorage.open(simNode.directory);
        }
        startNode(simNode);
        RaftNode node = simNode.node;
        RaftStorage storage = simNode.storage();
        String source = simNode.directory == null ? "from its storage" : "by reopening its RocksDB files";
        emit(EventKind.FAULT, "RESTART " + id + ": reloaded " + storage.termAndVote() + ", log ends at index "
                + storage.lastIndex() + ", lastApplied " + node.lastApplied() + " " + source
                + "; commitIndex restarts at lastApplied; starts as FOLLOWER");
        Snapshot state = snapshot(simNode);
        checkDurableInvariants(simNode, state);
        if (node.lastApplied() < simNode.highestApplied) {
            fail(id + " lastApplied went back from " + simNode.highestApplied + " to " + node.lastApplied());
        }
    }

    /** Makes the in-memory storage of {@code id} fail its writes (before or after completing them). */
    public void injectStorageFailure(String id, FailureMode mode) {
        SimNode simNode = requireMemoryNode(id);
        simNode.memoryStorage.setFailureMode(mode);
        simNode.failureInjected |= mode != FailureMode.NONE;
    }

    /** Makes the in-memory state machine of {@code id} fail its applies (before or after completing them). */
    public void injectApplyFailure(String id, FailureMode mode) {
        SimNode simNode = requireMemoryNode(id);
        simNode.memoryMachine.setFailureMode(mode);
        simNode.failureInjected |= mode != FailureMode.NONE;
    }

    /** Splits the network into groups; nodes not listed are each isolated on their own. */
    @SafeVarargs
    public final void partition(Collection<String>... groups) {
        List<Set<NodeId>> sets = new ArrayList<>();
        Set<NodeId> listed = new HashSet<>();
        for (Collection<String> group : groups) {
            Set<NodeId> set = new TreeSet<>();
            for (String id : group) {
                requireNode(id);
                NodeId nodeId = NodeId.of(id);
                if (!listed.add(nodeId)) {
                    throw new IllegalArgumentException(id + " appears in two partition groups");
                }
                set.add(nodeId);
            }
            sets.add(set);
        }
        for (NodeId id : cluster.voters()) {
            if (!listed.contains(id)) {
                sets.add(Set.of(id));
            }
        }
        partition = sets;
        emit(EventKind.FAULT, "PARTITION " + sets.stream().map(Simulation::names).collect(Collectors.joining(" | "))
                + " (in-flight messages across groups are lost)");
    }

    /** Cuts {@code id} off from every other node. */
    public void isolate(String id) {
        List<String> others = cluster.voters().stream().map(NodeId::value).filter(v -> !v.equals(id)).toList();
        partition(List.of(id), others);
    }

    public void heal() {
        partition = null;
        emit(EventKind.FAULT, "HEAL network: all nodes can reach each other again");
    }

    public void setNetwork(NetworkConfig network) {
        this.network = Objects.requireNonNull(network, "network");
    }

    // ================================================================ queries

    public long now() {
        return now;
    }

    public long seed() {
        return seed;
    }

    public ClusterConfig cluster() {
        return cluster;
    }

    public boolean isUp(String id) {
        return requireNode(id).node != null;
    }

    public Optional<RaftNode> node(String id) {
        return Optional.ofNullable(requireNode(id).node);
    }

    public RaftNode liveNode(String id) {
        return node(id).orElseThrow(() -> new IllegalStateException(id + " is down"));
    }

    /** The node's Raft storage; for RocksDB nodes only while the node is up. */
    public RaftStorage storage(String id) {
        SimNode simNode = requireNode(id);
        if (simNode.directory != null && simNode.nodeStorage == null) {
            throw new IllegalStateException(id + " is down; its RocksDB files are closed");
        }
        return simNode.storage();
    }

    public InMemoryRaftStorage memoryStorage(String id) {
        return requireMemoryNode(id).memoryStorage;
    }

    /** The node's state machine, for diagnostic inspection; for RocksDB nodes only while up. */
    public StateMachine stateMachine(String id) {
        SimNode simNode = requireNode(id);
        if (simNode.directory != null && simNode.nodeStorage == null) {
            throw new IllegalStateException(id + " is down; its RocksDB files are closed");
        }
        return simNode.stateMachine();
    }

    public InMemoryStateMachine memoryStateMachine(String id) {
        return requireMemoryNode(id).memoryMachine;
    }

    public List<RaftNode> liveNodes() {
        return nodes.values().stream().filter(n -> n.node != null).map(n -> n.node).toList();
    }

    /** Running, non-halted nodes that currently believe they are leader (possibly of old terms). */
    public List<RaftNode> leaders() {
        return liveNodes().stream().filter(n -> n.role() == Role.LEADER && !n.isHalted()).toList();
    }

    /**
     * A leader whose term and leadership a quorum of running nodes (including itself) currently
     * acknowledge. If several qualify, the one with the highest term.
     */
    public Optional<RaftNode> establishedLeader() {
        return leaders().stream()
                .filter(leader -> liveNodes().stream()
                                .filter(n -> n.currentTerm() == leader.currentTerm()
                                        && n.leaderId().equals(Optional.of(leader.id())))
                                .count()
                        >= cluster.quorum())
                .max(Comparator.comparingLong(leader -> leader.currentTerm()));
    }

    /** Every entry any node has considered committed, by index. Committed entries never change. */
    public NavigableMap<Long, LogEntry> committedHistory() {
        return Collections.unmodifiableNavigableMap(new TreeMap<>(committed));
    }

    /** The key-value state obtained by applying committed entries 1..{@code upToIndex} in order. */
    public SortedMap<Bytes, Bytes> modelState(long upToIndex) {
        TreeMap<Bytes, Bytes> model = new TreeMap<>();
        var sessions = new TreeMap<String,io.github.richardeyeboah.quantafs.raft.ClientSession>();
        for (long i = 1; i <= upToIndex; i++) {
            LogEntry entry = committed.get(i);
            if (entry == null) {
                throw new IllegalStateException("index " + i + " is not known to be committed");
            }
            Command effective=entry.command();
            if(effective instanceof Command.Request request) {
                if(io.github.richardeyeboah.quantafs.raft.ClientSession.isNext(sessions.get(request.clientId()),request)) {
                    sessions.put(request.clientId(),new io.github.richardeyeboah.quantafs.raft.ClientSession(request.sequence(),
                            io.github.richardeyeboah.quantafs.raft.ClientSession.fingerprint(request)));effective=request.mutation();
                } else effective=Command.noOp();
            }
            switch (effective) {
                case Command.Request ignored -> throw new IllegalStateException("nested request");
                case Command.Put put -> model.put(put.key(), put.value());
                case Command.Delete delete -> model.remove(delete.key());
                case Command.NoOp noOp -> { }
            }
        }
        return model;
    }

    /**
     * Throws unless every node is up and healthy, has applied the same, highest commit index, and
     * holds exactly the state obtained by replaying the committed history.
     */
    public void assertConverged() {
        long commit = liveNodes().stream().mapToLong(n -> n.commitIndex()).max().orElse(0);
        SortedMap<Bytes, Bytes> model = modelState(commit);
        for (SimNode simNode : nodes.values()) {
            String id = simNode.id.value();
            if (simNode.node == null || simNode.node.isHalted()) {
                fail(id + " is down or halted");
            }
            if (simNode.node.lastApplied() != commit || simNode.stateMachine().lastApplied().index() != commit) {
                fail(id + " applied " + simNode.node.lastApplied() + " but the cluster committed " + commit);
            }
            SortedMap<Bytes, Bytes> contents = simNode.stateMachine().diagnosticContents();
            if (!contents.equals(model)) {
                fail(id + " state " + contents + " differs from the committed history's state " + model);
            }
        }
        assertAcknowledgedRequestsCommitted();
    }

    /** Every request reported COMMITTED is in the committed history, at its index, unchanged. */
    public void assertAcknowledgedRequestsCommitted() {
        for (ClientRequest request : requests) {
            if (request.acknowledged()) {
                ProposeResult.Accepted accepted = request.accepted().orElseThrow();
                LogEntry entry = committed.get(accepted.index());
                if (entry == null || entry.term() != accepted.term() || !entry.command().equals(request.command)) {
                    fail("acknowledged request " + request.command + " at index " + accepted.index()
                            + " is not in the committed history (found " + entry + ")");
                }
            }
        }
    }

    public List<SimEvent> events() {
        return List.copyOf(events);
    }

    public List<LeaderRecord> leaderHistory() {
        return List.copyOf(leaderHistory);
    }

    public List<SentMessage> sentMessages() {
        return List.copyOf(sent);
    }

    public long messagesSent(String from, Class<? extends RaftMessage> type, long sinceTick) {
        NodeId id = NodeId.of(from);
        return sent.stream()
                .filter(s -> s.tick() >= sinceTick && s.message().from().equals(id) && type.isInstance(s.message()))
                .count();
    }

    public long deliveredCount() {
        return delivered;
    }

    public long droppedCount() {
        return dropped;
    }

    public long duplicatedCount() {
        return duplicated;
    }

    /** Closes the RocksDB files of running RocksDB-backed nodes. */
    @Override
    public void close() {
        for (SimNode simNode : nodes.values()) {
            if (simNode.nodeStorage != null) {
                simNode.nodeStorage.close();
                simNode.nodeStorage = null;
            }
        }
    }

    // ================================================================ internals

    private void startNode(SimNode simNode) {
        RandomSource seeded = new SeededRandomSource(nodeSeeds.split()); // always split, for determinism
        RandomSource random = simNode.firstRandom != null ? simNode.firstRandom : seeded;
        simNode.firstRandom = null;
        simNode.node = new RaftNode(simNode.id, cluster, timing, random, simNode.storage(), simNode.stateMachine());
        // Re-verify the restarted node's whole committed prefix (it starts at its applied marker)
        // against the global history, including entries applied just before it halted.
        simNode.knownCommitted = 0;
        simNode.failureInjected = simNode.directory == null
                && (simNode.memoryStorage.failureMode() != FailureMode.NONE
                        || simNode.memoryMachine.failureMode() != FailureMode.NONE);
    }

    private void deliver(RaftMessage message) {
        SimNode destination = nodes.get(message.to());
        if (destination == null || destination.node == null || !connected(message.from(), message.to())) {
            dropped++;
            return;
        }
        delivered++;
        Snapshot before = snapshot(destination);
        List<RaftMessage> out = destination.node.receive(message);
        observe(destination, before, message);
        send(out);
    }

    private void send(List<RaftMessage> messages) {
        for (RaftMessage message : messages) {
            sent.add(new SentMessage(now, message));
            boolean drop = networkRandom.nextDouble() < network.dropProbability();
            boolean duplicate = networkRandom.nextDouble() < network.duplicateProbability();
            if (drop) {
                dropped++;
                continue;
            }
            schedule(message);
            if (duplicate) {
                duplicated++;
                schedule(message);
            }
        }
    }

    private void schedule(RaftMessage message) {
        int delay = networkRandom.nextInt(network.minDelayTicks(), network.maxDelayTicks() + 1);
        inFlight.add(new InFlight(now + delay, sequence++, message));
    }

    private boolean connected(NodeId a, NodeId b) {
        return partition == null || partition.stream().anyMatch(group -> group.contains(a) && group.contains(b));
    }

    private static Snapshot snapshot(SimNode simNode) {
        RaftNode node = simNode.node;
        long lastIndex = -1;
        long lastTerm = -1;
        if (!node.isHalted()) {
            RaftStorage storage = simNode.storage();
            lastIndex = storage.lastIndex();
            lastTerm = storage.termAt(lastIndex);
        }
        return new Snapshot(
                node.role(), node.currentTerm(), node.votedFor(), node.leaderId(), node.votesGranted(),
                node.votesRejected(), node.isHalted(), node.electionTimeoutTicks(), lastIndex, lastTerm,
                node.commitIndex(), node.lastApplied());
    }

    private void observe(SimNode simNode, Snapshot before, RaftMessage cause) {
        RaftNode node = simNode.node;
        Snapshot after = snapshot(simNode);
        String who = simNode.id.value();

        if (after.term() < before.term()) {
            fail(who + " term decreased from " + before.term() + " to " + after.term());
        }
        if (!before.halted() && after.halted()) {
            if (!simNode.failureInjected) {
                fail(who + " halted without an injected failure: " + node.haltCause().orElseThrow());
            }
            emit(EventKind.FAULT, who + " HALTED: " + node.haltCause().map(e -> e.getMessage()).orElse("?")
                    + "; it sends nothing until restarted");
        }
        if (after.halted()) {
            collectOutcomes(simNode);
            return; // its storage may be unusable, and nothing it does is visible any more
        }
        checkDurableInvariants(simNode, after);
        if (after.role() == Role.LEADER && before.role() != Role.LEADER) {
            recordLeader(after.term(), simNode.id);
        }
        checkReplicationInvariants(simNode, before, after, cause);
        if (recordEvents || listener != null) {
            describeElection(simNode, before, after, cause);
            describeReplication(simNode, before, after);
        }
        collectOutcomes(simNode); // after this step's commits are recorded, so acks can be verified
    }

    private void collectOutcomes(SimNode simNode) {
        for (ProposalOutcome outcome : simNode.node.drainOutcomes()) {
            ClientRequest request = pendingRequests.remove(requestKey(simNode.id, outcome.index(), outcome.term()));
            if (request == null) {
                continue; // e.g. the client already gave up when the node crashed
            }
            request.outcome = outcome.status();
            request.completedAtTick = now;
            if (outcome.status() == ProposalOutcome.Status.COMMITTED) {
                LogEntry entry = committed.get(outcome.index());
                if (entry == null || entry.term() != outcome.term() || !entry.command().equals(request.command)) {
                    fail("COMMITTED reported for " + request.command + " at index " + outcome.index()
                            + ", but the committed entry there is " + entry);
                }
                emit(EventKind.CLIENT, simNode.id + " -> client: index " + outcome.index() + " committed and applied; "
                        + request.command + " OK");
            } else {
                emit(EventKind.CLIENT, simNode.id + " -> client: outcome of " + request.command + " at index "
                        + outcome.index() + " is UNKNOWN (" + simNode.id + " lost leadership before commit)");
            }
        }
    }

    private void checkDurableInvariants(SimNode simNode, Snapshot state) {
        NodeId id = simNode.id;
        TermAndVote durable = simNode.storage().termAndVote();
        if (state.term() != durable.currentTerm() || !state.votedFor().equals(durable.votedFor())) {
            fail(id + " memory (" + state.term() + ", " + state.votedFor() + ") differs from its storage (" + durable + ")");
        }
        long highest = highestTerm.getOrDefault(id, 0L);
        if (state.term() < highest) {
            fail(id + " went back from term " + highest + " to " + state.term());
        }
        highestTerm.put(id, Math.max(highest, state.term()));
        if (state.votedFor().isPresent()) {
            NodeId earlier = votesCast.computeIfAbsent(id, k -> new HashMap<>())
                    .putIfAbsent(state.term(), state.votedFor().get());
            if (earlier != null && !earlier.equals(state.votedFor().get())) {
                fail(id + " voted for both " + earlier + " and " + state.votedFor().get() + " in term " + state.term());
            }
        }
    }

    private void checkReplicationInvariants(SimNode simNode, Snapshot before, Snapshot after, RaftMessage cause) {
        String who = simNode.id.value();
        RaftStorage storage = simNode.storage();
        if (after.commitIndex() < before.commitIndex()) {
            fail(who + " commitIndex decreased from " + before.commitIndex() + " to " + after.commitIndex());
        }
        if (after.commitIndex() > after.lastIndex() || after.lastApplied() > after.commitIndex()) {
            fail(who + " has lastApplied " + after.lastApplied() + ", commitIndex " + after.commitIndex()
                    + ", last log index " + after.lastIndex());
        }
        if (simNode.stateMachine().lastApplied().index() != after.lastApplied()) {
            fail(who + " lastApplied " + after.lastApplied() + " differs from its state machine marker "
                    + simNode.stateMachine().lastApplied());
        }
        if (after.lastApplied() < simNode.highestApplied) {
            fail(who + " lastApplied went back from " + simNode.highestApplied + " to " + after.lastApplied());
        }
        simNode.highestApplied = after.lastApplied();

        boolean logMayHaveChanged;
        if (simNode.directory == null) {
            long writes = simNode.memoryStorage.writeCount();
            logMayHaveChanged = writes != simNode.checkedWrites;
            simNode.checkedWrites = writes;
        } else {
            logMayHaveChanged = cause instanceof AppendEntriesRequest
                    || after.lastIndex() != before.lastIndex()
                    || after.lastTerm() != before.lastTerm();
        }
        if (logMayHaveChanged) {
            checkLogMatching(simNode, storage);
        }
        for (long i = simNode.knownCommitted + 1; i <= after.commitIndex(); i++) {
            LogEntry entry = storage.entry(i);
            LogEntry earlier = committed.putIfAbsent(i, entry);
            if (earlier != null && !earlier.equals(entry)) {
                fail("committed entries disagree at index " + i + ": " + earlier + " vs " + entry + " on " + who);
            }
        }
        simNode.knownCommitted = Math.max(simNode.knownCommitted, after.commitIndex());

        if (simNode.directory == null) {
            List<LogEntry> applied = simNode.memoryMachine.appliedEntries();
            for (int k = simNode.appliedChecked; k < applied.size(); k++) {
                LogEntry entry = applied.get(k);
                LogEntry expected = committed.get(entry.index());
                if (!entry.equals(expected)) {
                    fail(who + " applied " + entry + " but the committed entry at that index is " + expected);
                }
                if (k > 0 && entry.index() != applied.get(k - 1).index() + 1) {
                    fail(who + " applied index " + entry.index() + " right after " + applied.get(k - 1).index());
                }
            }
            simNode.appliedChecked = applied.size();
        }
    }

    /** Log Matching: (index, term) identifies the whole prefix up to that index. */
    private void checkLogMatching(SimNode simNode, RaftStorage storage) {
        int hash = 17;
        for (LogEntry entry : storage.entries(1, storage.lastIndex())) {
            hash = 31 * hash + entry.hashCode();
            Integer earlier = prefixHashes.putIfAbsent(new IndexTerm(entry.index(), entry.term()), hash);
            if (earlier != null && earlier != hash) {
                fail("Log Matching violated on " + simNode.id + ": another log has entry (" + entry.index() + ", term "
                        + entry.term() + ") with a different prefix");
            }
        }
    }

    private void describeElection(SimNode simNode, Snapshot before, Snapshot after, RaftMessage cause) {
        String who = simNode.id.value();
        String from = cause == null ? "?" : cause.from().value();
        int size = cluster.size();
        if (after.role() == Role.CANDIDATE && after.term() > before.term()) {
            if (before.role() == Role.CANDIDATE) {
                emit(EventKind.ELECTION, who + " election for term " + before.term() + " failed: " + before.granted().size()
                        + "/" + size + " votes " + names(before.granted()) + ", needed " + cluster.quorum());
            }
            emit(EventKind.ELECTION, who + " " + before.role() + " -> CANDIDATE  term " + after.term()
                    + "  (election timeout after " + before.electionTimeout() + " ticks; votes for itself)");
        }
        if (after.role() == Role.LEADER && before.role() != Role.LEADER) {
            emit(EventKind.ELECTION, who + " CANDIDATE -> LEADER  term " + after.term() + "  votes " + after.granted().size()
                    + "/" + size + " " + names(after.granted()) + ", needed " + cluster.quorum()
                    + (after.rejected().isEmpty() ? "" : "; rejected by " + names(after.rejected())));
        }
        if (before.role() != Role.FOLLOWER && after.role() == Role.FOLLOWER) {
            String reason = after.term() > before.term()
                    ? "saw higher term " + after.term() + " from " + from
                    : "heartbeat from leader " + from + " of the same term";
            emit(EventKind.ELECTION, who + " " + before.role() + " -> FOLLOWER  term " + after.term() + "  (" + reason + ")");
        } else if (before.role() == Role.FOLLOWER && after.role() == Role.FOLLOWER && after.term() > before.term()) {
            emit(EventKind.ELECTION, who + " term " + before.term() + " -> " + after.term() + "  (higher term from " + from + ")");
        }
        if (after.votedFor().isPresent()
                && !after.votedFor().get().equals(simNode.id)
                && (after.term() != before.term() || before.votedFor().isEmpty())) {
            emit(EventKind.ELECTION, who + " votes for " + after.votedFor().get() + " in term " + after.term());
        }
        if (after.role() == Role.FOLLOWER && after.leaderId().isPresent() && !after.leaderId().equals(before.leaderId())) {
            emit(EventKind.ELECTION, who + " follows leader " + after.leaderId().get() + " in term " + after.term());
        }
    }

    private void describeReplication(SimNode simNode, Snapshot before, Snapshot after) {
        String who = simNode.id.value();
        RaftStorage storage = simNode.storage();
        if (after.lastIndex() != before.lastIndex() || after.lastTerm() != before.lastTerm()) {
            boolean repaired = after.lastIndex() < before.lastIndex()
                    || (before.lastIndex() > 0 && storage.termAt(before.lastIndex()) != before.lastTerm());
            if (repaired) {
                emit(EventKind.REPLICATION, who + " log: replaced conflicting uncommitted entries; now ends at index "
                        + after.lastIndex() + " (term " + after.lastTerm() + ")");
            } else {
                List<LogEntry> added = storage.entries(before.lastIndex() + 1, after.lastIndex());
                emit(EventKind.REPLICATION, who + " log: appended " + describe(added) + "; now ends at index "
                        + after.lastIndex());
            }
        }
        if (after.commitIndex() > before.commitIndex()) {
            emit(EventKind.REPLICATION, who + " commitIndex " + before.commitIndex() + " -> " + after.commitIndex());
        }
        if (after.lastApplied() > before.lastApplied()) {
            emit(EventKind.REPLICATION, who + " applied " + describe(storage.entries(before.lastApplied() + 1, after.lastApplied()))
                    + " to its state machine");
        }
    }

    private static String describe(List<LogEntry> entries) {
        return entries.stream().map(e -> e.index() + ":" + e.command() + "(t" + e.term() + ")")
                .collect(Collectors.joining(", ", "[", "]"));
    }

    private void recordLeader(long term, NodeId leader) {
        leaderHistory.add(new LeaderRecord(term, leader, now));
        NodeId existing = leaderByTerm.putIfAbsent(term, leader);
        if (existing != null && !existing.equals(leader)) {
            fail("Election Safety violated: " + existing + " and " + leader + " both elected in term " + term);
        }
    }

    private void fail(String message) {
        throw new IllegalStateException("seed " + seed + ", tick " + now + ": " + message);
    }

    private void emit(EventKind kind, String text) {
        if (!recordEvents && listener == null) {
            return;
        }
        SimEvent event = new SimEvent(now, kind, text);
        if (recordEvents) {
            events.add(event);
        }
        if (listener != null) {
            listener.accept(event);
        }
    }

    private SimNode requireNode(String id) {
        SimNode simNode = nodes.get(NodeId.of(id));
        if (simNode == null) {
            throw new IllegalArgumentException("unknown node " + id);
        }
        return simNode;
    }

    private SimNode requireMemoryNode(String id) {
        SimNode simNode = requireNode(id);
        if (simNode.directory != null) {
            throw new IllegalStateException(id + " uses RocksDB storage; failure injection needs in-memory nodes");
        }
        return simNode;
    }

    private static String requestKey(NodeId node, long index, long term) {
        return node + "/" + index + "/" + term;
    }

    private static String names(Set<NodeId> ids) {
        return ids.stream().sorted().map(id -> id.value()).collect(Collectors.joining(", ", "[", "]"));
    }

    /** Configures a simulation. Unspecified nodes start at term 0 with an empty log. */
    public static final class Builder {
        private final List<NodeId> voters;
        private long seed = 1;
        private ElectionTiming timing = ElectionTiming.DEFAULT;
        private NetworkConfig network = NetworkConfig.RELIABLE;
        private Consumer<SimEvent> listener;
        private boolean recordEvents = true;
        private Path rocksDbBase;
        private final Map<NodeId, TermAndVote> initialStates = new HashMap<>();
        private final Map<NodeId, List<LogEntry>> initialLogs = new HashMap<>();
        private final Map<NodeId, RandomSource> randomOverrides = new HashMap<>();

        private Builder(List<NodeId> voters) {
            this.voters = voters;
        }

        public Builder seed(long seed) {
            this.seed = seed;
            return this;
        }

        public Builder timing(ElectionTiming timing) {
            this.timing = Objects.requireNonNull(timing);
            return this;
        }

        public Builder network(NetworkConfig network) {
            this.network = Objects.requireNonNull(network);
            return this;
        }

        public Builder listener(Consumer<SimEvent> listener) {
            this.listener = listener;
            return this;
        }

        /** Whether to keep the event history (on by default; long randomized runs may turn it off). */
        public Builder recordEvents(boolean recordEvents) {
            this.recordEvents = recordEvents;
            return this;
        }

        /** Gives every node real RocksDB storage under {@code baseDirectory/<node id>}. */
        public Builder rocksDb(Path baseDirectory) {
            this.rocksDbBase = Objects.requireNonNull(baseDirectory);
            return this;
        }

        /** Pre-populates an in-memory node's storage, as if it had run before. */
        public Builder initialState(String id, TermAndVote state, List<LogEntry> log) {
            initialStates.put(NodeId.of(id), state);
            initialLogs.put(NodeId.of(id), List.copyOf(log));
            return this;
        }

        /** Replaces a node's randomness for its first incarnation only. */
        public Builder randomSource(String id, RandomSource random) {
            randomOverrides.put(NodeId.of(id), random);
            return this;
        }

        public Simulation build() {
            return new Simulation(this);
        }
    }
}
