package io.github.richardeyeboah.quantafs.raft.sim;

import io.github.richardeyeboah.quantafs.raft.ClusterConfig;
import io.github.richardeyeboah.quantafs.raft.ElectionTiming;
import io.github.richardeyeboah.quantafs.raft.InMemoryElectionStateStore;
import io.github.richardeyeboah.quantafs.raft.LogMetadata;
import io.github.richardeyeboah.quantafs.raft.LogPosition;
import io.github.richardeyeboah.quantafs.raft.NodeId;
import io.github.richardeyeboah.quantafs.raft.RaftMessage;
import io.github.richardeyeboah.quantafs.raft.RaftNode;
import io.github.richardeyeboah.quantafs.raft.RandomSource;
import io.github.richardeyeboah.quantafs.raft.Role;
import io.github.richardeyeboah.quantafs.raft.SeededRandomSource;
import io.github.richardeyeboah.quantafs.raft.TermAndVote;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Deterministic, single-threaded simulation of a cluster of {@link RaftNode}s.
 *
 * <p>The simulation owns logical time and the network. Each {@link #step()} advances time by one
 * tick, delivers every message due at that tick, then ticks every running node. A single seeded
 * random generator decides message delays, drops, and duplicates, and seeds each node's election
 * timeouts, so the same seed and the same sequence of calls always produce the same history. No
 * threads, sleeps, or wall-clock time are involved.
 *
 * <p>Faults: per-message delay (which reorders messages), drops, duplicates, network partitions,
 * node crashes, and restarts. A crashed node loses all volatile state; its
 * {@link InMemoryElectionStateStore} survives, modelling a durable disk.
 *
 * <p>After every node step the simulation checks invariants and throws {@link IllegalStateException}
 * if one is broken: at most one leader per term, no node's term ever decreases (including across
 * restarts), no node votes for two candidates in one term, and a node's in-memory term and vote
 * never run ahead of its store.
 *
 * <p>This is a test and demonstration harness for the election core. It stores no client data and
 * replicates nothing.
 */
public final class Simulation {

    /** Something observable happened at logical time {@code tick}. */
    public record SimEvent(long tick, String text) {
        @Override
        public String toString() {
            return String.format("[tick %4d] %s", tick, text);
        }
    }

    /** A message a node emitted at {@code tick}, before any network fault was applied. */
    public record SentMessage(long tick, RaftMessage message) {}

    private record InFlight(long deliverAt, long sequence, RaftMessage message) {}

    private record Snapshot(
            Role role,
            long term,
            Optional<NodeId> votedFor,
            Optional<NodeId> leaderId,
            Set<NodeId> granted,
            Set<NodeId> rejected,
            boolean halted,
            int electionTimeout) {}

    private static final class SimNode {
        final NodeId id;
        final InMemoryElectionStateStore store;
        final LogMetadata log;
        RandomSource firstRandom;
        RaftNode node; // null while crashed

        SimNode(NodeId id, InMemoryElectionStateStore store, LogMetadata log, RandomSource firstRandom) {
            this.id = id;
            this.store = store;
            this.log = log;
            this.firstRandom = firstRandom;
        }
    }

    private final long seed;
    private final ClusterConfig cluster;
    private final ElectionTiming timing;
    private NetworkConfig network;
    private final SplittableRandom networkRandom;
    private final SplittableRandom nodeSeeds;
    private final Consumer<SimEvent> listener;
    private final Map<NodeId, SimNode> nodes = new TreeMap<>();
    private final PriorityQueue<InFlight> inFlight =
            new PriorityQueue<>(
                    Comparator.comparingLong(InFlight::deliverAt).thenComparingLong(InFlight::sequence));
    private final List<SimEvent> events = new ArrayList<>();
    private final List<LeaderRecord> leaderHistory = new ArrayList<>();
    private final Map<Long, NodeId> leaderByTerm = new HashMap<>();
    private final List<SentMessage> sent = new ArrayList<>();
    private final Map<NodeId, Long> highestTerm = new HashMap<>();
    private final Map<NodeId, Map<Long, NodeId>> votesCast = new HashMap<>();
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
        for (NodeId id : cluster.voters()) {
            InMemoryElectionStateStore store =
                    new InMemoryElectionStateStore(builder.initialStates.getOrDefault(id, TermAndVote.INITIAL));
            LogMetadata log = LogMetadata.fixed(builder.logPositions.getOrDefault(id, LogPosition.EMPTY));
            SimNode simNode = new SimNode(id, store, log, builder.randomOverrides.get(id));
            nodes.put(id, simNode);
            startNode(simNode);
        }
    }

    public static Builder builder(String... voterIds) {
        return new Builder(Arrays.stream(voterIds).map(NodeId::of).toList());
    }

    // ---------------------------------------------------------------- time

    /** Advances logical time by one tick: deliver due messages, then tick every running node. */
    public void step() {
        now++;
        while (!inFlight.isEmpty() && inFlight.peek().deliverAt() <= now) {
            deliver(inFlight.poll().message());
        }
        for (SimNode simNode : nodes.values()) {
            if (simNode.node != null) {
                Snapshot before = snapshot(simNode.node);
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

    // ---------------------------------------------------------------- faults

    public void crash(String id) {
        SimNode simNode = requireNode(id);
        if (simNode.node == null) {
            throw new IllegalStateException(id + " is already down");
        }
        RaftNode node = simNode.node;
        simNode.node = null;
        emit("CRASH " + id + " (was " + node.role() + " in term " + node.currentTerm()
                + "); its volatile state is gone, its durable store survives");
    }

    public void restart(String id) {
        SimNode simNode = requireNode(id);
        if (simNode.node != null) {
            throw new IllegalStateException(id + " is already running");
        }
        startNode(simNode);
        RaftNode node = simNode.node;
        emit("RESTART " + id + ": reloaded " + simNode.store.load() + " from its durable store; starts as FOLLOWER");
        checkDurableInvariants(simNode, snapshot(node));
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
        emit("PARTITION " + sets.stream().map(Simulation::names).collect(Collectors.joining(" | "))
                + " (in-flight messages across groups are lost)");
    }

    /** Cuts {@code id} off from every other node. */
    public void isolate(String id) {
        List<String> others =
                cluster.voters().stream().map(NodeId::value).filter(v -> !v.equals(id)).toList();
        partition(List.of(id), others);
    }

    public void heal() {
        partition = null;
        emit("HEAL network: all nodes can reach each other again");
    }

    public void setNetwork(NetworkConfig network) {
        this.network = Objects.requireNonNull(network, "network");
    }

    // ---------------------------------------------------------------- queries

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

    /** The running node, or empty while it is crashed. */
    public Optional<RaftNode> node(String id) {
        return Optional.ofNullable(requireNode(id).node);
    }

    /** The running node; throws if it is crashed. */
    public RaftNode liveNode(String id) {
        return node(id).orElseThrow(() -> new IllegalStateException(id + " is down"));
    }

    public InMemoryElectionStateStore store(String id) {
        return requireNode(id).store;
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
                .max(Comparator.comparingLong(RaftNode::currentTerm));
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

    // ---------------------------------------------------------------- internals

    private void startNode(SimNode simNode) {
        // Always consume one split so other nodes' streams do not depend on overrides.
        RandomSource seeded = new SeededRandomSource(nodeSeeds.split());
        RandomSource random = simNode.firstRandom != null ? simNode.firstRandom : seeded;
        simNode.firstRandom = null;
        simNode.node = new RaftNode(simNode.id, cluster, timing, random, simNode.store, simNode.log);
    }

    private void deliver(RaftMessage message) {
        SimNode destination = nodes.get(message.to());
        if (destination == null || destination.node == null || !connected(message.from(), message.to())) {
            dropped++;
            return;
        }
        delivered++;
        Snapshot before = snapshot(destination.node);
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
        if (partition == null) {
            return true;
        }
        return partition.stream().anyMatch(group -> group.contains(a) && group.contains(b));
    }

    private static Snapshot snapshot(RaftNode node) {
        return new Snapshot(
                node.role(),
                node.currentTerm(),
                node.votedFor(),
                node.leaderId(),
                node.votesGranted(),
                node.votesRejected(),
                node.isHalted(),
                node.electionTimeoutTicks());
    }

    private void observe(SimNode simNode, Snapshot before, RaftMessage cause) {
        RaftNode node = simNode.node;
        Snapshot after = snapshot(node);
        String who = simNode.id.value();
        String from = cause == null ? "?" : cause.from().value();
        int size = cluster.size();

        if (after.term() < before.term()) {
            fail(who + " term decreased from " + before.term() + " to " + after.term());
        }
        checkDurableInvariants(simNode, after);

        if (!before.halted() && after.halted()) {
            emit(who + " HALTED: " + node.haltCause().map(Throwable::getMessage).orElse("?")
                    + "; it sends nothing until restarted");
        }
        if (after.role() == Role.CANDIDATE && after.term() > before.term()) {
            if (before.role() == Role.CANDIDATE) {
                emit(who + " election for term " + before.term() + " failed: " + before.granted().size() + "/"
                        + size + " votes " + names(before.granted()) + ", needed " + cluster.quorum());
            }
            emit(who + " " + before.role() + " -> CANDIDATE  term " + after.term() + "  (election timeout after "
                    + before.electionTimeout() + " ticks; votes for itself)");
        }
        if (after.role() == Role.LEADER && before.role() != Role.LEADER) {
            emit(who + " CANDIDATE -> LEADER  term " + after.term() + "  votes " + after.granted().size() + "/"
                    + size + " " + names(after.granted()) + ", needed " + cluster.quorum()
                    + (after.rejected().isEmpty() ? "" : "; rejected by " + names(after.rejected())));
            recordLeader(after.term(), simNode.id);
        }
        if (before.role() != Role.FOLLOWER && after.role() == Role.FOLLOWER) {
            String reason = after.term() > before.term()
                    ? "saw higher term " + after.term() + " from " + from
                    : "heartbeat from leader " + from + " of the same term";
            emit(who + " " + before.role() + " -> FOLLOWER  term " + after.term() + "  (" + reason + ")");
        } else if (before.role() == Role.FOLLOWER && after.role() == Role.FOLLOWER && after.term() > before.term()) {
            emit(who + " term " + before.term() + " -> " + after.term() + "  (higher term from " + from + ")");
        }
        if (after.votedFor().isPresent()
                && !after.votedFor().get().equals(simNode.id)
                && (after.term() != before.term() || before.votedFor().isEmpty())) {
            emit(who + " votes for " + after.votedFor().get() + " in term " + after.term());
        }
        if (after.role() == Role.FOLLOWER
                && after.leaderId().isPresent()
                && !after.leaderId().equals(before.leaderId())) {
            emit(who + " follows leader " + after.leaderId().get() + " in term " + after.term());
        }
    }

    private void checkDurableInvariants(SimNode simNode, Snapshot state) {
        NodeId id = simNode.id;
        TermAndVote durable = simNode.store.load();
        if (state.term() != durable.currentTerm() || !state.votedFor().equals(durable.votedFor())) {
            fail(id + " memory (" + state.term() + ", " + state.votedFor() + ") differs from its store (" + durable + ")");
        }
        long highest = highestTerm.getOrDefault(id, 0L);
        if (state.term() < highest) {
            fail(id + " went back from term " + highest + " to " + state.term());
        }
        highestTerm.put(id, Math.max(highest, state.term()));
        if (state.votedFor().isPresent()) {
            NodeId earlier =
                    votesCast.computeIfAbsent(id, k -> new HashMap<>()).putIfAbsent(state.term(), state.votedFor().get());
            if (earlier != null && !earlier.equals(state.votedFor().get())) {
                fail(id + " voted for both " + earlier + " and " + state.votedFor().get() + " in term " + state.term());
            }
        }
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

    private void emit(String text) {
        SimEvent event = new SimEvent(now, text);
        events.add(event);
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

    private static String names(Set<NodeId> ids) {
        return ids.stream().sorted().map(NodeId::value).collect(Collectors.joining(", ", "[", "]"));
    }

    /** Configures a simulation. Unspecified nodes start at term 0 with an empty log. */
    public static final class Builder {
        private final List<NodeId> voters;
        private long seed = 1;
        private ElectionTiming timing = ElectionTiming.DEFAULT;
        private NetworkConfig network = NetworkConfig.RELIABLE;
        private Consumer<SimEvent> listener;
        private final Map<NodeId, TermAndVote> initialStates = new HashMap<>();
        private final Map<NodeId, LogPosition> logPositions = new HashMap<>();
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

        /** Pre-populates a node's durable store, as if it had run before. */
        public Builder initialState(String id, TermAndVote state) {
            initialStates.put(NodeId.of(id), state);
            return this;
        }

        /** Fixture log metadata for a node (milestone 2A has no real log). */
        public Builder logPosition(String id, LogPosition position) {
            logPositions.put(NodeId.of(id), position);
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
