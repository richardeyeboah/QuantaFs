package io.github.richardeyeboah.quantafs.raft.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.richardeyeboah.quantafs.raft.Command;
import io.github.richardeyeboah.quantafs.raft.FailureMode;
import io.github.richardeyeboah.quantafs.raft.LogEntry;
import io.github.richardeyeboah.quantafs.raft.ProposalOutcome;
import io.github.richardeyeboah.quantafs.raft.ProposeResult;
import io.github.richardeyeboah.quantafs.raft.RaftNode;
import io.github.richardeyeboah.quantafs.raft.Role;
import io.github.richardeyeboah.quantafs.raft.storage.NodeStorage;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Replication scenarios in the deterministic simulator. Besides each test's own assertions, every
 * run enforces the simulator's invariants after every node step: election safety, log matching,
 * committed-entry agreement, ordered application of committed entries only, and that every
 * acknowledged proposal is in the committed history.
 */
class ReplicationSimulationTest {

    private static final int BUDGET = 2_000;

    static Stream<Arguments> sizesAndSeeds() {
        return IntStream.of(3, 5).boxed().flatMap(n -> LongStream.rangeClosed(1, 10).mapToObj(seed -> Arguments.of(n, seed)));
    }

    @ParameterizedTest(name = "{0} voters, seed {1}")
    @MethodSource("sizesAndSeeds")
    void pipelinedWritesAreCommittedAndAppliedInProposalOrderEverywhere(int voters, long seed) {
        Simulation sim = simulation(voters, seed).build();
        String leader = awaitLeader(sim).id().value();
        List<Simulation.ClientRequest> requests = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            requests.add(sim.propose(leader, command(i)));
        }
        assertTrue(sim.runUntil(s -> requests.stream().allMatch(r -> r.outcome().isPresent()), BUDGET));
        assertTrue(requests.stream().allMatch(Simulation.ClientRequest::acknowledged));
        long[] indexes = requests.stream().mapToLong(r -> r.accepted().orElseThrow().index()).toArray();
        for (int i = 1; i < indexes.length; i++) {
            assertEquals(indexes[i - 1] + 1, indexes[i], "consecutive log slots in proposal order");
        }
        awaitConvergence(sim);
    }

    @Test
    void followersRefuseProposalsAndPointToTheLeader() {
        Simulation sim = simulation(3, 2).build();
        RaftNode leader = awaitLeader(sim);
        String follower = otherThan(sim, leader);
        // A follower learns who leads only from that leader's AppendEntries; before that its hint is empty.
        assertTrue(sim.runUntil(s -> s.liveNode(follower).leaderId().equals(Optional.of(leader.id())), BUDGET));
        long before = sim.storage(follower).lastIndex();
        Simulation.ClientRequest request = sim.propose(follower, command(1));
        assertEquals(new ProposeResult.NotLeader(Optional.of(leader.id())), request.result());
        assertEquals(before, sim.storage(follower).lastIndex());
    }

    @Test
    void isolatedFormerLeaderCannotCommitAndItsUncommittedEntriesAreReplaced() {
        Simulation sim = simulation(5, 8).build();
        RaftNode old = awaitLeader(sim);
        String oldId = old.id().value();
        proposeAndAwaitAck(sim, oldId, command(0));

        sim.isolate(oldId);
        List<Simulation.ClientRequest> stranded = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            stranded.add(sim.propose(oldId, command(100 + i)));
        }
        assertTrue(sim.runUntil(s -> s.establishedLeader().filter(l -> l.currentTerm() > old.currentTerm()).isPresent(), BUDGET));
        String replacement = sim.establishedLeader().orElseThrow().id().value();
        proposeAndAwaitAck(sim, replacement, command(1));
        proposeAndAwaitAck(sim, replacement, command(2));
        assertEquals(Role.LEADER, old.role(), "the cut-off leader has not heard of the new term");
        assertTrue(stranded.stream().noneMatch(r -> r.outcome().isPresent()), "no success without a majority");

        sim.heal();
        awaitConvergence(sim);
        assertTrue(stranded.stream().allMatch(r -> r.outcome().equals(Optional.of(ProposalOutcome.Status.UNKNOWN))));
        for (Simulation.ClientRequest request : stranded) {
            assertTrue(sim.committedHistory().values().stream().noneMatch(e -> e.command().equals(request.command())));
            assertTrue(sim.storage(oldId).entries(1, sim.storage(oldId).lastIndex()).stream()
                    .noneMatch(e -> e.command().equals(request.command())), "replaced in the old leader's log");
        }
    }

    @ParameterizedTest(name = "{0} voters")
    @ValueSource(ints = {3, 5})
    void crashedFollowerCatchesUpAcrossManyBatches(int voters) {
        Simulation sim = simulation(voters, 4).build();
        RaftNode leader = awaitLeader(sim);
        String follower = otherThan(sim, leader);
        sim.crash(follower);
        int count = 3 * RaftNode.MAX_ENTRIES_PER_APPEND;
        List<Simulation.ClientRequest> requests = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            requests.add(sim.propose(leader.id().value(), command(i)));
        }
        assertTrue(sim.runUntil(s -> requests.stream().allMatch(Simulation.ClientRequest::acknowledged), BUDGET));
        sim.restart(follower);
        awaitConvergence(sim);
        assertTrue(sim.storage(follower).lastIndex() > count);
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 4, 5, 6, 7, 8})
    void lossyDuplicatingReorderingNetworkStillConverges(long seed) {
        Simulation sim = simulation(5, seed).network(new NetworkConfig(1, 12, 0.2, 0.3)).build();
        int proposed = 0;
        while (proposed < 25 && sim.now() < 5_000) {
            sim.run(15);
            Optional<RaftNode> leader = sim.establishedLeader();
            if (leader.isPresent()) {
                sim.propose(leader.get().id().value(), command(proposed++));
            }
        }
        assertTrue(sim.droppedCount() > 0 && sim.duplicatedCount() > 0, "faults were injected");
        sim.setNetwork(NetworkConfig.RELIABLE);
        RaftNode leader = awaitLeader(sim);
        proposeAndAwaitAck(sim, leader.id().value(), command(999));
        awaitConvergence(sim);
    }

    @Test
    void rocksDbBackedClusterRecoversFromRealReopens(@TempDir Path dir) {
        Simulation sim = simulation(3, 3).rocksDb(dir).build();
        try {
            RaftNode first = awaitLeader(sim);
            String firstId = first.id().value();
            for (int i = 0; i < 5; i++) {
                proposeAndAwaitAck(sim, firstId, command(i));
            }
            String follower = otherThan(sim, first);
            sim.crash(follower);
            for (int i = 5; i < 10; i++) {
                proposeAndAwaitAck(sim, firstId, command(i));
            }
            sim.crash(firstId);
            sim.restart(follower); // reopens its RocksDB files; it is behind and must catch up
            assertTrue(sim.runUntil(s -> s.establishedLeader().filter(l -> l.currentTerm() > first.currentTerm()).isPresent(), BUDGET));
            proposeAndAwaitAck(sim, sim.establishedLeader().orElseThrow().id().value(), command(10));
            sim.restart(firstId);
            awaitConvergence(sim);
        } finally {
            sim.close(); // releases every node's lock so the files can be reopened below
        }
        for (String id : List.of("n1", "n2", "n3")) {
            try (NodeStorage storage = NodeStorage.open(dir.resolve(id))) {
                assertEquals(storage.stateMachine().lastApplied().index(), storage.raftStorage().lastIndex());
                assertEquals(sim.modelState(storage.raftStorage().lastIndex()), storage.stateMachine().diagnosticContents());
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = FailureMode.class, names = {"FAIL_BEFORE", "FAIL_AFTER"})
    void followerWhoseApplyFailsRecoversWithoutSkippingOrRepeatingEntries(FailureMode mode) {
        Simulation sim = simulation(3, 6).build();
        RaftNode leader = awaitLeader(sim);
        String follower = otherThan(sim, leader);
        sim.injectApplyFailure(follower, mode);
        for (int i = 0; i < 3; i++) {
            proposeAndAwaitAck(sim, leader.id().value(), command(i)); // leader + the healthy follower suffice
        }
        assertTrue(sim.runUntil(s -> s.liveNode(follower).isHalted(), BUDGET));

        sim.crash(follower);
        sim.injectApplyFailure(follower, FailureMode.NONE);
        sim.restart(follower);
        awaitConvergence(sim);
        List<Long> applied = sim.memoryStateMachine(follower).appliedEntries().stream().map(LogEntry::index).toList();
        assertEquals(LongStream.rangeClosed(1, applied.size()).boxed().toList(), applied, "each index exactly once");
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = FailureMode.class, names = {"FAIL_BEFORE", "FAIL_AFTER"})
    void followerWhoseLogWriteFailsSendsNoAckAndRecoversAfterRestart(FailureMode mode) {
        Simulation sim = simulation(3, 7).build();
        RaftNode leader = awaitLeader(sim);
        String follower = otherThan(sim, leader);
        sim.injectStorageFailure(follower, mode);
        proposeAndAwaitAck(sim, leader.id().value(), command(1));
        assertTrue(sim.runUntil(s -> s.liveNode(follower).isHalted(), BUDGET));

        sim.crash(follower);
        sim.injectStorageFailure(follower, FailureMode.NONE);
        sim.restart(follower);
        awaitConvergence(sim);
    }

    @Test
    void leaderWhoseAppendFailsReportsUnknownAndTheClusterMovesOn() {
        Simulation sim = simulation(3, 5).build();
        RaftNode leader = awaitLeader(sim);
        String leaderId = leader.id().value();
        sim.injectStorageFailure(leaderId, FailureMode.FAIL_BEFORE);
        Simulation.ClientRequest request = sim.propose(leaderId, command(1));
        assertInstanceOf(ProposeResult.OutcomeUnknown.class, request.result());
        assertTrue(leader.isHalted());

        assertTrue(sim.runUntil(s -> s.establishedLeader().filter(l -> !l.id().equals(leader.id())).isPresent(), BUDGET));
        proposeAndAwaitAck(sim, sim.establishedLeader().orElseThrow().id().value(), command(2));
        sim.crash(leaderId);
        sim.injectStorageFailure(leaderId, FailureMode.NONE);
        sim.restart(leaderId);
        awaitConvergence(sim);
        // FAIL_BEFORE means the entry never reached the log, so here the unknown outcome was "not committed".
        assertFalse(sim.committedHistory().values().stream().anyMatch(e -> e.command().equals(command(1))));
    }

    // ================================================================ helpers

    static Simulation.Builder simulation(int voters, long seed) {
        return Simulation.builder(IntStream.rangeClosed(1, voters).mapToObj(i -> "n" + i).toArray(String[]::new)).seed(seed);
    }

    static RaftNode awaitLeader(Simulation sim) {
        assertTrue(sim.runUntil(s -> s.establishedLeader().isPresent(), BUDGET), "no leader within budget");
        return sim.establishedLeader().orElseThrow();
    }

    static void proposeAndAwaitAck(Simulation sim, String node, Command command) {
        Simulation.ClientRequest request = sim.propose(node, command);
        assertInstanceOf(ProposeResult.Accepted.class, request.result());
        assertTrue(sim.runUntil(s -> request.outcome().isPresent(), BUDGET), "no outcome for " + command);
        assertTrue(request.acknowledged(), command + " ended " + request.outcome());
    }

    /** Waits until every node runs, an established leader exists, and all have applied its commit index. */
    static void awaitConvergence(Simulation sim) {
        boolean converged = sim.runUntil(s -> {
            Optional<RaftNode> leader = s.establishedLeader();
            if (leader.isEmpty() || s.liveNodes().size() != s.cluster().size()) {
                return false;
            }
            long commit = leader.get().commitIndex();
            return s.liveNodes().stream().allMatch(n -> !n.isHalted() && n.commitIndex() == commit && n.lastApplied() == commit);
        }, BUDGET);
        assertTrue(converged, "cluster did not converge within " + BUDGET + " ticks");
        sim.assertConverged();
    }

    static String otherThan(Simulation sim, RaftNode leader) {
        return sim.liveNodes().stream().filter(n -> !n.id().equals(leader.id())).findFirst().orElseThrow().id().value();
    }

    /** Puts and deletes over a few keys (one binary), so later commands overwrite earlier ones. */
    static Command command(int i) {
        byte[] key = i % 5 == 4 ? new byte[] {0, (byte) 0xFF, (byte) (i % 3)} : ("k" + (i % 5)).getBytes(StandardCharsets.UTF_8);
        if (i % 4 == 3) {
            return Command.delete(key);
        }
        return Command.put(key, ("v" + i).getBytes(StandardCharsets.UTF_8));
    }
}
