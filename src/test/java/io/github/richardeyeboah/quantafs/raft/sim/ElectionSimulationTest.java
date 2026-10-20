package io.github.richardeyeboah.quantafs.raft.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.richardeyeboah.quantafs.raft.LogPosition;
import io.github.richardeyeboah.quantafs.raft.NodeId;
import io.github.richardeyeboah.quantafs.raft.RaftMessage;
import io.github.richardeyeboah.quantafs.raft.RaftNode;
import io.github.richardeyeboah.quantafs.raft.Role;
import io.github.richardeyeboah.quantafs.raft.TermAndVote;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Cluster-level election scenarios in the deterministic simulator. Every run also enforces the
 * simulator's built-in invariants (one leader per term, monotonic terms, one vote per term, memory
 * never ahead of storage) after every single node step.
 */
class ElectionSimulationTest {

    private static final int ELECTION_BUDGET = 500;

    static Stream<Arguments> clusterSizesAndSeeds() {
        return IntStream.of(3, 5).boxed().flatMap(n -> LongStream.rangeClosed(1, 25).mapToObj(seed -> Arguments.of(n, seed)));
    }

    @ParameterizedTest(name = "{0} voters, seed {1}")
    @MethodSource("clusterSizesAndSeeds")
    void initialElectionProducesExactlyOneLeaderThatEveryoneFollows(int voters, long seed) {
        Simulation sim = simulation(voters, seed).build();
        assertTrue(sim.runUntil(s -> s.establishedLeader().isPresent(), ELECTION_BUDGET));
        RaftNode leader = sim.establishedLeader().orElseThrow();
        assertTrue(sim.runUntil(s -> allFollow(s, leader), 50), "heartbeats reach every follower");
        assertEquals(List.of(leader), sim.leaders());
        assertEquals(List.of(), ElectionSafety.violations(sim.leaderHistory()));
    }

    @ParameterizedTest(name = "{0} voters")
    @ValueSource(ints = {3, 5})
    void heartbeatsKeepALeaderAndItsTermStable(int voters) {
        Simulation sim = simulation(voters, 11).build();
        RaftNode leader = awaitLeader(sim);
        int electionsSoFar = sim.leaderHistory().size();

        sim.run(2_000);

        assertEquals(Role.LEADER, leader.role());
        assertTrue(sim.liveNodes().stream().allMatch(n -> n.currentTerm() == leader.currentTerm()), "no term changes");
        assertEquals(electionsSoFar, sim.leaderHistory().size(), "no further elections");
    }

    @ParameterizedTest(name = "{0} voters, seed {1}")
    @MethodSource("clusterSizesAndSeeds")
    void crashedLeaderIsReplacedAndRejoinsAsFollower(int voters, long seed) {
        Simulation sim = simulation(voters, seed).build();
        RaftNode old = awaitLeader(sim);
        String oldId = old.id().value();
        sim.crash(oldId);

        assertTrue(sim.runUntil(s -> s.establishedLeader().filter(l -> l.currentTerm() > old.currentTerm()).isPresent(), ELECTION_BUDGET));
        RaftNode replacement = sim.establishedLeader().orElseThrow();
        assertNotEquals(old.id(), replacement.id());

        sim.restart(oldId);
        assertTrue(sim.runUntil(s -> s.liveNode(oldId).leaderId().equals(Optional.of(replacement.id())), ELECTION_BUDGET));
        assertEquals(Role.FOLLOWER, sim.liveNode(oldId).role());
    }

    @Test
    void fiveVotersStillElectALeaderWithTwoNodesDown() {
        Simulation sim = simulation(5, 3).build();
        RaftNode old = awaitLeader(sim);
        sim.crash(old.id().value());
        String other = sim.liveNodes().getFirst().id().value();
        sim.crash(other);

        assertTrue(sim.runUntil(s -> s.establishedLeader().filter(l -> l.currentTerm() > old.currentTerm()).isPresent(), ELECTION_BUDGET));
    }

    @ParameterizedTest(name = "{0} voters")
    @ValueSource(ints = {3, 5})
    void simultaneousCandidatesSplitTheVoteAndRetryInALaterTerm(int voters) {
        Simulation.Builder builder = simulation(voters, 5);
        // Every node draws the same first timeout, so all become candidates in term 1 at tick 10,
        // each votes for itself, and every RequestVote is refused. Later timeouts are randomized.
        for (int i = 1; i <= voters; i++) {
            builder.randomSource("n" + i, new ScriptedRandomSource(List.of(10), 100 + i));
        }
        Simulation sim = builder.build();

        sim.run(10);
        assertEquals(voters, sim.liveNodes().stream().filter(n -> n.role() == Role.CANDIDATE && n.currentTerm() == 1).count());

        RaftNode leader = awaitLeader(sim);
        assertTrue(leader.currentTerm() >= 2);
        assertTrue(sim.leaderHistory().stream().noneMatch(r -> r.term() == 1), "nobody won term 1");
        assertTrue(sim.events().stream().anyMatch(e -> e.text().contains("election for term 1 failed")));
    }

    @Test
    void isolatedOldLeaderKeepsBelievingItLeadsUntilItHearsTheHigherTerm() {
        Simulation sim = simulation(5, 8).build();
        RaftNode old = awaitLeader(sim);
        long oldTerm = old.currentTerm();
        sim.isolate(old.id().value());

        assertTrue(sim.runUntil(s -> s.establishedLeader().filter(l -> l.currentTerm() > oldTerm).isPresent(), ELECTION_BUDGET));
        RaftNode replacement = sim.establishedLeader().orElseThrow();

        // The documented nuance: nothing tells the isolated node it was replaced. It still sees
        // itself as leader of the older term (and in 2B could not commit anything there).
        assertEquals(Role.LEADER, old.role());
        assertEquals(oldTerm, old.currentTerm());
        assertEquals(2, sim.leaders().size(), "two self-declared leaders, of different terms");
        assertEquals(List.of(), ElectionSafety.violations(sim.leaderHistory()));

        sim.heal();
        assertTrue(sim.runUntil(s -> old.role() == Role.FOLLOWER && s.leaders().size() == 1, ELECTION_BUDGET));
        assertTrue(old.currentTerm() >= replacement.currentTerm());
    }

    @Test
    void minorityPartitionNeverElectsALeader() {
        Simulation sim = simulation(5, 21).build();
        sim.partition(List.of("n1", "n2"), List.of("n3", "n4", "n5"));
        sim.run(3_000);

        Set<NodeId> minority = Set.of(NodeId.of("n1"), NodeId.of("n2"));
        assertTrue(sim.leaderHistory().stream().noneMatch(r -> minority.contains(r.leader())));
        assertTrue(sim.liveNode("n1").currentTerm() > 1, "the minority kept trying, in ever higher terms");
        assertTrue(sim.establishedLeader().isPresent(), "the majority side elected a leader");
    }

    @Test
    void twoSurvivorsOfFiveNeverElectALeader() {
        Simulation sim = simulation(5, 4).build();
        RaftNode leader = awaitLeader(sim);
        sim.crash(leader.id().value());
        while (sim.liveNodes().size() > 2) {
            sim.crash(sim.liveNodes().getFirst().id().value());
        }
        int elections = sim.leaderHistory().size();
        long termAtCrash = sim.liveNodes().getFirst().currentTerm();

        sim.run(3_000);
        assertEquals(elections, sim.leaderHistory().size(), "2 of 5 is not a quorum");
        assertTrue(sim.leaders().isEmpty());
        assertTrue(sim.liveNodes().getFirst().currentTerm() > termAtCrash, "they keep campaigning");
    }

    @Test
    void rejoiningNodeWithAHigherTermForcesTheLeaderToStepDown() {
        Simulation sim = simulation(3, 6).build();
        RaftNode leader = awaitLeader(sim);
        String follower = sim.liveNodes().stream().filter(n -> n != leader).findFirst().orElseThrow().id().value();
        sim.isolate(follower);
        sim.run(400); // the isolated follower keeps timing out and raising its term
        long isolatedTerm = sim.liveNode(follower).currentTerm();
        assertTrue(isolatedTerm > leader.currentTerm() + 3);
        assertEquals(Role.LEADER, leader.role());

        sim.heal();
        assertTrue(sim.runUntil(s -> s.establishedLeader().filter(l -> l.currentTerm() >= isolatedTerm).isPresent(), ELECTION_BUDGET));
        assertTrue(sim.events().stream().anyMatch(e -> e.text().startsWith(leader.id() + " LEADER -> FOLLOWER")),
                "without PreVote, the returning node's higher term deposes the leader");
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
    void delayedDuplicatedReorderedAndDroppedMessagesNeverBreakSafety(long seed) {
        // Delays up to 15 ticks exceed the 10-tick minimum election timeout, so votes and
        // heartbeats regularly arrive after their term has ended.
        Simulation sim = simulation(5, seed).network(new NetworkConfig(1, 15, 0.1, 0.3)).build();
        sim.run(3_000);
        assertTrue(sim.duplicatedCount() > 0 && sim.droppedCount() > 0, "faults were actually injected");
        assertEquals(List.of(), ElectionSafety.violations(sim.leaderHistory()));

        sim.setNetwork(NetworkConfig.RELIABLE);
        assertTrue(sim.runUntil(s -> s.establishedLeader().isPresent(), ELECTION_BUDGET));
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 4, 5, 6, 7, 8})
    void onlyCandidatesWithUpToDateLogsCanWin(long seed) {
        // n3's fixture log is behind; it times out first but n1 and n2 refuse it (section 5.4.1).
        Simulation sim = Simulation.builder("n1", "n2", "n3")
                .seed(seed)
                .initialState("n1", TermAndVote.of(2, null))
                .initialState("n2", TermAndVote.of(2, null))
                .initialState("n3", TermAndVote.of(2, null))
                .logPosition("n1", new LogPosition(2, 5))
                .logPosition("n2", new LogPosition(2, 5))
                .logPosition("n3", new LogPosition(1, 1))
                .randomSource("n3", new ScriptedRandomSource(List.of(10), seed))
                .randomSource("n1", new ScriptedRandomSource(List.of(19), seed + 1))
                .randomSource("n2", new ScriptedRandomSource(List.of(17), seed + 2))
                .build();
        RaftNode leader = awaitLeader(sim);
        assertNotEquals(NodeId.of("n3"), leader.id());
        assertTrue(sim.leaderHistory().stream().noneMatch(r -> r.leader().equals(NodeId.of("n3"))));
        assertTrue(sim.events().stream().anyMatch(e -> e.text().startsWith("n3 FOLLOWER -> CANDIDATE")), "n3 did try");
    }

    @Test
    void nodeCrashedRightAfterVotingReloadsThatVote() {
        Simulation sim = Simulation.builder("n1", "n2", "n3")
                .seed(9)
                .randomSource("n1", new ScriptedRandomSource(List.of(10), 1))
                .randomSource("n2", new ScriptedRandomSource(List.of(19), 2))
                .randomSource("n3", new ScriptedRandomSource(List.of(19), 3))
                .build();
        assertTrue(sim.runUntil(s -> s.liveNode("n2").votedFor().equals(Optional.of(NodeId.of("n1"))), 50));
        sim.crash("n2");
        sim.restart("n2");
        assertEquals(1, sim.liveNode("n2").currentTerm());
        assertEquals(Optional.of(NodeId.of("n1")), sim.liveNode("n2").votedFor());
    }

    @Test
    void nodeWhoseDiskFailsGoesSilentAndTheClusterRecoversAfterItRestarts() {
        Simulation sim = simulation(3, 12).build();
        RaftNode leader = awaitLeader(sim);
        String broken = sim.liveNodes().stream().filter(n -> n != leader).findFirst().orElseThrow().id().value();
        sim.store(broken).failSaves(true);
        sim.crash(leader.id().value());

        assertTrue(sim.runUntil(s -> s.liveNode(broken).isHalted(), ELECTION_BUDGET));
        int sentBeforeHaltObserved = sim.sentMessages().size();
        sim.run(1_000);
        List<RaftMessage> afterHalt = sim.sentMessages().subList(sentBeforeHaltObserved, sim.sentMessages().size()).stream()
                .map(Simulation.SentMessage::message)
                .filter(m -> m.from().value().equals(broken))
                .toList();
        assertEquals(List.of(), afterHalt, "a node that cannot persist sends nothing at all");
        assertTrue(sim.leaders().isEmpty(), "one healthy node out of three is not a quorum");

        sim.store(broken).failSaves(false);
        sim.crash(broken);
        sim.restart(broken);
        assertTrue(sim.runUntil(s -> s.establishedLeader().isPresent(), ELECTION_BUDGET));
    }

    @Test
    void sameSeedReproducesTheSameHistoryAndDifferentSeedsDiffer() {
        List<Simulation.SimEvent> first = scriptedRun(42);
        List<Simulation.SimEvent> second = scriptedRun(42);
        assertEquals(first, second);
        assertTrue(LongStream.rangeClosed(43, 47).anyMatch(seed -> !scriptedRun(seed).equals(first)));
    }

    private static List<Simulation.SimEvent> scriptedRun(long seed) {
        Simulation sim = simulation(5, seed).network(new NetworkConfig(1, 6, 0.05, 0.1)).build();
        sim.run(200);
        sim.crash(sim.leaders().isEmpty() ? "n1" : sim.leaders().getFirst().id().value());
        sim.run(300);
        return sim.events();
    }

    private static Simulation.Builder simulation(int voters, long seed) {
        String[] ids = IntStream.rangeClosed(1, voters).mapToObj(i -> "n" + i).toArray(String[]::new);
        return Simulation.builder(ids).seed(seed);
    }

    private static RaftNode awaitLeader(Simulation sim) {
        assertTrue(sim.runUntil(s -> s.establishedLeader().isPresent(), ELECTION_BUDGET), "no leader within budget");
        return sim.establishedLeader().orElseThrow();
    }

    private static boolean allFollow(Simulation sim, RaftNode leader) {
        return sim.liveNodes().stream().allMatch(n -> n.leaderId().equals(Optional.of(leader.id())));
    }
}
