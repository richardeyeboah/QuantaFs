package io.github.richardeyeboah.quantafs.raft.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.richardeyeboah.quantafs.raft.NodeId;
import io.github.richardeyeboah.quantafs.raft.RaftNode;
import io.github.richardeyeboah.quantafs.raft.TermAndVote;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Randomized fault histories: crashes, restarts, partitions, heals, and lossy networks, generated
 * from a seed. The simulator checks its invariants after every node step; this test additionally
 * checks the recorded leader history and every node's sequence of durable writes, then requires a
 * leader to emerge once all faults are repaired. A failing seed reproduces exactly.
 */
class ElectionChaosTest {

    private static final int CHAOS_TICKS = 2_500;
    private static final int RECOVERY_BUDGET = 1_000;

    static Stream<Arguments> histories() {
        return IntStream.of(3, 5).boxed().flatMap(n -> LongStream.rangeClosed(1, 60).mapToObj(seed -> Arguments.of(n, seed)));
    }

    @ParameterizedTest(name = "{0} voters, seed {1}")
    @MethodSource("histories")
    void randomFaultHistoriesPreserveElectionSafetyAndRecover(int voters, long seed) {
        List<String> ids = IntStream.rangeClosed(1, voters).mapToObj(i -> "n" + i).toList();
        Simulation sim = Simulation.builder(ids.toArray(String[]::new))
                .seed(seed)
                .network(new NetworkConfig(1, 12, 0.05, 0.1))
                .build();
        SplittableRandom faults = new SplittableRandom(seed * 7919);

        while (sim.now() < CHAOS_TICKS) {
            sim.run(1 + faults.nextInt(40));
            injectFault(sim, ids, faults);
        }

        sim.heal();
        sim.setNetwork(NetworkConfig.RELIABLE);
        for (String id : ids) {
            if (!sim.isUp(id)) {
                sim.restart(id);
            }
        }
        assertTrue(sim.runUntil(s -> s.establishedLeader().isPresent(), RECOVERY_BUDGET),
                "a leader must emerge once the network is healthy and all nodes are up");

        assertEquals(List.of(), ElectionSafety.violations(sim.leaderHistory()));
        assertTrue(ElectionSafety.leadersByTerm(sim.leaderHistory()).size() > 1, "the history exercised several elections");
        for (String id : ids) {
            assertDurableHistoryIsSafe(id, sim.memoryStorage(id).termAndVoteHistory());
        }
    }

    private static void injectFault(Simulation sim, List<String> ids, SplittableRandom faults) {
        String target = ids.get(faults.nextInt(ids.size()));
        switch (faults.nextInt(6)) {
            case 0 -> {
                if (sim.isUp(target)) {
                    sim.crash(target);
                }
            }
            case 1 -> {
                if (!sim.isUp(target)) {
                    sim.restart(target);
                }
            }
            case 2 -> {
                List<String> left = new ArrayList<>();
                List<String> right = new ArrayList<>();
                for (String id : ids) {
                    (faults.nextBoolean() ? left : right).add(id);
                }
                sim.partition(left, right);
            }
            case 3 -> sim.heal();
            case 4 -> sim.setNetwork(new NetworkConfig(1, 2 + faults.nextInt(20), faults.nextDouble() * 0.3, faults.nextDouble() * 0.5));
            default -> {
                // Crash whoever currently believes it is leader, if anyone.
                List<RaftNode> leaders = sim.leaders();
                if (!leaders.isEmpty()) {
                    sim.crash(leaders.get(faults.nextInt(leaders.size())).id().value());
                }
            }
        }
    }

    /** Terms never decrease and at most one candidate is ever voted for in a term. */
    private static void assertDurableHistoryIsSafe(String id, List<TermAndVote> history) {
        long previousTerm = 0;
        Map<Long, NodeId> voteByTerm = new HashMap<>();
        for (TermAndVote state : history) {
            assertTrue(state.currentTerm() >= previousTerm, id + " term went backwards: " + history);
            previousTerm = state.currentTerm();
            state.votedFor().ifPresent(vote -> {
                NodeId earlier = voteByTerm.putIfAbsent(state.currentTerm(), vote);
                assertTrue(earlier == null || earlier.equals(vote), id + " voted twice in term " + state.currentTerm());
            });
        }
    }
}
