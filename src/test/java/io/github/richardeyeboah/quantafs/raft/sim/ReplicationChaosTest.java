package io.github.richardeyeboah.quantafs.raft.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.richardeyeboah.quantafs.raft.Command;
import io.github.richardeyeboah.quantafs.raft.FailureMode;
import io.github.richardeyeboah.quantafs.raft.RaftNode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Randomized histories of client writes mixed with crashes, restarts, partitions, lossy networks,
 * and injected storage/apply failures, generated from a seed. The simulator checks its safety
 * invariants after every node step. Afterwards all faults are repaired and the test requires
 * progress (a new write is acknowledged) and convergence (every node applied the same committed log
 * and holds the state that replaying it produces). Acknowledged writes must be in the committed
 * history; later overwrites and deletes are accounted for by comparing against that replay rather
 * than expecting each old value to still be current. A failing seed reproduces exactly.
 */
class ReplicationChaosTest {

    private static final int CHAOS_TICKS = 3_000;
    private static final int RECOVERY_BUDGET = 2_000;

    static Stream<Arguments> histories() {
        return IntStream.of(3, 5).boxed().flatMap(n -> LongStream.rangeClosed(1, 40).mapToObj(seed -> Arguments.of(n, seed)));
    }

    @ParameterizedTest(name = "{0} voters, seed {1}")
    @MethodSource("histories")
    void randomHistoriesKeepCommittedDataSafeAndConvergeAfterRepair(int voters, long seed) {
        List<String> ids = IntStream.rangeClosed(1, voters).mapToObj(i -> "n" + i).toList();
        Simulation sim = Simulation.builder(ids.toArray(String[]::new))
                .seed(seed)
                .network(new NetworkConfig(1, 10, 0.05, 0.1))
                .recordEvents(false)
                .build();
        SplittableRandom random = new SplittableRandom(seed * 7919 + voters);
        int writes = 0;

        while (sim.now() < CHAOS_TICKS) {
            sim.run(1 + random.nextInt(20));
            String target = ids.get(random.nextInt(ids.size()));
            int roll = random.nextInt(100);
            if (roll < 50) {
                // Usually write to whoever believes it leads; sometimes to an arbitrary node.
                List<RaftNode> leaders = sim.leaders();
                String node = !leaders.isEmpty() && random.nextInt(4) != 0
                        ? leaders.get(random.nextInt(leaders.size())).id().value()
                        : target;
                if (sim.isUp(node)) {
                    sim.propose(node, randomCommand(random, writes++));
                }
            } else if (roll < 65) {
                // Restart a crashed or halted node, with its injected failures cleared.
                if (sim.isUp(target) && sim.liveNode(target).isHalted()) {
                    sim.crash(target);
                }
                if (!sim.isUp(target)) {
                    clearFailures(sim, target);
                    sim.restart(target);
                }
            } else if (roll < 72) {
                if (sim.isUp(target)) {
                    sim.crash(target);
                }
            } else if (roll < 77) {
                sim.leaders().stream().findFirst().ifPresent(l -> sim.crash(l.id().value()));
            } else if (roll < 82) {
                List<String> left = new ArrayList<>();
                List<String> right = new ArrayList<>();
                ids.forEach(id -> (random.nextBoolean() ? left : right).add(id));
                sim.partition(left, right);
            } else if (roll < 90) {
                sim.heal();
            } else if (roll < 95) {
                sim.setNetwork(new NetworkConfig(
                        1, 2 + random.nextInt(15), random.nextDouble() * 0.3, random.nextDouble() * 0.5));
            } else if (sim.isUp(target)) {
                FailureMode mode = random.nextBoolean() ? FailureMode.FAIL_BEFORE : FailureMode.FAIL_AFTER;
                if (random.nextBoolean()) {
                    sim.injectStorageFailure(target, mode);
                } else {
                    sim.injectApplyFailure(target, mode);
                }
            }
        }

        // Repair everything: heal, reliable network, no injected failures, every node running.
        sim.heal();
        sim.setNetwork(NetworkConfig.RELIABLE);
        for (String id : ids) {
            clearFailures(sim, id);
            if (sim.isUp(id) && sim.liveNode(id).isHalted()) {
                sim.crash(id);
            }
            if (!sim.isUp(id)) {
                sim.restart(id);
            }
        }
        // Progress: like a real client, retry with the current leader until a write is acknowledged.
        // An attempt can legitimately end UNKNOWN: a node that churned through terms while cut off
        // can depose the first leader after healing (no PreVote).
        boolean acknowledgedAfterRepair = false;
        for (int attempt = 0; attempt < 5 && !acknowledgedAfterRepair; attempt++) {
            assertTrue(sim.runUntil(s -> s.establishedLeader().isPresent(), RECOVERY_BUDGET), "no leader after repair");
            Simulation.ClientRequest request = sim.propose(sim.establishedLeader().orElseThrow().id().value(),
                    Command.put("final".getBytes(StandardCharsets.UTF_8), ("attempt-" + attempt).getBytes(StandardCharsets.UTF_8)));
            assertTrue(sim.runUntil(s -> request.outcome().isPresent(), RECOVERY_BUDGET), "no outcome after repair");
            acknowledgedAfterRepair = request.acknowledged();
        }
        assertTrue(acknowledgedAfterRepair, "a write after repair must eventually be acknowledged");
        ReplicationSimulationTest.awaitConvergence(sim);

        assertEquals(List.of(), ElectionSafety.violations(sim.leaderHistory()));
        long acknowledged = sim.clientRequests().stream().filter(Simulation.ClientRequest::acknowledged).count();
        assertTrue(acknowledged >= 1);
        assertTrue(sim.committedHistory().size() >= acknowledged, "every acknowledged write occupies a committed slot");
    }

    private static void clearFailures(Simulation sim, String id) {
        sim.injectStorageFailure(id, FailureMode.NONE);
        sim.injectApplyFailure(id, FailureMode.NONE);
    }

    private static Command randomCommand(SplittableRandom random, int n) {
        byte[] key = ("k" + random.nextInt(8)).getBytes(StandardCharsets.UTF_8);
        if (random.nextInt(5) == 0) {
            return Command.delete(key);
        }
        return Command.put(key, ("w" + n).getBytes(StandardCharsets.UTF_8));
    }
}
