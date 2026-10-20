package io.github.richardeyeboah.quantafs.raft.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.richardeyeboah.quantafs.raft.InMemoryElectionStateStore;
import io.github.richardeyeboah.quantafs.raft.NodeId;
import io.github.richardeyeboah.quantafs.raft.TermAndVote;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The safety oracles must themselves catch violations, or passing tests would prove nothing. */
class ElectionSafetyTest {

    private static final NodeId A = NodeId.of("a");
    private static final NodeId B = NodeId.of("b");

    @Test
    void detectsTwoLeadersInOneTerm() {
        List<String> violations = ElectionSafety.violations(List.of(
                new LeaderRecord(1, A, 10), new LeaderRecord(2, B, 30), new LeaderRecord(2, A, 31)));
        assertEquals(1, violations.size());
        assertTrue(violations.getFirst().contains("term 2"), violations.getFirst());
    }

    @Test
    void allowsOneLeaderPerTermEvenWhenTermsOverlapInTime() {
        List<LeaderRecord> history = List.of(new LeaderRecord(1, A, 10), new LeaderRecord(2, B, 12));
        assertEquals(List.of(), ElectionSafety.violations(history));
        assertEquals(Map.of(1L, A, 2L, B), ElectionSafety.leadersByTerm(history));
    }

    @Test
    void simulatorRejectsAStoreThatForgetsItsVote() {
        // Tamper with durable state behind the node's back: the invariant check must notice that
        // memory and storage disagree.
        Simulation sim = Simulation.builder("n1", "n2", "n3").seed(1).build();
        InMemoryElectionStateStore store = sim.store("n1");
        store.save(TermAndVote.of(50, null));
        assertThrows(IllegalStateException.class, () -> sim.run(5));
    }
}
