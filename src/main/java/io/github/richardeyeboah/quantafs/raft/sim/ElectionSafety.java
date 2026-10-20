package io.github.richardeyeboah.quantafs.raft.sim;

import io.github.richardeyeboah.quantafs.raft.NodeId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Checks Raft's Election Safety property (paper, Figure 3): at most one leader can be elected in a
 * given term. Two different terms may each have a leader at the same moment; an isolated old leader
 * is expected to keep believing it leads its older term until it hears a higher term.
 */
public final class ElectionSafety {

    private ElectionSafety() {}

    /** Returns a description of every term in which two distinct nodes became leader. */
    public static List<String> violations(List<LeaderRecord> history) {
        Map<Long, LeaderRecord> firstLeaderByTerm = new HashMap<>();
        List<String> violations = new ArrayList<>();
        for (LeaderRecord record : history) {
            LeaderRecord first = firstLeaderByTerm.putIfAbsent(record.term(), record);
            if (first != null && !first.leader().equals(record.leader())) {
                violations.add(
                        "term " + record.term() + " has two leaders: " + first.leader() + " (tick "
                                + first.tick() + ") and " + record.leader() + " (tick " + record.tick() + ")");
            }
        }
        return violations;
    }

    /** Leader of each term, in term order. Assumes {@link #violations} is empty. */
    public static Map<Long, NodeId> leadersByTerm(List<LeaderRecord> history) {
        Map<Long, NodeId> leaders = new TreeMap<>();
        for (LeaderRecord record : history) {
            leaders.putIfAbsent(record.term(), record.leader());
        }
        return leaders;
    }
}
