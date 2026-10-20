package io.github.richardeyeboah.quantafs.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The Raft paper's Figure 8 (section 5.4.2): an entry from an earlier term can be stored on a
 * majority and still be overwritten, so a leader must not treat it as committed by counting
 * replicas. Five servers s1..s5, driven by hand.
 *
 * <p>Starting point (the paper's step c): s1 is leader of term 4. Index 2 holds an entry from term 2
 * that s1 created long ago, now present on s1, s2, and s3: a majority. s5 holds a different index-2
 * entry from term 3 that never spread. s4 has only index 1.
 */
class Figure8Test {

    private static final NodeId S1 = NodeId.of("s1");
    private static final NodeId S2 = NodeId.of("s2");
    private static final NodeId S3 = NodeId.of("s3");
    private static final NodeId S4 = NodeId.of("s4");
    private static final NodeId S5 = NodeId.of("s5");
    private static final ClusterConfig CLUSTER = ClusterConfig.of("s1", "s2", "s3", "s4", "s5");
    private static final ElectionTiming TIMING = new ElectionTiming(3, 10, 20);

    private static final LogEntry INDEX_1 = new LogEntry(1, 1, put("x", "1"));
    private static final LogEntry OLD_TERM_2 = new LogEntry(2, 2, put("x", "2"));
    private static final LogEntry TERM_3 = new LogEntry(2, 3, put("x", "3"));

    @Test
    void leaderDoesNotCommitAnOldTermEntryEvenWhenAMajorityStoresIt() {
        InMemoryRaftStorage storage = new InMemoryRaftStorage(TermAndVote.of(3, null), List.of(INDEX_1, OLD_TERM_2));
        InMemoryStateMachine machine = new InMemoryStateMachine();
        RaftNode s1 = electedLeader(storage, machine); // term 4, appends its NO_OP at index 3
        assertEquals(4, s1.currentTerm());

        s1.receive(new AppendEntriesResponse(S2, S1, 4, 4, true, 1, 2, 2));
        s1.receive(new AppendEntriesResponse(S3, S1, 4, 4, true, 1, 2, 2));
        assertEquals(0, s1.commitIndex(), "index 2 is on s1, s2, s3 (3 of 5) but is from term 2");
        assertEquals(List.of(), machine.appliedEntries());

        s1.receive(new AppendEntriesResponse(S2, S1, 4, 4, true, 2, 3, 3));
        assertEquals(0, s1.commitIndex(), "the term-4 NO_OP is on only 2 of 5");
        s1.receive(new AppendEntriesResponse(S3, S1, 4, 4, true, 2, 3, 3));
        assertEquals(3, s1.commitIndex(), "the NO_OP reached a majority; indexes 1 and 2 commit with it");
        assertEquals(3, machine.appliedEntries().size());
    }

    @Test
    void whyItMatters_theOldTermEntryOnAMajorityCanStillBeOverwritten() {
        // s1 crashes before its NO_OP spreads. s5 (last entry term 3) campaigns for term 5.
        RaftNode s2 = follower(S2, List.of(INDEX_1, OLD_TERM_2));
        RaftNode s3 = follower(S3, List.of(INDEX_1, OLD_TERM_2));
        RaftNode s4 = follower(S4, List.of(INDEX_1));
        LogPosition s5Log = TERM_3.position();
        for (RaftNode voter : List.of(s2, s3, s4)) {
            List<RaftMessage> reply = voter.receive(new RequestVoteRequest(S5, voter.id(), 5, s5Log));
            assertEquals(List.of(new RequestVoteResponse(voter.id(), S5, 5, true)), reply,
                    voter.id() + ": a last entry from term 3 is more up to date than one from term 2");
        }
        // With 4 of 5 votes s5 leads term 5 and replicates its own index 2, replacing the term-2 entry.
        s2.receive(new AppendEntriesRequest(S5, S2, 5, 1, 1, List.of(TERM_3), 0));
        assertEquals(TERM_3, storage(s2).entry(2));
        // Had s1 reported index 2 committed, a committed entry would now be gone. It did not: see above.
    }

    @Test
    void onceTheNoOpIsOnAMajorityTheStaleCandidateCannotWin() {
        RaftNode s2 = follower(S2, List.of(INDEX_1, OLD_TERM_2));
        LogEntry noOp = new LogEntry(3, 4, Command.noOp());
        s2.receive(new AppendEntriesRequest(S1, S2, 4, 2, 2, List.of(noOp), 0));
        List<RaftMessage> reply = s2.receive(new RequestVoteRequest(S5, S2, 5, TERM_3.position()));
        assertEquals(List.of(new RequestVoteResponse(S2, S5, 5, false)), reply,
                "s2's last entry is now from term 4; s5 (term 3) is behind and is refused");
    }

    private final Map<NodeId, InMemoryRaftStorage> storages = new HashMap<>();

    private RaftNode follower(NodeId id, List<LogEntry> log) {
        InMemoryRaftStorage storage = new InMemoryRaftStorage(TermAndVote.of(4, S1), log);
        storages.put(id, storage);
        return new RaftNode(id, CLUSTER, TIMING, (origin, bound) -> origin, storage, new InMemoryStateMachine());
    }

    private InMemoryRaftStorage storage(RaftNode node) {
        return storages.get(node.id());
    }

    private static RaftNode electedLeader(InMemoryRaftStorage storage, InMemoryStateMachine machine) {
        RaftNode s1 = new RaftNode(S1, CLUSTER, TIMING, (origin, bound) -> origin, storage, machine);
        for (int i = 0; i < 10; i++) {
            s1.tick();
        }
        s1.receive(new RequestVoteResponse(S2, S1, 4, true));
        s1.receive(new RequestVoteResponse(S3, S1, 4, true));
        assertEquals(Role.LEADER, s1.role());
        return s1;
    }

    private static Command put(String key, String value) {
        return Command.put(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }
}
