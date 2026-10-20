package io.github.richardeyeboah.quantafs.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.richardeyeboah.quantafs.raft.ProposalOutcome.Status;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Rule-level tests for log replication, commitment, application, and the proposal API, driven by
 * hand-built messages to one node ({@code n1}). Rule numbers refer to the Raft paper's Figure 2.
 */
class RaftReplicationTest {

    private static final NodeId N1 = NodeId.of("n1");
    private static final NodeId N2 = NodeId.of("n2");
    private static final NodeId N3 = NodeId.of("n3");
    private static final ClusterConfig THREE = ClusterConfig.of("n1", "n2", "n3");
    private static final ClusterConfig FIVE = ClusterConfig.of("n1", "n2", "n3", "n4", "n5");
    private static final ElectionTiming TIMING = new ElectionTiming(3, 10, 20);
    private static final RandomSource MIN_TIMEOUT = (origin, bound) -> origin;

    private InMemoryRaftStorage storage = new InMemoryRaftStorage();
    private final InMemoryStateMachine machine = new InMemoryStateMachine();

    // ================================================================ follower: AppendEntries receiver

    @Test
    void followerPersistsTermAndEntriesInOneWriteBeforeAcknowledging() {
        RaftNode node = node(THREE);
        List<RaftMessage> out = node.receive(append(N2, 1, 0, 0, List.of(e(1, 1, "a"), e(2, 1, "b")), 0));

        assertEquals(List.of(new AppendEntriesResponse(N1, N2, 1, 1, true, 0, 2, 2)), out);
        assertEquals(List.of(e(1, 1, "a"), e(2, 1, "b")), storage.entries(1, 2));
        assertEquals(TermAndVote.of(1, null), storage.termAndVote());
        assertEquals(1, storage.writeCount(), "the new term and the entries were one atomic write");
    }

    @Test
    void followerThatCannotPersistNeverAcknowledges() {
        RaftNode node = node(THREE);
        storage.setFailureMode(FailureMode.FAIL_BEFORE);
        assertEquals(List.of(), node.receive(append(N2, 1, 0, 0, List.of(e(1, 1, "a")), 0)));
        assertTrue(node.isHalted());
        assertEquals(0, storage.lastIndex());
    }

    @Test
    void followerCrashingRightAfterTheWriteStillSendsNoAcknowledgement() {
        RaftNode node = node(THREE);
        storage.setFailureMode(FailureMode.FAIL_AFTER);
        assertEquals(List.of(), node.receive(append(N2, 1, 0, 0, List.of(e(1, 1, "a")), 0)));
        assertTrue(node.isHalted());
        assertEquals(1, storage.lastIndex(), "the entry reached storage, but the leader was never told");
    }

    @Test
    void missingPrefixIsRejectedWithAHintAndChangesNoEntries() {
        RaftNode node = node(THREE);
        node.receive(append(N2, 1, 0, 0, List.of(e(1, 1, "a")), 0));
        // Rule 2: no entry at prevLogIndex 3.
        List<RaftMessage> out = node.receive(append(N2, 1, 3, 1, List.of(e(4, 1, "d")), 0));
        assertEquals(List.of(new AppendEntriesResponse(N1, N2, 1, 1, false, 3, 0, 1)), out);
        assertEquals(1, storage.lastIndex());
    }

    @Test
    void heartbeatObeysTheSameConsistencyCheck() {
        storage = new InMemoryRaftStorage(TermAndVote.of(2, null), List.of(e(1, 1, "a"), e(2, 1, "b"), e(3, 2, "c")));
        RaftNode node = node(THREE);
        // An empty AppendEntries whose prevLogTerm (3) differs from the local term at index 3 (2).
        List<RaftMessage> out = node.receive(append(N2, 3, 3, 3, List.of(), 0));
        assertEquals(List.of(new AppendEntriesResponse(N1, N2, 3, 3, false, 3, 0, 3)), out);
        assertEquals(3, storage.lastIndex(), "a rejected heartbeat removes nothing");
        assertEquals(3, node.currentTerm(), "but the higher term is still adopted");
    }

    @Test
    void conflictingUncommittedSuffixIsReplaced() {
        storage = new InMemoryRaftStorage(TermAndVote.of(2, null), List.of(e(1, 1, "a"), e(2, 2, "b"), e(3, 2, "c")));
        RaftNode node = node(THREE);
        // Rules 3-4: entry 2 conflicts (term 2 vs 3), so 2 and everything after it are replaced.
        List<RaftMessage> out = node.receive(append(N3, 3, 1, 1, List.of(e(2, 3, "x")), 0));
        assertEquals(List.of(new AppendEntriesResponse(N1, N3, 3, 3, true, 1, 2, 2)), out);
        assertEquals(List.of(e(1, 1, "a"), e(2, 3, "x")), storage.entries(1, storage.lastIndex()));
    }

    @Test
    void identicalDuplicateAppendIsHarmless() {
        RaftNode node = node(THREE);
        AppendEntriesRequest request = append(N2, 1, 0, 0, List.of(e(1, 1, "a"), e(2, 1, "b")), 0);
        List<RaftMessage> first = node.receive(request);
        long writes = storage.writeCount();
        assertEquals(first, node.receive(request));
        assertEquals(writes, storage.writeCount(), "nothing new to persist");
    }

    @Test
    void olderShorterAppendArrivingLateNeverTruncates() {
        RaftNode node = node(THREE);
        node.receive(append(N2, 1, 0, 0, List.of(e(1, 1, "a"), e(2, 1, "b"), e(3, 1, "c")), 0));
        // A reordered, older request that covers only entry 1: it matches, so nothing is removed.
        List<RaftMessage> out = node.receive(append(N2, 1, 0, 0, List.of(e(1, 1, "a")), 0));
        assertEquals(List.of(new AppendEntriesResponse(N1, N2, 1, 1, true, 0, 1, 3)), out);
        assertEquals(3, storage.lastIndex());
    }

    @Test
    void committedEntriesAreNeverTruncated() {
        RaftNode node = node(THREE);
        node.receive(append(N2, 1, 0, 0, List.of(e(1, 1, "a"), e(2, 1, "b")), 2));
        assertEquals(2, node.commitIndex());
        // A leader that conflicts with a committed entry indicates a bug; fail stop instead of obeying.
        assertThrows(IllegalStateException.class,
                () -> node.receive(append(N3, 2, 1, 1, List.of(e(2, 2, "z")), 0)));
        assertEquals(e(2, 1, "b"), storage.entry(2));
    }

    @Test
    void followerCommitIsLimitedToThePrefixTheLeaderVerified() {
        RaftNode node = node(THREE);
        node.receive(append(N2, 1, 0, 0, List.of(e(1, 1, "a"), e(2, 1, "b"), e(3, 1, "c")), 0));
        // A new leader's heartbeat verifies only index 1; entries 2-3 might yet be replaced.
        node.receive(append(N3, 2, 1, 1, List.of(), 3));
        assertEquals(1, node.commitIndex(), "rule 5: min(leaderCommit, index of last new entry)");
        assertEquals(List.of(e(1, 1, "a")), machine.appliedEntries());
    }

    @Test
    void followerAppliesOnlyCommittedEntriesInIndexOrder() {
        RaftNode node = node(THREE);
        node.receive(append(N2, 1, 0, 0, List.of(e(1, 1, "a"), e(2, 1, "b"), e(3, 1, "c")), 2));
        assertEquals(List.of(e(1, 1, "a"), e(2, 1, "b")), machine.appliedEntries());
        node.receive(append(N2, 1, 3, 1, List.of(), 3));
        assertEquals(List.of(e(1, 1, "a"), e(2, 1, "b"), e(3, 1, "c")), machine.appliedEntries());
        assertEquals(3, machine.diagnosticContents().size());
    }

    // ================================================================ leader: proposals and commitment

    @Test
    void successIsReportedOnlyAfterMajorityCommitAndLocalApplication() {
        RaftNode leader = leader(THREE);
        ProposeResult.Accepted accepted = assertInstanceOf(ProposeResult.Accepted.class, leader.propose(put("a")));
        assertEquals(2, accepted.index());
        LogEntry noOp = new LogEntry(1, 1, Command.noOp());
        LogEntry entry = new LogEntry(2, 1, put("a"));
        assertEquals(List.of(new AppendEntriesRequest(N1, N2, 1, 0, 0, List.of(noOp, entry), 0),
                new AppendEntriesRequest(N1, N3, 1, 0, 0, List.of(noOp, entry), 0)), accepted.messages());
        assertEquals(List.of(), leader.drainOutcomes(), "durably appended on the leader alone is not success");

        leader.receive(new AppendEntriesResponse(N2, N1, 1, 1, true, 0, 2, 2)); // leader + n2 = 2 of 3
        assertEquals(2, leader.commitIndex());
        assertEquals(2, leader.lastApplied());
        assertEquals(List.of(new ProposalOutcome(2, 1, Status.COMMITTED)), leader.drainOutcomes());
        assertEquals(Map.of(Bytes.utf8("a"), Bytes.utf8("v")), machine.diagnosticContents());
    }

    @Test
    void minorityReplicationNeverReportsSuccessAndLosingLeadershipReportsUnknown() {
        RaftNode leader = leader(FIVE);
        leader.propose(put("a"));
        leader.receive(new AppendEntriesResponse(N2, N1, 1, 1, true, 0, 2, 2)); // 2 of 5
        assertEquals(0, leader.commitIndex());
        assertEquals(List.of(), leader.drainOutcomes());

        leader.receive(new AppendEntriesResponse(N3, N1, 2, 1, false, 0, 0, 0)); // a higher term
        assertEquals(Role.FOLLOWER, leader.role());
        assertEquals(List.of(new ProposalOutcome(2, 1, Status.UNKNOWN)), leader.drainOutcomes(),
                "not reported as failed: a later leader may still commit it");
        assertEquals(List.of(), machine.appliedEntries());
    }

    @Test
    void proposalsToNonLeadersAreRejectedWithoutTouchingTheLog() {
        RaftNode node = node(THREE);
        node.receive(append(N2, 1, 0, 0, List.of(), 0));
        assertEquals(new ProposeResult.NotLeader(Optional.of(N2)), node.propose(put("a")));
        for (int i = 0; i < 10; i++) {
            node.tick(); // no more heartbeats: n1 becomes a candidate and forgets the leader
        }
        assertEquals(Role.CANDIDATE, node.role());
        assertEquals(new ProposeResult.NotLeader(Optional.empty()), node.propose(put("a")));
        assertEquals(0, storage.lastIndex());
    }

    @Test
    void staleOrDuplicatedResponsesCannotMoveProgressBackwards() {
        RaftNode leader = leader(THREE);
        leader.propose(put("a"));
        leader.propose(put("b")); // log: 1 NO_OP, 2 a, 3 b
        leader.receive(new AppendEntriesResponse(N2, N1, 1, 1, true, 0, 3, 3));
        leader.receive(new AppendEntriesResponse(N2, N1, 1, 1, true, 0, 1, 3)); // delayed older success
        leader.receive(new AppendEntriesResponse(N2, N1, 1, 1, false, 2, 0, 1)); // failure for an older probe
        assertEquals(Optional.of(3L), leader.matchIndex(N2));
        assertEquals(Optional.of(4L), leader.nextIndex(N2));
        assertEquals(3, leader.commitIndex());
    }

    @Test
    void rejectionMovesNextIndexBackOnceAndRetriesWhileDuplicatesAreIgnored() {
        storage = new InMemoryRaftStorage(TermAndVote.of(2, null), List.of(e(1, 1, "a"), e(2, 1, "b"), e(3, 2, "c")));
        RaftNode leader = leader(THREE); // term 3; NO_OP at 4; nextIndex 4 for everyone
        AppendEntriesResponse rejection = new AppendEntriesResponse(N2, N1, 3, 3, false, 3, 0, 1);

        List<RaftMessage> retry = leader.receive(rejection); // follower has only 1 entry
        LogEntry noOp = new LogEntry(4, 3, Command.noOp());
        assertEquals(List.of(new AppendEntriesRequest(N1, N2, 3, 1, 1, List.of(e(2, 1, "b"), e(3, 2, "c"), noOp), 0)),
                retry, "missing-prefix catch-up resends from index 2");
        assertEquals(List.of(), leader.receive(rejection), "the duplicate answers a probe no longer outstanding");
        assertEquals(Optional.of(2L), leader.nextIndex(N2));
    }

    @Test
    void responsesToRequestsFromAnEarlierTermAreIgnored() {
        storage = new InMemoryRaftStorage(TermAndVote.of(2, null), List.of(e(1, 1, "a")));
        RaftNode leader = leader(THREE); // term 3
        leader.receive(new AppendEntriesResponse(N2, N1, 3, 2, false, 1, 0, 0));
        leader.receive(new AppendEntriesResponse(N2, N1, 2, 2, true, 0, 1, 1));
        assertEquals(Optional.of(2L), leader.nextIndex(N2));
        assertEquals(Optional.of(0L), leader.matchIndex(N2));
    }

    @Test
    void inheritedEntriesCommitOnlyThroughACurrentTermEntry() {
        storage = new InMemoryRaftStorage(TermAndVote.of(2, null), List.of(e(1, 1, "a"), e(2, 1, "b"), e(3, 2, "c")));
        RaftNode leader = leader(THREE); // term 3, NO_OP at index 4

        leader.receive(new AppendEntriesResponse(N2, N1, 3, 3, true, 3, 3, 3));
        assertEquals(0, leader.commitIndex(), "index 3 is on a majority but from term 2: not committed by counting");

        leader.receive(new AppendEntriesResponse(N2, N1, 3, 3, true, 3, 4, 4));
        assertEquals(4, leader.commitIndex(), "the term-3 NO_OP commits, and with it every earlier entry");
        assertEquals(4, machine.appliedEntries().size());
    }

    @Test
    void singleVoterCommitsAndAppliesImmediately() {
        RaftNode leader = leader(ClusterConfig.of("n1"));
        ProposeResult.Accepted accepted = assertInstanceOf(ProposeResult.Accepted.class, leader.propose(put("a")));
        assertEquals(List.of(new ProposalOutcome(accepted.index(), 1, Status.COMMITTED)), leader.drainOutcomes());
    }

    // ================================================================ failures at durability boundaries

    @Test
    void proposalWhoseAppendFailsHasAnUnknownOutcomeAndTheLeaderGoesSilent() {
        RaftNode leader = leader(THREE);
        storage.setFailureMode(FailureMode.FAIL_AFTER);
        assertInstanceOf(ProposeResult.OutcomeUnknown.class, leader.propose(put("a")));
        assertTrue(leader.isHalted());
        assertEquals(2, storage.lastIndex(), "the entry may well be in the log: unknown, not failed");
        assertEquals(List.of(), leader.drainOutcomes(), "reported once, by propose itself");
        for (int i = 0; i < 10; i++) {
            assertEquals(List.of(), leader.tick());
        }
        assertInstanceOf(ProposeResult.Unavailable.class, leader.propose(put("b")));
    }

    @Test
    void applyFailingBeforeTheWriteIsAppliedAfterRestartWithoutSkipping() {
        RaftNode node = node(THREE);
        node.receive(append(N2, 1, 0, 0, List.of(e(1, 1, "a"), e(2, 1, "b")), 0));
        machine.setFailureMode(FailureMode.FAIL_BEFORE);
        assertEquals(List.of(), node.receive(append(N2, 1, 2, 1, List.of(), 2)), "no acknowledgement after a failed apply");
        assertTrue(node.isHalted());
        assertEquals(LogPosition.EMPTY, machine.lastApplied());

        machine.setFailureMode(FailureMode.NONE);
        RaftNode restarted = node(THREE);
        assertEquals(0, restarted.commitIndex(), "commitment is relearned, not inferred from the log");
        restarted.receive(append(N2, 1, 2, 1, List.of(), 2));
        assertEquals(List.of(e(1, 1, "a"), e(2, 1, "b")), machine.appliedEntries());
    }

    @Test
    void applyFailingAfterTheWriteIsNotReappliedAfterRestart() {
        RaftNode node = node(THREE);
        node.receive(append(N2, 1, 0, 0, List.of(e(1, 1, "a"), e(2, 1, "b")), 0));
        machine.setFailureMode(FailureMode.FAIL_AFTER);
        node.receive(append(N2, 1, 2, 1, List.of(), 2));
        assertTrue(node.isHalted());
        assertEquals(List.of(e(1, 1, "a")), machine.appliedEntries(), "entry 1 and its marker are durable");

        machine.setFailureMode(FailureMode.NONE);
        RaftNode restarted = node(THREE);
        assertEquals(1, restarted.lastApplied());
        restarted.receive(append(N2, 1, 2, 1, List.of(), 2));
        assertEquals(List.of(e(1, 1, "a"), e(2, 1, "b")), machine.appliedEntries(), "entry 1 was not applied twice");
    }

    @Test
    void restartStartsCommitIndexAtTheAppliedMarkerNotTheLogLength() {
        storage = new InMemoryRaftStorage(TermAndVote.of(1, null), List.of(e(1, 1, "a"), e(2, 1, "b"), e(3, 1, "c")));
        machine.apply(e(1, 1, "a"));
        RaftNode node = node(THREE);
        assertEquals(1, node.commitIndex());
        assertEquals(1, node.lastApplied());
    }

    @Test
    void refusesToStartWhenAppliedStateIsAheadOfOrDifferentFromTheLog() {
        machine.apply(e(1, 1, "a"));
        machine.apply(e(2, 1, "b"));
        storage = new InMemoryRaftStorage(TermAndVote.of(1, null), List.of(e(1, 1, "a")));
        assertThrows(PersistenceException.class, () -> node(THREE), "applied entry 2 is missing from the log");

        storage = new InMemoryRaftStorage(TermAndVote.of(2, null), List.of(e(1, 1, "a"), e(2, 2, "other")));
        assertThrows(PersistenceException.class, () -> node(THREE), "applied (term 1, index 2) differs from the log");
    }

    // ================================================================ helpers

    private RaftNode node(ClusterConfig cluster) {
        return new RaftNode(N1, cluster, TIMING, MIN_TIMEOUT, storage, machine);
    }

    /** n1 elected leader with the votes it needs; returns it right after election. */
    private RaftNode leader(ClusterConfig cluster) {
        RaftNode node = node(cluster);
        for (int i = 0; i < 10; i++) {
            node.tick();
        }
        for (NodeId peer : cluster.peersOf(N1)) {
            if (node.role() != Role.LEADER) {
                node.receive(new RequestVoteResponse(peer, N1, node.currentTerm(), true));
            }
        }
        assertEquals(Role.LEADER, node.role());
        return node;
    }

    private static AppendEntriesRequest append(
            NodeId leader, long term, long prevIndex, long prevTerm, List<LogEntry> entries, long leaderCommit) {
        return new AppendEntriesRequest(leader, N1, term, prevIndex, prevTerm, entries, leaderCommit);
    }

    private static LogEntry e(long index, long term, String key) {
        return new LogEntry(index, term, put(key));
    }

    private static Command put(String key) {
        return Command.put(key.getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8));
    }
}
