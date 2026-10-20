package io.github.richardeyeboah.quantafs.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Rule-level tests for one {@link RaftNode}, driven by explicit ticks and hand-built messages.
 * Section and rule names refer to the Raft paper, Figure 2, unless noted.
 */
class RaftNodeTest {

    private static final NodeId N1 = NodeId.of("n1");
    private static final NodeId N2 = NodeId.of("n2");
    private static final NodeId N3 = NodeId.of("n3");
    private static final NodeId N9 = NodeId.of("n9");
    private static final ClusterConfig THREE = ClusterConfig.of("n1", "n2", "n3");
    private static final ClusterConfig FIVE = ClusterConfig.of("n1", "n2", "n3", "n4", "n5");
    private static final ElectionTiming TIMING = new ElectionTiming(3, 10, 20);
    /** Always picks the minimum, so every election timeout is exactly 10 ticks. */
    private static final RandomSource MIN_TIMEOUT = (origin, bound) -> origin;

    private InMemoryElectionStateStore store = new InMemoryElectionStateStore();

    // ---------------------------------------------------------------- startup and timeouts

    @Test
    void startsAsFollowerWithPersistedTermAndVote() {
        store = new InMemoryElectionStateStore(TermAndVote.of(4, N2));
        RaftNode node = node(THREE);
        assertEquals(Role.FOLLOWER, node.role());
        assertEquals(4, node.currentTerm());
        assertEquals(Optional.of(N2), node.votedFor());
        assertEquals(Optional.empty(), node.leaderId());
    }

    @Test
    void electionTimeoutPersistsNewTermAndSelfVoteThenRequestsVotes() {
        RaftNode node = node(THREE);
        for (int i = 1; i < 10; i++) {
            assertEquals(List.of(), node.tick(), "tick " + i);
        }
        List<RaftMessage> out = node.tick();

        // Candidates: increment currentTerm, vote for self, send RequestVote to all other servers.
        assertEquals(Role.CANDIDATE, node.role());
        assertEquals(1, node.currentTerm());
        assertEquals(Optional.of(N1), node.votedFor());
        assertEquals(List.of(TermAndVote.of(1, N1)), store.history(), "persisted before sending");
        assertEquals(
                List.of(
                        new RequestVoteRequest(N1, N2, 1, LogPosition.EMPTY),
                        new RequestVoteRequest(N1, N3, 1, LogPosition.EMPTY)),
                out);
    }

    @Test
    void electionTimeoutIsDrawnFromInjectedRandomnessOverTheConfiguredRange() {
        List<int[]> calls = new ArrayList<>();
        RandomSource recording =
                (origin, bound) -> {
                    calls.add(new int[] {origin, bound});
                    return 17;
                };
        RaftNode node = new RaftNode(N1, THREE, TIMING, recording, store, LogMetadata.empty());
        assertEquals(17, node.electionTimeoutTicks());
        assertEquals(10, calls.get(0)[0]);
        assertEquals(20, calls.get(0)[1]);
    }

    @Test
    void candidateWhoseElectionTimesOutRetriesInANewTerm() {
        RaftNode node = node(THREE);
        electAsCandidate(node);
        assertEquals(1, node.currentTerm());
        // No votes arrive (a split vote looks the same to the candidate); the timer fires again.
        List<RaftMessage> retry = tickUntilOutput(node);
        assertEquals(Role.CANDIDATE, node.role());
        assertEquals(2, node.currentTerm());
        assertEquals(Set.of(N1), node.votesGranted(), "tally restarts with only the self vote");
        assertEquals(new RequestVoteRequest(N1, N2, 2, LogPosition.EMPTY), retry.get(0));
    }

    // ---------------------------------------------------------------- vote counting

    @Test
    void majorityIncludesTheCandidatesOwnVote() {
        RaftNode node = node(THREE);
        electAsCandidate(node);
        List<RaftMessage> out = node.receive(new RequestVoteResponse(N2, N1, 1, true));

        // Self + n2 = 2 of 3 voters: a majority. Leaders: send heartbeats immediately on election.
        assertEquals(Role.LEADER, node.role());
        assertEquals(Optional.of(N1), node.leaderId());
        assertEquals(
                List.of(new AppendEntriesRequest(N1, N2, 1), new AppendEntriesRequest(N1, N3, 1)), out);
    }

    @Test
    void duplicateGrantsFromOneVoterAreCountedOnce() {
        RaftNode node = node(FIVE);
        electAsCandidate(node);
        node.receive(new RequestVoteResponse(N2, N1, 1, true));
        node.receive(new RequestVoteResponse(N2, N1, 1, true));
        assertEquals(Role.CANDIDATE, node.role(), "self + n2 is 2 of 5, quorum is 3");
        assertEquals(Set.of(N1, N2), node.votesGranted());

        node.receive(new RequestVoteResponse(N3, N1, 1, true));
        assertEquals(Role.LEADER, node.role());
    }

    @Test
    void staleAndRejectingResponsesDoNotCount() {
        store = new InMemoryElectionStateStore(TermAndVote.of(1, null));
        RaftNode node = node(THREE);
        electAsCandidate(node);
        assertEquals(2, node.currentTerm());

        node.receive(new RequestVoteResponse(N2, N1, 1, true)); // granted, but for an old term
        node.receive(new RequestVoteResponse(N3, N1, 2, false));
        assertEquals(Role.CANDIDATE, node.role());
        assertEquals(Set.of(N1), node.votesGranted());
        assertEquals(Set.of(N3), node.votesRejected());
    }

    // ---------------------------------------------------------------- RequestVote receiver

    @Test
    void votesAtMostOncePerTermAndPersistsBeforeReplying() {
        RaftNode node = node(THREE);

        assertEquals(List.of(new RequestVoteResponse(N1, N2, 1, true)), node.receive(vote(N2, 1)));
        assertEquals(List.of(TermAndVote.of(1, N2)), store.history());

        // Receiver rule 2: votedFor is n2, so n3 is refused in the same term.
        assertEquals(List.of(new RequestVoteResponse(N1, N3, 1, false)), node.receive(vote(N3, 1)));
        // A duplicate request from the candidate already voted for is granted again, idempotently.
        assertEquals(List.of(new RequestVoteResponse(N1, N2, 1, true)), node.receive(vote(N2, 1)));
        assertEquals(1, store.history().size(), "no second write for the same vote");
    }

    @Test
    void rejectsRequestVoteFromLowerTermWithoutChangingState() {
        store = new InMemoryElectionStateStore(TermAndVote.of(5, null));
        RaftNode node = node(THREE);
        // Receiver rule 1: reply false if term < currentTerm, and tell the sender our term.
        assertEquals(List.of(new RequestVoteResponse(N1, N2, 5, false)), node.receive(vote(N2, 4)));
        assertEquals(Optional.empty(), node.votedFor());
        assertEquals(List.of(), store.history());
    }

    @Test
    void higherTermRequestVoteMakesLeaderStepDownAndVoteInOneAtomicSave() {
        RaftNode node = leaderOfTermOne(THREE);
        List<RaftMessage> out = node.receive(vote(N3, 2));

        // All servers: term T > currentTerm => currentTerm = T, convert to follower.
        assertEquals(Role.FOLLOWER, node.role());
        assertEquals(2, node.currentTerm());
        assertEquals(Optional.of(N3), node.votedFor());
        assertEquals(TermAndVote.of(2, N3), store.history().getLast(), "term and vote saved together");
        assertEquals(List.of(new RequestVoteResponse(N1, N3, 2, true)), out);
    }

    @ParameterizedTest(name = "voter last log (3,5), candidate last log ({0},{1}) -> granted={2}")
    @CsvSource({
        "2, 9, false", // lower last term loses even with a longer log
        "3, 4, false", // same last term, shorter log
        "3, 5, true", //  identical
        "3, 6, true", //  same last term, longer log
        "4, 1, true" //   higher last term wins even with a shorter log
    })
    void grantsOnlyToCandidatesWhoseLogIsAtLeastAsUpToDate(long candidateTerm, long candidateIndex, boolean granted) {
        store = new InMemoryElectionStateStore(TermAndVote.of(3, null));
        RaftNode node = node(THREE, LogMetadata.fixed(new LogPosition(3, 5)));
        LogPosition candidateLog = new LogPosition(candidateTerm, candidateIndex);

        // Section 5.4.1, election restriction.
        List<RaftMessage> out = node.receive(new RequestVoteRequest(N2, N1, 4, candidateLog));
        assertEquals(List.of(new RequestVoteResponse(N1, N2, 4, granted)), out);
        assertEquals(4, node.currentTerm(), "the higher term is adopted either way");
        assertEquals(granted ? Optional.of(N2) : Optional.empty(), node.votedFor());
    }

    @Test
    void refusingAStaleLogDoesNotUseUpTheVote() {
        store = new InMemoryElectionStateStore(TermAndVote.of(3, null));
        RaftNode node = node(THREE, LogMetadata.fixed(new LogPosition(3, 5)));
        node.receive(new RequestVoteRequest(N2, N1, 4, new LogPosition(2, 9)));
        List<RaftMessage> out = node.receive(new RequestVoteRequest(N3, N1, 4, new LogPosition(3, 5)));
        assertEquals(List.of(new RequestVoteResponse(N1, N3, 4, true)), out);
    }

    // ---------------------------------------------------------------- timers and heartbeats

    @Test
    void heartbeatsFromTheCurrentLeaderPreventElections() {
        RaftNode node = node(THREE);
        for (int round = 0; round < 50; round++) {
            for (int i = 0; i < 9; i++) {
                assertEquals(List.of(), node.tick());
            }
            List<RaftMessage> out = node.receive(new AppendEntriesRequest(N2, N1, 1));
            assertEquals(List.of(new AppendEntriesResponse(N1, N2, 1, true)), out);
        }
        assertEquals(Role.FOLLOWER, node.role());
        assertEquals(Optional.of(N2), node.leaderId());
        // Followers: once heartbeats stop, the election timeout fires.
        assertFalse(tickUntilOutput(node).isEmpty());
        assertEquals(Role.CANDIDATE, node.role());
    }

    @Test
    void staleHeartbeatIsRejectedAndDoesNotResetTheTimer() {
        store = new InMemoryElectionStateStore(TermAndVote.of(3, null));
        RaftNode node = node(THREE);
        tick(node, 9);
        // AppendEntries receiver rule 1: reply false if term < currentTerm.
        assertEquals(List.of(new AppendEntriesResponse(N1, N2, 3, false)), node.receive(new AppendEntriesRequest(N2, N1, 2)));
        assertFalse(node.tick().isEmpty(), "timer was not reset, so the 10th tick starts an election");
    }

    @Test
    void grantingAVoteResetsTheTimerButRefusingDoesNot() {
        RaftNode node = node(THREE);
        tick(node, 9);
        node.receive(vote(N2, 1)); // granted: timer restarts
        tick(node, 9);
        assertEquals(Role.FOLLOWER, node.role());

        node.receive(vote(N3, 1)); // refused: timer keeps running
        assertFalse(node.tick().isEmpty());
        assertEquals(Role.CANDIDATE, node.role());
    }

    @Test
    void candidateBecomesFollowerOnHeartbeatFromSameTermLeader() {
        RaftNode node = node(THREE);
        electAsCandidate(node);
        List<RaftMessage> out = node.receive(new AppendEntriesRequest(N2, N1, 1));
        assertEquals(Role.FOLLOWER, node.role());
        assertEquals(1, node.currentTerm());
        assertEquals(Optional.of(N1), node.votedFor(), "the self vote for term 1 stays recorded");
        assertEquals(Optional.of(N2), node.leaderId());
        assertEquals(List.of(new AppendEntriesResponse(N1, N2, 1, true)), out);
    }

    @Test
    void leaderSendsHeartbeatsEveryIntervalAndNeverTimesOut() {
        RaftNode node = leaderOfTermOne(THREE);
        for (int round = 0; round < 20; round++) {
            assertEquals(List.of(), node.tick());
            assertEquals(List.of(), node.tick());
            assertEquals(
                    List.of(new AppendEntriesRequest(N1, N2, 1), new AppendEntriesRequest(N1, N3, 1)), node.tick());
        }
        assertEquals(Role.LEADER, node.role());
        assertEquals(1, node.currentTerm());
    }

    @Test
    void leaderStepsDownWhenAResponseRevealsAHigherTerm() {
        RaftNode node = leaderOfTermOne(THREE);
        assertEquals(List.of(), node.receive(new AppendEntriesResponse(N2, N1, 3, false)));
        assertEquals(Role.FOLLOWER, node.role());
        assertEquals(3, node.currentTerm());
        assertEquals(Optional.empty(), node.votedFor());
        assertEquals(new TermAndVote(3, Optional.empty()), store.load());
    }

    @Test
    void candidateStepsDownWhenAVoteResponseRevealsAHigherTerm() {
        RaftNode node = node(THREE);
        electAsCandidate(node);
        node.receive(new RequestVoteResponse(N2, N1, 4, false));
        assertEquals(Role.FOLLOWER, node.role());
        assertEquals(4, node.currentTerm());
    }

    @Test
    void sameTermHeartbeatFromAnotherNodeWhileLeaderIsReportedAsSafetyViolation() {
        RaftNode node = leaderOfTermOne(THREE);
        assertThrows(IllegalStateException.class, () -> node.receive(new AppendEntriesRequest(N2, N1, 1)));
    }

    // ---------------------------------------------------------------- membership validation

    @Test
    void messagesFromNonMembersOrForOtherNodesCannotAffectState() {
        RaftNode node = node(THREE);
        assertEquals(List.of(), node.receive(new RequestVoteRequest(N9, N1, 99, LogPosition.EMPTY)));
        assertEquals(List.of(), node.receive(new AppendEntriesRequest(N9, N1, 99)));
        assertEquals(List.of(), node.receive(new AppendEntriesRequest(N2, N3, 99)));
        assertEquals(0, node.currentTerm());
        assertEquals(List.of(), store.history());
        assertEquals(3, node.ignoredMessageCount());
    }

    @Test
    void rejectsConfigurationsThatCannotBeCorrect() {
        assertThrows(IllegalArgumentException.class, () -> new RaftNode(N9, THREE, TIMING, MIN_TIMEOUT, store, LogMetadata.empty()));
        store = new InMemoryElectionStateStore(TermAndVote.of(1, null));
        assertThrows(IllegalStateException.class, () -> node(THREE, LogMetadata.fixed(new LogPosition(2, 5))));
    }

    @Test
    void singleVoterClusterElectsItself() {
        RaftNode node = node(ClusterConfig.of("n1"));
        tick(node, 10);
        assertEquals(Role.LEADER, node.role());
        assertEquals(1, node.currentTerm());
    }

    // ---------------------------------------------------------------- persistence: fail closed

    @Test
    void persistenceFailureWhenStartingElectionSendsNoRequestVotesAndHalts() {
        RaftNode node = node(THREE);
        store.failSaves(true);
        tick(node, 10);
        assertTrue(node.isHalted());
        assertEquals(Role.FOLLOWER, node.role());
        assertEquals(0, node.currentTerm(), "memory never runs ahead of stable storage");

        store.failSaves(false);
        assertEquals(List.of(), node.tick());
        assertEquals(List.of(), node.receive(vote(N2, 1)));
        assertEquals(List.of(), store.history(), "a halted node stays halted until restarted");
    }

    @Test
    void persistenceFailureWhenVotingSendsNoResponse() {
        RaftNode node = node(THREE);
        store.failSaves(true);
        assertEquals(List.of(), node.receive(vote(N2, 1)), "no vote may be sent unless it is durable");
        assertTrue(node.isHalted());
        assertEquals(Optional.empty(), node.votedFor());
    }

    @Test
    void persistenceFailureWhileSteppingDownStopsAllHeartbeats() {
        RaftNode node = leaderOfTermOne(THREE);
        store.failSaves(true);
        assertEquals(List.of(), node.receive(new AppendEntriesResponse(N2, N1, 2, false)));
        assertTrue(node.isHalted());
        for (int i = 0; i < 30; i++) {
            assertEquals(List.of(), node.tick(), "a halted leader must not keep asserting leadership");
        }
    }

    @Test
    void restartReloadsTheVoteAndRefusesASecondCandidateInTheSameTerm() {
        node(THREE).receive(vote(N2, 5));

        RaftNode restarted = node(THREE); // same store, fresh volatile state
        assertEquals(5, restarted.currentTerm());
        assertEquals(Optional.of(N2), restarted.votedFor());
        assertEquals(List.of(new RequestVoteResponse(N1, N3, 5, false)), restarted.receive(vote(N3, 5)));
        assertEquals(List.of(new RequestVoteResponse(N1, N2, 5, true)), restarted.receive(vote(N2, 5)));
    }

    // ---------------------------------------------------------------- helpers

    private RaftNode node(ClusterConfig cluster) {
        return node(cluster, LogMetadata.empty());
    }

    private RaftNode node(ClusterConfig cluster, LogMetadata log) {
        return new RaftNode(N1, cluster, TIMING, MIN_TIMEOUT, store, log);
    }

    private RaftNode leaderOfTermOne(ClusterConfig cluster) {
        RaftNode node = node(cluster);
        electAsCandidate(node);
        for (NodeId peer : cluster.peersOf(N1)) {
            if (node.role() != Role.LEADER) {
                node.receive(new RequestVoteResponse(peer, N1, node.currentTerm(), true));
            }
        }
        assertEquals(Role.LEADER, node.role());
        assertEquals(1, node.currentTerm());
        return node;
    }

    private static void electAsCandidate(RaftNode node) {
        tickUntilOutput(node);
        assertEquals(Role.CANDIDATE, node.role());
    }

    private static List<RaftMessage> tickUntilOutput(RaftNode node) {
        for (int i = 0; i < 100; i++) {
            List<RaftMessage> out = node.tick();
            if (!out.isEmpty()) {
                return out;
            }
        }
        return fail("no output within 100 ticks");
    }

    private static void tick(RaftNode node, int ticks) {
        for (int i = 0; i < ticks; i++) {
            node.tick();
        }
    }

    private static RequestVoteRequest vote(NodeId candidate, long term) {
        return new RequestVoteRequest(candidate, N1, term, LogPosition.EMPTY);
    }
}
