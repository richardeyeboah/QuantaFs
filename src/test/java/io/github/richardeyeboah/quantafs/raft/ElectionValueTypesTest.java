package io.github.richardeyeboah.quantafs.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

/** Validation and arithmetic of the small value types the election core relies on. */
class ElectionValueTypesTest {

    @Test
    void quorumIsAStrictMajorityOfAllVoters() {
        assertEquals(1, ClusterConfig.of("a").quorum());
        assertEquals(2, ClusterConfig.of("a", "b").quorum());
        assertEquals(2, ClusterConfig.of("a", "b", "c").quorum());
        assertEquals(3, ClusterConfig.of("a", "b", "c", "d").quorum());
        assertEquals(3, ClusterConfig.of("a", "b", "c", "d", "e").quorum());
    }

    @Test
    void membershipMustBeUniqueAndNonEmpty() {
        assertThrows(IllegalArgumentException.class, () -> ClusterConfig.of("a", "a"));
        assertThrows(IllegalArgumentException.class, () -> ClusterConfig.of(List.of()));
        assertEquals(List.of(NodeId.of("a"), NodeId.of("c")), ClusterConfig.of("c", "b", "a").peersOf(NodeId.of("b")));
        assertThrows(IllegalArgumentException.class, () -> NodeId.of(""));
        assertThrows(IllegalArgumentException.class, () -> NodeId.of("has space"));
    }

    @Test
    void timingRequiresHeartbeatsFasterThanTheElectionTimeout() {
        assertThrows(IllegalArgumentException.class, () -> new ElectionTiming(0, 10, 20));
        assertThrows(IllegalArgumentException.class, () -> new ElectionTiming(10, 10, 20));
        assertThrows(IllegalArgumentException.class, () -> new ElectionTiming(3, 10, 10));
    }

    @Test
    void logPositionsAreEitherEmptyOrReal() {
        assertThrows(IllegalArgumentException.class, () -> new LogPosition(0, 3));
        assertThrows(IllegalArgumentException.class, () -> new LogPosition(2, 0));
        assertThrows(IllegalArgumentException.class, () -> new LogPosition(-1, -1));
    }

    @Test
    void messagesRejectImpossibleTermsAndSelfAddressing() {
        NodeId a = NodeId.of("a");
        NodeId b = NodeId.of("b");
        assertThrows(IllegalArgumentException.class, () -> new AppendEntriesRequest(a, b, 0));
        assertThrows(IllegalArgumentException.class, () -> new AppendEntriesRequest(a, a, 1));
        assertThrows(IllegalArgumentException.class, () -> new RequestVoteRequest(a, b, 2, new LogPosition(3, 1)));
    }

    @Test
    void termAndVoteTransitionsFollowRaft() {
        NodeId a = NodeId.of("a");
        NodeId b = NodeId.of("b");
        assertThrows(IllegalArgumentException.class, () -> TermAndVote.of(0, a));
        TermAndVote.checkTransition(TermAndVote.of(1, null), TermAndVote.of(1, a));
        TermAndVote.checkTransition(TermAndVote.of(1, a), TermAndVote.of(2, null));
        assertThrows(IllegalArgumentException.class, () -> TermAndVote.checkTransition(TermAndVote.of(1, a), TermAndVote.of(1, b)));
        assertThrows(IllegalArgumentException.class, () -> TermAndVote.checkTransition(TermAndVote.of(2, null), TermAndVote.of(1, null)));
    }
}
