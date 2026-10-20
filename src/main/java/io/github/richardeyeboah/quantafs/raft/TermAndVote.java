package io.github.richardeyeboah.quantafs.raft;

import java.util.Objects;
import java.util.Optional;

/**
 * The election state Raft requires on stable storage (paper, Figure 2): the latest term this
 * server has seen and the candidate it voted for in that term, if any.
 */
public record TermAndVote(long currentTerm, Optional<NodeId> votedFor) {

    public static final TermAndVote INITIAL = new TermAndVote(0, Optional.empty());

    public TermAndVote {
        Objects.requireNonNull(votedFor, "votedFor");
        if (currentTerm < 0) {
            throw new IllegalArgumentException("term must be non-negative");
        }
        if (currentTerm == 0 && votedFor.isPresent()) {
            throw new IllegalArgumentException("no election happens in term 0, so no vote can exist");
        }
    }

    public static TermAndVote of(long term, NodeId votedFor) {
        return new TermAndVote(term, Optional.ofNullable(votedFor));
    }

    /**
     * Rejects transitions that Raft never makes: moving to a lower term, or changing a vote that
     * was already cast in the same term. Stores enforce this as a second line of defense.
     */
    public static void checkTransition(TermAndVote from, TermAndVote to) {
        if (to.currentTerm < from.currentTerm) {
            throw new IllegalArgumentException(
                    "term must not decrease: " + from.currentTerm + " -> " + to.currentTerm);
        }
        if (to.currentTerm == from.currentTerm
                && from.votedFor.isPresent()
                && !from.votedFor.equals(to.votedFor)) {
            throw new IllegalArgumentException(
                    "vote already cast for " + from.votedFor.get() + " in term " + from.currentTerm);
        }
    }

    @Override
    public String toString() {
        return "term " + currentTerm + ", votedFor " + votedFor.map(NodeId::value).orElse("none");
    }
}
