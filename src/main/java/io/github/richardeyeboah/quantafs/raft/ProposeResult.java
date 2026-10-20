package io.github.richardeyeboah.quantafs.raft;

import java.util.List;
import java.util.Optional;

/** Immediate answer to {@link RaftNode#propose}. Acceptance is not success; see {@link ProposalOutcome}. */
public sealed interface ProposeResult {

    /**
     * The leader durably appended the command at {@code index} in {@code term} and returns the
     * messages that start replicating it. Success is reported later, only after commitment and
     * local application.
     */
    record Accepted(long index, long term, List<RaftMessage> messages) implements ProposeResult {
        public Accepted {
            messages = List.copyOf(messages);
        }
    }

    /** This node is not the leader; nothing was appended. {@code leaderHint} is its best guess. */
    record NotLeader(Optional<NodeId> leaderHint) implements ProposeResult {}

    /** The node is halted; nothing was appended. */
    record Unavailable(String reason) implements ProposeResult {}

    /**
     * Persisting the entry failed, so the node halted. The entry may or may not be in its log and
     * could still be committed later; the outcome is unknown.
     */
    record OutcomeUnknown(String reason) implements ProposeResult {}
}
