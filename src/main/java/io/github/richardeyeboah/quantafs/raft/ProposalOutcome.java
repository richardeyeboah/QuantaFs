package io.github.richardeyeboah.quantafs.raft;

/**
 * Final status of an accepted proposal, reported by the leader that accepted it.
 *
 * <ul>
 *   <li>{@link Status#COMMITTED}: a majority stored the entry, it is committed, and this leader has
 *       applied it to its state machine. It will never be lost or reordered (under the storage
 *       assumptions).
 *   <li>{@link Status#UNKNOWN}: this node stopped being leader, or halted, before it could confirm
 *       commitment. The entry may still be committed by a later leader, or replaced. It is
 *       <em>not</em> known to have failed.
 * </ul>
 *
 * <p>Plain commands have no deduplication. A retry-safe client uses Command.Request with a stable
 * client ID and sequence; the state machine then suppresses repeated effects. UNKNOWN is never
 * permission to retry a plain command blindly.
 */
public record ProposalOutcome(long index, long term, Status status) {

    public enum Status {
        COMMITTED,
        UNKNOWN
    }
}
