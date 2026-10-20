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
 * <p>There is no deduplication: a client that retries after {@code UNKNOWN} may cause the command
 * to be applied twice. Request IDs are a later milestone.
 */
public record ProposalOutcome(long index, long term, Status status) {

    public enum Status {
        COMMITTED,
        UNKNOWN
    }
}
