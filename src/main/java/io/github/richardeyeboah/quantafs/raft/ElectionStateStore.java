package io.github.richardeyeboah.quantafs.raft;

/**
 * Stable storage for {@link TermAndVote}.
 *
 * <p>Contract:
 *
 * <ul>
 *   <li>{@link #save} is atomic: after a crash, {@link #load} returns either the previous state or
 *       the new one, never a mix of the two.
 *   <li>{@link #save} is durable when it returns, under the implementation's documented
 *       assumptions.
 *   <li>{@link #save} rejects a lower term or a changed vote within a term
 *       ({@link TermAndVote#checkTransition}).
 *   <li>Failures throw {@link PersistenceException}. After a failed save the caller must assume
 *       the on-disk state is unknown and stop using this instance.
 * </ul>
 */
public interface ElectionStateStore {

    TermAndVote load();

    void save(TermAndVote state);
}
