package io.github.richardeyeboah.quantafs.raft;

import java.util.SortedMap;

/**
 * The key-value state that committed log entries are applied to.
 *
 * <p>Contract:
 *
 * <ul>
 *   <li><b>Strict order:</b> {@link #apply} accepts only the entry at {@code lastApplied().index() + 1};
 *       anything else throws {@link StateMachineException}, so an entry can be neither skipped nor
 *       applied twice.
 *   <li><b>Atomic with its marker:</b> the command's effect (none for {@code NO_OP}) and the new
 *       {@code lastApplied} position are recorded together or not at all.
 *   <li><b>Durable on return</b> under the implementation's documented sync assumptions.
 *   <li>Failures throw {@link StateMachineException}; the caller must stop and later restart.
 * </ul>
 */
public interface StateMachine {

    /** Position (index and term) of the last applied entry; {@link LogPosition#EMPTY} if none. */
    LogPosition lastApplied();

    void apply(LogEntry entry);

    /**
     * <b>Diagnostic state inspection only.</b> A copy of all application data, read locally without
     * any consistency guarantee relative to the rest of the cluster. Used by tests and demos; it is
     * not a client read path.
     */
    SortedMap<Bytes, Bytes> diagnosticContents();

    static void checkNext(LogPosition lastApplied, LogEntry entry) {
        if (entry.index() != lastApplied.index() + 1) {
            throw new StateMachineException(
                    "entries must be applied in order: last applied " + lastApplied.index() + ", got " + entry.index());
        }
    }
}
