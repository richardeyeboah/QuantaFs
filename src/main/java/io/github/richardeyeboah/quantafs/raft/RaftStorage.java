package io.github.richardeyeboah.quantafs.raft;

import java.util.List;

/**
 * Durable state Raft requires on every server (paper, Figure 2): {@code currentTerm},
 * {@code votedFor}, and the log.
 *
 * <p>Contract:
 *
 * <ul>
 *   <li><b>Atomic:</b> {@link #write} applies its whole {@link StorageWrite} or none of it, including
 *       across a crash. After restart the storage reflects some prefix of the completed writes.
 *   <li><b>Durable on return:</b> when {@link #write} returns, the change survives a crash under the
 *       implementation's documented sync assumptions. Callers send any message that depends on the
 *       change only after that.
 *   <li><b>Rule-checked:</b> writes are validated with {@link StorageWrite#validate}; a violation
 *       throws {@link PersistenceException} and changes nothing.
 *   <li><b>Fail closed:</b> I/O failures throw {@link PersistenceException}. After one, the caller
 *       must stop using the instance; on-disk state is unknown until it is reopened.
 *   <li>Retained entries are contiguous from firstRetainedIndex(); a snapshot retains the preceding boundary term.
 * </ul>
 */
public interface RaftStorage {

    TermAndVote termAndVote();

    /** Last logical index, including the snapshot boundary when the suffix is empty. */
    long lastIndex();

    /** Term at a retained entry or snapshot boundary; compacted earlier indexes are unavailable. */
    long termAt(long index);

    LogEntry entry(long index);

    /** Entries {@code from..to}, inclusive; empty if {@code from > to}. */
    List<LogEntry> entries(long from, long to);

    void write(StorageWrite write);

    default java.util.Optional<Snapshot> snapshot() { return java.util.Optional.empty(); }
    default long firstRetainedIndex() { return snapshot().map(s -> s.position().index()+1).orElse(1L); }
    default void installSnapshot(Snapshot snapshot) { throw new UnsupportedOperationException("snapshots unsupported"); }


    default LogPosition lastPosition() {
        long last = lastIndex();
        return last == 0 ? LogPosition.EMPTY : new LogPosition(termAt(last), last);
    }

    static void checkIndex(long index, long lastIndex) {
        if (index < 0 || index > lastIndex) {
            throw new IllegalArgumentException("index " + index + " is outside the log [0, " + lastIndex + "]");
        }
    }
}
