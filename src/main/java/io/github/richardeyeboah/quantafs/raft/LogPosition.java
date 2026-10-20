package io.github.richardeyeboah.quantafs.raft;

/**
 * Term and index of the last entry in a log. {@code (0, 0)} means the log is empty; Raft log
 * indexes start at 1 and every real entry has a term of at least 1.
 */
public record LogPosition(long term, long index) {

    public static final LogPosition EMPTY = new LogPosition(0, 0);

    public LogPosition {
        if (term < 0 || index < 0) {
            throw new IllegalArgumentException("term and index must be non-negative");
        }
        if ((term == 0) != (index == 0)) {
            throw new IllegalArgumentException(
                    "an empty log is (term 0, index 0); a non-empty log has term >= 1 and index >= 1");
        }
    }

    /**
     * Raft's election restriction (paper, section 5.4.1): a log whose last entry has a higher term
     * is more up to date; with equal last terms, the longer log is more up to date. A voter grants
     * its vote only if the candidate's log is at least as up to date as its own.
     */
    public boolean isAtLeastAsUpToDateAs(LogPosition other) {
        if (term != other.term) {
            return term > other.term;
        }
        return index >= other.index;
    }

    @Override
    public String toString() {
        return "(term " + term + ", index " + index + ")";
    }
}
