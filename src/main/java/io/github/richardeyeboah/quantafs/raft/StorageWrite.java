package io.github.richardeyeboah.quantafs.raft;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongUnaryOperator;

/**
 * One atomic change to a node's Raft storage: optionally a new {@link TermAndVote}, optionally the
 * removal of every log entry from {@code truncateFrom} onward, then entries appended after that.
 * A storage implementation applies all of it or none of it.
 *
 * <p>Combining them matters: a follower that learns a higher term from a leader and accepts that
 * leader's entries records both in a single write, so after a crash its term and log can never
 * disagree.
 *
 * @param truncateFrom 0 for no truncation, otherwise the first index to remove
 */
public record StorageWrite(Optional<TermAndVote> termAndVote, long truncateFrom, List<LogEntry> append) {

    public StorageWrite {
        Objects.requireNonNull(termAndVote, "termAndVote");
        Objects.requireNonNull(append, "append");
        if (truncateFrom < 0) {
            throw new IllegalArgumentException("truncateFrom must be 0 (none) or a log index");
        }
        append = List.copyOf(append);
    }

    public static StorageWrite termAndVote(TermAndVote state) {
        return new StorageWrite(Optional.of(state), 0, List.of());
    }

    public static StorageWrite append(List<LogEntry> entries) {
        return new StorageWrite(Optional.empty(), 0, entries);
    }

    public boolean isEmpty() {
        return termAndVote.isEmpty() && truncateFrom == 0 && append.isEmpty();
    }

    /**
     * Checks this write against the current durable state. Implementations call this before
     * changing anything, so a rejected write leaves storage untouched.
     *
     * @param termAt term of the entry at an index in the current log (0 for index 0)
     * @throws IllegalArgumentException describing the violated rule
     */
    public void validate(TermAndVote current, long lastIndex, LongUnaryOperator termAt) {
        termAndVote.ifPresent(next -> TermAndVote.checkTransition(current, next));
        long effectiveTerm = termAndVote.map(TermAndVote::currentTerm).orElse(current.currentTerm());

        long base = lastIndex;
        if (truncateFrom > 0) {
            if (truncateFrom > lastIndex) {
                throw new IllegalArgumentException(
                        "cannot truncate from " + truncateFrom + ": log ends at " + lastIndex);
            }
            base = truncateFrom - 1;
        }
        long previousTerm = termAt.applyAsLong(base);
        long expectedIndex = base + 1;
        for (LogEntry entry : append) {
            if (entry.index() != expectedIndex) {
                throw new IllegalArgumentException(
                        "entries must be contiguous: expected index " + expectedIndex + ", got " + entry.index());
            }
            if (entry.term() < previousTerm) {
                throw new IllegalArgumentException(
                        "entry terms must not decrease: index " + entry.index() + " has term " + entry.term()
                                + " after term " + previousTerm);
            }
            if (entry.term() > effectiveTerm) {
                throw new IllegalArgumentException(
                        "entry " + entry.index() + " has term " + entry.term() + ", above current term " + effectiveTerm);
            }
            previousTerm = entry.term();
            expectedIndex++;
        }
    }
}
