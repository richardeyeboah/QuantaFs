package io.github.richardeyeboah.quantafs.raft;

import java.util.Objects;

/**
 * One immutable slot of the replicated log: its position ({@code index}, starting at 1), the term of
 * the leader that created it, and the command. Raft identifies an entry by (index, term): two logs
 * holding the same index and term hold the same command and identical preceding entries (the Log
 * Matching property).
 */
public record LogEntry(long index, long term, Command command) {

    public LogEntry {
        if (index < 1) {
            throw new IllegalArgumentException("log indexes start at 1, got " + index);
        }
        if (term < 1) {
            throw new IllegalArgumentException("entry term must be at least 1, got " + term);
        }
        Objects.requireNonNull(command, "command");
    }

    public LogPosition position() {
        return new LogPosition(term, index);
    }

    @Override
    public String toString() {
        return index + ":t" + term + ":" + command;
    }
}
