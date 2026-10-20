package io.github.richardeyeboah.quantafs.raft;

import java.util.Objects;

/**
 * The only information about the replicated log that leader election needs: the position of the
 * last entry. Milestone 2A has no log; callers supply fixed metadata. A durable log implementing
 * this interface arrives with log replication (milestone 2B).
 */
@FunctionalInterface
public interface LogMetadata {

    LogPosition lastEntry();

    static LogMetadata empty() {
        return fixed(LogPosition.EMPTY);
    }

    static LogMetadata fixed(LogPosition position) {
        Objects.requireNonNull(position, "position");
        return () -> position;
    }
}
