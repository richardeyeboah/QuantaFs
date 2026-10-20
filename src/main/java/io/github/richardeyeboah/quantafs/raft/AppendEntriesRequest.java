package io.github.richardeyeboah.quantafs.raft;

import java.util.List;
import java.util.Objects;

/**
 * AppendEntries RPC (paper, Figure 2). The leader ({@code from}) asks {@code to} to accept
 * {@code entries} immediately after the entry at {@code prevLogIndex}, which must have term
 * {@code prevLogTerm} in the follower's log (the consistency check). A heartbeat is the same message
 * with no entries and obeys the same check.
 *
 * @param prevLogIndex index of the entry just before {@code entries} (0 = start of the log)
 * @param prevLogTerm term of that entry (0 when {@code prevLogIndex} is 0)
 * @param leaderCommit the leader's commit index
 */
public record AppendEntriesRequest(
        NodeId from,
        NodeId to,
        long term,
        long prevLogIndex,
        long prevLogTerm,
        List<LogEntry> entries,
        long leaderCommit)
        implements RaftMessage {

    public AppendEntriesRequest {
        RaftMessage.validate(from, to, term);
        Objects.requireNonNull(entries, "entries");
        entries = List.copyOf(entries);
        if (prevLogIndex < 0 || leaderCommit < 0) {
            throw new IllegalArgumentException("prevLogIndex and leaderCommit must be non-negative");
        }
        if ((prevLogIndex == 0) != (prevLogTerm == 0)) {
            throw new IllegalArgumentException("prevLogTerm must be 0 exactly when prevLogIndex is 0");
        }
        if (prevLogTerm > term) {
            throw new IllegalArgumentException("prevLogTerm cannot exceed the leader's term");
        }
        long expected = prevLogIndex + 1;
        long previousTerm = prevLogTerm;
        for (LogEntry entry : entries) {
            if (entry.index() != expected || entry.term() < previousTerm || entry.term() > term) {
                throw new IllegalArgumentException("entries must follow prevLogIndex contiguously with "
                        + "non-decreasing terms no higher than the leader's term: " + entries);
            }
            expected++;
            previousTerm = entry.term();
        }
    }

    /** Index of the last entry this request covers: the follower's log matches up to here on success. */
    public long lastCoveredIndex() {
        return prevLogIndex + entries.size();
    }
}
