package io.github.richardeyeboah.quantafs.raft;

import java.util.Objects;

/**
 * RequestVote RPC arguments (paper, Figure 2): the candidate ({@code from}) asks {@code to} for its
 * vote in {@code term}, presenting the position of its last log entry.
 */
public record RequestVoteRequest(NodeId from, NodeId to, long term, LogPosition lastLog)
        implements RaftMessage {

    public RequestVoteRequest {
        RaftMessage.validate(from, to, term);
        Objects.requireNonNull(lastLog, "lastLog");
        if (lastLog.term() > term) {
            throw new IllegalArgumentException("a candidate's last log term cannot exceed its current term");
        }
    }
}
