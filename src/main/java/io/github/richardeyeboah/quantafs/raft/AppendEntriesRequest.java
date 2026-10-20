package io.github.richardeyeboah.quantafs.raft;

/**
 * AppendEntries RPC, heartbeat form only. The leader ({@code from}) asserts its leadership for
 * {@code term}.
 *
 * <p>Milestone 2A scope: the paper's AppendEntries also carries {@code prevLogIndex},
 * {@code prevLogTerm}, {@code entries[]}, and {@code leaderCommit}. Those exist to replicate and
 * commit log entries, which this milestone does not do, so they are deliberately absent rather than
 * sent as meaningless zeros. They are added with log replication (milestone 2B).
 */
public record AppendEntriesRequest(NodeId from, NodeId to, long term) implements RaftMessage {

    public AppendEntriesRequest {
        RaftMessage.validate(from, to, term);
    }
}
