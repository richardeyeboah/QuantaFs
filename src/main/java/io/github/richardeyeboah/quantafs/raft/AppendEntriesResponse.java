package io.github.richardeyeboah.quantafs.raft;

/**
 * AppendEntries RPC results. In milestone 2A {@code success} only means the follower accepted the
 * sender as leader for {@code term}; there is no log-consistency check yet. A response with a
 * higher term tells a stale leader to step down.
 */
public record AppendEntriesResponse(NodeId from, NodeId to, long term, boolean success)
        implements RaftMessage {

    public AppendEntriesResponse {
        RaftMessage.validate(from, to, term);
    }
}
