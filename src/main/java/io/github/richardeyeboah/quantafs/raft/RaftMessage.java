package io.github.richardeyeboah.quantafs.raft;

/**
 * A message between two voters. Every Raft message carries the sender's current term, which is how
 * servers discover that they are out of date (paper, section 5.1).
 */
public sealed interface RaftMessage
        permits RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse {

    NodeId from();

    NodeId to();

    long term();

    static void validate(NodeId from, NodeId to, long term) {
        if (from == null || to == null) {
            throw new IllegalArgumentException("from and to are required");
        }
        if (from.equals(to)) {
            throw new IllegalArgumentException("a node does not send messages to itself");
        }
        if (term < 1) {
            throw new IllegalArgumentException("message term must be at least 1, got " + term);
        }
    }
}
