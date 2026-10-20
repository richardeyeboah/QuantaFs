package io.github.richardeyeboah.quantafs.raft;

/** RequestVote RPC results (paper, Figure 2): the voter's current term and whether it voted yes. */
public record RequestVoteResponse(NodeId from, NodeId to, long term, boolean voteGranted)
        implements RaftMessage {

    public RequestVoteResponse {
        RaftMessage.validate(from, to, term);
    }
}
