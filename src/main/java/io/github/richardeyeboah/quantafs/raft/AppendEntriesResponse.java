package io.github.richardeyeboah.quantafs.raft;

/**
 * AppendEntries RPC result. Besides the paper's {@code term} and {@code success}, it echoes which
 * request it answers so the leader can safely use responses that arrive late, twice, or out of
 * order.
 *
 * @param term the responder's current term (a higher value makes the leader step down)
 * @param requestTerm the term of the request being answered
 * @param requestPrevLogIndex the {@code prevLogIndex} of the request being answered
 * @param matchIndex on success, the last index known to match the leader's log
 *     ({@code prevLogIndex + entries.size()}); 0 on failure
 * @param followerLastIndex the responder's last log index, a hint for skipping back faster
 */
public record AppendEntriesResponse(
        NodeId from,
        NodeId to,
        long term,
        long requestTerm,
        boolean success,
        long requestPrevLogIndex,
        long matchIndex,
        long followerLastIndex,
        long followerSnapshotIndex,
        long followerSnapshotTerm)
        implements RaftMessage {

    public AppendEntriesResponse(NodeId from,NodeId to,long term,long requestTerm,boolean success,
            long requestPrevLogIndex,long matchIndex,long followerLastIndex) {
        this(from,to,term,requestTerm,success,requestPrevLogIndex,matchIndex,followerLastIndex,0,0);
    }

    public AppendEntriesResponse {
        RaftMessage.validate(from, to, term);
        if(followerSnapshotIndex<0 || followerSnapshotIndex>followerLastIndex || followerSnapshotTerm<0
                || followerSnapshotTerm>term || (followerSnapshotIndex==0)!=(followerSnapshotTerm==0))
            throw new IllegalArgumentException("invalid snapshot hint");
        if (requestTerm < 1 || requestTerm > term) {
            throw new IllegalArgumentException("requestTerm must be between 1 and the responder's term");
        }
        if (requestPrevLogIndex < 0 || matchIndex < 0 || followerLastIndex < 0) {
            throw new IllegalArgumentException("indexes must be non-negative");
        }
        if (success && (term != requestTerm || matchIndex < requestPrevLogIndex)) {
            throw new IllegalArgumentException("a success answers a current-term request and covers prevLogIndex");
        }
        if (!success && matchIndex != 0) {
            throw new IllegalArgumentException("a failure carries no matchIndex");
        }
    }
}
