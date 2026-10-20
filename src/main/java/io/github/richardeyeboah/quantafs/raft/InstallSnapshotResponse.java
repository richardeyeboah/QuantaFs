package io.github.richardeyeboah.quantafs.raft;
public record InstallSnapshotResponse(NodeId from,NodeId to,long term,long snapshotIndex,boolean success) implements RaftMessage {
    public InstallSnapshotResponse {
        RaftMessage.validate(from,to,term);if(snapshotIndex<1) throw new IllegalArgumentException("invalid snapshot index");
    }
}
