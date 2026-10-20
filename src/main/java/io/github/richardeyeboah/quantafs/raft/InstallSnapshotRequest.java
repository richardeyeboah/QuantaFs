package io.github.richardeyeboah.quantafs.raft;
public record InstallSnapshotRequest(NodeId from,NodeId to,long term,Snapshot snapshot) implements RaftMessage {
    public InstallSnapshotRequest {
        RaftMessage.validate(from,to,term);
        if(snapshot==null || snapshot.position().term()>term) throw new IllegalArgumentException("invalid snapshot term");
    }
}
