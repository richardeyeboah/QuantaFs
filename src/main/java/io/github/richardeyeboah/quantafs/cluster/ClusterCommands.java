package io.github.richardeyeboah.quantafs.cluster;

import io.github.richardeyeboah.quantafs.api.v1.*;
import io.grpc.*;
import java.util.concurrent.TimeUnit;

public final class ClusterCommands {
    private ClusterCommands() {}
    public static int status(String[] args) {
        if(args.length!=2 || !args[0].equals("--target") || !args[1].matches("127\\.0\\.0\\.1:[0-9]+")) {
            System.err.println("cluster-status --target 127.0.0.1:5101"); return 2;
        }
        ManagedChannel channel=ManagedChannelBuilder.forTarget(args[1]).usePlaintext().build();
        try {
            NodeStatus s=RaftTransportGrpc.newBlockingStub(channel).withDeadlineAfter(2,TimeUnit.SECONDS)
                    .status(NodeStatusRequest.getDefaultInstance());
            System.out.printf("id=%s role=%s term=%d leader=%s commit=%d applied=%d halted=%s pending=%d snapshot=%d retained=%d snapshotLimited=%s%n",
                    s.getId(),s.getRole(),s.getTerm(),s.getLeader(),s.getCommitIndex(),s.getAppliedIndex(),
                    s.getHalted(),s.getPendingRequests(),s.getSnapshotIndex(),s.getRetainedEntries(),s.getSnapshotLimited());
            return s.getHalted() ? 3 : 0;
        } catch(StatusRuntimeException e) { System.err.println(e.getStatus()); return 3; }
        finally { channel.shutdownNow(); }
    }
}
