package io.github.richardeyeboah.quantafs.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.richardeyeboah.quantafs.api.v1.*;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.Test;

class ClusterCommandsTest {
    @Test
    void healthAcceptsEveryNonHaltedRoleAndRejectsHaltedNodes() throws Exception {
        for (String role : new String[] {"FOLLOWER", "CANDIDATE", "LEADER"}) {
            for (boolean halted : new boolean[] {false, true}) {
                var service = new RaftTransportGrpc.RaftTransportImplBase() {
                    @Override
                    public void status(NodeStatusRequest request, StreamObserver<NodeStatus> response) {
                        response.onNext(NodeStatus.newBuilder().setId("n1").setRole(role)
                                .setHalted(halted).build());
                        response.onCompleted();
                    }
                };
                var server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                        .addService(service).build().start();
                try {
                    assertEquals(halted ? 3 : 0, ClusterCommands.status(
                            new String[] {"--target", "127.0.0.1:" + server.getPort()}),
                            "role=" + role + ", halted=" + halted);
                } finally {
                    server.shutdownNow();
                    server.awaitTermination();
                }
            }
        }
    }

    @Test
    void failedStatusRpcIsUnhealthy() throws Exception {
        var server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                .addService(new RaftTransportGrpc.RaftTransportImplBase() {
                    @Override
                    public void status(NodeStatusRequest request, StreamObserver<NodeStatus> response) {
                        response.onError(Status.UNAVAILABLE.asRuntimeException());
                    }
                }).build().start();
        try {
            assertEquals(3, ClusterCommands.status(
                    new String[] {"--target", "127.0.0.1:" + server.getPort()}));
        } finally {
            server.shutdownNow();
            server.awaitTermination();
        }
    }
}
