package io.github.richardeyeboah.quantafs.cluster;

import io.github.richardeyeboah.quantafs.api.v1.*;
import io.grpc.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** Sequential client. A logical mutation must retain its ID and sequence across every retry. */
public final class ClusterClient implements AutoCloseable {
    private final List<ManagedChannel> channels;
    private final long budgetNanos;
    private int cursor;
    private String membership;
    public ClusterClient(List<Integer> ports,Duration budget) {
        if(ports.isEmpty() || budget.isNegative() || budget.isZero()) throw new IllegalArgumentException("seeds and positive timeout required");
        budgetNanos=budget.toNanos();
        channels=ports.stream().map(p -> ManagedChannelBuilder.forAddress("127.0.0.1",p).usePlaintext().build()).toList();
    }
    private synchronized <T> T execute(Function<KeyValueServiceGrpc.KeyValueServiceBlockingStub,T> call) {
        long end=System.nanoTime()+budgetNanos;
        StatusRuntimeException last=null;
        while(System.nanoTime()<end) {
            int target=cursor;
            try {
                long remaining=end-System.nanoTime();
                if(remaining<=0) break;
                // Probe the next seed instead of getting stuck trusting an isolated former leader.
                NodeStatus status=RaftTransportGrpc.newBlockingStub(channels.get(target))
                        .withDeadlineAfter(Math.min(remaining,TimeUnit.MILLISECONDS.toNanos(300)),TimeUnit.NANOSECONDS)
                        .status(NodeStatusRequest.getDefaultInstance());
                if(status.getClusterKey().isEmpty()) throw Status.UNIMPLEMENTED
                        .withDescription("cluster node does not support retry-safe client protocol; upgrade all nodes").asRuntimeException();
                if(membership==null) membership=status.getClusterKey();
                if(!membership.equals(status.getClusterKey())) throw Status.FAILED_PRECONDITION
                        .withDescription("seed endpoints belong to different clusters").asRuntimeException();
                if(!status.getRole().equals("LEADER") || status.getHalted()) {
                    cursor=(target+1)%channels.size(); pause(end); continue;
                }
                remaining=end-System.nanoTime();
                if(remaining<=0) break;
                return call.apply(KeyValueServiceGrpc.newBlockingStub(channels.get(target))
                        .withDeadlineAfter(Math.min(remaining,TimeUnit.SECONDS.toNanos(1)),TimeUnit.NANOSECONDS));
            } catch(StatusRuntimeException e) {
                last=e;
                if(!Set.of(Status.Code.UNAVAILABLE,Status.Code.DEADLINE_EXCEEDED,Status.Code.RESOURCE_EXHAUSTED)
                        .contains(e.getStatus().getCode())) throw e;
                cursor=(target+1)%channels.size(); pause(end);
            }
        }
        throw Status.DEADLINE_EXCEEDED.withDescription("cluster request deadline; mutation outcome may be unknown; retry the same identity")
                .withCause(last).asRuntimeException();
    }
    private static void pause(long end) {
        try { TimeUnit.NANOSECONDS.sleep(Math.max(0,Math.min(end-System.nanoTime(),TimeUnit.MILLISECONDS.toNanos(30)))); }
        catch(InterruptedException e) { Thread.currentThread().interrupt(); throw Status.CANCELLED.asRuntimeException(); }
    }
    public void put(PutRequest request) {
        if(request.getClientId().isEmpty() || request.getSequence()<1) throw new IllegalArgumentException("retryable puts require identity");
        execute(stub -> stub.put(request));
    }
    public void delete(DeleteRequest request) {
        if(request.getClientId().isEmpty() || request.getSequence()<1) throw new IllegalArgumentException("retryable deletes require identity");
        execute(stub -> stub.delete(request));
    }
    public GetResponse get(GetRequest request) {return execute(stub -> stub.get(request));}
    @Override public void close() {
        channels.forEach(ManagedChannel::shutdownNow);
        for(var channel:channels) try { channel.awaitTermination(2,TimeUnit.SECONDS); }
        catch(InterruptedException e) {Thread.currentThread().interrupt();}
    }
}
