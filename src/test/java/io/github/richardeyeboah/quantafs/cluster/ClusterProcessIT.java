package io.github.richardeyeboah.quantafs.cluster;

import com.google.protobuf.ByteString;
import io.github.richardeyeboah.quantafs.api.v1.*;
import io.grpc.*;
import java.net.ServerSocket;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

/** Packaged Java processes and real TCP/gRPC: no simulated messages or time. */
class ClusterProcessIT {
    @TempDir Path root;
    final List<Process> processes=new ArrayList<>();
    final List<ManagedChannel> channels=new ArrayList<>();
    final List<Integer> ports=new ArrayList<>();
    String peers;
    @AfterEach void stop() throws Exception {
        processes.forEach(p -> { if(p!=null) p.destroyForcibly(); });
        for(Process p:processes) if(p!=null) p.waitFor(10,TimeUnit.SECONDS);
        channels.forEach(ManagedChannel::shutdownNow);
        for(var c:channels) c.awaitTermination(5,TimeUnit.SECONDS);
    }
    @ParameterizedTest @ValueSource(ints={3,5})
    void realProcessesReplicateFailOverRecoverAndRefuseReadsWithoutQuorum(int count) throws Exception {
        List<ServerSocket> reservations=new ArrayList<>();
        try {
            for(int i=0;i<count;i++) { var s=new ServerSocket(0); reservations.add(s); ports.add(s.getLocalPort()); }
        } finally { for(var s:reservations) s.close(); }
        peers=java.util.stream.IntStream.range(0,count).mapToObj(i -> "n"+(i+1)+"=127.0.0.1:"+ports.get(i))
                .collect(java.util.stream.Collectors.joining(","));
        for(int i=0;i<count;i++) {
            processes.add(launch(i));
            channels.add(ManagedChannelBuilder.forAddress("127.0.0.1",ports.get(i)).usePlaintext().build());
        }
        try(var retryClient=new ClusterClient(ports,Duration.ofSeconds(20))) {
        int leader=awaitLeader(-1);
        ByteString key=ByteString.copyFrom(new byte[]{0,(byte)255,42});
        ByteString value=ByteString.copyFrom(new byte[]{0,10,(byte)254});
        kv(leader).put(PutRequest.newBuilder().setKey(key).setValue(value).build());
        assertEquals(value,kv(leader).get(GetRequest.newBuilder().setKey(key).build()).getValue());
        int follower=(leader+1)%count;
        await(() -> tryStatus(follower)!=null,"follower network startup");
        var rejection=assertThrows(StatusRuntimeException.class,() -> kv(follower).get(GetRequest.newBuilder().setKey(key).build()));
        assertEquals(Status.Code.UNAVAILABLE,rejection.getStatus().getCode());
        assertTrue(rejection.getStatus().getDescription().contains("leader="));
        assertEquals(Status.Code.INVALID_ARGUMENT,assertThrows(StatusRuntimeException.class,() ->
                kv(leader).put(PutRequest.getDefaultInstance())).getStatus().getCode());
        assertEquals(Status.Code.FAILED_PRECONDITION,assertThrows(StatusRuntimeException.class,() ->
                RaftTransportGrpc.newBlockingStub(channels.get(follower)).withDeadlineAfter(2,TimeUnit.SECONDS)
                        .deliver(RaftEnvelope.newBuilder().setClusterKey("wrong").build())).getStatus().getCode());
        var replay=PutRequest.newBuilder().setClientId("checkout").setSequence(1)
                .setKey(ByteString.copyFromUtf8("retry-key")).setValue(ByteString.copyFromUtf8("old")).build();
        retryClient.put(replay);
        kv(leader).put(PutRequest.newBuilder().setKey(replay.getKey()).setValue(ByteString.copyFromUtf8("newer")).build());
        retryClient.put(replay); // deliberately ignore first acknowledgement and repeat the same identity
        assertEquals("newer",retryClient.get(GetRequest.newBuilder().setKey(replay.getKey()).build()).getValue().toStringUtf8());
        assertEquals(Status.Code.ALREADY_EXISTS,assertThrows(StatusRuntimeException.class,() ->
                retryClient.put(replay.toBuilder().setValue(ByteString.copyFromUtf8("conflict")).build())).getStatus().getCode());
        processes.get(follower).destroyForcibly();assertTrue(processes.get(follower).waitFor(10,TimeUnit.SECONDS));
        for(int sequence=1;sequence<=130;sequence++) retryClient.put(PutRequest.newBuilder()
                .setClientId("batch").setSequence(sequence).setKey(ByteString.copyFromUtf8("workload"))
                .setValue(ByteString.copyFromUtf8("value"+sequence)).build());
        assertTrue(status(leader).getSnapshotIndex()>0,"leader should compact committed log history");
        processes.set(follower,launch(follower));
        long catchupTarget=status(leader).getAppliedIndex();
        await(() -> {var state=tryStatus(follower);return state!=null && state.getSnapshotIndex()>0
                && state.getAppliedIndex()>=catchupTarget && !state.getHalted();},"offline follower installs snapshot and catches up");
        processes.get(leader).destroyForcibly();
        assertTrue(processes.get(leader).waitFor(10,TimeUnit.SECONDS));
        int replacement=awaitLeader(leader);
        assertEquals(value,retryClient.get(GetRequest.newBuilder().setKey(key).build()).getValue());
        assertEquals(value,kv(replacement).get(GetRequest.newBuilder().setKey(key).build()).getValue());
        ByteString emptyKey=ByteString.copyFromUtf8("empty");
        kv(replacement).put(PutRequest.newBuilder().setKey(emptyKey).build());
        assertTrue(kv(replacement).get(GetRequest.newBuilder().setKey(emptyKey).build()).getFound());
        kv(replacement).delete(DeleteRequest.newBuilder().setKey(emptyKey).build());
        assertFalse(kv(replacement).get(GetRequest.newBuilder().setKey(emptyKey).build()).getFound());
        processes.set(leader,launch(leader));
        long applied=status(replacement).getAppliedIndex();
        await(() -> { var s=tryStatus(leader); return s!=null && !s.getHalted() && s.getAppliedIndex()>=applied; },
                "restarted former leader must catch up");

        // Actual loss of a majority: a former/current leader must not serve local stale reads.
        for(int i=0;i<count;i++) if(i!=replacement) {
            processes.get(i).destroyForcibly(); assertTrue(processes.get(i).waitFor(10,TimeUnit.SECONDS));
        }
        var readFailure=assertThrows(StatusRuntimeException.class,() -> kv(replacement)
                .get(GetRequest.newBuilder().setKey(key).build()));
        // A delayed higher-term message can make the survivor step down as the majority dies.
        // Both timeout and not-leader are safe; a successful stale read or halted node is not.
        assertTrue(Set.of(Status.Code.DEADLINE_EXCEEDED,Status.Code.UNAVAILABLE).contains(readFailure.getStatus().getCode()));
        assertFalse(status(replacement).getHalted());
        var writeFailure=assertThrows(StatusRuntimeException.class,() -> kv(replacement)
                .put(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("ambiguous")).setValue(value).build()));
        assertTrue(Set.of(Status.Code.DEADLINE_EXCEEDED,Status.Code.UNAVAILABLE).contains(writeFailure.getStatus().getCode()));
        assertFalse(status(replacement).getHalted());
        for(int i=0;i<count;i++) if(i!=replacement) processes.set(i,launch(i));
        await(() -> {
            for(int i=0;i<count;i++) {
                var state=tryStatus(i);
                if(state!=null && state.getRole().equals("LEADER") && !state.getHalted()) {
                    try { if(kv(i).get(GetRequest.newBuilder().setKey(key).build()).getValue().equals(value)) return true; }
                    catch(StatusRuntimeException ignored) { }
                }
            }
            return false;
        },"cluster must resume strongly ordered reads after majority recovery");

        // Stop every process, then reopen every node's own disk state.
        for(Process p:processes) { p.destroyForcibly(); assertTrue(p.waitFor(10,TimeUnit.SECONDS)); }
        for(int i=0;i<count;i++) processes.set(i,launch(i));
        int reopened=awaitLeader(-1);
        assertEquals(value,kv(reopened).get(GetRequest.newBuilder().setKey(key).build()).getValue());
        retryClient.put(replay);
        assertEquals("newer",retryClient.get(GetRequest.newBuilder().setKey(replay.getKey()).build()).getValue().toStringUtf8());
        assertEquals("value130",retryClient.get(GetRequest.newBuilder().setKey(ByteString.copyFromUtf8("workload")).build()).getValue().toStringUtf8());
        Process duplicate=launch(reopened);
        try { assertTrue(duplicate.waitFor(10,TimeUnit.SECONDS)); assertNotEquals(0,duplicate.exitValue()); }
        finally { duplicate.destroyForcibly(); }
        System.out.printf("Real %d-process cluster: confirmed binary data survived leader kill and full restart; quorum-less reads/writes refused; retry deduplication and snapshot catch-up verified%n",count);
        }
    }
    private Process launch(int i) throws Exception {
        String java=Path.of(System.getProperty("java.home"),"bin","java").toString();
        return new ProcessBuilder(java,"-jar",System.getProperty("quantafs.jar"),"cluster-server",
                "--id","n"+(i+1),"--peers",peers,"--data-dir",root.resolve("n"+(i+1)).toString())
                .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.appendTo(root.resolve("n"+(i+1)+".log").toFile())).start();
    }
    private KeyValueServiceGrpc.KeyValueServiceBlockingStub kv(int i) {
        return KeyValueServiceGrpc.newBlockingStub(channels.get(i)).withDeadlineAfter(8,TimeUnit.SECONDS);
    }
    private NodeStatus status(int i) {
        return RaftTransportGrpc.newBlockingStub(channels.get(i)).withDeadlineAfter(500,TimeUnit.MILLISECONDS)
                .status(NodeStatusRequest.getDefaultInstance());
    }
    private NodeStatus tryStatus(int i) { try {return status(i);} catch(StatusRuntimeException e) {return null;} }
    private int awaitLeader(int excluded) throws Exception {
        int[] found={-1};
        await(() -> {
            for(int i=0;i<processes.size();i++) if(i!=excluded && processes.get(i).isAlive()) {
                var s=tryStatus(i);
                // A committed current-term no-op means this leader has contacted a majority.
                if(s!=null && s.getRole().equals("LEADER") && !s.getHalted() && s.getCommitIndex()>0) {
                    try {
                        kv(i).get(GetRequest.newBuilder().setKey(ByteString.copyFromUtf8("__readiness__")).build());
                        found[0]=i; return true;
                    } catch(StatusRuntimeException ignored) { }

                }
            }
            return false;
        },"leader election");
        return found[0];
    }
    private void await(BooleanSupplier condition,String description) throws Exception {
        long end=System.nanoTime()+Duration.ofSeconds(30).toNanos();
        while(System.nanoTime()<end) { if(condition.getAsBoolean()) return; Thread.sleep(50); }
        StringBuilder logs=new StringBuilder(description);
        for(int i=0;i<processes.size();i++) logs.append('\n').append(Files.readString(root.resolve("n"+(i+1)+".log")));
        fail(logs.toString());
    }
}
