package io.github.richardeyeboah.quantafs.cluster;

import com.google.protobuf.ByteString;
import io.github.richardeyeboah.quantafs.api.v1.*;
import io.github.richardeyeboah.quantafs.raft.*;
import io.github.richardeyeboah.quantafs.raft.storage.*;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;

/** Real loopback gRPC transport around the single-threaded, durable consensus core. */
public final class ClusterServer implements AutoCloseable {
    private static final int MAX_PENDING=128, MAX_INFLIGHT_PER_PEER=2;
    private static final long REQUEST_TIMEOUT_SECONDS=5;
    private final ClusterSettings settings;
    private final ThreadPoolExecutor loop = new ThreadPoolExecutor(1,1,0,TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(128),r -> new Thread(r,"raft-events"),new ThreadPoolExecutor.AbortPolicy());
    private final ScheduledExecutorService clock=Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService rpcExecutor=new ThreadPoolExecutor(4,4,0,TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(128),new ThreadPoolExecutor.AbortPolicy());
    private final AtomicBoolean tickQueued=new AtomicBoolean();
    private final Map<NodeId,AtomicInteger> inFlight=new TreeMap<>();
    private final Map<NodeId,ManagedChannel> channels=new TreeMap<>();
    private final Map<Position,Pending> pending=new HashMap<>(); // event-loop ownership
    private final FileChannel ownerChannel;
    private final FileLock ownerLock;
    private RocksDbRaftStorage storage;
    private RocksDbStateMachine machine;
    private RaftNode node;
    private Server server;
    private volatile boolean closing, fatal;
    private record Position(long index,long term) {}
    private record Pending(Runnable success, java.util.function.Consumer<Status> failure, long expires) {}

    private ClusterServer(ClusterSettings settings) throws IOException {
        this.settings=settings;
        Files.createDirectories(settings.directory());
        ownerChannel=FileChannel.open(settings.directory().resolve("node.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
        FileLock acquired;
        try { acquired=ownerChannel.tryLock(); }
        catch (IOException | RuntimeException e) { ownerChannel.close(); throw e; }
        if (acquired==null) { ownerChannel.close(); throw new IOException("node directory is already owned"); }
        ownerLock=acquired;
    }
    public static ClusterServer start(ClusterSettings settings) throws IOException {
        ClusterServer runtime=new ClusterServer(settings);
        try { runtime.initialize(); return runtime; }
        catch (IOException | RuntimeException e) { runtime.close(); throw e; }
    }
    private void initialize() throws IOException {
        Path manifest=settings.directory().resolve("identity");
        String identity=settings.id().value()+"\n"+settings.clusterKey()+"\n";
        if (Files.exists(manifest)) {
            if (!Files.readString(manifest).equals(identity))
                throw new IOException("node identity or fixed membership differs from persisted configuration");
        } else {
            try(var files=Files.list(settings.directory())) {
                if(files.anyMatch(p -> !p.getFileName().toString().equals("node.lock")))
                    throw new IOException("non-empty directory without node identity; use a fresh node directory");
            }
            try(FileChannel file=FileChannel.open(manifest,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)) {
                ByteBuffer buffer=StandardCharsets.UTF_8.encode(identity);
                while(buffer.hasRemaining()) file.write(buffer);
                file.force(true);
            }
            try(FileChannel dir=FileChannel.open(settings.directory(),StandardOpenOption.READ)) { dir.force(true); }
        }
        storage=RocksDbRaftStorage.open(settings.directory().resolve("raft-log"));
        machine=RocksDbStateMachine.open(settings.directory().resolve("state-machine"));
        node=new RaftNode(settings.id(),ClusterConfig.of(settings.ports().keySet()),ElectionTiming.DEFAULT,
                new SeededRandomSource(ThreadLocalRandom.current().nextLong()),storage,machine);
        for(NodeId peer:settings.ports().keySet()) {
            if (!peer.equals(settings.id())) {
                inFlight.put(peer,new AtomicInteger());
                channels.put(peer,ManagedChannelBuilder
                    .forAddress("127.0.0.1",settings.ports().get(peer)).usePlaintext()
                    .maxInboundMessageSize(20*1024*1024).build());
            }
        }
        server=NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1",settings.ports().get(settings.id())))
                .executor(rpcExecutor).maxInboundMessageSize(20*1024*1024)
                .addService(new Transport()).addService(new KeyValues()).build().start();
        clock.scheduleWithFixedDelay(() -> {
            if (tickQueued.compareAndSet(false,true)) {
                if (!enqueue(() -> {
                    try { if(!fatal) send(node.tick()); expire(); }
                    finally { tickQueued.set(false); }
                })) tickQueued.set(false);
            }
        },50,50,TimeUnit.MILLISECONDS);
        System.out.println("QuantaFs cluster node "+settings.id()+" listening on 127.0.0.1:"
                +settings.ports().get(settings.id()));
    }
    private boolean enqueue(Runnable action) {
        if (closing) return false;
        try {
            loop.execute(() -> {
                try {
                    action.run();
                    finishOutcomes();
                    if(node!=null && node.isHalted()) failAll("consensus halted");
                } catch (RuntimeException e) {
                    fatal=true;
                    System.err.println("Cluster node halted: "+e);
                    failAll("node halted; write outcome unknown");
                }
            });
            return true;
        } catch(RejectedExecutionException e) { return false; }
    }
    private void send(List<RaftMessage> messages) {
        if (fatal || node.isHalted()) return;
        for(RaftMessage m:messages) {
            AtomicInteger active=inFlight.get(m.to());
            if(active.incrementAndGet()>MAX_INFLIGHT_PER_PEER) { active.decrementAndGet(); continue; }
            try {
                RaftTransportGrpc.newStub(channels.get(m.to())).withDeadlineAfter(500,TimeUnit.MILLISECONDS)
                        .deliver(WireCodec.encode(settings.clusterKey(),m),new StreamObserver<>() {
                            public void onNext(DeliveryReceipt ignored) {}
                            public void onError(Throwable error) { active.decrementAndGet(); }
                            public void onCompleted() { active.decrementAndGet(); }
                        });
            } catch(RuntimeException e) { active.decrementAndGet(); throw e; }
        }
    }
    private void finishOutcomes() {
        if(node==null) return;
        for(ProposalOutcome o:node.drainOutcomes()) {
            Pending p=pending.remove(new Position(o.index(),o.term()));
            if(p!=null) {
                if(o.status()==ProposalOutcome.Status.COMMITTED) {
                    try { p.success().run(); }
                    catch(RuntimeException e) { p.failure().accept(Status.INTERNAL.withDescription("application read failed")); throw e; }
                } else p.failure().accept(Status.UNAVAILABLE.withDescription("leadership changed; outcome unknown"));
            }
        }
    }
    private void expire() {
        long now=System.nanoTime();
        var iterator=pending.values().iterator();
        while(iterator.hasNext()) {
            Pending p=iterator.next();
            if(now>=p.expires()) {
                iterator.remove();
                p.failure().accept(Status.DEADLINE_EXCEEDED.withDescription("majority confirmation timed out; outcome unknown"));
            }
        }
    }
    private void failAll(String reason) {
        pending.values().forEach(p -> p.failure().accept(Status.UNAVAILABLE.withDescription(reason)));
        pending.clear();
    }
    private <T> void propose(StreamObserver<T> response,Command command,Supplier<T> result) {
        Context context=Context.current();
        AtomicBoolean finished=new AtomicBoolean();
        java.util.function.Consumer<Status> fail=status -> {
            if(finished.compareAndSet(false,true)) error(response,status);
        };
        context.addListener(ignored -> enqueue(() -> {
            pending.values().removeIf(p -> p.failure()==fail);
            fail.accept(Status.CANCELLED.withDescription("request cancelled; outcome unknown"));
        }),Runnable::run);
        if (!enqueue(() -> {
            if(context.isCancelled()) { fail.accept(Status.CANCELLED); return; }
            if(fatal || node.isHalted()) { fail.accept(Status.UNAVAILABLE.withDescription("node halted")); return; }
            // Include orphaned, timed-out proposals until the core resolves them: bounded disk/RAM admission.
            if(pending.size()>=MAX_PENDING || node.pendingProposalCount()>=MAX_PENDING) {
                fail.accept(Status.RESOURCE_EXHAUSTED.withDescription("too many unconfirmed requests")); return;
            }
            ProposeResult accepted;
            try { accepted=command==null ? node.proposeReadBarrier() : node.propose(command); }
            catch(RuntimeException e) {
                fail.accept(Status.UNAVAILABLE.withDescription("node failure; outcome unknown")); throw e;
            }
            switch(accepted) {
                case ProposeResult.Accepted a -> {
                    pending.put(new Position(a.index(),a.term()),new Pending(() -> {
                        T value=result.get();
                        if(finished.compareAndSet(false,true)) respond(response,value);
                    },fail,System.nanoTime()+TimeUnit.SECONDS.toNanos(REQUEST_TIMEOUT_SECONDS)));
                    send(a.messages());
                }
                case ProposeResult.NotLeader n -> fail.accept(Status.UNAVAILABLE.withDescription(
                        "not leader; leader="+n.leaderHint().map(NodeId::value).orElse("unknown")));
                case ProposeResult.Unavailable u -> fail.accept(Status.UNAVAILABLE.withDescription(u.reason()));
                case ProposeResult.OutcomeUnknown u -> fail.accept(Status.UNAVAILABLE.withDescription("write outcome unknown"));
            }
        })) fail.accept(Status.RESOURCE_EXHAUSTED.withDescription("node queue full or shutting down"));
    }
    private static <T> void respond(StreamObserver<T> response,T value) { response.onNext(value); response.onCompleted(); }
    private static void error(StreamObserver<?> response,Status status) { response.onError(status.asRuntimeException()); }

    private final class Transport extends RaftTransportGrpc.RaftTransportImplBase {
        @Override public void deliver(RaftEnvelope envelope,StreamObserver<DeliveryReceipt> response) {
            if(!settings.clusterKey().equals(envelope.getClusterKey())) {
                error(response,Status.FAILED_PRECONDITION.withDescription("different fixed membership")); return;
            }
            RaftMessage message;
            try {
                message=WireCodec.decode(envelope);
                if(!settings.id().equals(message.to()) || !settings.ports().containsKey(message.from()))
                    throw new IllegalArgumentException("invalid sender or receiver");
            } catch(IllegalArgumentException e) { error(response,Status.INVALID_ARGUMENT.withDescription(e.getMessage())); return; }
            if(!enqueue(() -> { if(!fatal) send(node.receive(message)); })) {
                error(response,Status.RESOURCE_EXHAUSTED); return;
            }
            respond(response,DeliveryReceipt.getDefaultInstance());
        }
        @Override public void status(NodeStatusRequest request,StreamObserver<NodeStatus> response) {
            if(!enqueue(() -> respond(response,NodeStatus.newBuilder().setId(settings.id().value())
                    .setRole(node.role().name()).setTerm(node.currentTerm())
                    .setLeader(node.leaderId().map(NodeId::value).orElse(""))
                    .setCommitIndex(node.commitIndex()).setAppliedIndex(node.lastApplied())
                    .setHalted(fatal||node.isHalted()).setPendingRequests(pending.size()).build())))
                error(response,Status.UNAVAILABLE);
        }
    }
    private final class KeyValues extends KeyValueServiceGrpc.KeyValueServiceImplBase {
        @Override public void put(PutRequest r,StreamObserver<PutResponse> response) {
            try { propose(response,Command.put(r.getKey().toByteArray(),r.getValue().toByteArray()),PutResponse::getDefaultInstance); }
            catch(IllegalArgumentException e) { error(response,Status.INVALID_ARGUMENT.withDescription(e.getMessage())); }
        }
        @Override public void delete(DeleteRequest r,StreamObserver<DeleteResponse> response) {
            try { propose(response,Command.delete(r.getKey().toByteArray()),DeleteResponse::getDefaultInstance); }
            catch(IllegalArgumentException e) { error(response,Status.INVALID_ARGUMENT.withDescription(e.getMessage())); }
        }
        @Override public void get(GetRequest r,StreamObserver<GetResponse> response) {
            try {
                Command.delete(r.getKey().toByteArray()); // shared key validation; no delete is proposed
                byte[] key=r.getKey().toByteArray();
                propose(response,null,() -> machine.readApplied(key).map(v -> GetResponse.newBuilder()
                        .setFound(true).setValue(ByteString.copyFrom(v)).build()).orElse(GetResponse.getDefaultInstance()));
            } catch(IllegalArgumentException e) { error(response,Status.INVALID_ARGUMENT.withDescription(e.getMessage())); }
        }
    }
    public void awaitTermination() throws InterruptedException { server.awaitTermination(); }
    @Override public synchronized void close() {
        if(closing) return;
        closing=true;
        clock.shutdownNow();
        if(server!=null) server.shutdownNow();
        channels.values().forEach(ManagedChannel::shutdownNow);
        // Cleanup is queued behind all storage users. Never close native handles under an active request.
        try { loop.execute(() -> failAll("server shutting down; outcome unknown")); }
        catch(RejectedExecutionException ignored) { }
        loop.shutdown();
        try {
            if(!loop.awaitTermination(30,TimeUnit.SECONDS))
                throw new IllegalStateException("event loop did not stop; storage deliberately left open");
            if(server!=null) server.awaitTermination(5,TimeUnit.SECONDS);
            for(ManagedChannel c:channels.values()) c.awaitTermination(2,TimeUnit.SECONDS);
        } catch(InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
        finally { rpcExecutor.shutdownNow(); }
        try { if(machine!=null) machine.close(); }
        finally {
            try { if(storage!=null) storage.close(); }
            finally {
                try { ownerLock.release(); ownerChannel.close(); }
                catch(IOException e) { throw new IllegalStateException("cannot release node directory",e); }
            }
        }
    }
}
