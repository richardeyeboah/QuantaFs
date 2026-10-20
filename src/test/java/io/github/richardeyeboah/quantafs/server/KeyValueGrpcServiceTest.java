package io.github.richardeyeboah.quantafs.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.github.richardeyeboah.quantafs.api.v1.DeleteRequest;
import io.github.richardeyeboah.quantafs.api.v1.GetRequest;
import io.github.richardeyeboah.quantafs.api.v1.GetResponse;
import io.github.richardeyeboah.quantafs.api.v1.KeyValueServiceGrpc;
import io.github.richardeyeboah.quantafs.api.v1.KeyValueServiceGrpc.KeyValueServiceBlockingStub;
import io.github.richardeyeboah.quantafs.api.v1.PutRequest;
import io.github.richardeyeboah.quantafs.storage.KeyValueStore;
import io.github.richardeyeboah.quantafs.storage.RocksDbKeyValueStore;
import io.github.richardeyeboah.quantafs.storage.StorageException;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

/** Exercises the service end to end through a real gRPC server and client over loopback TCP. */
class KeyValueGrpcServiceTest {

    @TempDir Path dataDir;

    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach
    void cleanUp() throws Exception {
        for (int i = resources.size() - 1; i >= 0; i--) {
            resources.get(i).close();
        }
    }

    @Test
    void putOverwritesPreviousValue() throws IOException {
        KeyValueServiceBlockingStub stub = connect(startServer());
        put(stub, "color", "red");
        put(stub, "color", "blue");
        assertEquals(found("blue"), get(stub, "color"));
    }

    @Test
    void missingKeyReturnsFoundFalse() throws IOException {
        KeyValueServiceBlockingStub stub = connect(startServer());
        GetResponse response = get(stub, "absent");
        assertFalse(response.getFound());
        assertTrue(response.getValue().isEmpty());
    }

    @Test
    void emptyValueIsFoundAndDistinctFromMissing() throws IOException {
        KeyValueServiceBlockingStub stub = connect(startServer());
        put(stub, "empty", "");
        assertEquals(found(""), get(stub, "empty"));
    }

    @Test
    void deleteRemovesKeyAndIsIdempotent() throws IOException {
        KeyValueServiceBlockingStub stub = connect(startServer());
        put(stub, "k", "v");
        delete(stub, "k");
        assertFalse(get(stub, "k").getFound());
        delete(stub, "k");
        delete(stub, "never-existed");
        assertFalse(get(stub, "k").getFound());
    }

    @Test
    void binaryKeysAndValuesRoundTrip() throws IOException {
        KeyValueServiceBlockingStub stub = connect(startServer());
        ByteString key = ByteString.copyFrom(new byte[] {0, (byte) 0xFF, 0x10});
        ByteString value = ByteString.copyFrom(new byte[] {(byte) 0xC3, 0x28, 0, (byte) 0x80});
        stub.put(PutRequest.newBuilder().setKey(key).setValue(value).build());
        GetResponse response = stub.get(GetRequest.newBuilder().setKey(key).build());
        assertTrue(response.getFound());
        assertEquals(value, response.getValue());
    }

    @Test
    void largestAllowedKeyAndValueAreAccepted() throws IOException {
        KeyValueServiceBlockingStub stub = connect(startServer());
        ByteString key = filled(RequestValidator.MAX_KEY_BYTES);
        ByteString value = filled(RequestValidator.MAX_VALUE_BYTES);
        stub.put(PutRequest.newBuilder().setKey(key).setValue(value).build());
        assertEquals(value, stub.get(GetRequest.newBuilder().setKey(key).build()).getValue());
    }

    @Test
    void invalidInputIsRejectedWithInvalidArgument() throws IOException {
        KeyValueServiceBlockingStub stub = connect(startServer());
        ByteString oversizedKey = filled(RequestValidator.MAX_KEY_BYTES + 1);
        ByteString oversizedValue = filled(RequestValidator.MAX_VALUE_BYTES + 1);

        assertStatus(Status.Code.INVALID_ARGUMENT, () -> stub.put(PutRequest.newBuilder().setValue(bytes("v")).build()));
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> stub.get(GetRequest.getDefaultInstance()));
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> stub.delete(DeleteRequest.getDefaultInstance()));
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> stub.get(GetRequest.newBuilder().setKey(oversizedKey).build()));
        assertStatus(
                Status.Code.INVALID_ARGUMENT,
                () -> stub.put(PutRequest.newBuilder().setKey(bytes("k")).setValue(oversizedValue).build()));

        assertFalse(get(stub, "k").getFound(), "rejected put must not be applied");
    }

    @Test
    void messageAboveTransportLimitIsRejectedWithResourceExhausted() throws IOException {
        KeyValueServiceBlockingStub stub = connect(startServer());
        ByteString huge = filled(RequestValidator.MAX_INBOUND_MESSAGE_BYTES + 1);
        assertStatus(
                Status.Code.RESOURCE_EXHAUSTED,
                () -> stub.put(PutRequest.newBuilder().setKey(bytes("k")).setValue(huge).build()));
    }

    @Test
    void dataPersistsAcrossServerRestart() throws IOException {
        QuantaServer first = startServer();
        KeyValueServiceBlockingStub stub = connect(first);
        put(stub, "kept", "v1");
        put(stub, "overwritten", "old");
        put(stub, "overwritten", "new");
        put(stub, "deleted", "x");
        delete(stub, "deleted");
        first.shutdown();

        KeyValueServiceBlockingStub restarted = connect(startServer());
        assertEquals(found("v1"), get(restarted, "kept"));
        assertEquals(found("new"), get(restarted, "overwritten"));
        assertFalse(get(restarted, "deleted").getFound());
    }

    @Test
    void storageFailureMapsToInternalWithoutLeakingDetails() throws IOException {
        KeyValueStore failing =
                new KeyValueStore() {
                    @Override
                    public Optional<byte[]> get(byte[] key) {
                        throw new StorageException("disk on fire at /secret/path");
                    }

                    @Override
                    public void put(byte[] key, byte[] value) {
                        throw new StorageException("disk on fire at /secret/path");
                    }

                    @Override
                    public void delete(byte[] key) {
                        throw new StorageException("disk on fire at /secret/path");
                    }

                    @Override
                    public void close() {}
                };
        KeyValueServiceBlockingStub stub = connect(startServer(failing));

        StatusRuntimeException e =
                assertThrows(StatusRuntimeException.class, () -> put(stub, "k", "v"));
        assertEquals(Status.Code.INTERNAL, e.getStatus().getCode());
        assertEquals("storage failure", e.getStatus().getDescription());
        assertStatus(Status.Code.INTERNAL, () -> get(stub, "k"));
        assertStatus(Status.Code.INTERNAL, () -> delete(stub, "k"));
    }

    @Test
    void closedStoreMapsToUnavailable() throws IOException {
        RocksDbKeyValueStore store = RocksDbKeyValueStore.open(dataDir);
        KeyValueServiceBlockingStub stub = connect(startServer(store));
        store.close();
        assertStatus(Status.Code.UNAVAILABLE, () -> put(stub, "k", "v"));
        assertStatus(Status.Code.UNAVAILABLE, () -> get(stub, "k"));
    }

    private QuantaServer startServer() throws IOException {
        return track(QuantaServer.start(config()));
    }

    private QuantaServer startServer(KeyValueStore store) throws IOException {
        return track(QuantaServer.start(config(), store));
    }

    private ServerConfig config() {
        return new ServerConfig("127.0.0.1", 0, dataDir, Duration.ofSeconds(5));
    }

    private QuantaServer track(QuantaServer server) {
        resources.add(server);
        return server;
    }

    private KeyValueServiceBlockingStub connect(QuantaServer server) {
        ManagedChannel channel =
                Grpc.newChannelBuilder("127.0.0.1:" + server.port(), InsecureChannelCredentials.create())
                        .maxInboundMessageSize(4 * 1024 * 1024)
                        .build();
        resources.add(
                () -> {
                    channel.shutdownNow();
                    channel.awaitTermination(5, TimeUnit.SECONDS);
                });
        return KeyValueServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS);
    }

    private static void put(KeyValueServiceBlockingStub stub, String key, String value) {
        stub.put(PutRequest.newBuilder().setKey(bytes(key)).setValue(bytes(value)).build());
    }

    private static GetResponse get(KeyValueServiceBlockingStub stub, String key) {
        return stub.get(GetRequest.newBuilder().setKey(bytes(key)).build());
    }

    private static void delete(KeyValueServiceBlockingStub stub, String key) {
        stub.delete(DeleteRequest.newBuilder().setKey(bytes(key)).build());
    }

    private static GetResponse found(String value) {
        return GetResponse.newBuilder().setFound(true).setValue(bytes(value)).build();
    }

    private static ByteString bytes(String s) {
        return ByteString.copyFromUtf8(s);
    }

    private static ByteString filled(int size) {
        byte[] data = new byte[size];
        Arrays.fill(data, (byte) 'x');
        return ByteString.copyFrom(data);
    }

    private static void assertStatus(Status.Code expected, Executable call) {
        StatusRuntimeException e = assertThrows(StatusRuntimeException.class, call);
        assertEquals(expected, e.getStatus().getCode(), String.valueOf(e.getStatus()));
    }
}
