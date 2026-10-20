package io.github.richardeyeboah.quantafs.server;

import com.google.protobuf.ByteString;
import io.github.richardeyeboah.quantafs.api.v1.DeleteRequest;
import io.github.richardeyeboah.quantafs.api.v1.DeleteResponse;
import io.github.richardeyeboah.quantafs.api.v1.GetRequest;
import io.github.richardeyeboah.quantafs.api.v1.GetResponse;
import io.github.richardeyeboah.quantafs.api.v1.KeyValueServiceGrpc;
import io.github.richardeyeboah.quantafs.api.v1.PutRequest;
import io.github.richardeyeboah.quantafs.api.v1.PutResponse;
import io.github.richardeyeboah.quantafs.storage.KeyValueStore;
import io.github.richardeyeboah.quantafs.storage.StorageException;
import io.github.richardeyeboah.quantafs.storage.StoreClosedException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * gRPC adapter: validates requests, delegates to {@link KeyValueStore}, and maps failures to gRPC
 * status codes. Contains no storage logic. A response is sent only after the store call returns,
 * so a successful write response implies the store's durability policy has been met.
 */
final class KeyValueGrpcService extends KeyValueServiceGrpc.KeyValueServiceImplBase {

    private static final Logger LOG = Logger.getLogger(KeyValueGrpcService.class.getName());

    private final KeyValueStore store;

    KeyValueGrpcService(KeyValueStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        respond(
                "Put",
                responseObserver,
                () -> {
                    RequestValidator.validateKey(request.getKey());
                    RequestValidator.validateValue(request.getValue());
                    store.put(request.getKey().toByteArray(), request.getValue().toByteArray());
                    return PutResponse.getDefaultInstance();
                });
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        respond(
                "Get",
                responseObserver,
                () -> {
                    RequestValidator.validateKey(request.getKey());
                    return store.get(request.getKey().toByteArray())
                            .map(
                                    value ->
                                            GetResponse.newBuilder()
                                                    .setFound(true)
                                                    .setValue(ByteString.copyFrom(value))
                                                    .build())
                            .orElse(GetResponse.newBuilder().setFound(false).build());
                });
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        respond(
                "Delete",
                responseObserver,
                () -> {
                    RequestValidator.validateKey(request.getKey());
                    store.delete(request.getKey().toByteArray());
                    return DeleteResponse.getDefaultInstance();
                });
    }

    private static <T> void respond(
            String method, StreamObserver<T> responseObserver, Supplier<T> handler) {
        T response;
        try {
            response = handler.get();
        } catch (RuntimeException e) {
            responseObserver.onError(toStatus(method, e));
            return;
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static StatusRuntimeException toStatus(String method, RuntimeException e) {
        if (e instanceof StatusRuntimeException statusException) {
            return statusException;
        }
        if (e instanceof StoreClosedException) {
            return Status.UNAVAILABLE.withDescription("server is shutting down").asRuntimeException();
        }
        if (e instanceof StorageException) {
            LOG.log(Level.SEVERE, method + " failed in storage", e);
            return Status.INTERNAL.withDescription("storage failure").asRuntimeException();
        }
        LOG.log(Level.SEVERE, method + " failed unexpectedly", e);
        return Status.INTERNAL.withDescription("internal error").asRuntimeException();
    }
}
