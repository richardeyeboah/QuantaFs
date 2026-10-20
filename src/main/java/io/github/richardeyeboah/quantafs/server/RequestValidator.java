package io.github.richardeyeboah.quantafs.server;

import com.google.protobuf.ByteString;
import io.grpc.Status;

/**
 * API-level input limits. Requests are validated before they reach storage, so a request that is
 * accepted can always be applied.
 */
public final class RequestValidator {

    public static final int MAX_KEY_BYTES = 1024;
    public static final int MAX_VALUE_BYTES = 1024 * 1024;

    /**
     * Upper bound for a whole request message on the wire. Larger than key + value limits so that
     * slightly oversized values get a descriptive INVALID_ARGUMENT; anything beyond this is
     * rejected by gRPC itself with RESOURCE_EXHAUSTED before it is buffered.
     */
    public static final int MAX_INBOUND_MESSAGE_BYTES = 2 * 1024 * 1024;

    private RequestValidator() {}

    static void validateKey(ByteString key) {
        if (key.isEmpty()) {
            throw Status.INVALID_ARGUMENT.withDescription("key must not be empty").asRuntimeException();
        }
        if (key.size() > MAX_KEY_BYTES) {
            throw Status.INVALID_ARGUMENT
                    .withDescription(
                            "key is " + key.size() + " bytes; maximum is " + MAX_KEY_BYTES)
                    .asRuntimeException();
        }
    }

    static void validateValue(ByteString value) {
        if (value.size() > MAX_VALUE_BYTES) {
            throw Status.INVALID_ARGUMENT
                    .withDescription(
                            "value is " + value.size() + " bytes; maximum is " + MAX_VALUE_BYTES)
                    .asRuntimeException();
        }
    }
}
