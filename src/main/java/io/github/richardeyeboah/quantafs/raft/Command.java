package io.github.richardeyeboah.quantafs.raft;

import java.util.Objects;

/**
 * An operation recorded in the replicated log. {@link Put} and {@link Delete} change the key-value
 * state machine; {@link NoOp} is internal: a new leader appends one so that it has an entry from its
 * own term to commit (Raft paper, section 8).
 *
 * <p>Keys and values are arbitrary bytes, validated against the same limits as the single-node API
 * when a command is created. An invalid command therefore cannot be proposed or logged.
 */
public sealed interface Command permits Command.Put, Command.Delete, Command.NoOp, Command.Request {

    int MAX_KEY_BYTES = 1024;
    int MAX_VALUE_BYTES = 1024 * 1024;

    static Put put(byte[] key, byte[] value) {
        return new Put(Bytes.copyOf(key), Bytes.copyOf(value));
    }

    static Delete delete(byte[] key) {
        return new Delete(Bytes.copyOf(key));
    }

    static NoOp noOp() {
        return NoOp.INSTANCE;
    }

    /** Creates or overwrites {@code key}. */
    record Put(Bytes key, Bytes value) implements Command {
        public Put {
            validateKey(key);
            Objects.requireNonNull(value, "value");
            if (value.size() > MAX_VALUE_BYTES) {
                throw new IllegalArgumentException(
                        "value is " + value.size() + " bytes; maximum is " + MAX_VALUE_BYTES);
            }
        }

        @Override
        public String toString() {
            return "PUT " + key + "=" + value;
        }
    }

    /** Removes {@code key}; removing a missing key is not an error. */
    record Delete(Bytes key) implements Command {
        public Delete {
            validateKey(key);
        }

        @Override
        public String toString() {
            return "DELETE " + key;
        }
    }

    /** Changes no application data; only advances the state machine's applied index. */
    final class NoOp implements Command {
        static final NoOp INSTANCE = new NoOp();

        private NoOp() {}

        @Override
        public String toString() {
            return "NO_OP";
        }
    }

    /** One sequential mutation session. The same identity must always carry the same payload. */
    record Request(String clientId, long sequence, Command mutation) implements Command {
        public Request {
            if (clientId == null || !clientId.matches("[A-Za-z0-9_.-]{1,64}") || sequence < 1)
                throw new IllegalArgumentException("client ID and positive sequence are required");
            if (!(mutation instanceof Put) && !(mutation instanceof Delete))
                throw new IllegalArgumentException("only put/delete can be session mutations");
        }
    }

    private static void validateKey(Bytes key) {
        Objects.requireNonNull(key, "key");
        if (key.size() == 0) {
            throw new IllegalArgumentException("key must not be empty");
        }
        if (key.size() > MAX_KEY_BYTES) {
            throw new IllegalArgumentException("key is " + key.size() + " bytes; maximum is " + MAX_KEY_BYTES);
        }
    }
}
