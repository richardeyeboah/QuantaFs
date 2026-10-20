package io.github.richardeyeboah.quantafs.storage;

import java.util.Optional;

/**
 * Local, durable key-value storage, independent of any network protocol.
 *
 * <p>Implementations must be safe for concurrent use. A {@link #put} or {@link #delete} that
 * returns normally is durable according to the implementation's documented synchronization
 * policy. Any method may throw {@link StorageException}; after a failed write the caller must
 * treat the outcome as unknown. Methods called after {@link #close} throw
 * {@link StoreClosedException}.
 */
public interface KeyValueStore extends AutoCloseable {

    /** Returns a copy of the current value, or empty if the key does not exist. */
    Optional<byte[]> get(byte[] key);

    /** Creates or overwrites the value for {@code key}. */
    void put(byte[] key, byte[] value);

    /** Removes {@code key}. Removing a missing key is not an error. */
    void delete(byte[] key);

    /** Releases all resources. Idempotent. Waits for in-progress operations to finish. */
    @Override
    void close();
}
