package io.github.richardeyeboah.quantafs.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

/**
 * {@link KeyValueStore} backed by an embedded RocksDB instance in a single data directory.
 *
 * <p><b>Synchronization policy:</b> every put and delete uses {@code WriteOptions.setSync(true)}.
 * RocksDB appends the write to its write-ahead log (WAL) and syncs the WAL file to the storage
 * device (fsync/fdatasync) before the call returns. A write that returns normally therefore
 * survives a process crash, and survives an OS crash or power loss only to the extent that the
 * OS, filesystem, and device honor sync requests. On reopen, RocksDB replays the WAL.
 *
 * <p><b>Lifecycle safety:</b> RocksDB handles are native memory; using one after close can crash
 * the JVM. Operations hold a shared lock and {@link #close} takes the exclusive lock, so close
 * waits for in-progress operations and later operations fail with {@link StoreClosedException}.
 *
 * <p>RocksDB holds an OS file lock on the data directory, so a second open of the same directory
 * (in this or another process) fails instead of corrupting data.
 */
public final class RocksDbKeyValueStore implements KeyValueStore {

    static {
        RocksDB.loadLibrary();
    }

    private final Path dataDir;
    private final Options options;
    private final WriteOptions syncWriteOptions;
    private final RocksDB db;

    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
    private boolean closed; // guarded by lifecycleLock

    private RocksDbKeyValueStore(
            Path dataDir, Options options, WriteOptions syncWriteOptions, RocksDB db) {
        this.dataDir = dataDir;
        this.options = options;
        this.syncWriteOptions = syncWriteOptions;
        this.db = db;
    }

    /** Opens the store in {@code dataDir}, creating the directory and database if missing. */
    public static RocksDbKeyValueStore open(Path dataDir) {
        Objects.requireNonNull(dataDir, "dataDir");
        Path dir = dataDir.toAbsolutePath().normalize();
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new StorageException("cannot create data directory " + dir, e);
        }

        Options options = new Options().setCreateIfMissing(true);
        WriteOptions syncWriteOptions = new WriteOptions().setSync(true);
        try {
            RocksDB db = RocksDB.open(options, dir.toString());
            return new RocksDbKeyValueStore(dir, options, syncWriteOptions, db);
        } catch (RocksDBException e) {
            syncWriteOptions.close();
            options.close();
            throw new StorageException("cannot open RocksDB at " + dir + ": " + e.getMessage(), e);
        }
    }

    public Path dataDir() {
        return dataDir;
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
        Objects.requireNonNull(key, "key");
        Lock lock = acquireOpen();
        try {
            return Optional.ofNullable(db.get(key));
        } catch (RocksDBException e) {
            throw new StorageException("get failed: " + e.getMessage(), e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        Lock lock = acquireOpen();
        try {
            db.put(syncWriteOptions, key, value);
        } catch (RocksDBException e) {
            throw new StorageException("put failed: " + e.getMessage(), e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(byte[] key) {
        Objects.requireNonNull(key, "key");
        Lock lock = acquireOpen();
        try {
            db.delete(syncWriteOptions, key);
        } catch (RocksDBException e) {
            throw new StorageException("delete failed: " + e.getMessage(), e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        Lock lock = lifecycleLock.writeLock();
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            try {
                db.closeE();
            } catch (RocksDBException e) {
                throw new StorageException("close failed: " + e.getMessage(), e);
            } finally {
                syncWriteOptions.close();
                options.close();
            }
        } finally {
            lock.unlock();
        }
    }

    private Lock acquireOpen() {
        Lock lock = lifecycleLock.readLock();
        lock.lock();
        if (closed) {
            lock.unlock();
            throw new StoreClosedException();
        }
        return lock;
    }
}
