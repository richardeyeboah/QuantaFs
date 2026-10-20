package io.github.richardeyeboah.quantafs.raft.storage;

import io.github.richardeyeboah.quantafs.raft.PersistenceException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * Everything one Raft node keeps on disk, under a single directory:
 *
 * <pre>
 * node-dir/
 *   node.lock        OS lock held while the node is open
 *   raft-log/        RocksDB: currentTerm, votedFor, log entries   ({@link RocksDbRaftStorage})
 *   state-machine/   RocksDB: user data + lastApplied marker       ({@link RocksDbStateMachine})
 * </pre>
 *
 * <p><b>Single owner:</b> opening takes an exclusive OS file lock on {@code node.lock}
 * ({@link FileChannel#tryLock()}). A second open fails, whether from another process or from this
 * JVM, instead of letting two nodes interleave writes to one log. The OS releases the lock when the
 * process dies, so a crashed node can restart without manual cleanup. Each RocksDB database also
 * holds its own lock.
 */
public final class NodeStorage implements AutoCloseable {

    public static final String LOCK_FILE = "node.lock";
    public static final String RAFT_LOG_DIR = "raft-log";
    public static final String STATE_MACHINE_DIR = "state-machine";

    private final Path directory;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final RocksDbRaftStorage raftStorage;
    private final RocksDbStateMachine stateMachine;
    private boolean closed;

    private NodeStorage(Path directory, FileChannel lockChannel, FileLock lock,
            RocksDbRaftStorage raftStorage, RocksDbStateMachine stateMachine) {
        this.directory = directory;
        this.lockChannel = lockChannel;
        this.lock = lock;
        this.raftStorage = raftStorage;
        this.stateMachine = stateMachine;
    }

    /** Locks {@code directory} and opens both databases, creating them if needed. */
    public static NodeStorage open(Path directory) {
        Objects.requireNonNull(directory, "directory");
        Path dir = directory.toAbsolutePath().normalize();
        FileChannel channel = null;
        FileLock lock = null;
        RocksDbRaftStorage raft = null;
        try {
            Files.createDirectories(dir);
            channel = FileChannel.open(dir.resolve(LOCK_FILE), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException e) {
                throw new PersistenceException("node directory " + dir + " is already open in this process");
            }
            if (lock == null) {
                throw new PersistenceException("node directory " + dir + " is locked by another process");
            }
            raft = RocksDbRaftStorage.open(dir.resolve(RAFT_LOG_DIR));
            RocksDbStateMachine machine = RocksDbStateMachine.open(dir.resolve(STATE_MACHINE_DIR));
            return new NodeStorage(dir, channel, lock, raft, machine);
        } catch (IOException e) {
            closeQuietly(raft, lock, channel);
            throw new PersistenceException("cannot open node directory " + dir, e);
        } catch (RuntimeException e) {
            closeQuietly(raft, lock, channel);
            throw e;
        }
    }

    public Path directory() {
        return directory;
    }

    public RocksDbRaftStorage raftStorage() {
        return raftStorage;
    }

    public RocksDbStateMachine stateMachine() {
        return stateMachine;
    }

    /** Closes both databases, then releases the lock. */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            stateMachine.close();
        } finally {
            try {
                raftStorage.close();
            } finally {
                closeQuietly(null, lock, lockChannel);
            }
        }
    }

    private static void closeQuietly(RocksDbRaftStorage raft, FileLock lock, FileChannel channel) {
        if (raft != null) {
            try {
                raft.close();
            } catch (RuntimeException ignored) {
                // already failing; the original error is more useful
            }
        }
        try {
            if (lock != null) {
                lock.release();
            }
            if (channel != null) {
                channel.close();
            }
        } catch (IOException ignored) {
            // the OS releases the lock when the channel or process goes away
        }
    }
}
