package io.github.richardeyeboah.quantafs.raft.storage;

import io.github.richardeyeboah.quantafs.raft.LogEntry;
import io.github.richardeyeboah.quantafs.raft.PersistenceException;
import io.github.richardeyeboah.quantafs.raft.RaftStorage;
import io.github.richardeyeboah.quantafs.raft.StorageWrite;
import io.github.richardeyeboah.quantafs.raft.TermAndVote;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WALRecoveryMode;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Durable {@link RaftStorage} in a dedicated RocksDB database, separate from application data.
 *
 * <p><b>Why RocksDB:</b> a {@link WriteBatch} commits several keys atomically through one
 * write-ahead-log record, so a term/vote change, a truncation, and appended entries become durable
 * together or not at all. With {@code sync=true}, RocksDB syncs that WAL record before
 * {@code write} returns. RocksDB also checksums its files and holds an OS lock on its directory. A
 * hand-written file format would have to reimplement all of this.
 *
 * <p><b>Layout:</b> key {@code 0x01} holds the encoded {@link TermAndVote}; key
 * {@code 0x02 + 8-byte big-endian index} holds each log entry, so RocksDB's key order is log order.
 * Every value carries its own CRC32 ({@link RecordCodec}).
 *
 * <p><b>Recovery:</b> opening scans the whole log and refuses to start ({@link PersistenceException})
 * on a checksum failure, a gap in indexes, an entry stored under the wrong index, decreasing terms, a
 * log without term/vote metadata, or a last entry from a term later than the stored term. The WAL
 * recovery mode tolerates only an incomplete final record (a write that never finished, and so was
 * never acknowledged) and fails on corruption anywhere else, rather than silently dropping
 * acknowledged writes after a damaged record.
 *
 * <p><b>Limits:</b> durability assumes the OS and device honor fsync (on macOS fsync does not flush
 * the drive cache). No compaction or snapshots: the log only grows.
 */
public final class RocksDbRaftStorage implements RaftStorage, AutoCloseable {

    static {
        RocksDB.loadLibrary();
    }

    static final byte[] TERM_AND_VOTE_KEY = {0x01};
    static final byte LOG_PREFIX = 0x02;

    private final Path directory;
    private final Options options;
    private final WriteOptions syncWrites;
    private final RocksDB db;
    private TermAndVote termAndVote; // guarded by this
    private final List<Long> terms = new ArrayList<>(); // terms.get(i) is the term of index i + 1
    private boolean failed;
    private boolean closed;

    private RocksDbRaftStorage(Path directory, Options options, WriteOptions syncWrites, RocksDB db) {
        this.directory = directory;
        this.options = options;
        this.syncWrites = syncWrites;
        this.db = db;
    }

    /**
     * Opens (creating if needed) the storage in {@code directory} and validates everything in it.
     *
     * @throws PersistenceException if it is locked by another process, unreadable, or inconsistent
     */
    public static RocksDbRaftStorage open(Path directory) {
        Objects.requireNonNull(directory, "directory");
        Path dir = directory.toAbsolutePath().normalize();
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new PersistenceException("cannot create Raft storage directory " + dir, e);
        }
        Options options = new Options();
        options.setCreateIfMissing(true);
        options.setParanoidChecks(true);
        options.setWalRecoveryMode(WALRecoveryMode.TolerateCorruptedTailRecords);
        WriteOptions syncWrites = new WriteOptions();
        syncWrites.setSync(true);
        RocksDB db;
        try {
            db = RocksDB.open(options, dir.toString());
        } catch (RocksDBException e) {
            syncWrites.close();
            options.close();
            throw new PersistenceException("cannot open Raft storage at " + dir + ": " + e.getMessage(), e);
        }
        RocksDbRaftStorage storage = new RocksDbRaftStorage(dir, options, syncWrites, db);
        try {
            storage.load();
            return storage;
        } catch (RuntimeException e) {
            storage.close();
            throw e;
        }
    }

    private void load() {
        TermAndVote stored = null;
        try (RocksIterator it = db.newIterator()) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                byte[] key = it.key();
                if (key.length == 1 && key[0] == TERM_AND_VOTE_KEY[0]) {
                    stored = RecordCodec.decodeTermAndVote(it.value());
                } else if (key.length == 9 && key[0] == LOG_PREFIX) {
                    long index = ByteBuffer.wrap(key, 1, 8).getLong();
                    long expected = terms.size() + 1;
                    if (index != expected) {
                        throw corrupt("log has a gap: expected index " + expected + ", found " + index);
                    }
                    LogEntry entry = RecordCodec.decodeEntry(it.value());
                    if (entry.index() != index) {
                        throw corrupt("entry " + entry.index() + " stored under index " + index);
                    }
                    if (!terms.isEmpty() && entry.term() < terms.getLast()) {
                        throw corrupt("entry terms decrease at index " + index);
                    }
                    terms.add(entry.term());
                } else {
                    throw corrupt("unexpected key of length " + key.length);
                }
            }
            it.status();
        } catch (IOException | IllegalArgumentException e) {
            throw corrupt(e.getMessage(), e);
        } catch (RocksDBException e) {
            throw new PersistenceException("cannot read Raft storage at " + directory + ": " + e.getMessage(), e);
        }
        if (stored == null) {
            if (!terms.isEmpty()) {
                throw corrupt("log entries exist without term/vote metadata");
            }
            stored = TermAndVote.INITIAL;
        }
        if (!terms.isEmpty() && terms.getLast() > stored.currentTerm()) {
            throw corrupt("last entry term " + terms.getLast() + " exceeds stored term " + stored.currentTerm());
        }
        termAndVote = stored;
    }

    public Path directory() {
        return directory;
    }

    @Override
    public synchronized TermAndVote termAndVote() {
        ensureUsable();
        return termAndVote;
    }

    @Override
    public synchronized long lastIndex() {
        ensureUsable();
        return terms.size();
    }

    @Override
    public synchronized long termAt(long index) {
        ensureUsable();
        RaftStorage.checkIndex(index, terms.size());
        return index == 0 ? 0 : terms.get((int) index - 1);
    }

    @Override
    public synchronized LogEntry entry(long index) {
        ensureUsable();
        if (index < 1 || index > terms.size()) {
            throw new IllegalArgumentException("no entry at index " + index + "; log ends at " + terms.size());
        }
        try {
            byte[] value = db.get(logKey(index));
            if (value == null) {
                throw corrupt("entry " + index + " disappeared");
            }
            LogEntry entry = RecordCodec.decodeEntry(value);
            if (entry.index() != index || entry.term() != terms.get((int) index - 1)) {
                throw corrupt("entry " + index + " does not match the log loaded at startup");
            }
            return entry;
        } catch (IOException e) {
            failed = true;
            throw corrupt("entry " + index + ": " + e.getMessage(), e);
        } catch (RocksDBException e) {
            failed = true;
            throw new PersistenceException("cannot read entry " + index + ": " + e.getMessage(), e);
        }
    }

    @Override
    public synchronized List<LogEntry> entries(long from, long to) {
        List<LogEntry> result = new ArrayList<>();
        for (long i = from; i <= to; i++) {
            result.add(entry(i));
        }
        return result;
    }

    @Override
    public synchronized void write(StorageWrite write) {
        Objects.requireNonNull(write, "write");
        ensureUsable();
        try {
            write.validate(termAndVote, terms.size(), this::termAt);
        } catch (IllegalArgumentException e) {
            throw new PersistenceException("rejected unsafe write: " + e.getMessage(), e);
        }
        try (WriteBatch batch = new WriteBatch()) {
            if (write.termAndVote().isPresent()) {
                batch.put(TERM_AND_VOTE_KEY, RecordCodec.encodeTermAndVote(write.termAndVote().get()));
            }
            if (write.truncateFrom() > 0) {
                for (long i = write.truncateFrom(); i <= terms.size(); i++) {
                    batch.delete(logKey(i));
                }
            }
            for (LogEntry entry : write.append()) {
                batch.put(logKey(entry.index()), RecordCodec.encodeEntry(entry));
            }
            db.write(syncWrites, batch); // one WAL record, synced before returning
        } catch (RocksDBException e) {
            failed = true;
            throw new PersistenceException("failed to persist Raft state in " + directory + ": " + e.getMessage(), e);
        }
        write.termAndVote().ifPresent(next -> termAndVote = next);
        if (write.truncateFrom() > 0) {
            terms.subList((int) write.truncateFrom() - 1, terms.size()).clear();
        }
        write.append().forEach(entry -> terms.add(entry.term()));
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            db.closeE();
        } catch (RocksDBException e) {
            throw new PersistenceException("failed to close Raft storage: " + e.getMessage(), e);
        } finally {
            syncWrites.close();
            options.close();
        }
    }

    private void ensureUsable() {
        if (closed) {
            throw new PersistenceException("Raft storage is closed");
        }
        if (failed) {
            throw new PersistenceException("an earlier operation failed; on-disk state is unknown, reopen the storage");
        }
    }

    static byte[] logKey(long index) {
        return ByteBuffer.allocate(9).put(LOG_PREFIX).putLong(index).array();
    }

    private PersistenceException corrupt(String detail) {
        return new PersistenceException("Raft storage at " + directory + " is corrupt; refusing to start: " + detail);
    }

    private PersistenceException corrupt(String detail, Throwable cause) {
        return new PersistenceException("Raft storage at " + directory + " is corrupt; refusing to start: " + detail, cause);
    }
}
