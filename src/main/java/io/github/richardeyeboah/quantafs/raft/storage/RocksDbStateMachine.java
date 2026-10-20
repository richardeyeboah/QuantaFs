package io.github.richardeyeboah.quantafs.raft.storage;

import io.github.richardeyeboah.quantafs.raft.Bytes;
import io.github.richardeyeboah.quantafs.raft.ClientSession;
import io.github.richardeyeboah.quantafs.raft.Command;
import io.github.richardeyeboah.quantafs.raft.LogEntry;
import io.github.richardeyeboah.quantafs.raft.LogPosition;
import io.github.richardeyeboah.quantafs.raft.StateMachine;
import io.github.richardeyeboah.quantafs.raft.StateMachineException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WALRecoveryMode;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * RocksDB-backed key-value {@link StateMachine}.
 *
 * <p>User data lives in the {@code default} column family; the {@code lastApplied} marker lives in a
 * separate {@code raft-meta} column family. Column families are separate key spaces inside one
 * database, so no user key, whatever its bytes, can collide with internal metadata, while one
 * {@link WriteBatch} can still update both atomically.
 *
 * <p>Each apply writes the command's effect (nothing for {@code NO_OP}) and the new marker in one
 * synced batch. After any crash the database therefore shows exactly the entries up to its marker:
 * on restart, entries after the marker are applied, and entries up to it never again.
 */
public final class RocksDbStateMachine implements StateMachine, AutoCloseable {

    static {
        RocksDB.loadLibrary();
    }

    static final byte[] META_COLUMN_FAMILY = "raft-meta".getBytes(StandardCharsets.UTF_8);
    static final byte[] LAST_APPLIED_KEY = "last-applied".getBytes(StandardCharsets.UTF_8);

    private final Path directory;
    private final DBOptions options;
    private final ColumnFamilyOptions columnOptions;
    private final WriteOptions syncWrites;
    private final RocksDB db;
    private final ColumnFamilyHandle data;
    private final ColumnFamilyHandle meta;
    private final List<ColumnFamilyHandle> handles;
    private LogPosition lastApplied; // guarded by this
    private boolean failed;
    private boolean closed;

    private RocksDbStateMachine(Path directory, DBOptions options, ColumnFamilyOptions columnOptions,
            WriteOptions syncWrites, RocksDB db, List<ColumnFamilyHandle> handles) {
        this.directory = directory;
        this.options = options;
        this.columnOptions = columnOptions;
        this.syncWrites = syncWrites;
        this.db = db;
        this.handles = handles;
        this.data = handles.get(0);
        this.meta = handles.get(1);
    }

    /** Opens (creating if needed) the state machine in {@code directory}. */
    public static RocksDbStateMachine open(Path directory) {
        Objects.requireNonNull(directory, "directory");
        Path dir = directory.toAbsolutePath().normalize();
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new StateMachineException("cannot create state machine directory " + dir, e);
        }
        DBOptions options = new DBOptions();
        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);
        options.setParanoidChecks(true);
        options.setWalRecoveryMode(WALRecoveryMode.TolerateCorruptedTailRecords);
        ColumnFamilyOptions columnOptions = new ColumnFamilyOptions();
        WriteOptions syncWrites = new WriteOptions();
        syncWrites.setSync(true);
        List<ColumnFamilyDescriptor> descriptors = List.of(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnOptions),
                new ColumnFamilyDescriptor(META_COLUMN_FAMILY, columnOptions));
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        RocksDB db;
        try {
            db = RocksDB.open(options, dir.toString(), descriptors, handles);
        } catch (RocksDBException e) {
            syncWrites.close();
            columnOptions.close();
            options.close();
            throw new StateMachineException("cannot open state machine at " + dir + ": " + e.getMessage(), e);
        }
        RocksDbStateMachine machine = new RocksDbStateMachine(dir, options, columnOptions, syncWrites, db, handles);
        try {
            byte[] marker = db.get(machine.meta, LAST_APPLIED_KEY);
            machine.lastApplied = marker == null ? LogPosition.EMPTY : RecordCodec.decodeApplied(marker);
            return machine;
        } catch (IOException | IllegalArgumentException e) {
            machine.close();
            throw new StateMachineException(
                    "state machine at " + dir + " is corrupt; refusing to start: " + e.getMessage(), e);
        } catch (RocksDBException e) {
            machine.close();
            throw new StateMachineException("cannot read state machine at " + dir + ": " + e.getMessage(), e);
        }
    }

    @Override
    public synchronized LogPosition lastApplied() {
        ensureUsable();
        return lastApplied;
    }

    @Override
    public synchronized void apply(LogEntry entry) {
        Objects.requireNonNull(entry, "entry");
        ensureUsable();
        StateMachine.checkNext(lastApplied, entry);
        try (WriteBatch batch = new WriteBatch()) {
            Command effective=entry.command();
            if(effective instanceof Command.Request request) {
                if(ClientSession.isNext(session(request.clientId()),request)) {
                    batch.put(meta,sessionKey(request.clientId()),
                            new ClientSession(request.sequence(),ClientSession.fingerprint(request)).encode());
                    effective=request.mutation();
                } else effective=Command.noOp();
            }
            switch (effective) {
                case Command.Request ignored -> throw new IllegalStateException("nested request");
                case Command.Put put -> batch.put(data, put.key().toByteArray(), put.value().toByteArray());
                case Command.Delete delete -> batch.delete(data, delete.key().toByteArray());
                case Command.NoOp noOp -> { }
            }
            batch.put(meta, LAST_APPLIED_KEY, RecordCodec.encodeApplied(entry.position()));
            db.write(syncWrites, batch);
        } catch (RocksDBException e) {
            failed = true;
            throw new StateMachineException("failed to apply entry " + entry.index() + ": " + e.getMessage(), e);
        }
        lastApplied = entry.position();
    }

    @Override
    public synchronized SortedMap<Bytes, Bytes> diagnosticContents() {
        ensureUsable();
        TreeMap<Bytes, Bytes> contents = new TreeMap<>();
        try (RocksIterator it = db.newIterator(data)) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                contents.put(Bytes.copyOf(it.key()), Bytes.copyOf(it.value()));
            }
            it.status();
        } catch (RocksDBException e) {
            throw new StateMachineException("cannot read state machine: " + e.getMessage(), e);
        }
        return Collections.unmodifiableSortedMap(contents);
    }

    @Override public synchronized io.github.richardeyeboah.quantafs.raft.Snapshot snapshot() {
        ensureUsable();var sessions=new TreeMap<String,ClientSession>();
        try(var it=db.newIterator(meta)) {
            for(it.seekToFirst();it.isValid();it.next()) {
                String key=new String(it.key(),StandardCharsets.UTF_8);
                if(key.startsWith("session:")) sessions.put(key.substring(8),ClientSession.decode(it.value()));
            }
            it.status();
        } catch(RocksDBException e) {throw new StateMachineException("cannot export sessions",e);}
        return new io.github.richardeyeboah.quantafs.raft.StateImage(lastApplied,diagnosticContents(),sessions).snapshot();
    }
    @Override public synchronized void restore(io.github.richardeyeboah.quantafs.raft.Snapshot snapshot) {
        ensureUsable();var image=io.github.richardeyeboah.quantafs.raft.StateImage.decode(snapshot);
        if(image.position().index()<lastApplied.index()) throw new StateMachineException("snapshot goes backwards");
        try(var batch=new WriteBatch();var dataIt=db.newIterator(data);var metaIt=db.newIterator(meta)) {
            for(dataIt.seekToFirst();dataIt.isValid();dataIt.next()) batch.delete(data,dataIt.key());dataIt.status();
            for(metaIt.seekToFirst();metaIt.isValid();metaIt.next()) batch.delete(meta,metaIt.key());metaIt.status();
            for(var e:image.data().entrySet()) batch.put(data,e.getKey().toByteArray(),e.getValue().toByteArray());
            for(var e:image.sessions().entrySet()) batch.put(meta,sessionKey(e.getKey()),e.getValue().encode());
            batch.put(meta,LAST_APPLIED_KEY,RecordCodec.encodeApplied(image.position()));
            db.write(syncWrites,batch);lastApplied=image.position();
        } catch(RocksDBException e) {failed=true;throw new StateMachineException("snapshot application failed",e);}
    }

    private static byte[] sessionKey(String id) {
        return ("session:"+id).getBytes(StandardCharsets.UTF_8);
    }
    private ClientSession session(String id) throws RocksDBException {
        byte[] record=db.get(meta,sessionKey(id));
        return record==null?null:ClientSession.decode(record);
    }
    public synchronized ClientSession.Result requestResult(Command.Request request) {
        ensureUsable();
        try { return ClientSession.result(session(request.clientId()),request); }
        catch(RocksDBException e) { throw new StateMachineException("cannot read session",e); }
    }

    /** Local lookup only. Cluster clients must first complete a Raft read barrier. */
    public synchronized java.util.Optional<byte[]> readApplied(byte[] key) {
        ensureUsable();
        try {
            return java.util.Optional.ofNullable(db.get(data, key));
        } catch (RocksDBException e) {
            failed = true;
            throw new StateMachineException("cannot read applied state", e);
        }
    }

    public Path directory() {
        return directory;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        handles.forEach(ColumnFamilyHandle::close);
        try {
            db.closeE();
        } catch (RocksDBException e) {
            throw new StateMachineException("failed to close state machine: " + e.getMessage(), e);
        } finally {
            syncWrites.close();
            columnOptions.close();
            options.close();
        }
    }

    private void ensureUsable() {
        if (closed) {
            throw new StateMachineException("state machine is closed");
        }
        if (failed) {
            throw new StateMachineException("an earlier apply failed; state is unknown, reopen the state machine");
        }
    }
}
