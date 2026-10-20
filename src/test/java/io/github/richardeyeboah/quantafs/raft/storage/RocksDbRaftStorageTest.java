package io.github.richardeyeboah.quantafs.raft.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.richardeyeboah.quantafs.raft.Command;
import io.github.richardeyeboah.quantafs.raft.LogEntry;
import io.github.richardeyeboah.quantafs.raft.NodeId;
import io.github.richardeyeboah.quantafs.raft.PersistenceException;
import io.github.richardeyeboah.quantafs.raft.StorageWrite;
import io.github.richardeyeboah.quantafs.raft.TermAndVote;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/** Damaged or unsafe storage must stop the node from starting, never look like a fresh node. */
class RocksDbRaftStorageTest {

    @TempDir Path dir;

    @BeforeEach
    void writeThreeEntries() {
        try (RocksDbRaftStorage storage = RocksDbRaftStorage.open(dir)) {
            storage.write(new StorageWrite(Optional.of(TermAndVote.of(2, NodeId.of("n2"))), 0,
                    List.of(entry(1, 1), entry(2, 1), entry(3, 2))));
        }
    }

    @Test
    void healthyStorageReopensWithEverything() {
        try (RocksDbRaftStorage storage = RocksDbRaftStorage.open(dir)) {
            assertEquals(3, storage.lastIndex());
            assertEquals(TermAndVote.of(2, NodeId.of("n2")), storage.termAndVote());
            assertEquals(entry(3, 2), storage.entry(3));
        }
    }

    @Test
    void singleFlippedBitInAnEntryFailsClosed() throws RocksDBException {
        byte[] damaged = RecordCodec.encodeEntry(entry(2, 1));
        damaged[10] ^= 0x01; // one bit inside the stored term
        raw(db -> db.put(RocksDbRaftStorage.logKey(2), damaged));
        assertCorrupt("checksum");
    }

    @Test
    void truncatedEntryFailsClosed() throws RocksDBException {
        raw(db -> db.put(RocksDbRaftStorage.logKey(2), new byte[] {2, 1, 0, 0, 7}));
        assertCorrupt("too short");
    }

    @Test
    void missingMiddleEntryFailsClosed() throws RocksDBException {
        raw(db -> db.delete(RocksDbRaftStorage.logKey(2)));
        assertCorrupt("gap");
    }

    @Test
    void entryStoredUnderTheWrongIndexFailsClosed() throws RocksDBException {
        raw(db -> db.put(RocksDbRaftStorage.logKey(2), RecordCodec.encodeEntry(entry(3, 2))));
        assertCorrupt("stored under index 2");
    }

    @Test
    void logWithoutTermAndVoteFailsClosedInsteadOfLookingFresh() throws RocksDBException {
        raw(db -> db.delete(RocksDbRaftStorage.TERM_AND_VOTE_KEY));
        assertCorrupt("without term/vote");
    }

    @Test
    void wrongRecordTypeUnderTheMetadataKeyFailsClosed() throws RocksDBException {
        raw(db -> db.put(RocksDbRaftStorage.TERM_AND_VOTE_KEY, RecordCodec.encodeEntry(entry(1, 1))));
        assertCorrupt("expected record type");
    }

    @Test
    void secondOpenOfTheSameDirectoryIsRefused() {
        try (RocksDbRaftStorage first = RocksDbRaftStorage.open(dir)) {
            PersistenceException e = assertThrows(PersistenceException.class, () -> RocksDbRaftStorage.open(dir));
            assertTrue(e.getMessage().contains("lock"), e.getMessage());
            assertEquals(3, first.lastIndex());
        }
    }

    @Test
    void closedStorageRefusesUse() {
        RocksDbRaftStorage storage = RocksDbRaftStorage.open(dir);
        storage.close();
        assertThrows(PersistenceException.class, storage::lastIndex);
        assertThrows(PersistenceException.class, () -> storage.write(StorageWrite.termAndVote(TermAndVote.of(3, null))));
    }

    private interface RawEdit {
        void apply(RocksDB db) throws RocksDBException;
    }

    private void raw(RawEdit edit) throws RocksDBException {
        try (Options options = new Options(); RocksDB db = RocksDB.open(options, dir.toString())) {
            edit.apply(db);
        }
    }

    private void assertCorrupt(String detail) {
        PersistenceException e = assertThrows(PersistenceException.class, () -> RocksDbRaftStorage.open(dir));
        assertTrue(e.getMessage().contains("refusing to start") && e.getMessage().contains(detail), e.getMessage());
    }

    private static LogEntry entry(long index, long term) {
        return new LogEntry(index, term, Command.put(new byte[] {(byte) index}, new byte[] {(byte) term}));
    }
}
