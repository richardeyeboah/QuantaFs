package io.github.richardeyeboah.quantafs.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Behavior every {@link RaftStorage} must share, so the in-memory test storage behaves like the
 * durable one. Includes the term/vote rules carried over from milestone 2A.
 */
abstract class RaftStorageContractTest {

    static final NodeId N2 = NodeId.of("n2");
    static final NodeId N3 = NodeId.of("n3");

    @TempDir Path dir;

    RaftStorage storage;

    abstract RaftStorage create(Path dir);

    /** The storage as a restarted process would see it. */
    abstract RaftStorage reopen(RaftStorage storage, Path dir);

    abstract void close(RaftStorage storage);

    @BeforeEach
    void setUp() {
        storage = create(dir);
    }

    @AfterEach
    void tearDown() {
        close(storage);
    }

    @Test
    void freshStorageIsTermZeroWithAnEmptyLog() {
        assertEquals(TermAndVote.INITIAL, storage.termAndVote());
        assertEquals(0, storage.lastIndex());
        assertEquals(0, storage.termAt(0));
        assertEquals(LogPosition.EMPTY, storage.lastPosition());
    }

    @Test
    void termVoteAndEntriesSurviveReopenTogether() {
        storage.write(new StorageWrite(Optional.of(TermAndVote.of(2, N2)), 0, List.of(entry(1, 1, "a"), entry(2, 2, "b"))));
        storage = reopen(storage, dir);
        assertEquals(TermAndVote.of(2, N2), storage.termAndVote());
        assertEquals(List.of(entry(1, 1, "a"), entry(2, 2, "b")), storage.entries(1, 2));
        assertEquals(new LogPosition(2, 2), storage.lastPosition());
    }

    @Test
    void truncationAndAppendHappenInOneWrite() {
        storage.write(new StorageWrite(Optional.of(TermAndVote.of(2, null)), 0,
                List.of(entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 2, "c"))));
        storage.write(new StorageWrite(Optional.of(TermAndVote.of(3, null)), 2, List.of(entry(2, 3, "x"))));
        storage = reopen(storage, dir);
        assertEquals(List.of(entry(1, 1, "a"), entry(2, 3, "x")), storage.entries(1, storage.lastIndex()));
        assertEquals(TermAndVote.of(3, null), storage.termAndVote());
    }

    @Test
    void rejectedWritesChangeNothing() {
        storage.write(new StorageWrite(Optional.of(TermAndVote.of(2, null)), 0, List.of(entry(1, 1, "a"), entry(2, 2, "b"))));
        // gap, decreasing terms, entry from a future term, truncation beyond the end
        assertThrows(PersistenceException.class, () -> storage.write(StorageWrite.append(List.of(entry(4, 2, "z")))));
        assertThrows(PersistenceException.class, () -> storage.write(StorageWrite.append(List.of(entry(3, 1, "z")))));
        assertThrows(PersistenceException.class, () -> storage.write(StorageWrite.append(List.of(entry(3, 9, "z")))));
        assertThrows(PersistenceException.class, () -> storage.write(new StorageWrite(Optional.empty(), 5, List.of())));
        // a term change and entries in one write are rejected as a unit
        assertThrows(PersistenceException.class, () -> storage.write(
                new StorageWrite(Optional.of(TermAndVote.of(5, null)), 0, List.of(entry(4, 5, "gap")))));
        storage = reopen(storage, dir);
        assertEquals(TermAndVote.of(2, null), storage.termAndVote());
        assertEquals(2, storage.lastIndex());
    }

    @Test
    void rejectsLoweringTheTermOrChangingAVote() {
        storage.write(StorageWrite.termAndVote(TermAndVote.of(5, N2)));
        assertThrows(PersistenceException.class, () -> storage.write(StorageWrite.termAndVote(TermAndVote.of(4, null))));
        assertThrows(PersistenceException.class, () -> storage.write(StorageWrite.termAndVote(TermAndVote.of(5, N3))));
        assertThrows(PersistenceException.class, () -> storage.write(StorageWrite.termAndVote(TermAndVote.of(5, null))));
        storage.write(StorageWrite.termAndVote(TermAndVote.of(5, N2))); // repeating the same vote is fine
        storage.write(StorageWrite.termAndVote(TermAndVote.of(6, N3))); // a new term allows a new vote
        assertEquals(TermAndVote.of(6, N3), reopen(storage, dir).termAndVote());
    }

    @Test
    void binaryCommandsRoundTrip() {
        Command put = Command.put(new byte[] {0, (byte) 0xFF}, new byte[] {(byte) 0x80, 0});
        Command delete = Command.delete(new byte[] {(byte) 0xFE});
        storage.write(new StorageWrite(Optional.of(TermAndVote.of(1, null)), 0,
                List.of(new LogEntry(1, 1, put), new LogEntry(2, 1, delete), new LogEntry(3, 1, Command.noOp()))));
        storage = reopen(storage, dir);
        assertEquals(List.of(new LogEntry(1, 1, put), new LogEntry(2, 1, delete), new LogEntry(3, 1, Command.noOp())),
                storage.entries(1, 3));
    }

    static LogEntry entry(long index, long term, String key) {
        return new LogEntry(index, term, Command.put(key.getBytes(StandardCharsets.UTF_8), new byte[] {1}));
    }
}
