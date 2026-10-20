package io.github.richardeyeboah.quantafs.raft.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.richardeyeboah.quantafs.raft.Bytes;
import io.github.richardeyeboah.quantafs.raft.Command;
import io.github.richardeyeboah.quantafs.raft.LogEntry;
import io.github.richardeyeboah.quantafs.raft.LogPosition;
import io.github.richardeyeboah.quantafs.raft.StateMachineException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

class RocksDbStateMachineTest {

    @TempDir Path dir;

    @Test
    void appliesPutDeleteAndNoOpWithTheMarkerAndSurvivesReopen() {
        try (RocksDbStateMachine machine = RocksDbStateMachine.open(dir)) {
            assertEquals(LogPosition.EMPTY, machine.lastApplied());
            machine.apply(new LogEntry(1, 1, put("a", "1")));
            machine.apply(new LogEntry(2, 1, put("b", "2")));
            machine.apply(new LogEntry(3, 2, Command.noOp()));
            machine.apply(new LogEntry(4, 2, Command.delete(utf8("a"))));
            assertEquals(new LogPosition(2, 4), machine.lastApplied());
        }
        try (RocksDbStateMachine machine = RocksDbStateMachine.open(dir)) {
            assertEquals(new LogPosition(2, 4), machine.lastApplied());
            assertEquals(Map.of(Bytes.utf8("b"), Bytes.utf8("2")), machine.diagnosticContents());
        }
    }

    @Test
    void entriesMustArriveInExactlyTheNextIndex() {
        try (RocksDbStateMachine machine = RocksDbStateMachine.open(dir)) {
            assertThrows(StateMachineException.class, () -> machine.apply(new LogEntry(2, 1, put("a", "1"))), "skip");
            machine.apply(new LogEntry(1, 1, put("a", "1")));
            assertThrows(StateMachineException.class, () -> machine.apply(new LogEntry(1, 1, put("a", "1"))), "replay");
            assertEquals(new LogPosition(1, 1), machine.lastApplied());
        }
    }

    @Test
    void userKeysCannotCollideWithTheInternalMarker() {
        try (RocksDbStateMachine machine = RocksDbStateMachine.open(dir)) {
            machine.apply(new LogEntry(1, 1, Command.put(RocksDbStateMachine.LAST_APPLIED_KEY, new byte[] {9})));
            machine.apply(new LogEntry(2, 1, Command.delete(RocksDbStateMachine.LAST_APPLIED_KEY)));
        }
        try (RocksDbStateMachine machine = RocksDbStateMachine.open(dir)) {
            assertEquals(new LogPosition(1, 2), machine.lastApplied(), "the marker lives in its own column family");
            assertEquals(Map.of(), machine.diagnosticContents());
        }
    }

    @Test
    void corruptMarkerFailsClosed() throws RocksDBException {
        try (RocksDbStateMachine machine = RocksDbStateMachine.open(dir)) {
            machine.apply(new LogEntry(1, 1, put("a", "1")));
        }
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        try (DBOptions options = new DBOptions(); ColumnFamilyOptions cf = new ColumnFamilyOptions();
                RocksDB db = RocksDB.open(options, dir.toString(), List.of(
                        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cf),
                        new ColumnFamilyDescriptor(RocksDbStateMachine.META_COLUMN_FAMILY, cf)), handles)) {
            db.put(handles.get(1), RocksDbStateMachine.LAST_APPLIED_KEY, new byte[] {3, 1, 1});
            handles.forEach(ColumnFamilyHandle::close);
        }
        StateMachineException e = assertThrows(StateMachineException.class, () -> RocksDbStateMachine.open(dir));
        assertTrue(e.getMessage().contains("refusing to start"), e.getMessage());
    }

    @Test
    void secondOpenIsRefused() {
        try (RocksDbStateMachine first = RocksDbStateMachine.open(dir)) {
            assertThrows(StateMachineException.class, () -> RocksDbStateMachine.open(dir));
            assertEquals(LogPosition.EMPTY, first.lastApplied(), "the owner is unaffected");
        }
    }

    private static Command put(String key, String value) {
        return Command.put(utf8(key), utf8(value));
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
