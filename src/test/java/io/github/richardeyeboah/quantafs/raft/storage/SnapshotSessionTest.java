package io.github.richardeyeboah.quantafs.raft.storage;

import io.github.richardeyeboah.quantafs.raft.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class SnapshotSessionTest {
    @TempDir Path root;
    private Command.Request request(long seq,String value) {
        return new Command.Request("checkout",seq,Command.put(new byte[]{0,1},value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }
    @Test void retryCannotOverwriteNewerDataAndSurvivesSnapshotRestoreAndReopen() {
        Snapshot image;
        try(var machine=RocksDbStateMachine.open(root.resolve("a"))) {
            machine.apply(new LogEntry(1,1,request(1,"old")));
            machine.apply(new LogEntry(2,1,Command.put(new byte[]{0,1},"new".getBytes())));
            machine.apply(new LogEntry(3,1,request(1,"old"))); // response was lost; don't reapply the old put
            assertEquals("new",new String(machine.readApplied(new byte[]{0,1}).orElseThrow()));
            assertEquals(ClientSession.Result.SUCCESS,machine.requestResult(request(1,"old")));
            machine.apply(new LogEntry(4,1,request(1,"conflicting payload")));
            assertEquals(ClientSession.Result.CONFLICT,machine.requestResult(request(1,"conflicting payload")));
            machine.apply(new LogEntry(5,1,request(3,"gap")));
            assertEquals(ClientSession.Result.GAP,machine.requestResult(request(3,"gap")));
            image=machine.snapshot();
        }
        try(var restored=RocksDbStateMachine.open(root.resolve("b"))) {
            restored.restore(image);
            restored.apply(new LogEntry(6,2,request(1,"old")));
            assertEquals("new",new String(restored.readApplied(new byte[]{0,1}).orElseThrow()));
            restored.apply(new LogEntry(7,2,request(2,"latest")));
            restored.apply(new LogEntry(8,2,request(1,"old")));
            assertEquals(ClientSession.Result.STALE,restored.requestResult(request(1,"old")));
        }
        try(var reopened=RocksDbStateMachine.open(root.resolve("b"))) {
            assertEquals(8,reopened.lastApplied().index());
            assertEquals("latest",new String(reopened.readApplied(new byte[]{0,1}).orElseThrow()));
            assertEquals(ClientSession.Result.SUCCESS,reopened.requestResult(request(2,"latest")));
        }
    }
    @Test void compactedLogKeepsItsBoundaryAndSuffixAcrossRestartAndRepairsMissingApplicationInstall() {
        Snapshot image;
        var machine=new InMemoryStateMachine();
        try(var log=RocksDbRaftStorage.open(root.resolve("log"))) {
            log.write(StorageWrite.termAndVote(new TermAndVote(2,Optional.empty())));
            var entries=new ArrayList<LogEntry>();
            for(int i=1;i<=10;i++) entries.add(new LogEntry(i,1,Command.noOp()));
            log.write(StorageWrite.append(entries));
            for(int i=1;i<=5;i++) machine.apply(entries.get(i-1));
            image=machine.snapshot();log.installSnapshot(image);
            assertEquals(6,log.firstRetainedIndex());assertEquals(10,log.lastIndex());assertEquals(1,log.termAt(5));
            assertThrows(IllegalArgumentException.class,() -> log.entry(1));
            assertThrows(PersistenceException.class,() -> log.write(new StorageWrite(Optional.empty(),4,List.of())));
        }
        try(var log=RocksDbRaftStorage.open(root.resolve("log"));var app=RocksDbStateMachine.open(root.resolve("app"))) {
            assertEquals(image,log.snapshot().orElseThrow());
            var node=new RaftNode(NodeId.of("n1"),ClusterConfig.of("n1","n2","n3"),ElectionTiming.DEFAULT,
                    new SeededRandomSource(7),log,app);
            assertEquals(5,node.lastApplied());assertEquals(5,node.commitIndex());
            assertEquals(5,app.lastApplied().index()); // crash between durable image and app install repaired
            log.write(StorageWrite.append(List.of(new LogEntry(11,2,Command.noOp()))));
            assertEquals(11,log.lastIndex());
        }
    }
    @Test void corruptOrMismatchedSnapshotCannotChangeState() {
        var app=new InMemoryStateMachine();app.apply(new LogEntry(1,1,request(1,"safe")));
        Snapshot image=app.snapshot();byte[] bad=image.image().toByteArray();bad[8]^=1;
        assertThrows(StateMachineException.class,() -> app.restore(new Snapshot(image.position(),Bytes.copyOf(bad))));
        assertEquals(Bytes.utf8("safe"),app.diagnosticContents().get(Bytes.copyOf(new byte[]{0,1})));
        assertThrows(StateMachineException.class,() -> app.restore(new Snapshot(new LogPosition(1,2),image.image())));
    }
}
