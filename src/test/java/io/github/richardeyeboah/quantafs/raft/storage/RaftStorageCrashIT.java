package io.github.richardeyeboah.quantafs.raft.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.github.richardeyeboah.quantafs.raft.Bytes;
import io.github.richardeyeboah.quantafs.raft.Command;
import io.github.richardeyeboah.quantafs.raft.LogEntry;
import io.github.richardeyeboah.quantafs.raft.LogPosition;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Kills a process with SIGKILL while it writes Raft storage and applies entries, then reopens the
 * node directory and checks what survived, three times on the same directory.
 *
 * <p>What this proves: RocksDB's atomic, synced write batches plus QuantaFs's recovery checks leave
 * term and log consistent after an abrupt process death (never a half-applied batch); every write
 * that returned before the kill survives; the applied state is exactly the replay of entries up to
 * its marker, so nothing is applied twice or skipped; and the OS releases the node lock.
 *
 * <p>What it does not prove: SIGKILL destroys the process, not the operating system's page cache, so
 * this is not a power-loss test. It would pass even if fsync did nothing.
 */
class RaftStorageCrashIT {

    private static final int PERSISTS_BEFORE_KILL = 40;

    @TempDir Path workDir;

    @Test
    void sigkillDuringAtomicBatchesLeavesTermLogAndAppliedStateConsistent() throws Exception {
        Path nodeDir = workDir.resolve("node");
        long previousTerm = 0;
        for (int round = 1; round <= 3; round++) {
            Path log = workDir.resolve("worker-" + round + ".log");
            Process worker = launch(nodeDir, log);
            awaitPersists(worker, log, previousTerm + PERSISTS_BEFORE_KILL);
            worker.destroyForcibly(); // SIGKILL: no shutdown hooks, no RocksDB close
            assertTrue(worker.waitFor(30, TimeUnit.SECONDS));

            List<String> lines = completeLines(log);
            long acknowledgedTerm = lastNumber(lines, "PERSISTED ", 1);
            long acknowledgedApplied = lastNumber(lines, "APPLIED ", 1);

            try (NodeStorage storage = NodeStorage.open(nodeDir)) { // the dead process's lock is gone
                long term = storage.raftStorage().termAndVote().currentTerm();
                assertTrue(term >= acknowledgedTerm, "acknowledged batch " + acknowledgedTerm + " lost; recovered term " + term);
                assertTrue(term <= acknowledgedTerm + 1, "at most the in-flight batch may have landed");
                List<LogEntry> expected = StorageCrashWorker.expectedLog(term);
                assertEquals(expected, storage.raftStorage().entries(1, storage.raftStorage().lastIndex()),
                        "term " + term + " recovered with exactly the log written by batch " + term);

                LogPosition applied = storage.stateMachine().lastApplied();
                assertTrue(applied.index() >= acknowledgedApplied, "acknowledged apply lost");
                assertTrue(applied.index() <= expected.size());
                assertEquals(applied.index() == 0 ? 0 : expected.get((int) applied.index() - 1).term(), applied.term());
                assertEquals(replay(expected, applied.index()), storage.stateMachine().diagnosticContents(),
                        "applied data is exactly entries 1.." + applied.index());
                previousTerm = term;
            }
        }
    }

    private static SortedMap<Bytes, Bytes> replay(List<LogEntry> log, long upTo) {
        TreeMap<Bytes, Bytes> state = new TreeMap<>();
        for (int i = 0; i < upTo; i++) {
            Command.Put put = (Command.Put) log.get(i).command();
            state.put(put.key(), put.value());
        }
        return state;
    }

    private static Process launch(Path nodeDir, Path log) throws IOException {
        String jar = System.getProperty("quantafs.jar");
        String testClasses = System.getProperty("quantafs.test-classes");
        assertNotNull(jar, "run via ./mvnw verify");
        assertNotNull(testClasses, "run via ./mvnw verify");
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        return new ProcessBuilder(java, "-cp", testClasses + File.pathSeparator + jar,
                        StorageCrashWorker.class.getName(), nodeDir.toString())
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
    }

    private static void awaitPersists(Process worker, Path log, long term) throws Exception {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(120));
        while (Instant.now().isBefore(deadline)) {
            List<String> lines = Files.exists(log) ? completeLines(log) : List.of();
            if (lastNumber(lines, "PERSISTED ", 1) >= term) {
                return;
            }
            if (!worker.isAlive()) {
                fail("worker exited early:\n" + String.join("\n", lines));
            }
            Thread.sleep(20);
        }
        fail("worker did not reach term " + term);
    }

    /** Lines terminated by a newline; a final line cut off by the kill is ignored. */
    private static List<String> completeLines(Path log) throws IOException {
        String content = Files.readString(log);
        int end = content.lastIndexOf('\n');
        return end < 0 ? List.of() : content.substring(0, end).lines().toList();
    }

    /** The {@code field}-th number on the last line starting with {@code prefix}, or 0. */
    private static long lastNumber(List<String> lines, String prefix, int field) {
        long value = 0;
        for (String line : lines) {
            String[] parts = line.split(" ");
            if (line.startsWith(prefix) && parts.length > field) {
                value = Long.parseLong(parts[field]);
            }
        }
        return value;
    }
}
