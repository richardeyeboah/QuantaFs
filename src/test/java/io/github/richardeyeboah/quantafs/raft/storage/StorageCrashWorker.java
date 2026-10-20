package io.github.richardeyeboah.quantafs.raft.storage;

import io.github.richardeyeboah.quantafs.raft.Command;
import io.github.richardeyeboah.quantafs.raft.LogEntry;
import io.github.richardeyeboah.quantafs.raft.StorageWrite;
import io.github.richardeyeboah.quantafs.raft.TermAndVote;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Child process for {@code RaftStorageCrashIT}; it is killed with SIGKILL at an arbitrary moment.
 *
 * <p>Batch k writes term k and log entries in one atomic {@link StorageWrite}. Every fourth batch
 * also replaces the last two (not yet applied) entries, exercising truncate-and-append. After each
 * write returns it prints {@code PERSISTED k lastIndex}; then it applies everything before batch k
 * and prints {@code APPLIED index}. Restarted on the same directory, it continues from what was
 * recovered.
 */
public final class StorageCrashWorker {

    private StorageCrashWorker() {}

    public static void main(String[] args) {
        try (NodeStorage storage = NodeStorage.open(Path.of(args[0]))) {
            RocksDbRaftStorage raft = storage.raftStorage();
            RocksDbStateMachine machine = storage.stateMachine();
            long term = raft.termAndVote().currentTerm();
            while (true) {
                term++;
                long last = raft.lastIndex();
                long truncateFrom = isReplacingBatch(term) ? last - 1 : 0;
                long first = truncateFrom > 0 ? truncateFrom : last + 1;
                if (truncateFrom > 0 && truncateFrom <= machine.lastApplied().index()) {
                    throw new IllegalStateException("would replace an applied entry");
                }
                raft.write(new StorageWrite(Optional.of(new TermAndVote(term, Optional.empty())), truncateFrom, batch(first, term)));
                System.out.println("PERSISTED " + term + " " + raft.lastIndex());
                System.out.flush();
                while (machine.lastApplied().index() < first - 1) {
                    machine.apply(raft.entry(machine.lastApplied().index() + 1));
                }
                System.out.println("APPLIED " + machine.lastApplied().index());
                System.out.flush();
            }
        }
    }

    static boolean isReplacingBatch(long term) {
        return term % 4 == 0;
    }

    static List<LogEntry> batch(long firstIndex, long term) {
        List<LogEntry> entries = new ArrayList<>();
        for (long index = firstIndex; index < firstIndex + 3; index++) {
            entries.add(new LogEntry(index, term, Command.put(
                    ("key-" + index).getBytes(StandardCharsets.UTF_8), ("term-" + term).getBytes(StandardCharsets.UTF_8))));
        }
        return entries;
    }

    /** The exact log the worker has written after completing batch {@code term}. */
    static List<LogEntry> expectedLog(long term) {
        List<LogEntry> log = new ArrayList<>();
        for (long k = 1; k <= term; k++) {
            long first = isReplacingBatch(k) ? log.size() - 1 : log.size() + 1;
            while (log.size() >= first) {
                log.removeLast();
            }
            log.addAll(batch(first, k));
        }
        return log;
    }
}
