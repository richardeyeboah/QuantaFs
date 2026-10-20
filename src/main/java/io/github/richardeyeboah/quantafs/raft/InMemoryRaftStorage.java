package io.github.richardeyeboah.quantafs.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Test and simulation {@link RaftStorage}. Nothing is written to disk. A simulated crash keeps this
 * object and discards only the {@link RaftNode}, which models storage that survives the crash.
 * Supports failure injection and records every term/vote change for assertions.
 */
public final class InMemoryRaftStorage implements RaftStorage {

    private TermAndVote termAndVote;
    private final List<LogEntry> log = new ArrayList<>();
    private final List<TermAndVote> termAndVoteHistory = new ArrayList<>();
    private FailureMode failureMode = FailureMode.NONE;
    private long writes;
    private Snapshot snapshot;
    private long base() { return snapshot==null?0:snapshot.position().index(); }
    @Override public synchronized Optional<Snapshot> snapshot() { return Optional.ofNullable(snapshot); }
    @Override public synchronized void installSnapshot(Snapshot next) {
        if(failureMode==FailureMode.FAIL_BEFORE) throw new PersistenceException("injected snapshot failure before save");
        StateImage.decode(next);
        if(next.position().index()<base() || next.position().term()>termAndVote.currentTerm())
            throw new PersistenceException("invalid snapshot boundary");
        long index=next.position().index();
        boolean keep=index<=lastIndex() && termAt(index)==next.position().term();
        if(keep) log.subList(0,(int)(index-base())).clear();else log.clear();
        snapshot=next;writes++;
        if(failureMode==FailureMode.FAIL_AFTER) throw new PersistenceException("injected snapshot failure after save");
    }

    public InMemoryRaftStorage() {
        this(TermAndVote.INITIAL, List.of());
    }

    /** Pre-populated storage, as if the node had run before. */
    public InMemoryRaftStorage(TermAndVote initial, List<LogEntry> entries) {
        this.termAndVote = TermAndVote.INITIAL;
        write(new StorageWrite(Optional.of(initial), 0, entries));
        termAndVoteHistory.clear();
        writes = 0;
    }

    @Override
    public synchronized TermAndVote termAndVote() {
        return termAndVote;
    }

    @Override
    public synchronized long lastIndex() {
        return base()+log.size();
    }

    @Override
    public synchronized long termAt(long index) {
        RaftStorage.checkIndex(index, lastIndex());
        if(index==base() && snapshot!=null) return snapshot.position().term();
        if(index<base()) throw new IllegalArgumentException("compacted index");
        return index == 0 ? 0 : log.get((int)(index-base()) - 1).term();
    }

    @Override
    public synchronized LogEntry entry(long index) {
        if (index <= base() || index > lastIndex()) {
            throw new IllegalArgumentException("no entry at index " + index + "; log ends at " + lastIndex());
        }
        return log.get((int)(index-base()) - 1);
    }

    @Override
    public synchronized List<LogEntry> entries(long from, long to) {
        if (from > to) {
            return List.of();
        }
        if(from<=base()) throw new IllegalArgumentException("compacted prefix");
        RaftStorage.checkIndex(from - 1, lastIndex());
        RaftStorage.checkIndex(to, lastIndex());
        return List.copyOf(log.subList((int)(from-base()) - 1, (int)(to-base())));
    }

    @Override
    public synchronized void write(StorageWrite write) {
        Objects.requireNonNull(write, "write");
        if (failureMode == FailureMode.FAIL_BEFORE) {
            throw new PersistenceException("injected storage failure before write");
        }
        try {
            if(write.truncateFrom()>0 && write.truncateFrom()<=base()) throw new IllegalArgumentException("truncate compacted prefix");
            write.validate(termAndVote, lastIndex(), this::termAt);
        } catch (IllegalArgumentException e) {
            throw new PersistenceException("rejected unsafe write: " + e.getMessage(), e);
        }
        write.termAndVote().ifPresent(next -> {
            if (!next.equals(termAndVote)) {
                termAndVoteHistory.add(next);
            }
            termAndVote = next;
        });
        if (write.truncateFrom() > 0) {
            log.subList((int)(write.truncateFrom()-base()) - 1, log.size()).clear();
        }
        log.addAll(write.append());
        writes++;
        if (failureMode == FailureMode.FAIL_AFTER) {
            throw new PersistenceException("injected storage failure after a completed write");
        }
    }

    public synchronized void setFailureMode(FailureMode mode) {
        this.failureMode = Objects.requireNonNull(mode, "mode");
    }

    public synchronized FailureMode failureMode() {
        return failureMode;
    }

    /** Every distinct term/vote this storage has recorded, oldest first. */
    public synchronized List<TermAndVote> termAndVoteHistory() {
        return List.copyOf(termAndVoteHistory);
    }

    /** Number of completed writes, for "nothing was persisted" assertions. */
    public synchronized long writeCount() {
        return writes;
    }

    /** A pre-populated log of no-op entries with the given terms, for election fixtures. */
    public static List<LogEntry> noOpLog(long... terms) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = 0; i < terms.length; i++) {
            entries.add(new LogEntry(i + 1, terms[i], Command.noOp()));
        }
        return entries;
    }
}
