package io.github.richardeyeboah.quantafs.raft;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Test and simulation {@link StateMachine}. Like {@link InMemoryRaftStorage}, it survives a
 * simulated crash because the simulator keeps the object. Records every applied entry and supports
 * failure injection on either side of an apply.
 */
public final class InMemoryStateMachine implements StateMachine {

    private final TreeMap<Bytes, Bytes> data = new TreeMap<>();
    private final List<LogEntry> applied = new ArrayList<>();
    private LogPosition lastApplied = LogPosition.EMPTY;
    private FailureMode failureMode = FailureMode.NONE;

    @Override
    public synchronized LogPosition lastApplied() {
        return lastApplied;
    }

    @Override
    public synchronized void apply(LogEntry entry) {
        Objects.requireNonNull(entry, "entry");
        if (failureMode == FailureMode.FAIL_BEFORE) {
            throw new StateMachineException("injected apply failure before applying " + entry.index());
        }
        StateMachine.checkNext(lastApplied, entry);
        switch (entry.command()) {
            case Command.Put put -> data.put(put.key(), put.value());
            case Command.Delete delete -> data.remove(delete.key());
            case Command.NoOp noOp -> { }
        }
        lastApplied = entry.position();
        applied.add(entry);
        if (failureMode == FailureMode.FAIL_AFTER) {
            throw new StateMachineException("injected apply failure after applying " + entry.index());
        }
    }

    @Override
    public synchronized SortedMap<Bytes, Bytes> diagnosticContents() {
        return Collections.unmodifiableSortedMap(new TreeMap<>(data));
    }

    public synchronized void setFailureMode(FailureMode mode) {
        this.failureMode = Objects.requireNonNull(mode, "mode");
    }

    public synchronized FailureMode failureMode() {
        return failureMode;
    }

    /** Every entry applied so far, in order, across all simulated restarts. */
    public synchronized List<LogEntry> appliedEntries() {
        return List.copyOf(applied);
    }
}
