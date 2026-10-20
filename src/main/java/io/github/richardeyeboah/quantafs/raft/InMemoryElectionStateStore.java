package io.github.richardeyeboah.quantafs.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Test and simulation store. Not durable across JVM restarts; "durability" comes from reusing the
 * same instance when a simulated node restarts, which models a disk that survives the crash.
 * Supports failure injection and records every successful save for assertions.
 */
public final class InMemoryElectionStateStore implements ElectionStateStore {

    private TermAndVote state;
    private boolean failSaves;
    private final List<TermAndVote> history = new ArrayList<>();

    public InMemoryElectionStateStore() {
        this(TermAndVote.INITIAL);
    }

    public InMemoryElectionStateStore(TermAndVote initial) {
        this.state = Objects.requireNonNull(initial, "initial");
    }

    @Override
    public synchronized TermAndVote load() {
        return state;
    }

    @Override
    public synchronized void save(TermAndVote next) {
        Objects.requireNonNull(next, "next");
        if (failSaves) {
            throw new PersistenceException("injected persistence failure");
        }
        try {
            TermAndVote.checkTransition(state, next);
        } catch (IllegalArgumentException e) {
            throw new PersistenceException("rejected unsafe transition: " + e.getMessage(), e);
        }
        state = next;
        history.add(next);
    }

    /** While enabled, every save throws and leaves the stored state unchanged. */
    public synchronized void failSaves(boolean enabled) {
        this.failSaves = enabled;
    }

    /** Every successfully saved state, oldest first. */
    public synchronized List<TermAndVote> history() {
        return List.copyOf(history);
    }
}
