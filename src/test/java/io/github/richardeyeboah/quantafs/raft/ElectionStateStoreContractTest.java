package io.github.richardeyeboah.quantafs.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Behavior every {@link ElectionStateStore} must share, so the test store matches the durable one. */
abstract class ElectionStateStoreContractTest {

    private static final NodeId N2 = NodeId.of("n2");
    private static final NodeId N3 = NodeId.of("n3");

    @TempDir Path dir;

    private ElectionStateStore store;

    /** Creates a fresh store in {@code dir}. */
    abstract ElectionStateStore create(Path dir);

    /** Returns the store as a restarted process would see it. */
    abstract ElectionStateStore reopen(ElectionStateStore store, Path dir);

    @BeforeEach
    void setUp() {
        store = create(dir);
    }

    @Test
    void freshStoreIsTermZeroWithNoVote() {
        assertEquals(TermAndVote.INITIAL, store.load());
    }

    @Test
    void savedStateIsVisibleAfterReopen() {
        store.save(TermAndVote.of(1, N2));
        store.save(TermAndVote.of(3, null));
        store.save(TermAndVote.of(3, N3));
        assertEquals(TermAndVote.of(3, N3), reopen(store, dir).load());
    }

    @Test
    void rejectsLoweringTheTerm() {
        store.save(TermAndVote.of(5, null));
        assertThrows(PersistenceException.class, () -> store.save(TermAndVote.of(4, null)));
        assertEquals(TermAndVote.of(5, null), reopen(store, dir).load());
    }

    @Test
    void rejectsChangingAVoteWithinATerm() {
        store.save(TermAndVote.of(2, N2));
        assertThrows(PersistenceException.class, () -> store.save(TermAndVote.of(2, N3)));
        assertThrows(PersistenceException.class, () -> store.save(TermAndVote.of(2, null)));
        assertEquals(TermAndVote.of(2, N2), reopen(store, dir).load());
    }

    @Test
    void allowsRepeatingTheSameVoteAndVotingAgainInALaterTerm() {
        store.save(TermAndVote.of(2, N2));
        store.save(TermAndVote.of(2, N2));
        store.save(TermAndVote.of(3, N3));
        assertEquals(TermAndVote.of(3, N3), reopen(store, dir).load());
    }
}
