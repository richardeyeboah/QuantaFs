package io.github.richardeyeboah.quantafs.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileElectionStateStoreTest {

    private static final NodeId N1 = NodeId.of("n1");
    private static final NodeId N2 = NodeId.of("n2");
    private static final NodeId N3 = NodeId.of("n3");

    @TempDir Path dir;

    @Test
    void writesOneStateFileAndLeavesNoTemporaryFile() throws IOException {
        FileElectionStateStore store = FileElectionStateStore.open(dir);
        store.save(TermAndVote.of(7, N2));
        try (var files = Files.list(dir)) {
            assertEquals(List.of(FileElectionStateStore.FILE_NAME), files.map(p -> p.getFileName().toString()).toList());
        }
    }

    @Test
    void encodingRoundTrips() throws IOException {
        for (TermAndVote state : List.of(TermAndVote.INITIAL, TermAndVote.of(1, null), TermAndVote.of(Long.MAX_VALUE, N3))) {
            assertEquals(state, FileElectionStateStore.decode(FileElectionStateStore.encode(state)));
        }
    }

    @Test
    void corruptStateFileFailsClosedInsteadOfResettingToTermZero() throws IOException {
        FileElectionStateStore.open(dir).save(TermAndVote.of(9, N2));
        Path file = dir.resolve(FileElectionStateStore.FILE_NAME);
        byte[] bytes = Files.readAllBytes(file);
        bytes[12] ^= 0x01; // flip one bit inside the term
        Files.write(file, bytes);

        PersistenceException e = assertThrows(PersistenceException.class, () -> FileElectionStateStore.open(dir));
        assertTrue(e.getMessage().contains("corrupt"), e.getMessage());
    }

    @Test
    void truncatedStateFileFailsClosed() throws IOException {
        FileElectionStateStore.open(dir).save(TermAndVote.of(9, N2));
        Path file = dir.resolve(FileElectionStateStore.FILE_NAME);
        byte[] bytes = Files.readAllBytes(file);
        Files.write(file, Arrays.copyOf(bytes, bytes.length - 3));
        assertThrows(PersistenceException.class, () -> FileElectionStateStore.open(dir));
    }

    @Test
    void leftoverTemporaryFileFromAnInterruptedWriteIsDiscarded() throws IOException {
        FileElectionStateStore.open(dir).save(TermAndVote.of(4, N2));
        Path temp = dir.resolve(FileElectionStateStore.TEMP_FILE_NAME);
        Files.write(temp, new byte[] {1, 2, 3}); // a crash before the rename leaves this behind

        FileElectionStateStore reopened = FileElectionStateStore.open(dir);
        assertEquals(TermAndVote.of(4, N2), reopened.load(), "the last renamed state wins");
        assertFalse(Files.exists(temp));
    }

    @Test
    void failedSaveThrowsAndPoisonsTheInstance() throws IOException {
        assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        FileElectionStateStore store = FileElectionStateStore.open(dir);
        store.save(TermAndVote.of(1, N2));
        Set<PosixFilePermission> original = Files.getPosixFilePermissions(dir);
        Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("r-x------"));
        try {
            assumeTrue(!Files.isWritable(dir), "running with privileges that ignore permissions");
            assertThrows(PersistenceException.class, () -> store.save(TermAndVote.of(2, N3)));
        } finally {
            Files.setPosixFilePermissions(dir, original);
        }
        // Even though the directory is writable again, this instance no longer trusts its view.
        assertThrows(PersistenceException.class, store::load);
        assertThrows(PersistenceException.class, () -> store.save(TermAndVote.of(3, null)));
        assertEquals(TermAndVote.of(1, N2), FileElectionStateStore.open(dir).load());
    }

    @Test
    void raftNodeRestartedOnTheSameDirectoryDoesNotVoteTwice() {
        ClusterConfig cluster = ClusterConfig.of("n1", "n2", "n3");
        RaftNode first = new RaftNode(
                N1, cluster, ElectionTiming.DEFAULT, new SeededRandomSource(1), FileElectionStateStore.open(dir), LogMetadata.empty());
        first.receive(new RequestVoteRequest(N2, N1, 3, LogPosition.EMPTY));

        // New store instance and new node: what a process restart sees.
        RaftNode restarted = new RaftNode(
                N1, cluster, ElectionTiming.DEFAULT, new SeededRandomSource(2), FileElectionStateStore.open(dir), LogMetadata.empty());
        assertEquals(3, restarted.currentTerm());
        assertEquals(Optional.of(N2), restarted.votedFor());
        assertEquals(
                List.of(new RequestVoteResponse(N1, N3, 3, false)),
                restarted.receive(new RequestVoteRequest(N3, N1, 3, LogPosition.EMPTY)));
    }
}
