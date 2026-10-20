package io.github.richardeyeboah.quantafs.raft.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.richardeyeboah.quantafs.raft.Bytes;
import io.github.richardeyeboah.quantafs.raft.ClusterConfig;
import io.github.richardeyeboah.quantafs.raft.Command;
import io.github.richardeyeboah.quantafs.raft.ElectionTiming;
import io.github.richardeyeboah.quantafs.raft.NodeId;
import io.github.richardeyeboah.quantafs.raft.PersistenceException;
import io.github.richardeyeboah.quantafs.raft.ProposalOutcome;
import io.github.richardeyeboah.quantafs.raft.ProposeResult;
import io.github.richardeyeboah.quantafs.raft.RaftNode;
import io.github.richardeyeboah.quantafs.raft.Role;
import io.github.richardeyeboah.quantafs.raft.SeededRandomSource;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NodeStorageTest {

    private static final NodeId N1 = NodeId.of("n1");

    @TempDir Path dir;

    @Test
    void onlyOneOwnerAtATime() {
        try (NodeStorage first = NodeStorage.open(dir)) {
            PersistenceException e = assertThrows(PersistenceException.class, () -> NodeStorage.open(dir));
            assertTrue(e.getMessage().contains("already open"), e.getMessage());
            assertEquals(0, first.raftStorage().lastIndex(), "the failed open did not disturb the owner");
        }
        try (NodeStorage reopened = NodeStorage.open(dir)) {
            assertEquals(dir.toAbsolutePath().normalize(), reopened.directory(), "the lock was released by close");
            assertTrue(Files.exists(dir.resolve(NodeStorage.LOCK_FILE)));
            assertTrue(Files.isDirectory(dir.resolve(NodeStorage.RAFT_LOG_DIR)));
            assertTrue(Files.isDirectory(dir.resolve(NodeStorage.STATE_MACHINE_DIR)));
        }
    }

    @Test
    void singleVoterNodeKeepsItsLogAndAppliedStateAcrossReopen() {
        try (NodeStorage storage = NodeStorage.open(dir)) {
            RaftNode node = node(storage);
            for (int i = 0; i < 100 && node.role() != Role.LEADER; i++) {
                node.tick(); // a single voter elects itself once its election timeout expires
            }
            assertEquals(Role.LEADER, node.role());
            ProposeResult result = node.propose(Command.put(utf8("k"), utf8("v")));
            ProposeResult.Accepted accepted = assertInstanceOf(ProposeResult.Accepted.class, result);
            // One voter is its own majority: committed and applied before propose returns.
            assertEquals(List.of(new ProposalOutcome(accepted.index(), accepted.term(), ProposalOutcome.Status.COMMITTED)),
                    node.drainOutcomes());
        }
        try (NodeStorage storage = NodeStorage.open(dir)) {
            RaftNode restarted = node(storage);
            assertEquals(2, storage.raftStorage().lastIndex(), "NO_OP + PUT");
            assertEquals(2, restarted.lastApplied());
            assertEquals(2, restarted.commitIndex(), "restarts at the applied marker");
            assertEquals(Map.of(Bytes.utf8("k"), Bytes.utf8("v")), storage.stateMachine().diagnosticContents());
        }
    }

    private static RaftNode node(NodeStorage storage) {
        return new RaftNode(N1, ClusterConfig.of("n1"), ElectionTiming.DEFAULT, new SeededRandomSource(1),
                storage.raftStorage(), storage.stateMachine());
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
