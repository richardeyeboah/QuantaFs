package io.github.richardeyeboah.quantafs.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.richardeyeboah.quantafs.server.RequestValidator;
import org.junit.jupiter.api.Test;

class CommandTest {

    @Test
    void limitsMatchTheSingleNodeApi() {
        assertEquals(RequestValidator.MAX_KEY_BYTES, Command.MAX_KEY_BYTES);
        assertEquals(RequestValidator.MAX_VALUE_BYTES, Command.MAX_VALUE_BYTES);
    }

    @Test
    void invalidCommandsCannotBeCreatedSoTheyCannotBeProposed() {
        assertThrows(IllegalArgumentException.class, () -> Command.put(new byte[0], new byte[] {1}));
        assertThrows(IllegalArgumentException.class, () -> Command.delete(new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> Command.put(new byte[Command.MAX_KEY_BYTES + 1], new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> Command.put(new byte[] {1}, new byte[Command.MAX_VALUE_BYTES + 1]));
        Command.put(new byte[Command.MAX_KEY_BYTES], new byte[Command.MAX_VALUE_BYTES]); // limits are inclusive
        Command.put(new byte[] {1}, new byte[0]); // an empty value is valid
    }

    @Test
    void binaryContentHasValueSemanticsAndIsDefensivelyCopied() {
        byte[] key = {0, (byte) 0xFF, 0x10};
        byte[] value = {(byte) 0xC3, 0x28};
        Command first = Command.put(key, value);
        key[0] = 9; // mutating the caller's array must not change the command
        assertEquals(Command.put(new byte[] {0, (byte) 0xFF, 0x10}, new byte[] {(byte) 0xC3, 0x28}), first);
        assertNotEquals(Command.put(new byte[] {0, (byte) 0xFF, 0x11}, value), first);
        assertEquals("PUT 0x00ff10=0xc328", first.toString());
    }

    @Test
    void proposingANoOpIsReserved() {
        RaftNode node = new RaftNode(NodeId.of("n1"), ClusterConfig.of("n1"), ElectionTiming.DEFAULT,
                new SeededRandomSource(1), new InMemoryRaftStorage(), new InMemoryStateMachine());
        assertThrows(IllegalArgumentException.class, () -> node.propose(Command.noOp()));
    }
}
