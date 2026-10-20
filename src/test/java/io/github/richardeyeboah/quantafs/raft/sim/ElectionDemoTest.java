package io.github.richardeyeboah.quantafs.raft.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class ElectionDemoTest {

    @Test
    void demoElectsCrashesAndReelectsDeterministically() {
        Result first = run("--seed", "7");
        Result second = run("--seed", "7");
        assertEquals(0, first.code(), first.err());
        assertEquals(first.out(), second.out(), "same seed, same output");
        assertTrue(first.out().contains("CANDIDATE -> LEADER  term 1"), first.out());
        assertTrue(first.out().contains("CRASH n4 (was LEADER in term 1)"), first.out());
        assertTrue(first.out().contains("CANDIDATE -> LEADER  term 2"), first.out());
        assertTrue(first.out().contains("RESTART n4: reloaded term 1, votedFor n4"), first.out());
        assertTrue(first.out().contains("Election safety: OK"), first.out());
    }

    @Test
    void threeVoterDemoAlsoSucceeds() {
        Result result = run("--voters", "3", "--seed", "3");
        assertEquals(0, result.code(), result.out());
        assertTrue(result.out().contains("quorum 2"));
    }

    @Test
    void rejectsInvalidArguments() {
        assertEquals(2, run("--voters", "4").code());
        assertEquals(2, run("--seed").code());
        assertEquals(2, run("--bogus").code());
    }

    private static Result run(String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int code = ElectionDemo.run(
                args, new PrintStream(out, true, StandardCharsets.UTF_8), new PrintStream(err, true, StandardCharsets.UTF_8));
        return new Result(code, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
    }

    private record Result(int code, String out, String err) {}
}
