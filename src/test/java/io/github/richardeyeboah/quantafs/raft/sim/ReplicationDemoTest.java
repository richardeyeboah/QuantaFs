package io.github.richardeyeboah.quantafs.raft.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class ReplicationDemoTest {

    @Test
    void demoReplicatesSurvivesALeaderCrashAndCatchesUpDeterministically() {
        Result first = run("--seed", "7");
        Result second = run("--seed", "7");
        assertEquals(0, first.code(), first.err() + first.out());
        assertEquals(first.withoutTempPath(), second.withoutTempPath(), "same seed, same output");
        String out = first.out();
        assertTrue(out.contains("n3 -> client: index 2 committed and applied; PUT user:1=alice OK"), out);
        assertTrue(out.contains("REJECTED: n1 is not the leader (hint: n3)"), out);
        assertTrue(out.contains("outcome of PUT user:4=dave is UNKNOWN"), out);
        assertTrue(out.contains("n3 log: replaced conflicting uncommitted entries"), out);
        assertTrue(out.contains("Checks: at most one leader per term"), out);
        assertFalse(Files.exists(first.tempPath()), "temporary storage is deleted afterwards");
    }

    @Test
    void fiveVoterDemoSucceedsAndKeepCanPreserveTheFiles() throws IOException {
        Result result = run("--voters", "5", "--seed", "2", "--keep");
        Path kept = result.tempPath();
        try { // delete the kept directory even if an assertion fails
            assertEquals(0, result.code(), result.err() + result.out());
            assertTrue(Files.isDirectory(kept.resolve("n1").resolve("raft-log")));
            assertTrue(Files.isDirectory(kept.resolve("n1").resolve("state-machine")));
        } finally {
            try (Stream<Path> paths = Files.walk(kept)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.delete(path);
                }
            }
        }
    }

    @Test
    void rejectsInvalidArguments() {
        assertEquals(2, run("--voters", "4").code());
        assertEquals(2, run("--bogus").code());
    }

    private static Result run(String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int code = ReplicationDemo.run(
                args, new PrintStream(out, true, StandardCharsets.UTF_8), new PrintStream(err, true, StandardCharsets.UTF_8));
        return new Result(code, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
    }

    private record Result(int code, String out, String err) {
        Path tempPath() {
            return out.lines().filter(l -> l.contains("quantafs-replication-demo-")).findFirst()
                    .map(l -> Path.of(l.trim().replaceFirst("^Storage kept at ", ""))).orElseThrow();
        }

        String withoutTempPath() {
            return out.replace(tempPath().toString(), "<tmp>");
        }
    }
}
