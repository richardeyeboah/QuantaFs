package io.github.richardeyeboah.quantafs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.protobuf.ByteString;
import io.github.richardeyeboah.quantafs.api.v1.DeleteRequest;
import io.github.richardeyeboah.quantafs.api.v1.GetRequest;
import io.github.richardeyeboah.quantafs.api.v1.GetResponse;
import io.github.richardeyeboah.quantafs.api.v1.KeyValueServiceGrpc;
import io.github.richardeyeboah.quantafs.api.v1.KeyValueServiceGrpc.KeyValueServiceBlockingStub;
import io.github.richardeyeboah.quantafs.api.v1.PutRequest;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs the packaged server jar as a separate OS process and terminates it from outside.
 *
 * <p>SIGKILL ends the process immediately: no shutdown hook, no RocksDB close, no memtable flush.
 * It does not discard data the OS has already accepted into its page cache, so these tests do not
 * simulate power loss or an OS crash.
 */
class ServerProcessIT {

    private static final Pattern LISTENING = Pattern.compile("QuantaFs listening on \\S+:(\\d+)");
    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(60);
    private static final int WRITERS = 4;
    private static final int KEYS_PER_WRITER = 50;
    private static final int ACKS_BEFORE_KILL = 400;

    @TempDir Path workDir;

    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach
    void cleanUp() throws Exception {
        for (int i = resources.size() - 1; i >= 0; i--) {
            resources.get(i).close();
        }
    }

    @Test
    void acknowledgedWritesSurviveSigkillDuringConcurrentWrites() throws Exception {
        Path dataDir = workDir.resolve("data");
        ServerProcess first = startServer(dataDir, "run-1.log");
        KeyValueServiceBlockingStub stub = connect(first.port());

        AtomicInteger acknowledged = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(WRITERS);
        List<Future<Writer>> futures = new ArrayList<>();
        for (int id = 0; id < WRITERS; id++) {
            futures.add(pool.submit(new Writer(id, stub, acknowledged)));
        }

        awaitAcknowledgements(acknowledged, first);
        first.process().destroyForcibly();
        assertTrue(first.process().waitFor(30, TimeUnit.SECONDS), "server did not die");
        pool.shutdown();

        List<Writer> writers = new ArrayList<>();
        for (Future<Writer> future : futures) {
            writers.add(future.get(60, TimeUnit.SECONDS));
        }
        assertFalse(
                Files.readString(first.log()).contains("Storage closed"),
                "SIGKILL must not run the graceful shutdown path");

        ServerProcess second = startServer(dataDir, "run-2.log");
        KeyValueServiceBlockingStub restarted = connect(second.port());

        int ambiguous = 0;
        int checkedKeys = 0;
        for (Writer writer : writers) {
            Op pending = writer.pending;
            for (Map.Entry<String, Optional<String>> entry : writer.ackedState.entrySet()) {
                if (pending != null && entry.getKey().equals(pending.key())) {
                    continue;
                }
                assertEquals(entry.getValue(), read(restarted, entry.getKey()), "key " + entry.getKey());
                checkedKeys++;
            }
            if (pending != null) {
                ambiguous++;
                Optional<String> before = writer.ackedState.getOrDefault(pending.key(), Optional.empty());
                Optional<String> actual = read(restarted, pending.key());
                assertTrue(
                        actual.equals(before) || actual.equals(pending.intended()),
                        "in-flight key "
                                + pending.key()
                                + " must hold its last acknowledged state "
                                + before
                                + " or the in-flight state "
                                + pending.intended()
                                + ", got "
                                + actual);
            }
        }
        System.out.printf(
                "SIGKILL recovery: %d acknowledged operations, %d keys verified exactly, "
                        + "%d in-flight operations with unknown outcome%n",
                acknowledged.get(), checkedKeys, ambiguous);
        assertTrue(acknowledged.get() >= ACKS_BEFORE_KILL);
    }

    @Test
    void sigtermDrainsAndClosesStorageThenDataIsReadableAfterRestart() throws Exception {
        Path dataDir = workDir.resolve("data");
        ServerProcess first = startServer(dataDir, "run-1.log");
        KeyValueServiceBlockingStub stub = connect(first.port());
        stub.put(PutRequest.newBuilder().setKey(utf8("greeting")).setValue(utf8("hello")).build());

        first.process().destroy();
        assertTrue(first.process().waitFor(30, TimeUnit.SECONDS), "server did not stop on SIGTERM");
        String log = Files.readString(first.log());
        assertTrue(log.contains("gRPC server stopped"), log);
        assertTrue(log.contains("Storage closed"), log);

        ServerProcess second = startServer(dataDir, "run-2.log");
        assertEquals(Optional.of("hello"), read(connect(second.port()), "greeting"));
    }

    @Test
    void secondServerOnSameDataDirectoryRefusesToStart() throws Exception {
        Path dataDir = workDir.resolve("data");
        ServerProcess first = startServer(dataDir, "run-1.log");

        Path secondLog = workDir.resolve("run-2.log");
        Process second = launch(dataDir, secondLog);
        assertTrue(second.waitFor(60, TimeUnit.SECONDS), "second server should exit");
        assertEquals(1, second.exitValue());
        String log = Files.readString(secondLog);
        assertTrue(log.contains("lock"), log);

        KeyValueServiceBlockingStub stub = connect(first.port());
        stub.put(PutRequest.newBuilder().setKey(utf8("k")).setValue(utf8("v")).build());
        assertEquals(Optional.of("v"), read(stub, "k"));
    }

    /** Writes to its own key range, recording the state of each key after every acknowledged op. */
    private static final class Writer implements Callable<Writer> {
        private final int id;
        private final KeyValueServiceBlockingStub stub;
        private final AtomicInteger acknowledged;
        final Map<String, Optional<String>> ackedState = new HashMap<>();
        Op pending;

        Writer(int id, KeyValueServiceBlockingStub stub, AtomicInteger acknowledged) {
            this.id = id;
            this.stub = stub;
            this.acknowledged = acknowledged;
        }

        @Override
        public Writer call() {
            for (int i = 0; ; i++) {
                String key = "w" + id + "-k" + (i % KEYS_PER_WRITER);
                boolean isDelete = i % 7 == 6;
                Optional<String> intended = isDelete ? Optional.empty() : Optional.of("w" + id + "-v" + i);
                pending = new Op(key, intended);
                try {
                    KeyValueServiceBlockingStub call = stub.withDeadlineAfter(10, TimeUnit.SECONDS);
                    if (isDelete) {
                        call.delete(DeleteRequest.newBuilder().setKey(utf8(key)).build());
                    } else {
                        call.put(PutRequest.newBuilder().setKey(utf8(key)).setValue(utf8(intended.get())).build());
                    }
                } catch (StatusRuntimeException e) {
                    return this;
                }
                ackedState.put(key, intended);
                pending = null;
                acknowledged.incrementAndGet();
            }
        }
    }

    private record Op(String key, Optional<String> intended) {}

    private record ServerProcess(Process process, int port, Path log) {}

    private void awaitAcknowledgements(AtomicInteger acknowledged, ServerProcess server)
            throws Exception {
        Instant deadline = Instant.now().plusSeconds(120);
        while (acknowledged.get() < ACKS_BEFORE_KILL) {
            if (Instant.now().isAfter(deadline) || !server.process().isAlive()) {
                fail("only " + acknowledged.get() + " writes acknowledged\n" + Files.readString(server.log()));
            }
            Thread.sleep(5);
        }
    }

    private ServerProcess startServer(Path dataDir, String logName) throws Exception {
        Path log = workDir.resolve(logName);
        Process process = launch(dataDir, log);
        resources.add(
                () -> {
                    process.destroyForcibly();
                    process.waitFor(30, TimeUnit.SECONDS);
                });
        Instant deadline = Instant.now().plus(STARTUP_TIMEOUT);
        while (Instant.now().isBefore(deadline)) {
            String output = Files.exists(log) ? Files.readString(log) : "";
            Matcher matcher = LISTENING.matcher(output);
            if (matcher.find()) {
                return new ServerProcess(process, Integer.parseInt(matcher.group(1)), log);
            }
            if (!process.isAlive()) {
                fail("server exited with " + process.exitValue() + " before listening\n" + output);
            }
            Thread.sleep(20);
        }
        return fail("server did not start within " + STARTUP_TIMEOUT + "\n" + Files.readString(log));
    }

    private static Process launch(Path dataDir, Path log) throws IOException {
        String jar = System.getProperty("quantafs.jar");
        assertNotNull(jar, "system property quantafs.jar is not set; run via ./mvnw verify");
        assertTrue(Files.isRegularFile(Path.of(jar)), "missing " + jar + "; run ./mvnw verify");
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        return new ProcessBuilder(
                        java, "-jar", jar, "server",
                        "--port", "0",
                        "--data-dir", dataDir.toString(),
                        "--shutdown-grace-seconds", "5")
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(log.toFile()))
                .start();
    }

    private KeyValueServiceBlockingStub connect(int port) {
        ManagedChannel channel =
                Grpc.newChannelBuilder("127.0.0.1:" + port, InsecureChannelCredentials.create()).build();
        resources.add(
                () -> {
                    channel.shutdownNow();
                    channel.awaitTermination(5, TimeUnit.SECONDS);
                });
        return KeyValueServiceGrpc.newBlockingStub(channel);
    }

    private static Optional<String> read(KeyValueServiceBlockingStub stub, String key) {
        GetResponse response =
                stub.withDeadlineAfter(10, TimeUnit.SECONDS)
                        .get(GetRequest.newBuilder().setKey(utf8(key)).build());
        return response.getFound() ? Optional.of(response.getValue().toStringUtf8()) : Optional.empty();
    }

    private static ByteString utf8(String s) {
        return ByteString.copyFromUtf8(s);
    }
}
