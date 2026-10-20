package io.github.richardeyeboah.quantafs.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.richardeyeboah.quantafs.server.QuantaServer;
import io.github.richardeyeboah.quantafs.server.ServerConfig;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class QuantaCliTest {

    @TempDir Path dataDir;

    private QuantaServer server;
    private String target;

    @BeforeEach
    void startServer() throws IOException {
        server = QuantaServer.start(new ServerConfig("127.0.0.1", 0, dataDir, Duration.ofSeconds(5)));
        target = "127.0.0.1:" + server.port();
    }

    @AfterEach
    void stopServer() {
        server.shutdown();
    }

    @Test
    void putGetDeleteRoundTrip() {
        assertEquals(new Result(0, "OK\n", ""), run("put", "greeting", "hello world"));
        assertEquals(new Result(0, "hello world\n", ""), run("get", "greeting"));
        assertEquals(new Result(0, "OK\n", ""), run("delete", "greeting"));
        assertEquals(new Result(1, "", "NOT FOUND\n"), run("get", "greeting"));
    }

    @Test
    void invalidArgumentIsReportedWithRpcErrorExitCode() {
        Result result = run("put", "", "value");
        assertEquals(QuantaCli.EXIT_RPC_ERROR, result.code());
        assertTrue(result.err().startsWith("error: INVALID_ARGUMENT"), result.err());
    }

    @Test
    void usageErrorsDoNotContactServer() {
        assertEquals(QuantaCli.EXIT_USAGE, run().code());
        assertEquals(QuantaCli.EXIT_USAGE, run("get").code());
        assertEquals(QuantaCli.EXIT_USAGE, run("frobnicate", "k").code());
    }

    @Test
    void unreachableServerIsRpcError() {
        server.shutdown();
        Result result = run("get", "k");
        assertEquals(QuantaCli.EXIT_RPC_ERROR, result.code());
        assertTrue(result.err().startsWith("error: UNAVAILABLE"), result.err());
    }

    private Result run(String... command) {
        String[] args = new String[command.length + 4];
        args[0] = "--target";
        args[1] = target;
        args[2] = "--timeout-seconds";
        args[3] = "5";
        System.arraycopy(command, 0, args, 4, command.length);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int code =
                QuantaCli.run(
                        args,
                        new PrintStream(out, true, StandardCharsets.UTF_8),
                        new PrintStream(err, true, StandardCharsets.UTF_8));
        String errText = err.toString(StandardCharsets.UTF_8);
        if (code == QuantaCli.EXIT_USAGE) {
            errText = errText.lines().findFirst().orElse("");
        }
        return new Result(code, out.toString(StandardCharsets.UTF_8), errText);
    }

    private record Result(int code, String out, String err) {}
}
