package io.github.richardeyeboah.quantafs.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ServerConfigTest {

    @Test
    void appliesDefaults() {
        ServerConfig config = ServerConfig.parse(new String[] {"--data-dir", "d"}, Map.of());
        assertEquals("127.0.0.1", config.host());
        assertEquals(50051, config.port());
        assertEquals(Path.of("d"), config.dataDir());
        assertEquals(Duration.ofSeconds(10), config.shutdownGrace());
    }

    @Test
    void flagsOverrideEnvironment() {
        ServerConfig config =
                ServerConfig.parse(
                        new String[] {"--port=7000", "--data-dir", "flag-dir", "--host", "0.0.0.0"},
                        Map.of("QUANTAFS_PORT", "6000", "QUANTAFS_DATA_DIR", "env-dir"));
        assertEquals(7000, config.port());
        assertEquals(Path.of("flag-dir"), config.dataDir());
        assertEquals("0.0.0.0", config.host());
    }

    @Test
    void readsEnvironment() {
        ServerConfig config =
                ServerConfig.parse(
                        new String[0],
                        Map.of(
                                "QUANTAFS_PORT", "6000",
                                "QUANTAFS_DATA_DIR", "env-dir",
                                "QUANTAFS_SHUTDOWN_GRACE_SECONDS", "3"));
        assertEquals(6000, config.port());
        assertEquals(Path.of("env-dir"), config.dataDir());
        assertEquals(Duration.ofSeconds(3), config.shutdownGrace());
    }

    @Test
    void rejectsInvalidInput() {
        assertThrows(IllegalArgumentException.class, () -> parse());
        assertThrows(IllegalArgumentException.class, () -> parse("--data-dir"));
        assertThrows(IllegalArgumentException.class, () -> parse("--data-dir", "d", "--port", "70000"));
        assertThrows(IllegalArgumentException.class, () -> parse("--data-dir", "d", "--port", "abc"));
        assertThrows(IllegalArgumentException.class, () -> parse("--data-dir", "d", "--bogus", "1"));
        assertThrows(
                IllegalArgumentException.class,
                () -> parse("--data-dir", "d", "--shutdown-grace-seconds", "-1"));
    }

    private static ServerConfig parse(String... args) {
        return ServerConfig.parse(args, Map.of());
    }
}
