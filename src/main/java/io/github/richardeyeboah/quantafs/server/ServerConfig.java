package io.github.richardeyeboah.quantafs.server;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * Server settings. Command-line flags take precedence over environment variables, which take
 * precedence over defaults. The data directory has no default so data is never written to an
 * unexpected location.
 *
 * @param host address to bind; defaults to loopback so the unauthenticated, plaintext API is
 *     not exposed to the network unless explicitly requested
 * @param port TCP port; 0 asks the OS for a free port
 * @param dataDir RocksDB data directory
 * @param shutdownGrace how long in-flight RPCs may run after shutdown begins
 */
public record ServerConfig(String host, int port, Path dataDir, Duration shutdownGrace) {

    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final int DEFAULT_PORT = 50051;
    public static final Duration DEFAULT_SHUTDOWN_GRACE = Duration.ofSeconds(10);

    public static final String USAGE =
            """
            Usage: server --data-dir <path> [--host <address>] [--port <port>] \
            [--shutdown-grace-seconds <seconds>]

              --data-dir                 RocksDB data directory (required)   env QUANTAFS_DATA_DIR
              --host                     bind address (default 127.0.0.1)    env QUANTAFS_HOST
              --port                     TCP port, 0 = any free port (default 50051)
                                                                             env QUANTAFS_PORT
              --shutdown-grace-seconds   time allowed for in-flight RPCs on shutdown (default 10)
                                                                             env QUANTAFS_SHUTDOWN_GRACE_SECONDS
            """;

    public ServerConfig {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(dataDir, "dataDir");
        Objects.requireNonNull(shutdownGrace, "shutdownGrace");
        if (host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port < 0 || port > 65_535) {
            throw new IllegalArgumentException("port must be between 0 and 65535, got " + port);
        }
        if (shutdownGrace.isNegative()) {
            throw new IllegalArgumentException("shutdown grace must not be negative");
        }
    }

    /**
     * Parses {@code --flag value} or {@code --flag=value} arguments.
     *
     * @throws IllegalArgumentException with a user-facing message on invalid input
     */
    public static ServerConfig parse(String[] args, Map<String, String> env) {
        String host = env.getOrDefault("QUANTAFS_HOST", DEFAULT_HOST);
        String port = env.getOrDefault("QUANTAFS_PORT", Integer.toString(DEFAULT_PORT));
        String dataDir = env.get("QUANTAFS_DATA_DIR");
        String grace =
                env.getOrDefault(
                        "QUANTAFS_SHUTDOWN_GRACE_SECONDS",
                        Long.toString(DEFAULT_SHUTDOWN_GRACE.toSeconds()));

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            String name;
            String value;
            int eq = arg.indexOf('=');
            if (arg.startsWith("--") && eq > 0) {
                name = arg.substring(0, eq);
                value = arg.substring(eq + 1);
            } else {
                name = arg;
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("missing value for " + name);
                }
                value = args[++i];
            }
            switch (name) {
                case "--host" -> host = value;
                case "--port" -> port = value;
                case "--data-dir" -> dataDir = value;
                case "--shutdown-grace-seconds" -> grace = value;
                default -> throw new IllegalArgumentException("unknown option: " + name);
            }
        }

        if (dataDir == null || dataDir.isBlank()) {
            throw new IllegalArgumentException("--data-dir is required");
        }
        return new ServerConfig(
                host,
                parseInt("port", port),
                Path.of(dataDir),
                Duration.ofSeconds(parseInt("shutdown grace seconds", grace)));
    }

    private static int parseInt(String what, String value) {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(what + " must be an integer, got '" + value + "'");
        }
    }
}
