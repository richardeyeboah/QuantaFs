package io.github.richardeyeboah.quantafs;

import io.github.richardeyeboah.quantafs.client.QuantaCli;
import io.github.richardeyeboah.quantafs.raft.sim.ElectionDemo;
import io.github.richardeyeboah.quantafs.raft.sim.ReplicationDemo;
import io.github.richardeyeboah.quantafs.server.QuantaServer;
import io.github.richardeyeboah.quantafs.server.ServerConfig;
import io.github.richardeyeboah.quantafs.storage.StorageException;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Entry point for {@code java -jar quantafs.jar (server|client|election-demo) ...}. */
public final class Main {

    private static final String USAGE =
            """
            Usage: java -jar quantafs.jar <server|client|cluster-server|cluster-status|election-demo|replication-demo> [options]

              server            single-node durable key-value server (milestone 1)
              client            command-line client for the server
              cluster-server    real local Raft node (3 or 5 fixed voters)
              cluster-status    inspect a local cluster node's role and progress
              election-demo     deterministic simulation of Raft leader election (milestone 2A)
              replication-demo  deterministic simulation of Raft log replication with per-node
                                RocksDB storage in a temporary directory (milestone 2B)

            The demos run simulated nodes in one process. They are not connected to the server.

            Run "<command> --help" for details.
            """;

    private Main() {}

    public static void main(String[] args) {
        if (System.getProperty("java.util.logging.manager") == null) {
            System.setProperty("java.util.logging.manager", ShutdownSafeLogManager.class.getName());
        }
        if (System.getProperty("java.util.logging.SimpleFormatter.format") == null) {
            System.setProperty(
                    "java.util.logging.SimpleFormatter.format",
                    "%1$tF %1$tT.%1$tL %4$s %3$s - %5$s%6$s%n");
        }
        if (args.length == 0) {
            System.err.print(USAGE);
            System.exit(2);
        }
        String[] rest = Arrays.copyOfRange(args, 1, args.length);
        switch (args[0]) {
            case "server" -> {
                // On a clean stop, return instead of calling System.exit: the shutdown hook may
                // still be closing storage, and System.exit would block behind it.
                int code = runServer(rest);
                if (code != 0) {
                    System.exit(code);
                }
            }
            case "cluster-server" -> runCluster(rest);
            case "cluster-status" -> System.exit(io.github.richardeyeboah.quantafs.cluster.ClusterCommands.status(rest));
            case "client" -> System.exit(QuantaCli.run(rest, System.out, System.err));
            case "election-demo" -> System.exit(ElectionDemo.run(rest, System.out, System.err));
            case "replication-demo" -> System.exit(ReplicationDemo.run(rest, System.out, System.err));
            case "-h", "--help", "help" -> System.out.print(USAGE);
            default -> {
                System.err.println("error: unknown command: " + args[0]);
                System.err.print(USAGE);
                System.exit(2);
            }
        }
    }

    private static void runCluster(String[] args) {
        if (Arrays.asList(args).contains("--help")) {
            System.out.println(io.github.richardeyeboah.quantafs.cluster.ClusterSettings.USAGE); return;
        }
        try (var cluster = io.github.richardeyeboah.quantafs.cluster.ClusterServer.start(
                io.github.richardeyeboah.quantafs.cluster.ClusterSettings.parse(args))) {
            Thread hook = new Thread(cluster::close, "cluster-shutdown");
            Runtime.getRuntime().addShutdownHook(hook);
            try { cluster.awaitTermination(); }
            finally { try { Runtime.getRuntime().removeShutdownHook(hook); } catch(IllegalStateException ignored) {} }
        } catch (Exception e) {
            System.err.println("Cluster startup/runtime failed: " + e.getMessage()); System.exit(1);
        }
    }

    private static int runServer(String[] args) {
        Logger log = Logger.getLogger(Main.class.getName());
        if (Arrays.asList(args).contains("--help") || Arrays.asList(args).contains("-h")) {
            System.out.print(ServerConfig.USAGE);
            return 0;
        }

        ServerConfig config;
        try {
            config = ServerConfig.parse(args, System.getenv());
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.err.print(ServerConfig.USAGE);
            return 2;
        }

        QuantaServer server;
        try {
            server = QuantaServer.start(config);
        } catch (IOException | StorageException e) {
            log.log(Level.SEVERE, "Failed to start: " + e.getMessage(), e);
            return 1;
        }

        // Runs on SIGTERM (e.g. docker stop, kubectl delete) and SIGINT (Ctrl-C), not on SIGKILL.
        ShutdownSafeLogManager.deferResetUntilReleased();
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    server.shutdown();
                                    ShutdownSafeLogManager.releaseAndReset();
                                },
                                "quantafs-shutdown"));
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            server.shutdown();
        }
        return 0;
    }
}
