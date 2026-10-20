package io.github.richardeyeboah.quantafs.server;

import io.github.richardeyeboah.quantafs.storage.KeyValueStore;
import io.github.richardeyeboah.quantafs.storage.RocksDbKeyValueStore;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Owns the gRPC server and the store, and shuts them down in a safe order: stop accepting RPCs,
 * let in-flight RPCs finish (up to the grace period), cancel the rest, then close the store.
 */
public final class QuantaServer implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(QuantaServer.class.getName());
    private static final Duration FORCED_SHUTDOWN_WAIT = Duration.ofSeconds(5);

    private final ServerConfig config;
    private final Server server;
    private final KeyValueStore store;
    private boolean stopped; // guarded by this

    private QuantaServer(ServerConfig config, Server server, KeyValueStore store) {
        this.config = config;
        this.server = server;
        this.store = store;
    }

    /** Opens the RocksDB store in the configured data directory and starts serving. */
    public static QuantaServer start(ServerConfig config) throws IOException {
        Objects.requireNonNull(config, "config");
        RocksDbKeyValueStore store = RocksDbKeyValueStore.open(config.dataDir());
        try {
            return start(config, store);
        } catch (IOException | RuntimeException e) {
            store.close();
            throw e;
        }
    }

    /** Starts serving {@code store}. On success the returned server owns and closes the store. */
    static QuantaServer start(ServerConfig config, KeyValueStore store) throws IOException {
        Server server =
                NettyServerBuilder.forAddress(new InetSocketAddress(config.host(), config.port()))
                        .maxInboundMessageSize(RequestValidator.MAX_INBOUND_MESSAGE_BYTES)
                        .addService(new KeyValueGrpcService(store))
                        .build()
                        .start();
        LOG.info(
                () ->
                        "QuantaFs listening on "
                                + config.host()
                                + ":"
                                + server.getPort()
                                + " (data dir "
                                + config.dataDir().toAbsolutePath().normalize()
                                + ")");
        return new QuantaServer(config, server, store);
    }

    /** The bound port; useful when the configured port is 0. */
    public int port() {
        return server.getPort();
    }

    /** Blocks until the gRPC server has terminated. */
    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    /** Gracefully stops the server and closes the store. Idempotent and thread-safe. */
    public synchronized void shutdown() {
        if (stopped) {
            return;
        }
        stopped = true;
        Duration grace = config.shutdownGrace();
        LOG.info(() -> "Shutdown requested; draining in-flight RPCs for up to " + grace.toSeconds() + "s");
        server.shutdown();
        try {
            if (!server.awaitTermination(grace.toMillis(), TimeUnit.MILLISECONDS)) {
                LOG.warning("Grace period elapsed; cancelling remaining RPCs");
                server.shutdownNow();
                server.awaitTermination(FORCED_SHUTDOWN_WAIT.toMillis(), TimeUnit.MILLISECONDS);
            }
            LOG.info("gRPC server stopped");
        } catch (InterruptedException e) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            try {
                store.close();
                LOG.info("Storage closed");
            } catch (RuntimeException e) {
                LOG.log(Level.SEVERE, "Failed to close storage cleanly", e);
            }
        }
    }

    @Override
    public void close() {
        shutdown();
    }
}
