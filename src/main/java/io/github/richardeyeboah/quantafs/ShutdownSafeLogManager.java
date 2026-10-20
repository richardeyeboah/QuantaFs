package io.github.richardeyeboah.quantafs;

import java.util.logging.LogManager;

/**
 * The JDK's {@link LogManager} registers its own shutdown hook that closes every log handler. JVM
 * shutdown hooks run concurrently, so messages logged while the server drains RPCs and closes
 * storage would be silently dropped. This manager defers that reset until the application has
 * finished shutting down.
 *
 * <p>Activated by the {@code java.util.logging.manager} system property, which must be set before
 * the first logger is created.
 */
public final class ShutdownSafeLogManager extends LogManager {

    private volatile boolean deferReset;

    @Override
    public void reset() {
        if (!deferReset) {
            super.reset();
        }
    }

    static void deferResetUntilReleased() {
        if (LogManager.getLogManager() instanceof ShutdownSafeLogManager manager) {
            manager.deferReset = true;
        }
    }

    static void releaseAndReset() {
        if (LogManager.getLogManager() instanceof ShutdownSafeLogManager manager) {
            manager.deferReset = false;
            manager.reset();
        }
    }
}
