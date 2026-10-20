package io.github.richardeyeboah.quantafs.raft.sim;

/**
 * Simulated network behavior. Each message gets an independent random delay in
 * {@code [minDelayTicks, maxDelayTicks]}, so messages can arrive out of order. Each message may
 * also be dropped, or delivered twice.
 */
public record NetworkConfig(
        int minDelayTicks, int maxDelayTicks, double dropProbability, double duplicateProbability) {

    /** Reliable network with 1-3 ticks of delay. */
    public static final NetworkConfig RELIABLE = new NetworkConfig(1, 3, 0.0, 0.0);

    public NetworkConfig {
        if (minDelayTicks < 1) {
            throw new IllegalArgumentException("delay must be at least 1 tick");
        }
        if (maxDelayTicks < minDelayTicks) {
            throw new IllegalArgumentException("maxDelayTicks must be >= minDelayTicks");
        }
        if (dropProbability < 0 || dropProbability >= 1) {
            throw new IllegalArgumentException("dropProbability must be in [0, 1)");
        }
        if (duplicateProbability < 0 || duplicateProbability > 1) {
            throw new IllegalArgumentException("duplicateProbability must be in [0, 1]");
        }
    }
}
