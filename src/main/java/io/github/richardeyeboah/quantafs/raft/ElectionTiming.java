package io.github.richardeyeboah.quantafs.raft;

/**
 * Timing in logical ticks. The core never reads a clock; whoever drives it decides how much real
 * time one tick represents (the simulator uses none at all).
 *
 * @param heartbeatIntervalTicks how often a leader sends heartbeats
 * @param electionTimeoutMinTicks inclusive lower bound of the randomized election timeout
 * @param electionTimeoutMaxTicks exclusive upper bound of the randomized election timeout
 */
public record ElectionTiming(
        int heartbeatIntervalTicks, int electionTimeoutMinTicks, int electionTimeoutMaxTicks) {

    /** Heartbeat every 3 ticks; election timeout drawn from [10, 20) ticks. */
    public static final ElectionTiming DEFAULT = new ElectionTiming(3, 10, 20);

    public ElectionTiming {
        if (heartbeatIntervalTicks < 1) {
            throw new IllegalArgumentException("heartbeat interval must be at least 1 tick");
        }
        if (electionTimeoutMinTicks <= heartbeatIntervalTicks) {
            throw new IllegalArgumentException(
                    "election timeout minimum must exceed the heartbeat interval, or followers "
                            + "would time out between heartbeats");
        }
        if (electionTimeoutMaxTicks <= electionTimeoutMinTicks) {
            throw new IllegalArgumentException(
                    "election timeout range must be non-empty: [min, max) with max > min");
        }
    }
}
