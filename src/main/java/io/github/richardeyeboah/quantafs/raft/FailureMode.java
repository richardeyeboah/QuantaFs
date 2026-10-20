package io.github.richardeyeboah.quantafs.raft;

/** Failure injection for the in-memory storage and state machine used in tests and simulation. */
public enum FailureMode {
    /** Operate normally. */
    NONE,
    /** Throw before changing anything: the operation definitely did not happen. */
    FAIL_BEFORE,
    /**
     * Complete the operation, then throw: it happened, but the caller is told it failed. This is
     * the "crashed right after the disk write, before the caller recorded success" boundary.
     */
    FAIL_AFTER
}
