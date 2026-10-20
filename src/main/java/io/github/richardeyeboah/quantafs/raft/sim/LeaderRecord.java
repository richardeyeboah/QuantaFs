package io.github.richardeyeboah.quantafs.raft.sim;

import io.github.richardeyeboah.quantafs.raft.NodeId;

/** A node became leader of {@code term} at logical time {@code tick}. */
public record LeaderRecord(long term, NodeId leader, long tick) {}
