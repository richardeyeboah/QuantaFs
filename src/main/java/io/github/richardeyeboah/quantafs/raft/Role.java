package io.github.richardeyeboah.quantafs.raft;

/** The three Raft server states (Raft paper, section 5.1). */
public enum Role {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
