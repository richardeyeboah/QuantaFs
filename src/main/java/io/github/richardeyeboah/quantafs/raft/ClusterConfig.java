package io.github.richardeyeboah.quantafs.raft;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Fixed voting membership. Every node in a cluster must be configured with the same voter set;
 * membership changes are out of scope.
 */
public final class ClusterConfig {

    private final SortedSet<NodeId> voters;

    private ClusterConfig(SortedSet<NodeId> voters) {
        this.voters = voters;
    }

    public static ClusterConfig of(Collection<NodeId> voters) {
        Objects.requireNonNull(voters, "voters");
        TreeSet<NodeId> set = new TreeSet<>(voters);
        if (set.isEmpty()) {
            throw new IllegalArgumentException("a cluster needs at least one voter");
        }
        if (set.size() != voters.size()) {
            throw new IllegalArgumentException("voter ids must be unique: " + voters);
        }
        return new ClusterConfig(set);
    }

    public static ClusterConfig of(String... voterIds) {
        return of(Arrays.stream(voterIds).map(NodeId::of).toList());
    }

    /** Voters in sorted order. */
    public List<NodeId> voters() {
        return List.copyOf(voters);
    }

    public int size() {
        return voters.size();
    }

    public boolean contains(NodeId id) {
        return voters.contains(id);
    }

    /** Votes needed to win an election: a strict majority of all voters, counting the candidate. */
    public int quorum() {
        return voters.size() / 2 + 1;
    }

    /** All voters except {@code self}, in sorted order. */
    public List<NodeId> peersOf(NodeId self) {
        if (!contains(self)) {
            throw new IllegalArgumentException(self + " is not a voter in " + voters);
        }
        return voters.stream().filter(id -> !id.equals(self)).toList();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ClusterConfig other && voters.equals(other.voters);
    }

    @Override
    public int hashCode() {
        return voters.hashCode();
    }

    @Override
    public String toString() {
        return "ClusterConfig" + voters;
    }
}
