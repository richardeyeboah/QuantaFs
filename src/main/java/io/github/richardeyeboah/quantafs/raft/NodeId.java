package io.github.richardeyeboah.quantafs.raft;

import java.util.Objects;
import java.util.regex.Pattern;

/** Stable, unique identity of a Raft voter, for example {@code n1}. */
public record NodeId(String value) implements Comparable<NodeId> {

    private static final Pattern VALID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_.-]{0,63}");

    public NodeId {
        Objects.requireNonNull(value, "value");
        if (!VALID.matcher(value).matches()) {
            throw new IllegalArgumentException(
                    "node id must be 1-64 characters of letters, digits, '_', '.', '-': '" + value + "'");
        }
    }

    public static NodeId of(String value) {
        return new NodeId(value);
    }

    @Override
    public int compareTo(NodeId other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
