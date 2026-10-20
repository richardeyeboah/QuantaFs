package io.github.richardeyeboah.quantafs.raft;

/** Injected randomness, so elections are reproducible from a seed. */
@FunctionalInterface
public interface RandomSource {

    /** Returns a value in {@code [origin, bound)}. */
    int nextInt(int origin, int bound);
}
