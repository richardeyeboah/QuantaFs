package io.github.richardeyeboah.quantafs.raft;

import java.util.SplittableRandom;

/** Deterministic {@link RandomSource}: the same seed always yields the same sequence. */
public final class SeededRandomSource implements RandomSource {

    private final SplittableRandom random;

    public SeededRandomSource(long seed) {
        this(new SplittableRandom(seed));
    }

    public SeededRandomSource(SplittableRandom random) {
        this.random = random;
    }

    @Override
    public int nextInt(int origin, int bound) {
        return random.nextInt(origin, bound);
    }
}
