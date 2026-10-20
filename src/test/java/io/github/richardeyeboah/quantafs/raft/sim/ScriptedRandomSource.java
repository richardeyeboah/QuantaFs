package io.github.richardeyeboah.quantafs.raft.sim;

import io.github.richardeyeboah.quantafs.raft.RandomSource;
import io.github.richardeyeboah.quantafs.raft.SeededRandomSource;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/** Returns scripted values first (each must lie in the requested range), then seeded values. */
final class ScriptedRandomSource implements RandomSource {

    private final Deque<Integer> scripted;
    private final RandomSource fallback;

    ScriptedRandomSource(List<Integer> scripted, long fallbackSeed) {
        this.scripted = new ArrayDeque<>(scripted);
        this.fallback = new SeededRandomSource(fallbackSeed);
    }

    @Override
    public int nextInt(int origin, int bound) {
        if (scripted.isEmpty()) {
            return fallback.nextInt(origin, bound);
        }
        int value = scripted.pop();
        if (value < origin || value >= bound) {
            throw new IllegalArgumentException(value + " is outside [" + origin + ", " + bound + ")");
        }
        return value;
    }
}
