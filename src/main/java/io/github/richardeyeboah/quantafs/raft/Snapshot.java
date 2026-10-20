package io.github.richardeyeboah.quantafs.raft;
import java.util.Objects;
/** Immutable state-machine image at a committed log boundary. Transfer limit is explicit. */
public record Snapshot(LogPosition position,Bytes image) {
    public static final int MAX_BYTES=8*1024*1024;
    public Snapshot {
        Objects.requireNonNull(position);Objects.requireNonNull(image);
        if(position.index()<1 || image.size()>MAX_BYTES) throw new IllegalArgumentException("invalid/oversized snapshot");
    }
}
