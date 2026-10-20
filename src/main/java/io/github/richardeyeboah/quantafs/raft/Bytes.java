package io.github.richardeyeboah.quantafs.raft;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Immutable byte sequence with value equality and unsigned lexicographic ordering, so binary keys
 * and values can be compared and used as map keys.
 */
public final class Bytes implements Comparable<Bytes> {

    private final byte[] data;

    private Bytes(byte[] data) {
        this.data = data;
    }

    public static Bytes copyOf(byte[] data) {
        return new Bytes(Objects.requireNonNull(data, "data").clone());
    }

    public static Bytes utf8(String text) {
        return new Bytes(text.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] toByteArray() {
        return data.clone();
    }

    public int size() {
        return data.length;
    }

    @Override
    public int compareTo(Bytes other) {
        return Arrays.compareUnsigned(data, other.data);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Bytes other && Arrays.equals(data, other.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    /** Printable UTF-8 text as-is, anything else as {@code 0x...} hex. */
    @Override
    public String toString() {
        try {
            String text = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(data))
                    .toString();
            if (text.chars().allMatch(c -> c >= 0x20 && c != 0x7f)) {
                return text;
            }
        } catch (CharacterCodingException e) {
            // fall through to hex
        }
        return "0x" + HexFormat.of().formatHex(data);
    }
}
