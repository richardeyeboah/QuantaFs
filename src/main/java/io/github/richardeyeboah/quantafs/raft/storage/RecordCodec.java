package io.github.richardeyeboah.quantafs.raft.storage;

import io.github.richardeyeboah.quantafs.raft.Bytes;
import io.github.richardeyeboah.quantafs.raft.Command;
import io.github.richardeyeboah.quantafs.raft.LogEntry;
import io.github.richardeyeboah.quantafs.raft.LogPosition;
import io.github.richardeyeboah.quantafs.raft.NodeId;
import io.github.richardeyeboah.quantafs.raft.TermAndVote;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.zip.CRC32;

/**
 * Binary encoding of the records QuantaFs stores in RocksDB. Every record is
 * {@code [type][format version][payload][CRC32 of everything before it]}. RocksDB already checksums
 * its own blocks; this per-record checksum additionally catches a value that is well-formed for
 * RocksDB but wrong for us (for example, a record of the wrong type written under a key).
 */
final class RecordCodec {

    private static final byte TERM_AND_VOTE = 1;
    private static final byte LOG_ENTRY = 2;
    private static final byte APPLIED_MARKER = 3;
    private static final byte FORMAT_VERSION = 1;

    private static final byte CMD_NO_OP = 0;
    private static final byte CMD_PUT = 1;
    private static final byte CMD_DELETE = 2;

    private RecordCodec() {}

    static byte[] encodeTermAndVote(TermAndVote state) {
        return frame(TERM_AND_VOTE, out -> {
            out.writeLong(state.currentTerm());
            out.writeBoolean(state.votedFor().isPresent());
            if (state.votedFor().isPresent()) {
                out.writeUTF(state.votedFor().get().value());
            }
        });
    }

    static TermAndVote decodeTermAndVote(byte[] bytes) throws IOException {
        DataInputStream in = unframe(TERM_AND_VOTE, bytes);
        long term = in.readLong();
        Optional<NodeId> vote = in.readBoolean() ? Optional.of(NodeId.of(in.readUTF())) : Optional.empty();
        expectEnd(in);
        return new TermAndVote(term, vote);
    }

    static byte[] encodeEntry(LogEntry entry) {
        return frame(LOG_ENTRY, out -> {
            out.writeLong(entry.index());
            out.writeLong(entry.term());
            switch (entry.command()) {
                case Command.Request request -> {
                    out.writeByte(3); out.writeUTF(request.clientId()); out.writeLong(request.sequence());
                    writeBytes(out, Bytes.copyOf(encodeEntry(new LogEntry(entry.index(),entry.term(),request.mutation()))));
                }
                case Command.NoOp noOp -> out.writeByte(CMD_NO_OP);
                case Command.Put put -> {
                    out.writeByte(CMD_PUT);
                    writeBytes(out, put.key());
                    writeBytes(out, put.value());
                }
                case Command.Delete delete -> {
                    out.writeByte(CMD_DELETE);
                    writeBytes(out, delete.key());
                }
            }
        });
    }

    static LogEntry decodeEntry(byte[] bytes) throws IOException {
        DataInputStream in = unframe(LOG_ENTRY, bytes);
        long index = in.readLong();
        long term = in.readLong();
        byte type = in.readByte();
        Command command = switch (type) {
            case 3 -> new Command.Request(in.readUTF(),in.readLong(),decodeEntry(readBytes(in).toByteArray()).command());
            case CMD_NO_OP -> Command.noOp();
            case CMD_PUT -> new Command.Put(readBytes(in), readBytes(in));
            case CMD_DELETE -> new Command.Delete(readBytes(in));
            default -> throw new IOException("unknown command type " + type);
        };
        expectEnd(in);
        return new LogEntry(index, term, command);
    }

    static byte[] encodeSnapshot(io.github.richardeyeboah.quantafs.raft.Snapshot snapshot) {
        return frame((byte)4,out -> {out.writeLong(snapshot.position().term());out.writeLong(snapshot.position().index());writeBytes(out,snapshot.image());});
    }
    static io.github.richardeyeboah.quantafs.raft.Snapshot decodeSnapshot(byte[] bytes) throws IOException {
        var in=unframe((byte)4,bytes);
        var result=new io.github.richardeyeboah.quantafs.raft.Snapshot(new LogPosition(in.readLong(),in.readLong()),readBytes(in));
        expectEnd(in);io.github.richardeyeboah.quantafs.raft.StateImage.decode(result);return result;
    }
    static byte[] encodeApplied(LogPosition position) {
        return frame(APPLIED_MARKER, out -> {
            out.writeLong(position.term());
            out.writeLong(position.index());
        });
    }

    static LogPosition decodeApplied(byte[] bytes) throws IOException {
        DataInputStream in = unframe(APPLIED_MARKER, bytes);
        LogPosition position = new LogPosition(in.readLong(), in.readLong());
        expectEnd(in);
        return position;
    }

    private interface Body {
        void write(DataOutputStream out) throws IOException;
    }

    private static byte[] frame(byte type, Body body) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeByte(type);
            out.writeByte(FORMAT_VERSION);
            body.write(out);
            out.flush();
            CRC32 crc = new CRC32();
            crc.update(bytes.toByteArray());
            out.writeLong(crc.getValue());
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("in-memory encoding cannot fail", e);
        }
    }

    private static DataInputStream unframe(byte expectedType, byte[] bytes) throws IOException {
        if (bytes.length < 2 + Long.BYTES) {
            throw new IOException("record too short (" + bytes.length + " bytes)");
        }
        int payloadEnd = bytes.length - Long.BYTES;
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, payloadEnd);
        if (crc.getValue() != ByteBuffer.wrap(bytes, payloadEnd, Long.BYTES).getLong()) {
            throw new IOException("checksum mismatch");
        }
        if (bytes[0] != expectedType) {
            throw new IOException("expected record type " + expectedType + ", found " + bytes[0]);
        }
        if (bytes[1] != FORMAT_VERSION) {
            throw new IOException("unsupported format version " + bytes[1]);
        }
        return new DataInputStream(new ByteArrayInputStream(bytes, 2, payloadEnd - 2));
    }

    private static void writeBytes(DataOutputStream out, Bytes value) throws IOException {
        byte[] data = value.toByteArray();
        out.writeInt(data.length);
        out.write(data);
    }

    private static Bytes readBytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > in.available()) {
            throw new IOException("invalid length " + length);
        }
        return Bytes.copyOf(in.readNBytes(length));
    }

    private static void expectEnd(DataInputStream in) throws IOException {
        if (in.available() != 0) {
            throw new IOException("unexpected trailing bytes");
        }
    }
}
