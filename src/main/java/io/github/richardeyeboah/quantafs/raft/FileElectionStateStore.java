package io.github.richardeyeboah.quantafs.raft;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.CRC32;

/**
 * Durable {@link ElectionStateStore} that keeps {@link TermAndVote} in one small file, separate
 * from any application data.
 *
 * <p><b>Atomic, durable write:</b> encode the state with a checksum, write it to
 * {@value #TEMP_FILE_NAME}, fsync that file, atomically rename it over {@value #FILE_NAME}, then
 * fsync the directory so the rename itself is durable. A crash at any point leaves either the old
 * file or the new one. Durability assumes the OS and device honor fsync; on macOS, fsync does not
 * force the drive's own cache to flush.
 *
 * <p><b>Fail closed:</b> a corrupt or unreadable state file is an error, never silently treated as
 * term 0, because forgetting a vote could let this node vote twice in one term. After any failed
 * save the instance refuses further use; reopening re-reads what actually reached disk.
 *
 * <p>Only one process may use a directory at a time; this class does not lock it.
 */
public final class FileElectionStateStore implements ElectionStateStore {

    static final String FILE_NAME = "election-state";
    static final String TEMP_FILE_NAME = "election-state.tmp";
    private static final int MAGIC = 0x51465345; // "QFSE"
    private static final int FORMAT_VERSION = 1;

    private final Path directory;
    private final Path file;
    private final Path tempFile;
    private TermAndVote current; // guarded by this
    private boolean failed; // guarded by this

    private FileElectionStateStore(Path directory, TermAndVote current) {
        this.directory = directory;
        this.file = directory.resolve(FILE_NAME);
        this.tempFile = directory.resolve(TEMP_FILE_NAME);
        this.current = current;
    }

    /**
     * Opens the store, creating {@code directory} if needed. A missing state file means this node
     * has never persisted election state and starts at term 0.
     *
     * @throws PersistenceException if the directory cannot be created or the file is corrupt
     */
    public static FileElectionStateStore open(Path directory) {
        Objects.requireNonNull(directory, "directory");
        Path dir = directory.toAbsolutePath().normalize();
        try {
            Files.createDirectories(dir);
            // A leftover temp file is an incomplete write that was never renamed into place.
            Files.deleteIfExists(dir.resolve(TEMP_FILE_NAME));
            return new FileElectionStateStore(dir, read(dir.resolve(FILE_NAME)));
        } catch (IOException e) {
            throw new PersistenceException("cannot open election state in " + dir, e);
        }
    }

    public Path directory() {
        return directory;
    }

    @Override
    public synchronized TermAndVote load() {
        ensureUsable();
        return current;
    }

    @Override
    public synchronized void save(TermAndVote next) {
        Objects.requireNonNull(next, "next");
        ensureUsable();
        try {
            TermAndVote.checkTransition(current, next);
        } catch (IllegalArgumentException e) {
            throw new PersistenceException("rejected unsafe transition: " + e.getMessage(), e);
        }
        try {
            writeDurably(encode(next));
            current = next;
        } catch (IOException | RuntimeException e) {
            failed = true;
            throw new PersistenceException("failed to persist " + next + " in " + directory, e);
        }
    }

    private void ensureUsable() {
        if (failed) {
            throw new PersistenceException(
                    "an earlier save failed; on-disk state is unknown, reopen the store");
        }
    }

    private void writeDurably(byte[] bytes) throws IOException {
        try (FileChannel channel =
                FileChannel.open(
                        tempFile,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }
        Files.move(tempFile, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        try (FileChannel dir = FileChannel.open(directory, StandardOpenOption.READ)) {
            dir.force(true);
        }
    }

    static byte[] encode(TermAndVote state) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(MAGIC);
            out.writeInt(FORMAT_VERSION);
            out.writeLong(state.currentTerm());
            out.writeBoolean(state.votedFor().isPresent());
            if (state.votedFor().isPresent()) {
                out.writeUTF(state.votedFor().get().value());
            }
            CRC32 crc = new CRC32();
            crc.update(bytes.toByteArray());
            out.writeLong(crc.getValue());
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("in-memory encoding cannot fail", e);
        }
    }

    private static TermAndVote read(Path file) throws IOException {
        byte[] bytes;
        try {
            bytes = Files.readAllBytes(file);
        } catch (NoSuchFileException e) {
            return TermAndVote.INITIAL;
        }
        try {
            return decode(bytes);
        } catch (IOException | IllegalArgumentException e) {
            throw new PersistenceException(
                    "election state file " + file + " is corrupt; refusing to start: " + e.getMessage(), e);
        }
    }

    static TermAndVote decode(byte[] bytes) throws IOException {
        if (bytes.length < Long.BYTES) {
            throw new IOException("file too short (" + bytes.length + " bytes)");
        }
        int payloadLength = bytes.length - Long.BYTES;
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, payloadLength);
        long storedCrc = ByteBuffer.wrap(bytes, payloadLength, Long.BYTES).getLong();
        if (crc.getValue() != storedCrc) {
            throw new IOException("checksum mismatch");
        }
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes, 0, payloadLength));
        if (in.readInt() != MAGIC) {
            throw new IOException("not an election state file");
        }
        int version = in.readInt();
        if (version != FORMAT_VERSION) {
            throw new IOException("unsupported format version " + version);
        }
        long term = in.readLong();
        Optional<NodeId> votedFor =
                in.readBoolean() ? Optional.of(NodeId.of(in.readUTF())) : Optional.empty();
        if (in.available() != 0) {
            throw new IOException("unexpected trailing bytes");
        }
        return new TermAndVote(term, votedFor);
    }
}
