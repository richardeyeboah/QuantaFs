package io.github.richardeyeboah.quantafs.storage;

/** A failure inside the storage engine (I/O error, corruption, failed open, and so on). */
public class StorageException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public StorageException(String message) {
        super(message);
    }

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
