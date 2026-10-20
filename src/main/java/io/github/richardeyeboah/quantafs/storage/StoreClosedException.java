package io.github.richardeyeboah.quantafs.storage;

/** Thrown when an operation is attempted on a store that has been closed. */
public final class StoreClosedException extends StorageException {

    private static final long serialVersionUID = 1L;

    public StoreClosedException() {
        super("store is closed");
    }
}
