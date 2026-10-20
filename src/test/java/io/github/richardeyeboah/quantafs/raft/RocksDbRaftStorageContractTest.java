package io.github.richardeyeboah.quantafs.raft;

import io.github.richardeyeboah.quantafs.raft.storage.RocksDbRaftStorage;
import java.nio.file.Path;

class RocksDbRaftStorageContractTest extends RaftStorageContractTest {

    @Override
    RaftStorage create(Path dir) {
        return RocksDbRaftStorage.open(dir);
    }

    @Override
    RaftStorage reopen(RaftStorage storage, Path dir) {
        ((RocksDbRaftStorage) storage).close(); // release RocksDB's lock, then open as a new process would
        return RocksDbRaftStorage.open(dir);
    }

    @Override
    void close(RaftStorage storage) {
        ((RocksDbRaftStorage) storage).close();
    }
}
