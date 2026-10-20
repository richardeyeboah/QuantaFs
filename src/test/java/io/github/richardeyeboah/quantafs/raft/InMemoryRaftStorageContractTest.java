package io.github.richardeyeboah.quantafs.raft;

import java.nio.file.Path;

class InMemoryRaftStorageContractTest extends RaftStorageContractTest {

    @Override
    RaftStorage create(Path dir) {
        return new InMemoryRaftStorage();
    }

    @Override
    RaftStorage reopen(RaftStorage storage, Path dir) {
        return storage; // the object itself stands in for the surviving disk
    }

    @Override
    void close(RaftStorage storage) {}
}
