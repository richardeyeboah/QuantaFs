package io.github.richardeyeboah.quantafs.raft;

import java.nio.file.Path;

class FileElectionStateStoreContractTest extends ElectionStateStoreContractTest {

    @Override
    ElectionStateStore create(Path dir) {
        return FileElectionStateStore.open(dir);
    }

    @Override
    ElectionStateStore reopen(ElectionStateStore store, Path dir) {
        return FileElectionStateStore.open(dir);
    }
}
