package io.github.richardeyeboah.quantafs.raft;

import java.nio.file.Path;

class InMemoryElectionStateStoreContractTest extends ElectionStateStoreContractTest {

    @Override
    ElectionStateStore create(Path dir) {
        return new InMemoryElectionStateStore();
    }

    @Override
    ElectionStateStore reopen(ElectionStateStore store, Path dir) {
        return store; // the instance itself stands in for the surviving disk
    }
}
