package io.github.richardeyeboah.quantafs.storage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RocksDbKeyValueStoreTest {

    @TempDir Path dataDir;

    @Test
    void putThenGetReturnsValue() {
        try (RocksDbKeyValueStore store = RocksDbKeyValueStore.open(dataDir)) {
            store.put(bytes("k"), bytes("v"));
            assertArrayEquals(bytes("v"), store.get(bytes("k")).orElseThrow());
        }
    }

    @Test
    void putOverwritesExistingValue() {
        try (RocksDbKeyValueStore store = RocksDbKeyValueStore.open(dataDir)) {
            store.put(bytes("k"), bytes("first"));
            store.put(bytes("k"), bytes("second"));
            assertArrayEquals(bytes("second"), store.get(bytes("k")).orElseThrow());
        }
    }

    @Test
    void missingKeyIsEmpty() {
        try (RocksDbKeyValueStore store = RocksDbKeyValueStore.open(dataDir)) {
            assertEquals(Optional.empty(), store.get(bytes("absent")));
        }
    }

    @Test
    void emptyValueIsStoredAndDistinctFromMissing() {
        try (RocksDbKeyValueStore store = RocksDbKeyValueStore.open(dataDir)) {
            store.put(bytes("k"), new byte[0]);
            assertArrayEquals(new byte[0], store.get(bytes("k")).orElseThrow());
        }
    }

    @Test
    void deleteRemovesKeyAndIsIdempotent() {
        try (RocksDbKeyValueStore store = RocksDbKeyValueStore.open(dataDir)) {
            store.put(bytes("k"), bytes("v"));
            store.delete(bytes("k"));
            assertTrue(store.get(bytes("k")).isEmpty());
            store.delete(bytes("k"));
            store.delete(bytes("never-existed"));
            assertTrue(store.get(bytes("k")).isEmpty());
        }
    }

    @Test
    void arbitraryBinaryKeysAndValuesRoundTrip() {
        byte[] key = {0, (byte) 0xFF, 0, 1};
        byte[] value = new byte[256];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) i;
        }
        try (RocksDbKeyValueStore store = RocksDbKeyValueStore.open(dataDir)) {
            store.put(key, value);
            assertArrayEquals(value, store.get(key).orElseThrow());
        }
    }

    @Test
    void dataSurvivesCloseAndReopen() {
        try (RocksDbKeyValueStore store = RocksDbKeyValueStore.open(dataDir)) {
            store.put(bytes("kept"), bytes("v1"));
            store.put(bytes("overwritten"), bytes("old"));
            store.put(bytes("overwritten"), bytes("new"));
            store.put(bytes("deleted"), bytes("gone"));
            store.delete(bytes("deleted"));
        }
        try (RocksDbKeyValueStore store = RocksDbKeyValueStore.open(dataDir)) {
            assertArrayEquals(bytes("v1"), store.get(bytes("kept")).orElseThrow());
            assertArrayEquals(bytes("new"), store.get(bytes("overwritten")).orElseThrow());
            assertTrue(store.get(bytes("deleted")).isEmpty());
        }
    }

    @Test
    void operationsAfterCloseFailWithStoreClosed() {
        RocksDbKeyValueStore store = RocksDbKeyValueStore.open(dataDir);
        store.close();
        store.close();
        assertThrows(StoreClosedException.class, () -> store.get(bytes("k")));
        assertThrows(StoreClosedException.class, () -> store.put(bytes("k"), bytes("v")));
        assertThrows(StoreClosedException.class, () -> store.delete(bytes("k")));
    }

    @Test
    void secondOpenOfSameDirectoryFails() {
        try (RocksDbKeyValueStore store = RocksDbKeyValueStore.open(dataDir)) {
            store.put(bytes("k"), bytes("v"));
            StorageException e =
                    assertThrows(StorageException.class, () -> RocksDbKeyValueStore.open(dataDir));
            assertTrue(e.getMessage().contains("lock"), e.getMessage());
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }
}
