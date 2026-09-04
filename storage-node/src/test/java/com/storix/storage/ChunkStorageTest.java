package com.storix.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ChunkStorage.
 */
class ChunkStorageTest {

    @TempDir
    Path tempDir;

    private ChunkStorage storage;

    @BeforeEach
    void setUp() throws IOException {
        storage = new ChunkStorage(tempDir);
    }

    @Test
    void putAndGetChunk() throws IOException {
        byte[] data = "Hello, World!".getBytes();
        storage.putChunk("test-chunk", data);

        byte[] retrieved = storage.getChunk("test-chunk");
        assertArrayEquals(data, retrieved);
    }

    @Test
    void getNonExistentChunk() {
        assertThrows(IOException.class, () -> storage.getChunk("nonexistent"));
    }

    @Test
    void deleteChunk() throws IOException {
        byte[] data = "test data".getBytes();
        storage.putChunk("to-delete", data);

        assertTrue(storage.chunkExists("to-delete"));

        boolean deleted = storage.deleteChunk("to-delete");
        assertTrue(deleted);
        assertFalse(storage.chunkExists("to-delete"));
    }

    @Test
    void deleteNonExistentChunk() throws IOException {
        boolean deleted = storage.deleteChunk("nonexistent");
        assertFalse(deleted);
    }

    @Test
    void chunkExists() throws IOException {
        assertFalse(storage.chunkExists("missing"));

        storage.putChunk("existing", new byte[10]);
        assertTrue(storage.chunkExists("existing"));
    }

    @Test
    void putOverwritesExisting() throws IOException {
        storage.putChunk("overwrite", "original".getBytes());
        storage.putChunk("overwrite", "replacement".getBytes());

        byte[] data = storage.getChunk("overwrite");
        assertEquals("replacement", new String(data));
    }

    @Test
    void sanitizesChunkId() throws IOException {
        // Chunk IDs with special characters should be sanitized
        storage.putChunk("test/../../../etc/passwd", "malicious".getBytes());

        // Should not escape the storage directory
        assertTrue(storage.chunkExists("test/../../../etc/passwd"));
    }

    @Test
    void emptyChunk() throws IOException {
        storage.putChunk("empty", new byte[0]);
        byte[] data = storage.getChunk("empty");
        assertEquals(0, data.length);
    }

    @Test
    void largeChunk() throws IOException {
        byte[] largeData = new byte[1024 * 1024]; // 1 MB
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }

        storage.putChunk("large", largeData);
        byte[] retrieved = storage.getChunk("large");

        assertArrayEquals(largeData, retrieved);
    }
}
