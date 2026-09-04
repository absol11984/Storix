package com.storix.metadata;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for MetadataStore.
 */
class MetadataStoreTest {

    @TempDir
    Path tempDir;

    private Path metadataFile;
    private MetadataStore store;

    @BeforeEach
    void setUp() throws IOException {
        metadataFile = tempDir.resolve("metadata.json");
        store = new MetadataStore(metadataFile);
    }

    @Test
    void createObject() throws IOException {
        ObjectMetadata metadata = new ObjectMetadata("test.txt", 1024, 512);
        metadata.addChunk(new ChunkInfo("chunk-0", 0, 512, "node-1", "localhost", 8080));

        store.createObject(metadata);

        assertTrue(store.objectExists("test.txt"));
        ObjectMetadata retrieved = store.getObject("test.txt").orElseThrow();
        assertEquals("test.txt", retrieved.getObjectName());
        assertEquals(1024, retrieved.getFileSize());
        assertEquals(1, retrieved.getChunkCount());
    }

    @Test
    void createDuplicateObject() {
        ObjectMetadata metadata = new ObjectMetadata("duplicate.txt", 100, 50);
        store.createObject(metadata);

        assertThrows(IllegalStateException.class, () -> store.createObject(metadata));
    }

    @Test
    void getObjectNotFound() {
        assertFalse(store.getObject("nonexistent").isPresent());
    }

    @Test
    void updateObject() throws IOException {
        ObjectMetadata metadata = new ObjectMetadata("update.txt", 100, 50);
        store.createObject(metadata);

        metadata.setFileSize(200);
        store.updateObject(metadata);

        ObjectMetadata retrieved = store.getObject("update.txt").orElseThrow();
        assertEquals(200, retrieved.getFileSize());
    }

    @Test
    void updateNonExistentObject() {
        ObjectMetadata metadata = new ObjectMetadata("nonexistent.txt", 100, 50);
        assertThrows(IllegalArgumentException.class, () -> store.updateObject(metadata));
    }

    @Test
    void deleteObject() throws IOException {
        ObjectMetadata metadata = new ObjectMetadata("delete.txt", 100, 50);
        store.createObject(metadata);

        assertTrue(store.deleteObject("delete.txt"));
        assertFalse(store.objectExists("delete.txt"));
    }

    @Test
    void deleteNonExistentObject() {
        assertFalse(store.deleteObject("nonexistent.txt"));
    }

    @Test
    void listObjects() throws IOException {
        store.createObject(new ObjectMetadata("file1.txt", 100, 50));
        store.createObject(new ObjectMetadata("file2.txt", 200, 50));
        store.createObject(new ObjectMetadata("file3.txt", 300, 50));

        assertEquals(3, store.listObjects().size());
        assertTrue(store.listObjects().contains("file1.txt"));
        assertTrue(store.listObjects().contains("file2.txt"));
        assertTrue(store.listObjects().contains("file3.txt"));
    }

    @Test
    void persistenceAcrossRestart() throws IOException {
        // Create and store metadata
        ObjectMetadata metadata = new ObjectMetadata("persistent.txt", 1024, 512);
        metadata.addChunk(new ChunkInfo("chunk-0", 0, 512, "node-1", "localhost", 8080));
        metadata.addChunk(new ChunkInfo("chunk-1", 1, 512, "node-1", "localhost", 8080));
        store.createObject(metadata);

        // Verify file was written
        assertTrue(Files.exists(metadataFile));

        // Create new store instance (simulating restart)
        MetadataStore newStore = new MetadataStore(metadataFile);

        // Verify data persisted
        assertTrue(newStore.objectExists("persistent.txt"));
        ObjectMetadata retrieved = newStore.getObject("persistent.txt").orElseThrow();
        assertEquals("persistent.txt", retrieved.getObjectName());
        assertEquals(1024, retrieved.getFileSize());
        assertEquals(2, retrieved.getChunkCount());
    }

    @Test
    void multipleChunks() throws IOException {
        ObjectMetadata metadata = new ObjectMetadata("multi.bin", 5000, 1000);
        for (int i = 0; i < 5; i++) {
            metadata.addChunk(new ChunkInfo("chunk-" + i, i, 1000, "node-1", "localhost", 8080));
        }

        store.createObject(metadata);

        ObjectMetadata retrieved = store.getObject("multi.bin").orElseThrow();
        assertEquals(5, retrieved.getChunkCount());

        // Verify chunk order is preserved
        for (int i = 0; i < 5; i++) {
            assertEquals("chunk-" + i, retrieved.getChunks().get(i).getChunkId());
            assertEquals(i, retrieved.getChunks().get(i).getChunkIndex());
        }
    }
}
