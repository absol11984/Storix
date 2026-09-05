package com.storix.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for chunk integrity verification.
 */
class CorruptionDetectionTest {

    @TempDir
    Path tempDir;

    @Test
    void testVerifyIntegritySuccess() throws IOException {
        ChunkStorage storage = new ChunkStorage(tempDir);

        // Write a chunk
        String chunkId = "test-chunk";
        byte[] data = "test data content".getBytes();
        storage.putChunk(chunkId, data);

        // Verify with correct checksum
        String checksum = computeSha256(data);
        assertTrue(storage.verifyIntegrity(chunkId, checksum));
    }

    @Test
    void testVerifyIntegrityFailure() throws IOException {
        ChunkStorage storage = new ChunkStorage(tempDir);

        // Write a chunk
        String chunkId = "test-chunk";
        byte[] data = "test data content".getBytes();
        storage.putChunk(chunkId, data);

        // Verify with wrong checksum
        assertFalse(storage.verifyIntegrity(chunkId, "wrongchecksum"));

        // Verify with correct checksum for different data
        byte[] differentData = "different data".getBytes();
        String differentChecksum = computeSha256(differentData);
        assertFalse(storage.verifyIntegrity(chunkId, differentChecksum));
    }

    @Test
    void testQuarantineCorruptChunk() throws IOException {
        ChunkStorage storage = new ChunkStorage(tempDir);

        // Write a chunk
        String chunkId = "test-chunk";
        byte[] data = "test data content".getBytes();
        storage.putChunk(chunkId, data);

        // Quarantine the chunk
        storage.quarantineCorruptChunk(chunkId);

        // Original file should be gone
        assertFalse(storage.chunkExists(chunkId));

        // Quarantine directory should contain the chunk
        Path quarantineDir = storage.getQuarantineDir();
        assertTrue(Files.exists(quarantineDir));
        assertTrue(Files.list(quarantineDir).findAny().isPresent());
    }

    @Test
    void testVerifyIntegrityNoChecksum() throws IOException {
        ChunkStorage storage = new ChunkStorage(tempDir);

        // Write a chunk
        String chunkId = "test-chunk";
        byte[] data = "test data content".getBytes();
        storage.putChunk(chunkId, data);

        // Empty checksum should pass (no verification)
        assertTrue(storage.verifyIntegrity(chunkId, ""));
        assertTrue(storage.verifyIntegrity(chunkId, null));
    }

    @Test
    void testQuarantineNonExistentChunk() throws IOException {
        ChunkStorage storage = new ChunkStorage(tempDir);

        // Should not throw
        storage.quarantineCorruptChunk("non-existent-chunk");
    }

    private String computeSha256(byte[] data) {
        try {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data);
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }
}
