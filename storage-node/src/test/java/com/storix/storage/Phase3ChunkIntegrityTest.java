package com.storix.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 3 tests for chunk storage integrity verification.
 * Validates checksum verification, quarantine on corruption, and explicit verification.
 */
class Phase3ChunkIntegrityTest {

    @TempDir
    Path tempDir;

    private ChunkStorage storage;

    @BeforeEach
    void setUp() throws IOException {
        storage = new ChunkStorage(tempDir);
    }

    /**
     * Test: Checksum verification detects corrupted chunks.
     */
    @Test
    void testChecksumMismatchDetectsCorruption() throws IOException {
        byte[] data = "test data".getBytes();
        storage.putChunk("chunk-1", data);

        // Valid checksum should pass
        String correctChecksum = computeSHA256(data);
        assertTrue(storage.verifyIntegrity("chunk-1", correctChecksum),
                "Valid checksum should pass verification");

        // Wrong checksum should fail
        String wrongChecksum = "0000000000000000000000000000000000000000000000000000000000000000";
        assertFalse(storage.verifyIntegrity("chunk-1", wrongChecksum),
                "Wrong checksum should fail verification");
    }

    /**
     * Test: Corrupted chunk gets quarantined.
     */
    @Test
    void testCorruptedChunkQuarantined() throws IOException {
        byte[] data = "original data".getBytes();
        storage.putChunk("chunk-2", data);

        // Verify it exists
        assertTrue(storage.chunkExists("chunk-2"));

        // Quarantine it
        storage.quarantineCorruptChunk("chunk-2");

        // Original should be gone
        assertFalse(storage.chunkExists("chunk-2"),
                "Quarantined chunk should be removed from main storage");

        // Check quarantine directory has the file
        Path quarantineDir = storage.getQuarantineDir();
        long quarantinedCount = java.nio.file.Files.list(quarantineDir)
                .filter(p -> p.getFileName().toString().startsWith("chunk-2_"))
                .count();
        assertEquals(1, quarantinedCount,
                "Quarantined chunk should exist in quarantine directory");
    }

    /**
     * Test: Empty or null checksum bypasses verification.
     */
    @Test
    void testEmptyChecksumBypassesVerification() throws IOException {
        byte[] data = "data without checksum".getBytes();
        storage.putChunk("chunk-3", data);

        // Null checksum should pass
        assertTrue(storage.verifyIntegrity("chunk-3", null),
                "Null checksum should bypass verification");

        // Empty checksum should pass
        assertTrue(storage.verifyIntegrity("chunk-3", ""),
                "Empty checksum should bypass verification");
    }

    /**
     * Test: Verify non-existent chunk throws IOException.
     */
    @Test
    void testVerifyNonExistentChunk() {
        assertThrows(IOException.class,
                () -> storage.verifyIntegrity("nonexistent", "abc123"),
                "Verifying non-existent chunk should throw IOException");
    }

    /**
     * Test: Quarantine non-existent chunk is a no-op.
     */
    @Test
    void testQuarantineNonExistentChunk() throws IOException {
        // Should not throw
        assertDoesNotThrow(() -> storage.quarantineCorruptChunk("nonexistent"),
                "Quarantining non-existent chunk should be a no-op");
    }

    /**
     * Helper to compute SHA-256 checksum (same as ChunkStorage).
     */
    private String computeSHA256(byte[] data) {
        try {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data);
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
