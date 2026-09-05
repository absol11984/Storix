package com.storix.metadata.wal;

import com.storix.metadata.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CRITICAL: Missing manifest test.
 *
 * Scenario:
 * - CURRENT = 10
 * - gen-10/ directory exists but manifest.json is missing
 *
 * Expected:
 * - validateGeneration() throws IOException
 * - loadAuthoritativeState() propagates the error
 */
class GenerationMissingManifestTest {

    @TempDir
    Path tempDir;

    @Test
    void testMissingManifestIsDetected() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Missing Manifest Detection");
        System.out.println("========================================\n");

        Path storageDir = tempDir.resolve("storage");
        Files.createDirectories(storageDir);

        // Create generation 10
        GenerationManager gm = new GenerationManager(storageDir);

        Map<String, ObjectMetadata> gen10Data = new HashMap<>();
        ObjectMetadata obj = new ObjectMetadata("A", 1000L, 4096);
        obj.addChunk(new ChunkInfo("chunk-A", 0, 4096, List.of("node1"), "hash-A"));
        gen10Data.put("A", obj);

        gm.createCandidateGeneration(10);
        byte[] gen10Snapshot = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(gen10Data);
        int gen10Checksum = computeChecksum(gen10Snapshot);

        gm.writeMetadata(10, gen10Data);
        gm.writeSnapshot(10, 10, 1, gen10Snapshot, gen10Checksum);
        gm.writeManifest(10, 10, 1, gen10Checksum);
        gm.switchCurrent(10);

        System.out.println("Created gen-10 with manifest");

        // DELETE the manifest file
        Path manifestFile = storageDir.resolve("generations/gen-10/manifest.json");
        Files.delete(manifestFile);

        System.out.println("Deleted manifest.json");

        // Verify that validation detects missing manifest
        GenerationManager recoveredGm = new GenerationManager(storageDir);
        assertEquals(10, recoveredGm.getCurrentGeneration());

        // Validation should fail
        IOException exception = assertThrows(IOException.class, () -> {
            recoveredGm.loadAuthoritativeState();
        }, "Should detect missing manifest");

        assertTrue(exception.getMessage().contains("Manifest file not found") ||
                   exception.getMessage().contains("manifest"),
                   "Error should mention missing manifest");

        System.out.println("\n========================================");
        System.out.println("TEST: Missing Manifest Detection - PASSED");
        System.out.println("  Missing manifest detected ✓");
        System.out.println("========================================\n");
    }

    private int computeChecksum(byte[] data) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }
}
