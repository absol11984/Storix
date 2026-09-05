package com.storix.metadata.wal;

import com.storix.metadata.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CRITICAL: Missing metadata test.
 *
 * Scenario:
 * - CURRENT = 10
 * - gen-10/ directory exists but metadata.json is missing
 *
 * Expected:
 * - validateGeneration() throws IOException
 * - loadAuthoritativeState() propagates the error
 */
class GenerationMissingMetadataTest {

    @TempDir
    Path tempDir;

    @Test
    void testMissingMetadataIsDetected() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Missing Metadata Detection");
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

        System.out.println("Created gen-10 with metadata");

        // DELETE the metadata file
        Path metadataFile = storageDir.resolve("generations/gen-10/metadata.json");
        Files.delete(metadataFile);

        System.out.println("Deleted metadata.json");

        // Verify that validation detects missing metadata
        GenerationManager recoveredGm = new GenerationManager(storageDir);
        assertEquals(10, recoveredGm.getCurrentGeneration());

        // Validation should fail
        IOException exception = assertThrows(IOException.class, () -> {
            recoveredGm.loadAuthoritativeState();
        }, "Should detect missing metadata");

        assertTrue(exception.getMessage().contains("Metadata file not found") ||
                   exception.getMessage().contains("metadata"),
                   "Error should mention missing metadata");

        System.out.println("\n========================================");
        System.out.println("TEST: Missing Metadata Detection - PASSED");
        System.out.println("  Missing metadata detected ✓");
        System.out.println("========================================\n");
    }

    private int computeChecksum(byte[] data) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }
}
