package com.storix.metadata.wal;

import com.storix.metadata.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CRITICAL: Corrupted snapshot test.
 *
 * Scenario:
 * - CURRENT = 10
 * - gen-10 manifest says snapshotChecksum = X
 * - snapshot.bin is corrupted (different checksum)
 *
 * Expected:
 * - validateGeneration() throws IOException
 * - loadAuthoritativeState() propagates the error
 * - System should NOT use corrupted data
 */
class GenerationCorruptedSnapshotTest {

    @TempDir
    Path tempDir;

    @Test
    void testCorruptedSnapshotIsDetected() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Corrupted Snapshot Detection");
        System.out.println("========================================\n");

        Path storageDir = tempDir.resolve("storage");
        Files.createDirectories(storageDir);

        // Create generation 10 as authoritative
        GenerationManager gm = new GenerationManager(storageDir);

        Map<String, ObjectMetadata> gen10Data = new HashMap<>();
        ObjectMetadata obj = new ObjectMetadata("A", 1000L, 4096);
        obj.addChunk(new ChunkInfo("chunk-A", 0, 4096, List.of("node1"), "hash-A"));
        gen10Data.put("A", obj);

        gm.createCandidateGeneration(10);
        byte[] gen10Snapshot = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(gen10Data);
        int correctChecksum = computeChecksum(gen10Snapshot);

        // Write correct snapshot
        gm.writeMetadata(10, gen10Data);
        gm.writeSnapshot(10, 10, 1, gen10Snapshot, correctChecksum);

        // Write manifest with CORRECT checksum
        gm.writeManifest(10, 10, 1, correctChecksum);

        // Switch to make it authoritative
        gm.switchCurrent(10);

        System.out.println("Created gen-10 with correct checksum: " + correctChecksum);

        // CORRUPT the snapshot file
        Path snapshotFile = storageDir.resolve("generations/gen-10/snapshot.bin");
        byte[] corrupted = "CORRUPTED_DATA".getBytes();
        Files.write(snapshotFile, corrupted);

        System.out.println("Corrupted snapshot file");

        // Verify that validation detects corruption
        GenerationManager recoveredGm = new GenerationManager(storageDir);
        assertEquals(10, recoveredGm.getCurrentGeneration());

        // Validation should fail
        IOException exception = assertThrows(IOException.class, () -> {
            recoveredGm.loadAuthoritativeState();
        }, "Should detect corrupted snapshot");

        // Corruption detection can manifest as:
        // 1. Size check failure (file too small)
        // 2. Magic validation failure (invalid header)
        // 3. Checksum mismatch (data corrupted)
        assertTrue(
            exception.getMessage().contains("checksum") ||
            exception.getMessage().contains("mismatch") ||
            exception.getMessage().contains("too small") ||
            exception.getMessage().contains("magic") ||
            exception.getMessage().contains("header"),
            "Error should detect corruption: " + exception.getMessage()
        );

        System.out.println("\n========================================");
        System.out.println("TEST: Corrupted Snapshot Detection - PASSED");
        System.out.println("  Corruption detected ✓");
        System.out.println("========================================\n");
    }

    private int computeChecksum(byte[] data) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }
}
