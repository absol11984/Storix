package com.storix.metadata.wal;

import com.storix.metadata.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for GenerationManager core functionality.
 */
class GenerationManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void testListGenerations() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: List Generations");
        System.out.println("========================================\n");

        Path storageDir = tempDir.resolve("storage");
        Files.createDirectories(storageDir);

        GenerationManager gm = new GenerationManager(storageDir);

        // Initially empty
        assertTrue(gm.listGenerations().isEmpty());

        // Create gen-1 (don't make it current)
        gm.createCandidateGeneration(1);
        assertTrue(gm.listGenerations().contains(1L));

        // Create gen-2
        gm.createCandidateGeneration(2);
        assertTrue(gm.listGenerations().contains(2L));

        // Create gen-3 and make it current
        gm.createCandidateGeneration(3);
        Map<String, ObjectMetadata> data = new HashMap<>();
        byte[] snapshot = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(data);
        int checksum = computeChecksum(snapshot);
        gm.writeMetadata(3, data);
        gm.writeSnapshot(3, 3, 1, snapshot, checksum);
        gm.writeManifest(3, 3, 1, checksum);
        gm.switchCurrent(3);

        List<Long> generations = gm.listGenerations();
        assertEquals(3, generations.size());
        assertTrue(generations.contains(1L));
        assertTrue(generations.contains(2L));
        assertTrue(generations.contains(3L));

        System.out.println("\n========================================");
        System.out.println("TEST: List Generations - PASSED");
        System.out.println("  Found generations: " + generations);
        System.out.println("========================================\n");
    }

    @Test
    void testCleanupOldCandidates() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Cleanup Old Candidates");
        System.out.println("========================================\n");

        Path storageDir = tempDir.resolve("storage");
        Files.createDirectories(storageDir);

        // Create gen-10 as current
        {
            GenerationManager gm = new GenerationManager(storageDir);
            Map<String, ObjectMetadata> data = new HashMap<>();
            byte[] snapshot = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(data);
            int checksum = computeChecksum(snapshot);
            gm.createCandidateGeneration(10);
            gm.writeMetadata(10, data);
            gm.writeSnapshot(10, 10, 1, snapshot, checksum);
            gm.writeManifest(10, 10, 1, checksum);
            gm.switchCurrent(10);
        }

        // Create uncommitted candidates gen-11 and gen-12
        {
            GenerationManager gm = new GenerationManager(storageDir);
            for (long gen : List.of(11L, 12L)) {
                Map<String, ObjectMetadata> data = new HashMap<>();
                byte[] snapshot = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(data);
                int checksum = computeChecksum(snapshot);
                gm.createCandidateGeneration(gen);
                gm.writeMetadata(gen, data);
                gm.writeSnapshot(gen, gen, 1, snapshot, checksum);
                gm.writeManifest(gen, gen, 1, checksum);
            }
        }

        // Before cleanup, all exist
        GenerationManager checkGm = new GenerationManager(storageDir);
        assertEquals(3, checkGm.listGenerations().size());

        // Run cleanup
        checkGm.cleanupOldCandidates();

        // After cleanup, only current should remain
        List<Long> remaining = checkGm.listGenerations();
        assertEquals(1, remaining.size());
        assertTrue(remaining.contains(10L));

        System.out.println("\n========================================");
        System.out.println("TEST: Cleanup Old Candidates - PASSED");
        System.out.println("  Remaining generations: " + remaining);
        System.out.println("========================================\n");
    }

    @Test
    void testGenerationExists() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Generation Exists");
        System.out.println("========================================\n");

        Path storageDir = tempDir.resolve("storage");
        Files.createDirectories(storageDir);

        GenerationManager gm = new GenerationManager(storageDir);

        assertFalse(gm.generationExists(1));
        assertFalse(gm.generationExists(10));

        gm.createCandidateGeneration(5);
        assertTrue(gm.generationExists(5));
        assertFalse(gm.generationExists(6));

        System.out.println("\n========================================");
        System.out.println("TEST: Generation Exists - PASSED");
        System.out.println("========================================\n");
    }

    @Test
    void testSwitchCurrentRequiresExistingGeneration() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: SwitchCurrent Requires Existing Generation");
        System.out.println("========================================\n");

        Path storageDir = tempDir.resolve("storage");
        Files.createDirectories(storageDir);

        GenerationManager gm = new GenerationManager(storageDir);

        assertThrows(IOException.class, () -> {
            gm.switchCurrent(999);
        }, "Should not switch to non-existent generation");

        System.out.println("\n========================================");
        System.out.println("TEST: SwitchCurrent Requires Existing Generation - PASSED");
        System.out.println("========================================\n");
    }

    @Test
    void testIsAuthoritative() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Is Authoritative");
        System.out.println("========================================\n");

        Path storageDir = tempDir.resolve("storage");
        Files.createDirectories(storageDir);

        GenerationManager gm = new GenerationManager(storageDir);

        // No current yet
        assertFalse(gm.isAuthoritative(1));
        assertFalse(gm.isAuthoritative(10));

        // Create and switch to gen-10
        Map<String, ObjectMetadata> data = new HashMap<>();
        byte[] snapshot = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(data);
        int checksum = computeChecksum(snapshot);
        gm.createCandidateGeneration(10);
        gm.writeMetadata(10, data);
        gm.writeSnapshot(10, 10, 1, snapshot, checksum);
        gm.writeManifest(10, 10, 1, checksum);
        gm.switchCurrent(10);

        assertTrue(gm.isAuthoritative(10));
        assertFalse(gm.isAuthoritative(11));

        System.out.println("\n========================================");
        System.out.println("TEST: Is Authoritative - PASSED");
        System.out.println("========================================\n");
    }

    private int computeChecksum(byte[] data) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }
}
