package com.storix.metadata.wal;

import com.storix.metadata.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CRITICAL: Multiple restart test.
 *
 * Scenario:
 * - Start with CURRENT = 10
 * - Restart #1: should recover gen-10
 * - Upgrade to gen-11
 * - Restart #2: should recover gen-11
 * - Upgrade to gen-12
 * - Restart #3: should recover gen-12
 *
 * Expected:
 * - Each restart correctly recovers the latest committed generation
 */
class GenerationMultipleRestartTest {

    @TempDir
    Path tempDir;

    @Test
    void testMultipleRestartsRecoverCorrectGeneration() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Multiple Restart Recovery");
        System.out.println("========================================\n");

        Path storageDir = tempDir.resolve("storage");
        Files.createDirectories(storageDir);

        // Create gen-10
        {
            GenerationManager gm = new GenerationManager(storageDir);
            Map<String, ObjectMetadata> data = new HashMap<>();
            ObjectMetadata obj = new ObjectMetadata("A", 1000L, 4096);
            obj.addChunk(new ChunkInfo("chunk-A", 0, 4096, List.of("node1"), "hash-A"));
            data.put("A", obj);

            byte[] snapshot = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(data);
            int checksum = computeChecksum(snapshot);
            gm.createCandidateGeneration(10);
            gm.writeMetadata(10, data);
            gm.writeSnapshot(10, 10, 1, snapshot, checksum);
            gm.writeManifest(10, 10, 1, checksum);
            gm.switchCurrent(10);
        }
        System.out.println("Created gen-10\n");

        // Restart #1 - should recover gen-10
        {
            GenerationManager gm = new GenerationManager(storageDir);
            assertEquals(10, gm.getCurrentGeneration());
            var state = gm.loadAuthoritativeState();
            assertEquals(10, state.generation());
            assertTrue(state.objects().containsKey("A"));
            assertEquals(1, state.objects().size());
            System.out.println("Restart #1: Recovered gen-10 ✓");
        }

        // Upgrade to gen-11
        {
            GenerationManager gm = new GenerationManager(storageDir);
            Map<String, ObjectMetadata> data = new HashMap<>();
            for (String name : List.of("A", "B")) {
                ObjectMetadata obj = new ObjectMetadata(name, 1000L, 4096);
                obj.addChunk(new ChunkInfo("chunk-" + name, 0, 4096, List.of("node1"), "hash-" + name));
                data.put(name, obj);
            }

            byte[] snapshot = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(data);
            int checksum = computeChecksum(snapshot);
            gm.createCandidateGeneration(11);
            gm.writeMetadata(11, data);
            gm.writeSnapshot(11, 11, 1, snapshot, checksum);
            gm.writeManifest(11, 11, 1, checksum);
            gm.switchCurrent(11);
        }
        System.out.println("Upgraded to gen-11\n");

        // Restart #2 - should recover gen-11
        {
            GenerationManager gm = new GenerationManager(storageDir);
            assertEquals(11, gm.getCurrentGeneration());
            var state = gm.loadAuthoritativeState();
            assertEquals(11, state.generation());
            assertTrue(state.objects().containsKey("A"));
            assertTrue(state.objects().containsKey("B"));
            assertEquals(2, state.objects().size());
            System.out.println("Restart #2: Recovered gen-11 ✓");
        }

        // Upgrade to gen-12
        {
            GenerationManager gm = new GenerationManager(storageDir);
            Map<String, ObjectMetadata> data = new HashMap<>();
            for (String name : List.of("A", "B", "C")) {
                ObjectMetadata obj = new ObjectMetadata(name, 1000L, 4096);
                obj.addChunk(new ChunkInfo("chunk-" + name, 0, 4096, List.of("node1"), "hash-" + name));
                data.put(name, obj);
            }

            byte[] snapshot = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(data);
            int checksum = computeChecksum(snapshot);
            gm.createCandidateGeneration(12);
            gm.writeMetadata(12, data);
            gm.writeSnapshot(12, 12, 1, snapshot, checksum);
            gm.writeManifest(12, 12, 1, checksum);
            gm.switchCurrent(12);
        }
        System.out.println("Upgraded to gen-12\n");

        // Restart #3 - should recover gen-12
        {
            GenerationManager gm = new GenerationManager(storageDir);
            assertEquals(12, gm.getCurrentGeneration());
            var state = gm.loadAuthoritativeState();
            assertEquals(12, state.generation());
            assertTrue(state.objects().containsKey("A"));
            assertTrue(state.objects().containsKey("B"));
            assertTrue(state.objects().containsKey("C"));
            assertEquals(3, state.objects().size());
            System.out.println("Restart #3: Recovered gen-12 ✓");
        }

        System.out.println("\n========================================");
        System.out.println("TEST: Multiple Restart Recovery - PASSED");
        System.out.println("  Restart #1: gen-10 ✓");
        System.out.println("  Restart #2: gen-11 ✓");
        System.out.println("  Restart #3: gen-12 ✓");
        System.out.println("========================================\n");
    }

    private int computeChecksum(byte[] data) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }
}
