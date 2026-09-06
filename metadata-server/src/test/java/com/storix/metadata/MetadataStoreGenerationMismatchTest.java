package com.storix.metadata;

import com.storix.metadata.wal.GenerationManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for MetadataStore + GenerationManager integration.
 *
 * Verifies that when MetadataStore operates in GenerationManager/cluster mode:
 * 1. The authoritative generation comes from GenerationManager (via CURRENT)
 * 2. The legacy .generation file does NOT override the authoritative value
 * 3. currentGeneration matches GenerationManager's authoritative generation
 */
class MetadataStoreGenerationMismatchTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @TempDir
    Path tempDir;

    private static ObjectMetadata makeObject(String name, long fileSize) {
        ObjectMetadata obj = new ObjectMetadata(name, fileSize, 4096);
        obj.addChunk(new ChunkInfo(
            "chunk-" + name, 0, (int) Math.min(fileSize, 4096),
            List.of("node1", "node2"),
            "hash-" + name
        ));
        return obj;
    }

    // ===== TEST 1: Generation mismatch cannot override authoritative generation =====

    /**
     * Regression test: Legacy .generation file must NOT override authoritative GenerationManager value.
     *
     * Scenario:
     * - CURRENT = generation 2
     * - GenerationManager authoritative state = generation 2
     * - Legacy metadata.generation = 1
     *
     * Expected:
     * - store.getGeneration() == 2 (from GenerationManager, NOT from legacy file)
     * - store.wasLoadedFromGeneration() == true
     * - Loaded objects come from generation 2
     */
    @Test
    void testLegacyGenerationFileCannotOverrideAuthoritativeGeneration() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Legacy Generation File Override Prevention");
        System.out.println("========================================\n");

        Path raftDir = tempDir.resolve("generation-mismatch-test");
        Path metaFile = tempDir.resolve("metadata-generation-mismatch.json");
        Files.createDirectories(raftDir);

        // Setup GenerationManager
        GenerationManager genMgr = new GenerationManager(raftDir);
        genMgr.initializeFirstGeneration();

        // Create generation 2 with specific objects (A, B, C)
        System.out.println("Creating generation 2 with objects: A, B, C");
        Map<String, ObjectMetadata> gen2Objects = new HashMap<>();
        for (String name : List.of("A", "B", "C")) {
            gen2Objects.put(name, makeObject(name, 1000L));
        }

        long gen2 = genMgr.prepareNextGeneration(3);
        byte[] snapshotData = objectMapper.writeValueAsBytes(gen2Objects);
        int checksum = computeChecksum(snapshotData);
        genMgr.writeMetadata(gen2, gen2Objects);
        genMgr.writeSnapshot(gen2, 3, 1, snapshotData, checksum);
        genMgr.writeManifest(gen2, 3, 1, checksum);
        genMgr.switchCurrent(gen2);

        // Verify generation 2 is current
        assertEquals(2, genMgr.getCurrentGeneration());
        System.out.println("Generation 2 is now current");

        // Now create the legacy .generation file with WRONG value (1 instead of 2)
        // This simulates the scenario where the legacy file has stale data
        Path legacyGenerationFile = metaFile.resolveSibling(metaFile.getFileName() + ".generation");
        Files.writeString(legacyGenerationFile, "1"); // WRONG: should be 2
        System.out.println("Created legacy .generation file with WRONG value: 1");
        System.out.println("  (CURRENT is actually: 2)");

        // Also create legacy metadata.json with different objects (X, Y instead of A, B, C)
        Map<String, ObjectMetadata> legacyObjects = new HashMap<>();
        legacyObjects.put("X", makeObject("X", 999L));
        legacyObjects.put("Y", makeObject("Y", 888L));
        objectMapper.writeValue(metaFile.toFile(), legacyObjects);
        System.out.println("Created legacy metadata.json with objects: X, Y");

        // Create MetadataStore - it should use GenerationManager, NOT legacy files
        System.out.println("\nCreating MetadataStore with GenerationManager...");
        MetadataStore store = new MetadataStore(metaFile, genMgr);

        // CRITICAL ASSERTIONS:
        // 1. Generation should come from GenerationManager (2), NOT from legacy file (1)
        assertEquals(2, store.getGeneration(),
            "Generation should be 2 (from GenerationManager), NOT 1 (from legacy file)");
        System.out.println("✓ Generation is correctly 2 (from GenerationManager)");

        // 2. Should report loading from generation
        assertTrue(store.wasLoadedFromGeneration(),
            "Should report loading from GenerationManager");
        System.out.println("✓ Loaded from GenerationManager");

        // 3. Loaded objects should come from generation 2, NOT from legacy file
        Set<String> loadedObjects = new HashSet<>(store.listObjects());
        assertEquals(Set.of("A", "B", "C"), loadedObjects,
            "Loaded objects should be A, B, C (from generation 2), NOT X, Y (from legacy file)");
        System.out.println("✓ Loaded objects are A, B, C (from generation 2)");

        // 4. Legacy objects should NOT be present
        assertFalse(store.objectExists("X"),
            "Legacy object X should NOT be loaded");
        assertFalse(store.objectExists("Y"),
            "Legacy object Y should NOT be loaded");
        System.out.println("✓ Legacy objects X, Y correctly ignored");

        // 5. Generation 2 objects should be present
        assertTrue(store.objectExists("A"));
        assertTrue(store.objectExists("B"));
        assertTrue(store.objectExists("C"));
        System.out.println("✓ Generation 2 objects A, B, C all present");

        System.out.println("\n========================================");
        System.out.println("TEST: Legacy Generation File Override Prevention - PASSED");
        System.out.println("========================================\n");
    }

    // ===== TEST 2: Generation matches GenerationManager on fresh start =====

    /**
     * Test that on fresh start with GenerationManager, the generation matches.
     */
    @Test
    void testGenerationMatchesGenerationManagerOnFreshStart() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Generation Matches GenerationManager (Fresh Start)");
        System.out.println("========================================\n");

        Path raftDir = tempDir.resolve("fresh-start-test");
        Path metaFile = tempDir.resolve("metadata-fresh.json");
        Files.createDirectories(raftDir);

        // Setup GenerationManager with initializeFirstGeneration()
        GenerationManager genMgr = new GenerationManager(raftDir);
        genMgr.initializeFirstGeneration();

        long expectedGen = genMgr.getCurrentGeneration();
        System.out.println("Expected generation from GenerationManager: " + expectedGen);

        // Create MetadataStore
        MetadataStore store = new MetadataStore(metaFile, genMgr);

        // Verify generation matches
        assertEquals(expectedGen, store.getGeneration(),
            "Generation should match GenerationManager's current generation");
        assertTrue(store.wasLoadedFromGeneration(),
            "Should report loading from GenerationManager");
        System.out.println("✓ Generation matches: " + store.getGeneration());

        System.out.println("\n========================================");
        System.out.println("TEST: Generation Matches GenerationManager - PASSED");
        System.out.println("========================================\n");
    }

    // ===== TEST 3: Multiple generation switches =====

    /**
     * Test that after multiple generation switches, MetadataStore always reflects current generation.
     */
    @Test
    void testGenerationMatchesAfterMultipleSwitches() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Generation Matches After Multiple Switches");
        System.out.println("========================================\n");

        Path raftDir = tempDir.resolve("multi-switch-test");
        Path metaFile = tempDir.resolve("metadata-multi.json");
        Files.createDirectories(raftDir);

        // Setup
        GenerationManager genMgr = new GenerationManager(raftDir);
        genMgr.initializeFirstGeneration();

        // Switch through generations 1, 2, 3
        for (int genNum = 1; genNum <= 3; genNum++) {
            System.out.println("\nSwitching to generation " + genNum + "...");

            Map<String, ObjectMetadata> objects = new HashMap<>();
            for (int i = 1; i <= genNum; i++) {
                String name = "obj" + i;
                objects.put(name, makeObject(name, 1000L * i));
            }

            if (genNum == 1) {
                // Gen 1 already exists from initializeFirstGeneration as empty
                // Create gen 2 with objects for gen 1, then switch to gen 2
                // (generations are immutable, so we can't modify gen 1)
                long gen2 = genMgr.prepareNextGeneration(1);
                byte[] snapshotData = objectMapper.writeValueAsBytes(objects);
                int checksum = computeChecksum(snapshotData);
                genMgr.writeMetadata(gen2, objects);
                genMgr.writeSnapshot(gen2, 1, 1, snapshotData, checksum);
                genMgr.writeManifest(gen2, 1, 1, checksum);
                genMgr.switchCurrent(gen2);
            } else {
                long newGen = genMgr.prepareNextGeneration(genNum);
                byte[] snapshotData = objectMapper.writeValueAsBytes(objects);
                int checksum = computeChecksum(snapshotData);
                genMgr.writeMetadata(newGen, objects);
                genMgr.writeSnapshot(newGen, genNum, 1, snapshotData, checksum);
                genMgr.writeManifest(newGen, genNum, 1, checksum);
                genMgr.switchCurrent(newGen);
            }

            // Create new MetadataStore
            MetadataStore store = new MetadataStore(metaFile, genMgr);

            // Generation will be genNum + 1 (since we start at gen 1 but immediately switch to gen 2)
            int expectedGen = genNum + 1;

            // Verify generation matches
            assertEquals(expectedGen, store.getGeneration(),
                "Generation should be " + expectedGen);
            assertTrue(store.wasLoadedFromGeneration());

            // Verify objects
            for (int i = 1; i <= genNum; i++) {
                assertTrue(store.objectExists("obj" + i),
                    "obj" + i + " should exist in generation " + expectedGen);
            }

            System.out.println("✓ Generation " + expectedGen + " verified: " + store.listObjects().size() + " objects");
        }

        System.out.println("\n========================================");
        System.out.println("TEST: Generation Matches After Multiple Switches - PASSED");
        System.out.println("========================================\n");
    }

    private static int computeChecksum(byte[] data) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }
}
