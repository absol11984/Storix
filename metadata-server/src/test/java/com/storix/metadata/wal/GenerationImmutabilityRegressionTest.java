package com.storix.metadata.wal;

import com.storix.metadata.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression test proving that normal metadata mutations do NOT modify generation files.
 *
 * Model: WAL is the mutable source, generation directories are immutable snapshots.
 *
 * Test flow:
 * 1. Create generation as candidate (not yet authoritative)
 * 2. Write state and switch CURRENT to make it authoritative
 * 3. Record checksums of generation files
 * 4. Perform normal mutations
 * 5. Verify generation files are UNCHANGED (immutable)
 * 6. Verify flat file (metadata.json) contains the mutated state
 * 7. Restart and verify state is reconstructed correctly (from flat file)
 */
class GenerationImmutabilityRegressionTest {

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

    private static int computeChecksum(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }

    // ===== TEST 1: Normal mutations don't modify generation files =====

    /**
     * CRITICAL REGRESSION TEST: Normal mutations must NOT modify generation files.
     *
     * This proves the correct model:
     * - WAL = mutable source for normal operations
     * - Generation directories = immutable snapshots (created only during compactLog/InstallSnapshot)
     * - Flat file = mutable state (reconstructed from WAL on restart)
     */
    @Test
    void testNormalMutationsDoNotModifyGenerationFiles() throws Exception {
        System.out.println("\n========================================");
        System.out.println("REGRESSION TEST: Normal mutations do not modify generation files");
        System.out.println("========================================\n");

        Path raftDir = tempDir.resolve("normal-mutations-test");
        Path metaFile = tempDir.resolve("normal-meta.json");
        Files.createDirectories(raftDir);

        // Step 1: Create GenerationManager and prepare first generation
        GenerationManager genMgr = new GenerationManager(raftDir);

        // Create gen-1 as a candidate (not authoritative yet)
        genMgr.createCandidateGeneration(1);

        // Write initial state to gen-1
        Map<String, ObjectMetadata> initialState = new HashMap<>();
        initialState.put("objA", makeObject("objA", 1000));
        initialState.put("objB", makeObject("objB", 2000));
        genMgr.writeMetadata(1, initialState);
        byte[] initialSnapshot = objectMapper.writeValueAsBytes(initialState);
        int initialChecksum = computeChecksum(initialSnapshot);
        genMgr.writeSnapshot(1, 0, 0, initialSnapshot, initialChecksum);
        genMgr.writeManifest(1, 0, 0, initialChecksum);

        // Switch CURRENT to make gen-1 authoritative
        genMgr.switchCurrent(1);
        System.out.println("Step 1: Gen 1 is now authoritative");

        // Compute checksums of generation files (after switchCurrent)
        Path genDir = genMgr.getGenerationDir(1);
        Path metadataFile = genDir.resolve("metadata.json");
        Path snapshotFile = genDir.resolve("snapshot.bin");

        byte[] initialMetaBytes = Files.readAllBytes(metadataFile);
        byte[] initialSnapshotBytes = Files.readAllBytes(snapshotFile);
        int initialMetaChecksum = computeChecksum(initialMetaBytes);
        int initialSnapshotChecksum = computeChecksum(initialSnapshotBytes);

        System.out.println("  - metadata.json checksum: " + initialMetaChecksum);
        System.out.println("  - snapshot.bin checksum: " + initialSnapshotChecksum);

        // Step 2: Create MetadataStore and perform normal mutations
        System.out.println("\nStep 2: Creating MetadataStore and performing mutations...");
        MetadataStore store = new MetadataStore(metaFile, genMgr);

        // Mutation 1: Create new object
        store.createObject(makeObject("objC", 3000));
        System.out.println("  - Created objC");

        // Mutation 2: Update existing object
        ObjectMetadata objA = store.getObject("objA").orElseThrow();
        objA.setFileSize(9999);
        store.updateObject(objA);
        System.out.println("  - Updated objA");

        // Mutation 3: Delete object
        store.deleteObject("objB");
        System.out.println("  - Deleted objB");

        // Mutation 4: Create another object
        store.createObject(makeObject("objD", 4000));
        System.out.println("  - Created objD");

        // Save to persist to flat file
        store.save();
        System.out.println("  - Saved (writes to flat file)");

        // Verify in-memory state has all mutations
        Set<String> inMemoryObjects = new HashSet<>(store.listObjects());
        assertTrue(inMemoryObjects.contains("objA"), "objA should exist");
        assertTrue(inMemoryObjects.contains("objC"), "objC should exist");
        assertTrue(inMemoryObjects.contains("objD"), "objD should exist");
        assertFalse(inMemoryObjects.contains("objB"), "objB should NOT exist");
        System.out.println("  - In-memory state verified: " + inMemoryObjects);

        // Step 3: Verify generation files are UNCHANGED (immutable)
        System.out.println("\nStep 3: Verifying generation files are UNCHANGED...");

        byte[] currentMetaBytes = Files.readAllBytes(metadataFile);
        byte[] currentSnapshotBytes = Files.readAllBytes(snapshotFile);
        int currentMetaChecksum = computeChecksum(currentMetaBytes);
        int currentSnapshotChecksum = computeChecksum(currentSnapshotBytes);

        System.out.println("  - metadata.json checksum: " + currentMetaChecksum);
        System.out.println("  - snapshot.bin checksum: " + currentSnapshotChecksum);

        assertEquals(initialMetaChecksum, currentMetaChecksum,
            "FATAL: metadata.json was modified by normal mutations! " +
            "Generation files must be IMMUTABLE after becoming authoritative.");
        assertEquals(initialSnapshotChecksum, currentSnapshotChecksum,
            "FATAL: snapshot.bin was modified by normal mutations! " +
            "Generation files must be IMMUTABLE after becoming authoritative.");

        System.out.println("  - Generation files are UNCHANGED (immutable)");

        // Step 4: Verify flat file has the mutated state
        System.out.println("\nStep 4: Verifying flat file has mutated state...");
        Map<String, Object> flatFileState = objectMapper.readValue(metaFile.toFile(), Map.class);
        Set<String> flatFileObjects = new HashSet<>(flatFileState.keySet());
        System.out.println("  - Flat file state: " + flatFileObjects);

        assertTrue(flatFileObjects.contains("objA"), "Flat file should have objA");
        assertTrue(flatFileObjects.contains("objC"), "Flat file should have objC");
        assertTrue(flatFileObjects.contains("objD"), "Flat file should have objD");
        assertFalse(flatFileObjects.contains("objB"), "Flat file should NOT have objB");
        System.out.println("  - Flat file correctly contains mutated state");

        // Step 5: Restart and verify state is reconstructed correctly
        System.out.println("\nStep 5: Restarting and verifying state reconstruction...");

        MetadataStore restartedStore = new MetadataStore(metaFile, genMgr);
        Set<String> restartedObjects = new HashSet<>(restartedStore.listObjects());

        assertEquals(inMemoryObjects, restartedObjects,
            "State must be reconstructed correctly after restart");
        System.out.println("  - Restarted state: " + restartedObjects);
        System.out.println("  - State matches in-memory state: " + inMemoryObjects.equals(restartedObjects));

        System.out.println("\n========================================");
        System.out.println("REGRESSION TEST: Normal mutations do not modify generation files - PASSED");
        System.out.println("========================================\n");
    }

    // ===== TEST 2: Create new generation via compactLog =====

    /**
     * Tests that compactLog correctly creates a NEW generation with the current state,
     * and the old generation remains immutable.
     */
    @Test
    void testCompactLogCreatesNewGenerationOldRemainsImmutable() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: compactLog creates new generation, old remains immutable");
        System.out.println("========================================\n");

        Path raftDir = tempDir.resolve("compactlog-immutability-test");
        Path metaFile = tempDir.resolve("compactlog-meta.json");
        Files.createDirectories(raftDir);

        // Create GenerationManager and prepare gen-1
        GenerationManager genMgr = new GenerationManager(raftDir);
        genMgr.createCandidateGeneration(1);

        // Write initial state to gen 1
        Map<String, ObjectMetadata> gen1State = new HashMap<>();
        gen1State.put("objA", makeObject("objA", 1000));
        gen1State.put("objB", makeObject("objB", 2000));
        genMgr.writeMetadata(1, gen1State);
        byte[] gen1Snapshot = objectMapper.writeValueAsBytes(gen1State);
        int gen1Checksum = computeChecksum(gen1Snapshot);
        genMgr.writeSnapshot(1, 0, 0, gen1Snapshot, gen1Checksum);
        genMgr.writeManifest(1, 0, 0, gen1Checksum);
        genMgr.switchCurrent(1);

        // Record checksums of gen 1 files
        Path gen1Dir = genMgr.getGenerationDir(1);
        byte[] gen1MetaBytes = Files.readAllBytes(gen1Dir.resolve("metadata.json"));
        int gen1MetaChecksum = computeChecksum(gen1MetaBytes);
        System.out.println("Step 1: Gen 1 is authoritative");

        // Create MetadataStore and add more objects
        MetadataStore store = new MetadataStore(metaFile, genMgr);
        store.createObject(makeObject("objC", 3000));
        store.createObject(makeObject("objD", 4000));
        store.save();

        Map<String, ObjectMetadata> newState = new HashMap<>();
        for (String name : store.listObjects()) {
            newState.put(name, store.getObject(name).orElseThrow());
        }

        // Now simulate compactLog: prepare new generation
        System.out.println("\nStep 2: Preparing new generation via compactLog...");
        long newGen = genMgr.prepareNextGeneration(5);
        assertEquals(2, newGen);

        // Write new state to gen 2
        byte[] newSnapshot = objectMapper.writeValueAsBytes(newState);
        int snapshotChecksum = computeChecksum(newSnapshot);
        genMgr.writeMetadata(2, newState);
        genMgr.writeSnapshot(2, 5, 1, newSnapshot, snapshotChecksum);
        genMgr.writeManifest(2, 5, 1, snapshotChecksum);

        // Switch to new generation
        genMgr.switchCurrent(2);
        System.out.println("  - Switched CURRENT to gen 2");

        // Verify gen 1 files are UNCHANGED (immutable)
        System.out.println("\nStep 3: Verifying gen 1 files are UNCHANGED...");
        byte[] currentGen1MetaBytes = Files.readAllBytes(gen1Dir.resolve("metadata.json"));
        int currentGen1MetaChecksum = computeChecksum(currentGen1MetaBytes);

        assertEquals(gen1MetaChecksum, currentGen1MetaChecksum,
            "FATAL: Gen 1 files were modified! Generations are immutable.");
        System.out.println("  - Gen 1 files are UNCHANGED (immutable)");

        // Verify new generation has new state
        System.out.println("\nStep 4: Verifying gen 2 has new state...");
        Map<String, ObjectMetadata> gen2State = genMgr.readMetadata(2);
        assertTrue(gen2State.containsKey("objC"), "Gen 2 should have objC");
        assertTrue(gen2State.containsKey("objD"), "Gen 2 should have objD");
        System.out.println("  - Gen 2 has new state: " + gen2State.keySet());

        System.out.println("\n========================================");
        System.out.println("TEST: compactLog creates new generation, old remains immutable - PASSED");
        System.out.println("========================================\n");
    }

    // ===== TEST 3: Generation immutability protection =====

    /**
     * Tests that GenerationManager throws an exception if we try to modify an
     * authoritative generation.
     */
    @Test
    void testCannotModifyAuthoritativeGeneration() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Cannot modify authoritative generation");
        System.out.println("========================================\n");

        Path raftDir = tempDir.resolve("immutability-protection-test");
        Files.createDirectories(raftDir);

        // Create GenerationManager and prepare gen-1
        GenerationManager genMgr = new GenerationManager(raftDir);
        genMgr.createCandidateGeneration(1);

        // Write initial state to make generation complete
        Map<String, ObjectMetadata> state = new HashMap<>();
        state.put("objA", makeObject("objA", 1000));
        genMgr.writeMetadata(1, state);
        byte[] snapshot = objectMapper.writeValueAsBytes(state);
        int snapshotChecksum = computeChecksum(snapshot);
        genMgr.writeSnapshot(1, 0, 0, snapshot, snapshotChecksum);
        genMgr.writeManifest(1, 0, 0, snapshotChecksum);

        // Switch to make gen-1 authoritative
        genMgr.switchCurrent(1);
        System.out.println("Step 1: Gen 1 is now authoritative");

        // Try to write metadata to authoritative generation - should throw
        System.out.println("\nStep 2: Attempting to modify metadata of authoritative generation...");
        IOException thrown = assertThrows(IOException.class, () -> {
            Map<String, ObjectMetadata> newState = new HashMap<>();
            newState.put("objB", makeObject("objB", 2000));
            genMgr.writeMetadata(1, newState);
        });

        assertTrue(thrown.getMessage().contains("IMMUTABLE") ||
                   thrown.getMessage().contains("FATAL"),
            "Error should mention immutability: " + thrown.getMessage());
        System.out.println("  - Exception thrown as expected: " + thrown.getMessage());

        // Try to write snapshot to authoritative generation - should throw
        System.out.println("\nStep 3: Attempting to modify snapshot of authoritative generation...");
        thrown = assertThrows(IOException.class, () -> {
            byte[] newSnapshot = objectMapper.writeValueAsBytes(new HashMap<>());
            genMgr.writeSnapshot(1, 1, 1, newSnapshot, computeChecksum(newSnapshot));
        });

        assertTrue(thrown.getMessage().contains("IMMUTABLE") ||
                   thrown.getMessage().contains("FATAL"),
            "Error should mention immutability: " + thrown.getMessage());
        System.out.println("  - Exception thrown as expected: " + thrown.getMessage());

        // Try to write manifest to authoritative generation - should throw
        System.out.println("\nStep 4: Attempting to modify manifest of authoritative generation...");
        thrown = assertThrows(IOException.class, () -> {
            genMgr.writeManifest(1, 1, 1, 12345);
        });

        assertTrue(thrown.getMessage().contains("IMMUTABLE") ||
                   thrown.getMessage().contains("FATAL"),
            "Error should mention immutability: " + thrown.getMessage());
        System.out.println("  - Exception thrown as expected: " + thrown.getMessage());

        System.out.println("\n========================================");
        System.out.println("TEST: Cannot modify authoritative generation - PASSED");
        System.out.println("========================================\n");
    }
}
