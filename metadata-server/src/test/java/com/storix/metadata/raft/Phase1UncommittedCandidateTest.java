package com.storix.metadata.raft;

import com.storix.metadata.*;
import com.storix.metadata.wal.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.*;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 1 Uncommitted Candidate Test
 *
 * CRITICAL TEST: Verifies that when a candidate generation exists WITHOUT CURRENT pointing to it,
 * the OLD generation is recovered (not the uncommitted candidate).
 *
 * Architecture:
 * - GenerationManager/CURRENT is the ONLY authoritative source
 * - A generation is authoritative ONLY if CURRENT points to it
 * - Uncommitted candidates (directories without CURRENT switch) are ignored on restart
 *
 * Test flow:
 * 1. Create generation 10 with CURRENT pointing to it (authoritative)
 * 2. Create generation 11 directory WITHOUT switching CURRENT (candidate only)
 * 3. Restart
 * 4. Verify generation 10 is recovered (gen-11 ignored)
 */
class Phase1UncommittedCandidateTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @TempDir
    Path tempDir;

    /**
     * CRITICAL: Uncommitted candidates must be ignored on restart.
     *
     * Setup:
     * - Generation 10: CURRENT points to it (committed/authoritative)
     * - Generation 11: directory exists but CURRENT does NOT point to it (candidate only)
     *
     * Expected: Recovery returns generation 10 (CURRENT's generation)
     */
    @Test
    void testUncommittedCandidateIsIgnored() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Uncommitted Candidate Is Ignored");
        System.out.println("========================================\n");

        Path raftDir = tempDir.resolve("raft");
        Path metaFile = tempDir.resolve("metadata.json");
        Files.createDirectories(raftDir);

        // Create GenerationManager and initialize first generation
        GenerationManager genMgr = new GenerationManager(raftDir);
        genMgr.initializeFirstGeneration();
        assertEquals(1, genMgr.getCurrentGeneration(), "First generation should be 1");

        // Create generation 10 with state A, B, C
        long gen10 = 10;
        genMgr.createCandidateGeneration(gen10);
        MetadataStore store10 = new MetadataStore(metaFile);
        for (String name : List.of("A", "B", "C")) {
            ObjectMetadata obj = makeObject(name, 1000L);
            store10.createObjectDirect(obj);
        }
        Map<String, ObjectMetadata> objects10 = getObjectsMap(store10);
        genMgr.writeMetadata(gen10, objects10);
        // Write snapshot and manifest for complete generation
        byte[] snapshot10 = objectMapper.writeValueAsBytes(objects10);
        int checksum10 = computeChecksum(snapshot10);
        genMgr.writeSnapshot(gen10, 10, 0, snapshot10, checksum10);
        genMgr.writeManifest(gen10, 10, 0, checksum10);
        genMgr.switchCurrent(gen10);
        System.out.println("Created generation 10 with CURRENT pointing to it (authoritative)");

        // Now create generation 11 WITHOUT switching CURRENT (candidate only)
        long gen11 = 11;
        genMgr.createCandidateGeneration(gen11);
        MetadataStore store11 = new MetadataStore(tempDir.resolve("candidate.json"));
        for (String name : List.of("A", "B", "C", "D")) {
            ObjectMetadata obj = makeObject(name, 1000L);
            store11.createObjectDirect(obj);
        }
        genMgr.writeMetadata(gen11, getObjectsMap(store11));
        // NOTE: We do NOT call switchCurrent(gen11) - gen-11 remains a candidate
        System.out.println("Created generation 11 WITHOUT switching CURRENT (candidate only)");
        System.out.println("State: CURRENT -> gen-10, gen-11 is candidate");

        // Verify state before restart
        assertEquals(10, genMgr.getCurrentGeneration(), "CURRENT should point to gen-10");
        assertTrue(Files.exists(genMgr.getGenerationDir(10)), "gen-10 directory should exist");
        assertTrue(Files.exists(genMgr.getGenerationDir(11)), "gen-11 directory should exist");

        // Restart with fresh GenerationManager
        System.out.println("\nRestarting with fresh GenerationManager...");
        GenerationManager newGenMgr = new GenerationManager(raftDir);

        // CRITICAL ASSERTION: Must recover generation 10 (CURRENT's generation)
        long recoveredGen = newGenMgr.getCurrentGeneration();
        assertEquals(10, recoveredGen, "Must recover generation 10 (CURRENT's generation), NOT uncommitted gen-11");

        // Verify the state
        GenerationManager.GenerationState state = newGenMgr.loadAuthoritativeState();
        assertNotNull(state, "State should be loadable");
        assertEquals(10, state.generation(), "Recovered state should be from generation 10");
        assertEquals(3, state.objects().size(), "Should have 3 objects (A, B, C)");
        assertTrue(state.objects().containsKey("A"), "Object A should exist");
        assertTrue(state.objects().containsKey("B"), "Object B should exist");
        assertTrue(state.objects().containsKey("C"), "Object C should exist");
        assertFalse(state.objects().containsKey("D"), "Object D should NOT exist (only in uncommitted gen-11)");

        System.out.println("SUCCESS: Recovered generation 10 (authoritative)");
        System.out.println("        Uncommitted generation 11 was correctly ignored");
        System.out.println("        Objects recovered: A, B, C (NOT D)");

        System.out.println("\n========================================");
        System.out.println("TEST: Uncommitted Candidate Is Ignored - PASSED");
        System.out.println("========================================\n");
    }

    private static ObjectMetadata makeObject(String name, long fileSize) {
        ObjectMetadata obj = new ObjectMetadata(name, fileSize, 4096);
        obj.addChunk(new ChunkInfo(
            "chunk-" + name, 0, (int) Math.min(fileSize, 4096),
            java.util.List.of("node1", "node2"),
            "hash-" + name
        ));
        return obj;
    }

    private static int computeChecksum(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }

    private static java.util.Map<String, ObjectMetadata> getObjectsMap(MetadataStore store) {
        java.util.Map<String, ObjectMetadata> map = new java.util.HashMap<>();
        for (String name : store.listObjects()) {
            map.put(name, store.getObject(name).orElseThrow());
        }
        return map;
    }
}
