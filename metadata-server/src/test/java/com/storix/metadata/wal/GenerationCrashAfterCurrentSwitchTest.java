package com.storix.metadata.wal;

import com.storix.metadata.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CRITICAL: Crash AFTER CURRENT switch test.
 *
 * Scenario:
 * - CURRENT = 10 (gen-10 is authoritative)
 * - gen-11 prepared, all files written and fsynced
 * - gen-11 VALIDATED
 * - CURRENT switched to 11 (atomically)
 * - fsync COMPLETED
 * - CRASH
 *
 * Expected after restart:
 * - CURRENT = 11
 * - gen-11 is authoritative
 * - gen-11 data recovered
 */
class GenerationCrashAfterCurrentSwitchTest {

    @TempDir
    Path tempDir;

    @Test
    void testCrashAfterCurrentSwitchRecoversNewGeneration() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Crash After CURRENT Switch");
        System.out.println("========================================\n");

        Path storageDir = tempDir.resolve("storage");
        Files.createDirectories(storageDir);

        // Step 1: Create generation 10 as authoritative
        System.out.println("Step 1: Creating generation 10 as authoritative...");
        GenerationManager gm = new GenerationManager(storageDir);

        Map<String, ObjectMetadata> gen10Data = new HashMap<>();
        for (String name : List.of("A", "B", "C")) {
            ObjectMetadata obj = new ObjectMetadata(name, 1000L, 4096);
            obj.addChunk(new ChunkInfo("chunk-" + name, 0, 4096, List.of("node1"), "hash-" + name));
            gen10Data.put(name, obj);
        }

        gm.createCandidateGeneration(10);
        byte[] gen10Snapshot = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(gen10Data);
        int gen10Checksum = computeChecksum(gen10Snapshot);
        gm.writeMetadata(10, gen10Data);
        gm.writeSnapshot(10, 10, 1, gen10Snapshot, gen10Checksum);
        gm.writeManifest(10, 10, 1, gen10Checksum);
        gm.switchCurrent(10);

        System.out.println("Generation 10 created and made authoritative");

        // Step 2: Create generation 11 and switch CURRENT
        System.out.println("\nStep 2: Creating generation 11 and switching CURRENT...");
        Map<String, ObjectMetadata> gen11Data = new HashMap<>();
        for (String name : List.of("A", "B", "C", "D")) {
            ObjectMetadata obj = new ObjectMetadata(name, 1000L, 4096);
            obj.addChunk(new ChunkInfo("chunk-" + name, 0, 4096, List.of("node1"), "hash-" + name));
            gen11Data.put(name, obj);
        }

        gm.createCandidateGeneration(11);
        byte[] gen11Snapshot = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(gen11Data);
        int gen11Checksum = computeChecksum(gen11Snapshot);
        gm.writeMetadata(11, gen11Data);
        gm.writeSnapshot(11, 11, 1, gen11Snapshot, gen11Checksum);
        gm.writeManifest(11, 11, 1, gen11Checksum);

        // Switch CURRENT to 11 - this is the commit point
        gm.switchCurrent(11);

        System.out.println("CURRENT switched to generation 11");
        assertEquals(11, gm.getCurrentGeneration(), "CURRENT should be 11");

        // Verify gen-10 is still there (shouldn't be deleted)
        assertTrue(gm.generationExists(10), "gen-10 should still exist (immutability)");

        System.out.println("\nStep 3: Simulating crash...\n");

        // Step 3: SIMULATE CRASH - create fresh GenerationManager
        GenerationManager recoveredGm = new GenerationManager(storageDir);

        // Step 4: Verify new generation is recovered
        System.out.println("Step 4: Verifying recovery...");

        assertEquals(11, recoveredGm.getCurrentGeneration(),
            "CURRENT must be 11 (gen-11 is now authoritative)");

        // Load authoritative state
        var state = recoveredGm.loadAuthoritativeState();
        assertNotNull(state, "Should have authoritative state");
        assertEquals(11, state.generation(), "Authoritative generation must be 11");

        // Verify gen-11 data
        assertEquals(4, state.objects().size(), "Must have A B C D (gen 11 data)");
        assertTrue(state.objects().containsKey("A"), "Object A must exist");
        assertTrue(state.objects().containsKey("B"), "Object B must exist");
        assertTrue(state.objects().containsKey("C"), "Object C must exist");
        assertTrue(state.objects().containsKey("D"), "Object D must exist (gen 11)");

        // Verify manifest data
        assertEquals(11, state.lastIncludedIndex(), "Last included index must be 11");
        assertEquals(1, state.lastIncludedTerm(), "Last included term must be 1");

        System.out.println("\n========================================");
        System.out.println("TEST: Crash After CURRENT Switch - PASSED");
        System.out.println("  CURRENT = 11 ✓");
        System.out.println("  gen-11 data recovered ✓");
        System.out.println("  gen-10 preserved (immutable) ✓");
        System.out.println("========================================\n");
    }

    private int computeChecksum(byte[] data) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }
}
