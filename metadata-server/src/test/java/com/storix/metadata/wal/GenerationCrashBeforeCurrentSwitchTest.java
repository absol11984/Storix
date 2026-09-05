package com.storix.metadata.wal;

import com.storix.metadata.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CRITICAL: Crash BEFORE CURRENT switch test.
 *
 * Scenario:
 * - CURRENT = 10 (gen-10 is authoritative)
 * - gen-11 prepared, all files written and fsynced
 * - gen-11 VALIDATED completely
 * - CRASH BEFORE atomic CURRENT switch
 *
 * Expected after restart:
 * - CURRENT = 10
 * - gen-10 is authoritative
 * - gen-11 is IGNORED (never became authoritative)
 */
class GenerationCrashBeforeCurrentSwitchTest {

    @TempDir
    Path tempDir;

    @Test
    void testCrashBeforeCurrentSwitchRecoversOldGeneration() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Crash Before CURRENT Switch");
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
        assertEquals(10, gm.getCurrentGeneration(), "CURRENT should be 10");

        // Step 2: Create generation 11 as candidate (prepared but NOT committed)
        System.out.println("\nStep 2: Creating generation 11 as candidate...");
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

        System.out.println("Generation 11 prepared and fully fsynced");
        System.out.println("CRASH - BEFORE CURRENT SWITCH\n");

        // Step 3: SIMULATE CRASH by creating new GenerationManager
        // The crash happens AFTER gen-11 files are written but BEFORE switchCurrent(11)
        // So CURRENT still points to 10
        System.out.println("Step 3: Simulating crash - creating fresh GenerationManager...");
        GenerationManager recoveredGm = new GenerationManager(storageDir);

        // Step 4: Verify old generation is recovered
        System.out.println("\nStep 4: Verifying recovery...");

        assertEquals(10, recoveredGm.getCurrentGeneration(),
            "CURRENT must remain 10 (gen-11 never became authoritative)");

        // gen-11 should still exist on disk (not cleaned up)
        // but should be ignored because CURRENT=10
        assertTrue(recoveredGm.generationExists(11),
            "gen-11 still exists (not auto-deleted)");

        // Load authoritative state
        var state = recoveredGm.loadAuthoritativeState();
        assertNotNull(state, "Should have authoritative state");
        assertEquals(10, state.generation(), "Authoritative generation must be 10");

        // Verify gen-10 data
        assertEquals(3, state.objects().size(), "Must have A B C (gen 10 data)");
        assertTrue(state.objects().containsKey("A"), "Object A must exist");
        assertTrue(state.objects().containsKey("B"), "Object B must exist");
        assertTrue(state.objects().containsKey("C"), "Object C must exist");
        assertFalse(state.objects().containsKey("D"), "Object D must NOT exist (gen 11)");

        System.out.println("\n========================================");
        System.out.println("TEST: Crash Before CURRENT Switch - PASSED");
        System.out.println("  CURRENT = 10 ✓");
        System.out.println("  gen-10 data recovered ✓");
        System.out.println("  gen-11 ignored (DNE) ✓");
        System.out.println("========================================\n");
    }

    private int computeChecksum(byte[] data) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }
}
