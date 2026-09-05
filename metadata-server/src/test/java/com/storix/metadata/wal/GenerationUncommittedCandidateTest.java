package com.storix.metadata.wal;

import com.storix.metadata.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CRITICAL: Uncommitted candidate test.
 *
 * Scenario:
 * - CURRENT = 10 (gen-10 is authoritative)
 * - gen-11 exists but never switched to
 *
 * Expected after restart:
 * - CURRENT = 10
 * - gen-10 is authoritative
 * - gen-11 is ignored (never became authoritative)
 */
class GenerationUncommittedCandidateTest {

    @TempDir
    Path tempDir;

    @Test
    void testUncommittedCandidateIsIgnored() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Uncommitted Candidate Ignored");
        System.out.println("========================================\n");

        Path storageDir = tempDir.resolve("storage");
        Files.createDirectories(storageDir);

        // Create generation 10 as authoritative
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

        // Create gen-11 but DON'T switch to it (uncommitted candidate)
        Map<String, ObjectMetadata> gen11Data = new HashMap<>();
        for (String name : List.of("X", "Y", "Z")) {
            ObjectMetadata obj = new ObjectMetadata(name, 2000L, 4096);
            obj.addChunk(new ChunkInfo("chunk-" + name, 0, 4096, List.of("node1"), "hash-" + name));
            gen11Data.put(name, obj);
        }

        gm.createCandidateGeneration(11);
        byte[] gen11Snapshot = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(gen11Data);
        int gen11Checksum = computeChecksum(gen11Snapshot);
        gm.writeMetadata(11, gen11Data);
        gm.writeSnapshot(11, 11, 1, gen11Snapshot, gen11Checksum);
        gm.writeManifest(11, 11, 1, gen11Checksum);

        System.out.println("Created gen-10 (committed) and gen-11 (UNCOMMITTED)");
        System.out.println("CURRENT = 10, gen-11 is candidate only\n");

        // Restart
        GenerationManager recoveredGm = new GenerationManager(storageDir);

        // Verify gen-10 is authoritative
        assertEquals(10, recoveredGm.getCurrentGeneration(),
            "CURRENT must be 10 (gen-11 never became authoritative)");

        var state = recoveredGm.loadAuthoritativeState();
        assertNotNull(state);
        assertEquals(10, state.generation());

        // Verify gen-10 data
        assertTrue(state.objects().containsKey("A"));
        assertTrue(state.objects().containsKey("B"));
        assertTrue(state.objects().containsKey("C"));
        assertFalse(state.objects().containsKey("X"), "gen-11 objects must not appear");

        System.out.println("\n========================================");
        System.out.println("TEST: Uncommitted Candidate Ignored - PASSED");
        System.out.println("========================================\n");
    }

    private int computeChecksum(byte[] data) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }
}
