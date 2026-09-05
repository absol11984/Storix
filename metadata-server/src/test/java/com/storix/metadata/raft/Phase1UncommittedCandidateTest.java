package com.storix.metadata.raft;

import com.storix.metadata.*;
import com.storix.metadata.wal.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 1 Uncommitted Candidate Test
 *
 * CRITICAL TEST: Verifies that when a candidate snapshot exists without a commit marker,
 * the OLD committed generation is recovered (not the uncommitted candidate).
 *
 * This test manually creates snapshot files to ensure we have:
 * - Generation 10: snapshot exists + commit marker (committed)
 * - Generation 11: snapshot exists only (UNCOMMITTED candidate)
 *
 * On restart, the system MUST recover generation 10.
 */
class Phase1UncommittedCandidateTest {

    @TempDir
    Path tempDir;

    /**
     * CRITICAL: Uncommitted candidates must be ignored on restart.
     *
     * Setup:
     * - Generation 10: committed (snapshot-10 + generation-10.committed)
     * - Generation 11: UNCOMMITTED candidate (snapshot-11 only, NO commit marker)
     *
     * Expected: Recovery returns generation 10 (the only committed generation)
     */
    @Test
    void testUncommittedCandidateIsIgnored() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Uncommitted Candidate Is Ignored");
        System.out.println("========================================\n");

        Path snapDir = tempDir.resolve("snapshots");
        Path metaFile = tempDir.resolve("metadata.json");
        Files.createDirectories(snapDir);

        // Create committed metadata for generation 10 (A, B, C)
        MetadataStore store = new MetadataStore(metaFile);
        for (String name : List.of("A", "B", "C")) {
            ObjectMetadata obj = makeObject(name, 1000L);
            store.createObjectDirect(obj);
        }
        store.setGeneration(10);
        store.save();

        System.out.println("Created committed metadata for generation 10: objects A B C");

        // Create committed snapshot for generation 10
        SnapshotManager sm = new SnapshotManager(snapDir, store);
        sm.takeSnapshot(10, 1);
        System.out.println("Created snapshot-10 with commit marker");

        // Now create an UNCOMMITTED candidate for generation 11
        // Create new metadata with object D (candidate state)
        MetadataStore candidateStore = new MetadataStore(tempDir.resolve("candidate.json"));
        for (String name : List.of("A", "B", "C", "D")) {
            ObjectMetadata obj = makeObject(name, 1000L);
            candidateStore.createObjectDirect(obj);
        }
        candidateStore.setGeneration(11);
        candidateStore.save();

        // Manually create snapshot-11 file WITHOUT a commit marker
        // We bypass SnapshotManager.takeSnapshot() because it auto-commits
        byte[] stateData = new com.fasterxml.jackson.databind.ObjectMapper()
            .writeValueAsBytes(candidateStore.listObjects().stream()
                .collect(java.util.stream.Collectors.toMap(
                    s -> s,
                    s -> candidateStore.getObject(s).orElseThrow()
                )));

        int checksum = computeChecksum(stateData);

        // Write snapshot-11 file manually
        Path snapshotFile = snapDir.resolve("snapshot-11");
        try (FileOutputStream fos = new FileOutputStream(snapshotFile.toFile());
             java.nio.channels.FileChannel fc = fos.getChannel()) {

            ByteBuffer headerBuf = ByteBuffer.allocate(12 + 16 + 4);
            headerBuf.putLong(0x534E415053484F54L); // SNAPSHOT magic
            headerBuf.putInt(1); // version
            headerBuf.putLong(11); // lastIncludedIndex
            headerBuf.putLong(1); // lastIncludedTerm
            headerBuf.putInt(stateData.length);
            headerBuf.flip();
            fc.write(headerBuf);
            fc.write(ByteBuffer.wrap(stateData));

            ByteBuffer checksumBuf = ByteBuffer.allocate(4);
            checksumBuf.putInt(checksum);
            checksumBuf.flip();
            fc.write(checksumBuf);
            fc.force(true);
        }

        System.out.println("Created UNCOMMITTED snapshot-11 (NO commit marker)");
        System.out.println("State: Generation 10 is COMMITTED, Generation 11 is CANDIDATE only");

        // Verify state: generation 10 has commit marker, generation 11 does NOT
        Path commitMarker10 = snapDir.resolve("generation-10.committed");
        Path commitMarker11 = snapDir.resolve("generation-11.committed");
        assertTrue(Files.exists(commitMarker10), "Generation 10 must have commit marker");
        assertFalse(Files.exists(commitMarker11), "Generation 11 must NOT have commit marker");

        // Now restart with fresh stores
        System.out.println("\nRestarting with fresh stores...");
        MetadataStore newStore = new MetadataStore(metaFile);
        SnapshotManager newSm = new SnapshotManager(snapDir, newStore);

        // Load latest snapshot - should return generation 10 (only committed one)
        var latestSnap = newSm.loadLatestSnapshot();

        // CRITICAL ASSERTION: Must recover COMMITTED generation 10, NOT uncommitted 11
        assertTrue(latestSnap.isPresent(), "Should find committed snapshot");
        assertEquals(10, latestSnap.get().lastIncludedIndex(),
            "Must recover COMMITTED generation 10, NOT uncommitted 11");

        System.out.println("SUCCESS: Recovered generation 10 (committed)");
        System.out.println("        Uncommitted generation 11 was correctly ignored");

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
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }
}
