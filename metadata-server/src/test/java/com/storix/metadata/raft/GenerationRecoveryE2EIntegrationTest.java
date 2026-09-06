package com.storix.metadata.raft;

import com.storix.metadata.*;
import com.storix.metadata.wal.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * REAL End-to-End Integration Test for Generation Authority Recovery
 *
 * This test proves the complete production recovery path:
 * 1. Real metadata writes through Raft/WAL
 * 2. Snapshot/generation creation
 * 3. More writes after snapshot
 * 4. Shutdown
 * 5. NEW MetadataServer (completely fresh instance using same directories)
 * 6. Recovery: CURRENT → generation's metadata.json → WAL replay
 * 7. Verify EXACT state matches
 *
 * IMPORTANT: This test MUST NOT:
 * - Manually call restoreFromSnapshot()
 * - Manually call WAL recovery
 * - Manually apply WAL entries
 * - Manually repair/delete persistence files
 * - Directly mutate MetadataStore to construct test state
 *
 * The test MUST use the production MetadataServer startup path.
 */
class GenerationRecoveryE2EIntegrationTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int BASE_PORT = 18000;

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

    // ===== TEST: Complete Production Recovery Path =====

    /**
     * CRITICAL E2E TEST: Real server restart with generation + WAL recovery
     *
     * This test verifies:
     * - Snapshot state: A, B, C (created before snapshot)
     * - Post-snapshot operations: create D, create E, delete B, update A's size
     * - Expected final state: A(updated), C, D, E
     *
     * The test uses ONLY the production MetadataServer startup path.
     * No manual state construction or WAL manipulation.
     */
    @Test
    void testProductionServerRestartWithGenerationAndWALRecovery() throws Exception {
        System.out.println("\n========================================");
        System.out.println("E2E TEST: Production Server Restart Recovery");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 100;
        int followerPort = BASE_PORT + 101;

        Path leaderRaftDir = tempDir.resolve("leader-raft-e2e");
        Path followerRaftDir = tempDir.resolve("follower-raft-e2e");
        Path leaderMetaFile = tempDir.resolve("leader-meta-e2e.json");
        Path followerMetaFile = tempDir.resolve("follower-meta-e2e.json");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);

        // ===== PHASE 1: Setup Leader with Raft/WAL/Generation =====
        System.out.println("PHASE 1: Setting up leader with GenerationManager");
        System.out.println("-----------------------------------------------");

        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);
        MetadataStore leaderStore = new MetadataStore(leaderMetaFile);
        GenerationManager leaderGenMgr = new GenerationManager(leaderRaftDir);
        leaderGenMgr.initializeFirstGeneration();
        SnapshotManager leaderSnapshotMgr = new SnapshotManager(leaderRaftDir.resolve("snapshots"), leaderStore);
        WAL leaderWal = new WAL(leaderRaftDir.resolve("wal.dat"));
        RaftLog leaderLog = new RaftLog(leaderWal);

        RaftNode leader = new RaftNode(leaderConfig, leaderRaftDir, leaderLog, leaderWal);
        leader.setGenerationManager(leaderGenMgr);
        leader.setSnapshotManager(leaderSnapshotMgr);
        leader.setMetadataStore(leaderStore);
        MetadataStateMachine leaderStateMachine = new MetadataStateMachine(leaderStore);
        leader.setLogEntryApplier(entry -> {
            try { leaderStateMachine.apply(entry); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        // ===== PHASE 2: Create objects A, B, C before snapshot =====
        System.out.println("\nPHASE 2: Creating objects A, B, C (before snapshot)");
        System.out.println("-------------------------------------------------");

        leader.start();
        Thread.sleep(200); // Wait for leader election

        for (String name : List.of("A", "B", "C")) {
            ObjectMetadata obj = makeObject(name, 1000L);
            byte[] data = objectMapper.writeValueAsBytes(obj);
            LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
            leader.submit(entry);
            System.out.println("  Created: " + name);
        }
        Thread.sleep(200);

        long snapshotIndex = 3; // After creating A, B, C
        System.out.println("Created objects A, B, C at log index 3");

        // ===== PHASE 3: Create snapshot/generation =====
        System.out.println("\nPHASE 3: Creating snapshot/generation");
        System.out.println("-------------------------------------");

        // Compact log to create snapshot and generation
        leader.compactLog(snapshotIndex);
        Thread.sleep(200);

        // Verify generation was created
        long genAfterSnapshot = leaderGenMgr.getCurrentGeneration();
        System.out.println("Generation after snapshot: " + genAfterSnapshot);

        // Verify snapshot boundary
        var boundary = leaderGenMgr.getSnapshotBoundary();
        assertNotNull(boundary, "Snapshot boundary should exist after compaction");
        System.out.println("Snapshot boundary: index=" + boundary.lastIncludedIndex() +
                ", term=" + boundary.lastIncludedTerm());

        // ===== PHASE 4: Post-snapshot writes =====
        System.out.println("\nPHASE 4: Post-snapshot writes");
        System.out.println("-----------------------------");

        // Create D
        ObjectMetadata objD = makeObject("D", 4000L);
        byte[] dataD = objectMapper.writeValueAsBytes(objD);
        leader.submit(LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, dataD));
        System.out.println("  Created: D");

        // Create E
        ObjectMetadata objE = makeObject("E", 5000L);
        byte[] dataE = objectMapper.writeValueAsBytes(objE);
        leader.submit(LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, dataE));
        System.out.println("  Created: E");

        // Delete B
        byte[] dataDeleteB = "B".getBytes();
        leader.submit(LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.DELETE_OBJECT, dataDeleteB));
        System.out.println("  Deleted: B");

        // Update A (change file size)
        ObjectMetadata objAUpdated = makeObject("A", 9999L);
        byte[] dataAUpdated = objectMapper.writeValueAsBytes(objAUpdated);
        leader.submit(LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.UPDATE_OBJECT, dataAUpdated));
        System.out.println("  Updated: A (size changed to 9999)");

        Thread.sleep(200);

        // Expected final state: A(updated), C, D, E (B was deleted)
        Map<String, Long> expectedFinalState = new HashMap<>();
        expectedFinalState.put("A", 9999L); // Updated size
        expectedFinalState.put("C", 1000L); // Unchanged
        expectedFinalState.put("D", 4000L); // Created
        expectedFinalState.put("E", 5000L); // Created

        // Record final state before shutdown
        Map<String, Long> stateBeforeShutdown = new HashMap<>();
        for (String name : leaderStore.listObjects()) {
            ObjectMetadata obj = leaderStore.getObject(name).orElseThrow();
            stateBeforeShutdown.put(name, obj.getFileSize());
        }
        System.out.println("State before shutdown: " + stateBeforeShutdown);

        // Verify state is as expected
        assertEquals(4, stateBeforeShutdown.size());
        assertTrue(stateBeforeShutdown.containsKey("A"));
        assertEquals(9999L, stateBeforeShutdown.get("A"));
        assertTrue(stateBeforeShutdown.containsKey("C"));
        assertTrue(stateBeforeShutdown.containsKey("D"));
        assertTrue(stateBeforeShutdown.containsKey("E"));
        assertFalse(stateBeforeShutdown.containsKey("B"), "B should be deleted");

        // Record exact values for verification after restart
        Set<String> expectedObjects = new HashSet<>(stateBeforeShutdown.keySet());
        Map<String, Long> expectedSizes = new HashMap<>(stateBeforeShutdown);

        // ===== PHASE 5: Shutdown =====
        System.out.println("\nPHASE 5: Shutting down leader");
        System.out.println("-------------------------------");
        leader.stop();

        // ===== PHASE 6: Start NEW MetadataServer (production path) =====
        System.out.println("\nPHASE 6: Starting NEW MetadataServer");
        System.out.println("------------------------------------");

        // Create completely fresh MetadataServer using the same directories
        // This uses the production startup path:
        // 1. Read CURRENT
        // 2. Load committed generation's metadata.json
        // 3. Get snapshot boundary from generation
        // 4. Replay WAL entries after lastIncludedIndex
        ClusterConfig recoveryConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);
        MetadataServer recoveredServer = new MetadataServer(
            leaderPort,
            leaderMetaFile,
            2, 6000, 2000,
            recoveryConfig,
            leaderRaftDir
        );

        // ===== PHASE 7: Verify EXACT state recovered =====
        System.out.println("\nPHASE 7: Verifying exact state recovery");
        System.out.println("----------------------------------------");

        MetadataStore recoveredStore = recoveredServer.getMetadataStore();
        GenerationManager recoveredGenMgr = recoveredServer.getGenerationManager();
        RaftNode recoveredRaft = recoveredServer.getRaftNode();

        // Verify generation is preserved
        long recoveredGen = recoveredGenMgr.getCurrentGeneration();
        System.out.println("Recovered generation: " + recoveredGen);
        assertEquals(genAfterSnapshot, recoveredGen, "Generation should be preserved");

        // Verify snapshot boundary
        var recoveredBoundary = recoveredGenMgr.getSnapshotBoundary();
        assertNotNull(recoveredBoundary, "Snapshot boundary should exist after recovery");
        System.out.println("Recovered boundary: index=" + recoveredBoundary.lastIncludedIndex() +
                ", term=" + recoveredBoundary.lastIncludedTerm());

        // Verify Raft state
        if (recoveredRaft != null) {
            System.out.println("Raft state: term=" + recoveredRaft.getCurrentTerm() +
                    ", lastLogIndex=" + recoveredRaft.getRaftLog().getLastLogIndex() +
                    ", commitIndex=" + recoveredRaft.getRaftLog().getCommitIndex() +
                    ", lastApplied=" + recoveredRaft.getRaftLog().getLastApplied());
        }

        // Verify EXACT objects exist
        Map<String, Long> stateAfterRecovery = new HashMap<>();
        for (String name : recoveredStore.listObjects()) {
            ObjectMetadata obj = recoveredStore.getObject(name).orElseThrow();
            stateAfterRecovery.put(name, obj.getFileSize());
        }
        System.out.println("State after recovery: " + stateAfterRecovery);

        // CRITICAL: Exact match
        assertEquals(expectedObjects, stateAfterRecovery.keySet(),
            "Objects should match exactly after recovery");

        // Verify each object's state
        for (String name : expectedObjects) {
            ObjectMetadata recovered = recoveredStore.getObject(name).orElseThrow();
            assertEquals(expectedSizes.get(name), recovered.getFileSize(),
                "Object " + name + " should have exact same file size");
        }

        // Specific assertions
        assertTrue(recoveredStore.objectExists("A"), "A should exist (updated)");
        assertTrue(recoveredStore.objectExists("C"), "C should exist (unchanged)");
        assertTrue(recoveredStore.objectExists("D"), "D should exist (created post-snapshot)");
        assertTrue(recoveredStore.objectExists("E"), "E should exist (created post-snapshot)");
        assertFalse(recoveredStore.objectExists("B"), "B should NOT exist (deleted post-snapshot)");

        // Verify A's updated size
        ObjectMetadata recoveredA = recoveredStore.getObject("A").orElseThrow();
        assertEquals(9999L, recoveredA.getFileSize(), "A should have updated size");

        System.out.println("\n========================================");
        System.out.println("E2E TEST: Production Server Restart Recovery - PASSED");
        System.out.println("========================================");
        System.out.println("\nVERIFIED:");
        System.out.println("  - Generation ID: " + recoveredGen + " (preserved)");
        System.out.println("  - Snapshot boundary: index=" + recoveredBoundary.lastIncludedIndex() +
                ", term=" + recoveredBoundary.lastIncludedTerm());
        System.out.println("  - Objects: " + stateAfterRecovery.keySet());
        System.out.println("  - State exactly matches before shutdown");

        recoveredServer.stop();
    }

    // ===== TEST: Verify generation is immutable during normal writes =====

    /**
     * Regression test: Normal metadata writes do NOT modify the immutable generation directory.
     *
     * This test proves that:
     * 1. Create generation 1 with objects A, B, C
     * 2. Make more writes after the snapshot (D, E, F)
     * 3. Verify generation 1's metadata.json is NOT modified and still has A, B, C
     *
     * The generation is immutable once committed via CURRENT.
     */
    @Test
    void testNormalWritesDoNotModifyImmutableGeneration() throws Exception {
        System.out.println("\n========================================");
        System.out.println("REGRESSION TEST: Generation Immutability");
        System.out.println("========================================\n");

        Path raftDir = tempDir.resolve("immutability-test");
        Path metaFile = tempDir.resolve("meta-immutable.json");
        Files.createDirectories(raftDir);

        // Setup
        GenerationManager genMgr = new GenerationManager(raftDir);
        genMgr.initializeFirstGeneration();

        MetadataStore store = new MetadataStore(metaFile, genMgr);

        // Create A, B, C in store (goes to flat file, NOT to generation)
        System.out.println("Creating objects: A, B, C");
        for (String name : List.of("A", "B", "C")) {
            store.createObject(makeObject(name, 1000L));
        }

        // Now create generation 2 with A, B, C
        // Generation 1 was already created as empty by initializeFirstGeneration()
        System.out.println("\nCreating generation 2 with A, B, C");
        long gen2 = genMgr.prepareNextGeneration(3);

        Map<String, ObjectMetadata> gen2Objects = new HashMap<>();
        for (String name : List.of("A", "B", "C")) {
            gen2Objects.put(name, store.getObject(name).orElseThrow());
        }

        byte[] snapshotData = objectMapper.writeValueAsBytes(gen2Objects);
        int checksum = computeChecksum(snapshotData);
        genMgr.writeMetadata(gen2, gen2Objects);
        genMgr.writeSnapshot(gen2, 3, 1, snapshotData, checksum);
        genMgr.writeManifest(gen2, 3, 1, checksum);
        genMgr.switchCurrent(gen2);

        // Get paths
        long gen1 = 1; // Generation 1 from initialization
        Path gen1Metadata = genMgr.getGenerationDir(gen1).resolve("metadata.json");
        Path gen2Metadata = genMgr.getGenerationDir(gen2).resolve("metadata.json");
        System.out.println("Generation 1 metadata at: " + gen1Metadata);
        System.out.println("Generation 2 metadata at: " + gen2Metadata);

        // Read generation 1's metadata - should be empty (from initialization)
        Map<String, ObjectMetadata> gen1State = genMgr.readMetadata(gen1);
        System.out.println("Generation 1 objects: " + gen1State.keySet() + " (should be empty)");
        assertEquals(0, gen1State.size(), "Generation 1 should be empty (from initialization)");

        // Read generation 2's metadata - should have A, B, C
        Map<String, ObjectMetadata> gen2State = genMgr.readMetadata(gen2);
        System.out.println("Generation 2 objects: " + gen2State.keySet() + " (should be A, B, C)");
        assertEquals(3, gen2State.size(), "Generation 2 should have A, B, C");

        // Get timestamps
        long gen1MetadataModTime = Files.getLastModifiedTime(gen1Metadata).toMillis();
        long gen2MetadataModTime = Files.getLastModifiedTime(gen2Metadata).toMillis();
        System.out.println("\nGeneration 1 metadata last modified: " + gen1MetadataModTime);
        System.out.println("Generation 2 metadata last modified: " + gen2MetadataModTime);

        // Wait a bit to ensure timestamp would change if file is modified
        Thread.sleep(50);

        // Now do normal writes (these go to flat file / WAL, NOT to generation)
        System.out.println("\nPerforming normal writes: D, E, F");
        for (String name : List.of("D", "E", "F")) {
            store.createObject(makeObject(name, 2000L));
        }

        // Verify state has all 6 objects in the store
        assertEquals(6, store.listObjects().size());

        // Check that generation files were NOT modified
        long gen1MetadataModTimeAfter = Files.getLastModifiedTime(gen1Metadata).toMillis();
        long gen2MetadataModTimeAfter = Files.getLastModifiedTime(gen2Metadata).toMillis();
        System.out.println("\nAfter writes:");
        System.out.println("Generation 1 metadata last modified: " + gen1MetadataModTimeAfter);
        System.out.println("Generation 2 metadata last modified: " + gen2MetadataModTimeAfter);

        assertEquals(gen1MetadataModTime, gen1MetadataModTimeAfter,
            "Generation 1 metadata.json should NOT be modified by normal writes");
        assertEquals(gen2MetadataModTime, gen2MetadataModTimeAfter,
            "Generation 2 metadata.json should NOT be modified by normal writes");

        // Verify generations still have original state
        Map<String, ObjectMetadata> gen1StateAfter = genMgr.readMetadata(gen1);
        Map<String, ObjectMetadata> gen2StateAfter = genMgr.readMetadata(gen2);

        assertEquals(0, gen1StateAfter.size(), "Generation 1 should still be empty");
        assertEquals(3, gen2StateAfter.size(), "Generation 2 should still have A, B, C");
        assertTrue(gen2StateAfter.containsKey("A"));
        assertTrue(gen2StateAfter.containsKey("B"));
        assertTrue(gen2StateAfter.containsKey("C"));
        assertFalse(gen2StateAfter.containsKey("D"), "Generation 2 should NOT have D");
        assertFalse(gen2StateAfter.containsKey("E"), "Generation 2 should NOT have E");
        assertFalse(gen2StateAfter.containsKey("F"), "Generation 2 should NOT have F");

        System.out.println("\nGeneration immutability VERIFIED:");
        System.out.println("  - Generation 1 unchanged (empty)");
        System.out.println("  - Generation 2 unchanged (A, B, C only)");
        System.out.println("  - Normal writes (D, E, F) did NOT modify either generation");

        System.out.println("\n========================================");
        System.out.println("REGRESSION TEST: Generation Immutability - PASSED");
        System.out.println("========================================\n");
    }

    private static int computeChecksum(byte[] data) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }
}
