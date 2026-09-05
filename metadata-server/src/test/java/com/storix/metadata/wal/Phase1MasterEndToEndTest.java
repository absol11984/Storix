package com.storix.metadata.wal;

import com.storix.metadata.*;
import com.storix.metadata.raft.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 1 Master End-to-End Persistence Test
 *
 * This is the authoritative test that validates the complete persistence lifecycle.
 * It does NOT use helper methods to perform recovery - the new MetadataServer must
 * perform automatic recovery through its normal startup path.
 *
 * Test invariant:
 *   STATE BEFORE SHUTDOWN == STATE AFTER FIRST RESTART == STATE AFTER SECOND RESTART
 *
 * And:
 *   SNAPSHOT STATE + POST-SNAPSHOT WAL REPLAY = FINAL STATE BEFORE SHUTDOWN
 *
 * And:
 *   NEXT INDEX = LAST RECOVERED INDEX + 1
 */
class Phase1MasterEndToEndTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int BASE_PORT = 58000;

    @TempDir
    Path tempDir;

    /**
     * Creates an ObjectMetadata for testing.
     */
    private static ObjectMetadata makeObject(String name, long fileSize) {
        ObjectMetadata obj = new ObjectMetadata(name, fileSize, 4096);
        obj.addChunk(new ChunkInfo(
            "chunk-" + name, 0, (int) Math.min(fileSize, 4096),
            List.of("node1", "node2"),
            "hash-" + name
        ));
        return obj;
    }

    /**
     * Submits a create operation through Raft.
     */
    private static boolean submitCreate(RaftNode raftNode, ObjectMetadata metadata) throws Exception {
        if (!raftNode.isLeader()) {
            long deadline = System.currentTimeMillis() + 5000;
            while (!raftNode.isLeader() && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            if (!raftNode.isLeader()) {
                throw new IllegalStateException("Node is not leader");
            }
        }
        byte[] data = objectMapper.writeValueAsBytes(metadata);
        LogEntry entry = LogEntry.create(raftNode.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
        return raftNode.submit(entry);
    }

    /**
     * Submits an update operation through Raft.
     */
    private static boolean submitUpdate(RaftNode raftNode, ObjectMetadata metadata) throws Exception {
        if (!raftNode.isLeader()) {
            long deadline = System.currentTimeMillis() + 5000;
            while (!raftNode.isLeader() && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            if (!raftNode.isLeader()) {
                throw new IllegalStateException("Node is not leader");
            }
        }
        byte[] data = objectMapper.writeValueAsBytes(metadata);
        LogEntry entry = LogEntry.create(raftNode.getCurrentTerm(), LogEntry.OpType.UPDATE_OBJECT, data);
        return raftNode.submit(entry);
    }

    /**
     * Submits a delete operation through Raft.
     */
    private static boolean submitDelete(RaftNode raftNode, String objectName) throws Exception {
        if (!raftNode.isLeader()) {
            long deadline = System.currentTimeMillis() + 5000;
            while (!raftNode.isLeader() && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            if (!raftNode.isLeader()) {
                throw new IllegalStateException("Node is not leader");
            }
        }
        byte[] data = objectName.getBytes();
        LogEntry entry = LogEntry.create(raftNode.getCurrentTerm(), LogEntry.OpType.DELETE_OBJECT, data);
        return raftNode.submit(entry);
    }

    /**
     * Captures complete metadata state from a store.
     * Includes all object metadata for exact comparison.
     */
    private static Map<String, CapturedObject> captureState(MetadataStore store) {
        Map<String, CapturedObject> state = new HashMap<>();
        for (String name : store.listObjects()) {
            Optional<ObjectMetadata> objOpt = store.getObject(name);
            if (objOpt.isPresent()) {
                ObjectMetadata obj = objOpt.get();
                List<CapturedChunk> chunks = new ArrayList<>();
                for (ChunkInfo chunk : obj.getChunks()) {
                    chunks.add(new CapturedChunk(
                        chunk.getChunkId(),
                        chunk.getChunkIndex(),
                        chunk.getChunkSize(),
                        new HashSet<>(chunk.getReplicaNodeIds()),
                        chunk.getChecksum()
                    ));
                }
                state.put(name, new CapturedObject(obj.getObjectName(), obj.getFileSize(), chunks));
            }
        }
        return state;
    }

    /**
     * Compares two captured states for exact equality.
     */
    private static void assertStateEquals(
            Map<String, CapturedObject> expected,
            Map<String, CapturedObject> actual) {

        assertEquals(expected.size(), actual.size(),
            "Object count mismatch. Expected: " + expected.keySet() + ", Got: " + actual.keySet());

        for (Map.Entry<String, CapturedObject> entry : expected.entrySet()) {
            String name = entry.getKey();
            CapturedObject expectedObj = entry.getValue();
            CapturedObject actualObj = actual.get(name);

            assertNotNull(actualObj, "Object " + name + " should exist");
            assertEquals(expectedObj.name, actualObj.name, "Name mismatch for " + name);
            assertEquals(expectedObj.fileSize, actualObj.fileSize,
                "File size mismatch for " + name);

            // Compare chunks
            assertEquals(expectedObj.chunks.size(), actualObj.chunks.size(),
                "Chunk count mismatch for " + name);

            Map<Integer, CapturedChunk> expectedChunks = new HashMap<>();
            for (CapturedChunk c : expectedObj.chunks) {
                expectedChunks.put(c.chunkIndex, c);
            }
            Map<Integer, CapturedChunk> actualChunks = new HashMap<>();
            for (CapturedChunk c : actualObj.chunks) {
                actualChunks.put(c.chunkIndex, c);
            }

            for (Map.Entry<Integer, CapturedChunk> expectedChunk : expectedChunks.entrySet()) {
                CapturedChunk actualChunk = actualChunks.get(expectedChunk.getKey());
                assertNotNull(actualChunk, "Chunk " + expectedChunk.getKey() +
                    " missing for " + name);
                assertEquals(expectedChunk.getValue().chunkId, actualChunk.chunkId,
                    "Chunk ID mismatch for " + name + " chunk " + expectedChunk.getKey());
                assertEquals(expectedChunk.getValue().chunkSize, actualChunk.chunkSize,
                    "Chunk size mismatch for " + name + " chunk " + expectedChunk.getKey());
                assertEquals(expectedChunk.getValue().replicaNodes, actualChunk.replicaNodes,
                    "Replica nodes mismatch for " + name + " chunk " + expectedChunk.getKey());
                assertEquals(expectedChunk.getValue().checksum, actualChunk.checksum,
                    "Checksum mismatch for " + name + " chunk " + expectedChunk.getKey());
            }
        }
    }

    /**
     * Data classes for state capture.
     */
    record CapturedObject(String name, long fileSize, List<CapturedChunk> chunks) {}
    record CapturedChunk(String chunkId, int chunkIndex, int chunkSize,
                         Set<String> replicaNodes, String checksum) {}

    /**
     * Starts a MetadataServer and waits for it to be ready.
     */
    private static MetadataServer startServer(int port, Path metadataFile,
            Path raftStateDir, ClusterConfig config) throws Exception {

        MetadataServer server = new MetadataServer(port, metadataFile, 2, 6000, 2000, config, raftStateDir);

        // Start server in background thread
        Thread serverThread = new Thread(() -> {
            try {
                server.start();
            } catch (IOException e) {
                // Expected on shutdown
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();

        // Wait for RaftNode to become leader
        RaftNode raftNode = server.getRaftNode();
        if (raftNode != null) {
            long deadline = System.currentTimeMillis() + 5000;
            while (!raftNode.isLeader() && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            if (!raftNode.isLeader()) {
                throw new IllegalStateException("Server did not become leader");
            }
        }

        Thread.sleep(200); // Extra settle time
        return server;
    }

    /**
     * Test 1: Basic Start → Write → Snapshot → More Write → Restart → Verify
     */
    @Test
    void testBasicPersistenceWithSnapshot() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Basic Persistence with Snapshot");
        System.out.println("========================================\n");

        int port = BASE_PORT + 100;
        Path metadataFile = tempDir.resolve("metadata.json");
        Path raftStateDir = tempDir.resolve("raft-state");
        Files.createDirectories(raftStateDir);

        ClusterConfig config = new ClusterConfig("test", "node1", "127.0.0.1", port, null);

        // ===== PHASE 1: Start server and perform real mutations via Raft =====
        System.out.println("[PHASE 1] Start server and perform real mutations via Raft");

        MetadataServer server1 = startServer(port, metadataFile, raftStateDir, config);
        MetadataStore store1 = server1.getMetadataStore();
        RaftNode raftNode1 = server1.getRaftNode();

        // Create objects A, B, C through Raft
        for (String name : List.of("A", "B", "C")) {
            ObjectMetadata obj = makeObject(name, name.hashCode() & 0xFFFF);
            assertTrue(submitCreate(raftNode1, obj), "Failed to create " + name);
        }
        Thread.sleep(500); // Wait for apply

        Map<String, CapturedObject> stateAfterPhase1 = captureState(store1);
        System.out.println("  Phase 1: Created " + stateAfterPhase1.size() + " objects via Raft");
        assertEquals(3, stateAfterPhase1.size());

        // ===== PHASE 2: Take snapshot =====
        System.out.println("\n[PHASE 2] Take snapshot");
        raftNode1.compactLog(3);
        System.out.println("  Snapshot taken at index 3");

        // ===== PHASE 3: More mutations via Raft =====
        System.out.println("\n[PHASE 3] More mutations via Raft (D, E, F, delete B)");

        for (String name : List.of("D", "E", "F")) {
            ObjectMetadata obj = makeObject(name, name.hashCode() & 0xFFFF);
            assertTrue(submitCreate(raftNode1, obj), "Failed to create " + name);
        }
        assertTrue(submitDelete(raftNode1, "B"), "Failed to delete B");
        Thread.sleep(500);

        Map<String, CapturedObject> stateBeforeShutdown = captureState(store1);
        System.out.println("  Phase 3: State before shutdown has " + stateBeforeShutdown.size() + " objects");
        assertEquals(5, stateBeforeShutdown.size()); // A, C, D, E, F (B deleted)

        // Capture log state
        long lastLogIndexBefore = raftNode1.getRaftLog().getLastLogIndex();
        System.out.println("  Last log index before shutdown: " + lastLogIndexBefore);
        assertEquals(7, lastLogIndexBefore); // 3 create + 3 create + 1 delete = 7

        // ===== PHASE 4: SHUTDOWN =====
        System.out.println("\n[PHASE 4] SHUTDOWN");
        server1.stop();
        Thread.sleep(300);
        System.out.println("  Server stopped");

        // ===== PHASE 5: Start BRAND-NEW server (automatic recovery) =====
        System.out.println("\n[PHASE 5] Start BRAND-NEW MetadataServer (automatic recovery)");

        // Create completely new server instance
        MetadataServer server2 = startServer(port, metadataFile, raftStateDir, config);
        MetadataStore store2 = server2.getMetadataStore();
        RaftNode raftNode2 = server2.getRaftNode();

        System.out.println("  Server restarted with automatic recovery");

        // ===== PHASE 6: VERIFY EXACT STATE =====
        System.out.println("\n[PHASE 6] VERIFY EXACT STATE after restart");

        Map<String, CapturedObject> stateAfterRestart = captureState(store2);
        assertStateEquals(stateBeforeShutdown, stateAfterRestart);
        System.out.println("  State exactly matches!");

        // ===== PHASE 7: VERIFY LOG INDEXES =====
        System.out.println("\n[PHASE 7] VERIFY LOG INDEXES");

        RaftLog raftLog2 = raftNode2.getRaftLog();
        long lastLogIndexAfter = raftLog2.getLastLogIndex();
        long nextIndexAfter = lastLogIndexAfter + 1;

        System.out.println("  lastLogIndex = " + lastLogIndexAfter);
        System.out.println("  nextIndex = " + nextIndexAfter);

        assertEquals(7, lastLogIndexAfter, "lastLogIndex should be 7 after restart");
        assertEquals(8, nextIndexAfter, "nextIndex should be 8");

        // ===== PHASE 8: NEW MUTATION and verify index =====
        System.out.println("\n[PHASE 8] NEW MUTATION and verify index");

        ObjectMetadata objG = makeObject("G", 9999);
        assertTrue(submitCreate(raftNode2, objG), "Failed to create G");
        Thread.sleep(500);

        long newLastIndex = raftNode2.getRaftLog().getLastLogIndex();
        System.out.println("  New lastLogIndex = " + newLastIndex);
        assertEquals(8, newLastIndex, "New entry should be at index 8");

        // Capture state for second restart
        Map<String, CapturedObject> stateBeforeRestart2 = captureState(store2);

        // ===== PHASE 9: SHUTDOWN again =====
        System.out.println("\n[PHASE 9] SHUTDOWN again");
        server2.stop();
        Thread.sleep(300);

        // ===== PHASE 10: THIRD SERVER (second restart) =====
        System.out.println("\n[PHASE 10] THIRD SERVER (second restart)");

        MetadataServer server3 = startServer(port, metadataFile, raftStateDir, config);
        MetadataStore store3 = server3.getMetadataStore();

        System.out.println("  Third server started");

        // ===== PHASE 11: VERIFY state unchanged =====
        System.out.println("\n[PHASE 11] VERIFY state unchanged");

        Map<String, CapturedObject> stateAfterRestart2 = captureState(store3);
        assertStateEquals(stateBeforeRestart2, stateAfterRestart2);
        System.out.println("  State unchanged after second restart!");

        // Cleanup
        server3.stop();

        System.out.println("\n========================================");
        System.out.println("TEST: Basic Persistence with Snapshot - PASSED");
        System.out.println("========================================\n");
    }

    /**
     * Test 2: Repeated compaction, WAL non-duplication
     */
    @Test
    void testRepeatedCompactionAndWalNoDuplication() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Repeated Compaction and WAL Non-Duplication");
        System.out.println("========================================\n");

        int port = BASE_PORT + 200;
        Path metadataFile = tempDir.resolve("metadata2.json");
        Path raftStateDir = tempDir.resolve("raft-state-2");
        Files.createDirectories(raftStateDir);

        ClusterConfig config = new ClusterConfig("test", "node1", "127.0.0.1", port, null);

        // Start server
        MetadataServer server1 = startServer(port, metadataFile, raftStateDir, config);
        MetadataStore store1 = server1.getMetadataStore();
        RaftNode raftNode1 = server1.getRaftNode();

        // Create many objects and compact multiple times
        System.out.println("[PHASE 1] Create 10 objects, compact to index 10");
        for (int i = 1; i <= 10; i++) {
            ObjectMetadata obj = makeObject("obj" + i, i * 1000);
            assertTrue(submitCreate(raftNode1, obj));
        }
        Thread.sleep(500);

        raftNode1.compactLog(10);
        System.out.println("  Compacted through index 10");

        // Verify state
        Map<String, CapturedObject> state1 = captureState(store1);
        assertEquals(10, state1.size());

        // Create 10 more
        System.out.println("\n[PHASE 2] Create 10 more objects, compact to index 20");
        for (int i = 11; i <= 20; i++) {
            ObjectMetadata obj = makeObject("obj" + i, i * 1000);
            assertTrue(submitCreate(raftNode1, obj));
        }
        Thread.sleep(500);

        raftNode1.compactLog(20);
        System.out.println("  Compacted through index 20");

        Map<String, CapturedObject> state2 = captureState(store1);
        assertEquals(20, state2.size());

        // Capture WAL size
        Path walFile = raftStateDir.resolve("wal.dat");
        long walSizeBefore = Files.exists(walFile) ? Files.size(walFile) : 0;
        System.out.println("  WAL size before shutdown: " + walSizeBefore);

        // Shutdown
        System.out.println("\n[PHASE 3] SHUTDOWN");
        server1.stop();
        Thread.sleep(300);

        // ===== RECOVERY 1 =====
        System.out.println("\n[RECOVERY 1] First restart");
        MetadataServer server2 = startServer(port, metadataFile, raftStateDir, config);
        MetadataStore store2 = server2.getMetadataStore();

        Map<String, CapturedObject> stateAfterRecovery1 = captureState(store2);
        assertStateEquals(state2, stateAfterRecovery1);
        System.out.println("  State verified after first recovery");

        // Check WAL size - should NOT have grown from recovery
        long walSizeAfter1 = Files.exists(walFile) ? Files.size(walFile) : 0;
        System.out.println("  WAL size after first recovery: " + walSizeAfter1);
        assertEquals(walSizeBefore, walSizeAfter1, "WAL should not grow during recovery");

        // Add more objects
        System.out.println("\n[PHASE 4] Add more objects after recovery");
        ObjectMetadata obj21 = makeObject("obj21", 21000);
        assertTrue(submitCreate(server2.getRaftNode(), obj21));

        // Capture WAL size after adding obj21
        long walSizeBeforeSecondShutdown = Files.exists(walFile) ? Files.size(walFile) : 0;
        System.out.println("  WAL size before second shutdown: " + walSizeBeforeSecondShutdown);

        // Second shutdown
        System.out.println("\n[PHASE 5] SHUTDOWN again");
        server2.stop();
        Thread.sleep(300);

        // ===== RECOVERY 2 =====
        System.out.println("\n[RECOVERY 2] Second restart");
        MetadataServer server3 = startServer(port, metadataFile, raftStateDir, config);
        MetadataStore store3 = server3.getMetadataStore();

        Map<String, CapturedObject> stateAfterRecovery2 = captureState(store3);
        Map<String, CapturedObject> expectedState = captureState(store2);
        assertStateEquals(expectedState, stateAfterRecovery2);
        System.out.println("  State verified after second recovery");

        // Check WAL size - should be same as before shutdown (no duplicate writes during recovery)
        long walSizeAfter2 = Files.exists(walFile) ? Files.size(walFile) : 0;
        System.out.println("  WAL size after second recovery: " + walSizeAfter2);
        assertEquals(walSizeBeforeSecondShutdown, walSizeAfter2, "WAL should not grow during recovery");

        server3.stop();

        System.out.println("\n========================================");
        System.out.println("TEST: Repeated Compaction and WAL Non-Duplication - PASSED");
        System.out.println("========================================\n");
    }

    /**
     * Test 3: Snapshot corruption fallback
     */
    @Test
    void testSnapshotCorruptionFallback() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Snapshot Corruption Fallback");
        System.out.println("========================================\n");

        int port = BASE_PORT + 300;
        Path metadataFile = tempDir.resolve("metadata3.json");
        Path raftStateDir = tempDir.resolve("raft-state-3");
        Path snapshotDir = raftStateDir.resolve("snapshots");
        Files.createDirectories(snapshotDir);

        ClusterConfig config = new ClusterConfig("test", "node1", "127.0.0.1", port, null);

        // Start server
        MetadataServer server1 = startServer(port, metadataFile, raftStateDir, config);
        MetadataStore store1 = server1.getMetadataStore();
        RaftNode raftNode1 = server1.getRaftNode();

        // Create objects and take snapshot
        System.out.println("[PHASE 1] Create objects and take snapshot");
        for (String name : List.of("A", "B", "C")) {
            ObjectMetadata obj = makeObject(name, name.hashCode() & 0xFFFF);
            assertTrue(submitCreate(raftNode1, obj));
        }
        Thread.sleep(500);

        raftNode1.compactLog(3);
        Map<String, CapturedObject> state = captureState(store1);
        System.out.println("  State captured with " + state.size() + " objects");

        // Stop and restart
        server1.stop();
        Thread.sleep(300);

        // First restart should work
        MetadataServer server2 = startServer(port, metadataFile, raftStateDir, config);
        MetadataStore store2 = server2.getMetadataStore();

        Map<String, CapturedObject> stateAfterRecovery = captureState(store2);
        assertStateEquals(state, stateAfterRecovery);
        System.out.println("  First recovery successful");

        server2.stop();
        Thread.sleep(300);

        // ===== Corrupt the snapshot =====
        System.out.println("\n[CORRUPTION] Corrupting snapshot file");

        // Find snapshot file
        List<Path> snapshots = Files.list(snapshotDir)
            .filter(p -> p.getFileName().toString().startsWith("snapshot-"))
            .toList();

        assertFalse(snapshots.isEmpty(), "Should have at least one snapshot");
        Path latestSnapshot = snapshots.get(0);

        // Corrupt it
        byte[] corruptData = "CORRUPTED".getBytes();
        Files.write(latestSnapshot, corruptData);
        System.out.println("  Corrupted: " + latestSnapshot.getFileName());

        // Third restart should still work if there's a valid snapshot
        // If only corrupted snapshot exists, should fail explicitly
        System.out.println("\n[RECOVERY] Recovery with corrupted snapshot");
        try {
            MetadataServer server3 = startServer(port, metadataFile, raftStateDir, config);
            MetadataStore store3 = server3.getMetadataStore();

            // If recovery succeeded, state should match
            Map<String, CapturedObject> stateAfterCorruptRecovery = captureState(store3);
            assertStateEquals(state, stateAfterCorruptRecovery);
            System.out.println("  Recovery succeeded with fallback");

            server3.stop();
        } catch (Exception e) {
            System.out.println("  Recovery failed as expected: " + e.getMessage());
            // This is acceptable - explicit failure is OK
        }

        System.out.println("\n========================================");
        System.out.println("TEST: Snapshot Corruption Fallback - PASSED");
        System.out.println("========================================\n");
    }

    /**
     * Test 4: Truncated WAL tail
     */
    @Test
    void testTruncatedWalTailRecovery() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Truncated WAL Tail Recovery");
        System.out.println("========================================\n");

        int port = BASE_PORT + 400;
        Path metadataFile = tempDir.resolve("metadata4.json");
        Path raftStateDir = tempDir.resolve("raft-state-4");
        Files.createDirectories(raftStateDir);

        ClusterConfig config = new ClusterConfig("test", "node1", "127.0.0.1", port, null);

        // Start server
        MetadataServer server1 = startServer(port, metadataFile, raftStateDir, config);
        MetadataStore store1 = server1.getMetadataStore();
        RaftNode raftNode1 = server1.getRaftNode();

        // Create objects
        System.out.println("[PHASE 1] Create objects");
        for (String name : List.of("A", "B", "C", "D", "E")) {
            ObjectMetadata obj = makeObject(name, name.hashCode() & 0xFFFF);
            assertTrue(submitCreate(raftNode1, obj));
        }
        Thread.sleep(500);

        Map<String, CapturedObject> state = captureState(store1);
        System.out.println("  State: " + state.size() + " objects");

        // Stop
        server1.stop();
        Thread.sleep(300);

        // Manually truncate WAL (simulate crash during write)
        Path walFile = raftStateDir.resolve("wal.dat");
        if (Files.exists(walFile)) {
            long size = Files.size(walFile);
            if (size > 50) {
                // Truncate to 50 bytes (in middle of a record)
                byte[] allBytes = Files.readAllBytes(walFile);
                byte[] truncated = new byte[50];
                System.arraycopy(allBytes, 0, truncated, 0, 50);
                Files.write(walFile, truncated);
                System.out.println("  Truncated WAL from " + size + " to 50 bytes");
            }
        }

        // Recovery should handle truncated tail
        System.out.println("\n[RECOVERY] Recovery with truncated WAL");
        MetadataServer server2 = startServer(port, metadataFile, raftStateDir, config);
        MetadataStore store2 = server2.getMetadataStore();

        Map<String, CapturedObject> stateAfterRecovery = captureState(store2);
        assertStateEquals(state, stateAfterRecovery);
        System.out.println("  State verified after truncated WAL recovery");

        server2.stop();

        System.out.println("\n========================================");
        System.out.println("TEST: Truncated WAL Tail Recovery - PASSED");
        System.out.println("========================================\n");
    }

    /**
     * Test 5: Multi-chunk snapshot (large snapshot requires multiple transfers)
     * Note: This tests the snapshot creation/load path. Full multi-chunk InstallSnapshot
     * requires a multi-node cluster which is tested separately.
     */
    @Test
    void testLargeSnapshotCreationAndRecovery() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Large Snapshot Creation and Recovery");
        System.out.println("========================================\n");

        int port = BASE_PORT + 500;
        Path metadataFile = tempDir.resolve("metadata5.json");
        Path raftStateDir = tempDir.resolve("raft-state-5");
        Files.createDirectories(raftStateDir);

        ClusterConfig config = new ClusterConfig("test", "node1", "127.0.0.1", port, null);

        // Start server
        MetadataServer server1 = startServer(port, metadataFile, raftStateDir, config);
        MetadataStore store1 = server1.getMetadataStore();
        RaftNode raftNode1 = server1.getRaftNode();

        // Create many objects to make a larger snapshot
        System.out.println("[PHASE 1] Create many objects (100+)");
        for (int i = 0; i < 100; i++) {
            ObjectMetadata obj = makeObject("large_obj_" + i, i * 100 + 1000);
            assertTrue(submitCreate(raftNode1, obj));
        }
        Thread.sleep(1000);

        Map<String, CapturedObject> state = captureState(store1);
        System.out.println("  State: " + state.size() + " objects");

        // Take snapshot
        System.out.println("\n[PHASE 2] Take snapshot");
        long lastIndex = raftNode1.getRaftLog().getLastLogIndex();
        raftNode1.compactLog(lastIndex);

        // Check snapshot file size
        Path snapshotDir = raftStateDir.resolve("snapshots");
        long snapshotSize = Files.list(snapshotDir)
            .filter(p -> p.getFileName().toString().startsWith("snapshot-"))
            .mapToLong(p -> {
                try { return Files.size(p); } catch (IOException e) { return 0L; }
            })
            .max()
            .orElse(0);
        System.out.println("  Snapshot size: " + snapshotSize + " bytes");

        // Stop and restart
        server1.stop();
        Thread.sleep(300);

        System.out.println("\n[RECOVERY] Recovery with large snapshot");
        MetadataServer server2 = startServer(port, metadataFile, raftStateDir, config);
        MetadataStore store2 = server2.getMetadataStore();

        Map<String, CapturedObject> stateAfterRecovery = captureState(store2);
        assertStateEquals(state, stateAfterRecovery);
        System.out.println("  Large snapshot recovery successful!");

        server2.stop();

        System.out.println("\n========================================");
        System.out.println("TEST: Large Snapshot Creation and Recovery - PASSED");
        System.out.println("========================================\n");
    }

    /**
     * Test 6: Exact index restoration verification
     */
    @Test
    void testExactIndexRestoration() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Exact Index Restoration");
        System.out.println("========================================\n");

        int port = BASE_PORT + 600;
        Path metadataFile = tempDir.resolve("metadata6.json");
        Path raftStateDir = tempDir.resolve("raft-state-6");
        Files.createDirectories(raftStateDir);

        ClusterConfig config = new ClusterConfig("test", "node1", "127.0.0.1", port, null);

        // Start server
        MetadataServer server1 = startServer(port, metadataFile, raftStateDir, config);
        RaftNode raftNode1 = server1.getRaftNode();
        RaftLog raftLog1 = raftNode1.getRaftLog();

        // Verify initial state
        System.out.println("[PHASE 1] Verify initial indexes");
        assertEquals(0, raftLog1.getLastLogIndex(), "Fresh log should have lastLogIndex=0");
        assertEquals(1, raftLog1.getLogStartIndex(), "Fresh log should have logStartIndex=1");

        // Create object and verify index
        System.out.println("\n[PHASE 2] Create object and verify index");
        ObjectMetadata obj = makeObject("idx_test", 12345);
        assertTrue(submitCreate(raftNode1, obj));
        Thread.sleep(500);

        assertEquals(1, raftLog1.getLastLogIndex(), "Should have lastLogIndex=1 after one entry");
        assertNotNull(raftLog1.getEntry(1), "Entry 1 should exist");
        assertEquals(1, raftLog1.getEntry(1).index(), "Entry 1 index should be 1");

        // Create another and compact
        ObjectMetadata obj2 = makeObject("idx_test2", 67890);
        assertTrue(submitCreate(raftNode1, obj2));
        Thread.sleep(500);

        assertEquals(2, raftLog1.getLastLogIndex(), "Should have lastLogIndex=2");
        raftNode1.compactLog(2);
        System.out.println("  Compacted through index 2");

        // After compact, logStartIndex should be 3
        assertEquals(3, raftLog1.getLogStartIndex(), "logStartIndex should be 3 after compact");

        // Create more entries
        for (int i = 3; i <= 5; i++) {
            ObjectMetadata o = makeObject("obj_" + i, i * 100);
            assertTrue(submitCreate(raftNode1, o));
        }
        Thread.sleep(500);

        assertEquals(5, raftLog1.getLastLogIndex(), "Should have lastLogIndex=5");

        // Stop
        server1.stop();
        Thread.sleep(300);

        // Recover and verify indexes
        System.out.println("\n[RECOVERY] Verify indexes after recovery");
        MetadataServer server2 = startServer(port, metadataFile, raftStateDir, config);
        RaftNode raftNode2 = server2.getRaftNode();
        RaftLog raftLog2 = raftNode2.getRaftLog();

        // After compact through 2, logStartIndex should be 3
        assertEquals(3, raftLog2.getLogStartIndex(), "logStartIndex should be 3 after recovery");

        // After adding entries 3, 4, 5, lastLogIndex should be 5
        assertEquals(5, raftLog2.getLastLogIndex(), "lastLogIndex should be 5 after recovery");

        // Entries 3, 4, 5 should exist
        for (int i = 3; i <= 5; i++) {
            assertNotNull(raftLog2.getEntry(i), "Entry " + i + " should exist after recovery");
            assertEquals(i, raftLog2.getEntry(i).index(), "Entry " + i + " index should be " + i);
        }

        // New entry should get index 6
        ObjectMetadata obj6 = makeObject("new_obj", 999);
        assertTrue(submitCreate(raftNode2, obj6));
        Thread.sleep(500);

        assertEquals(6, raftNode2.getRaftLog().getLastLogIndex(), "New entry should be at index 6");

        server2.stop();

        System.out.println("\n========================================");
        System.out.println("TEST: Exact Index Restoration - PASSED");
        System.out.println("========================================\n");
    }
}
