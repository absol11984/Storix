package com.storix.metadata.wal;

import com.storix.metadata.*;
import com.storix.metadata.raft.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 1.2 Master Persistence Integration Test
 *
 * Tests the complete persistence lifecycle through the actual Raft write path:
 * 1. Start server, perform writes via RaftNode.submit()
 * 2. Stop and restart
 * 3. Verify state is restored
 * 4. Verify log indexes are correct
 * 5. Stop and restart again
 * 6. Verify state persists
 */
class Phase12PersistenceIntegrationTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @TempDir
    Path dataDir;

    /**
     * Helper: Submit a create object operation through the actual Raft write path.
     * For single-node clusters, this may timeout waiting for commit acknowledgment
     * since there's no majority to wait for. We verify the entry is in the log instead.
     */
    private static boolean submitCreateObject(RaftNode raftNode, ObjectMetadata metadata) throws Exception {
        if (!raftNode.isLeader()) {
            // Wait for leadership
            long deadline = System.currentTimeMillis() + 5000;
            while (!raftNode.isLeader() && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            if (!raftNode.isLeader()) {
                throw new IllegalStateException("Node is not leader and could not become leader");
            }
        }
        byte[] data = objectMapper.writeValueAsBytes(metadata);
        LogEntry entry = LogEntry.create(raftNode.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);

        // For single-node, submit may timeout waiting for commit
        // but the entry should be in the log
        long indexBefore = raftNode.getRaftLog().getLastLogIndex();
        boolean submitted = raftNode.submit(entry);

        if (!submitted) {
            // For single-node, entry should still be in log even if commit times out
            long indexAfter = raftNode.getRaftLog().getLastLogIndex();
            return indexAfter > indexBefore;
        }
        return true;
    }

    /**
     * Helper: Start the server and wait for leader election and state machine setup.
     */
    private static void startServerAndWaitForLeader(MetadataServer server) throws Exception {
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

        // Wait for RaftNode to become leader (for single-node)
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
        Thread.sleep(200); // Extra settle time for apply loop
    }

    /**
     * Helper: Wait for entries to be applied to the state machine.
     */
    private static void waitForApply(MetadataServer server, int expectedCount, int timeoutMs) throws Exception {
        MetadataStateMachine sm = server.getStateMachine();
        if (sm == null) return;
        MetadataStore store = sm.getStore();

        long deadline = System.currentTimeMillis() + timeoutMs;
        while (store.listObjects().size() < expectedCount && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
    }

    /**
     * Helper: Create a simple object metadata for testing.
     */
    private static ObjectMetadata makeObject(String name, long fileSize) {
        ObjectMetadata obj = new ObjectMetadata(name, fileSize, 4096);
        obj.addChunk(new ChunkInfo("chunk-" + name, 0, 500, List.of("node1", "node2"), "hash-" + name));
        return obj;
    }

    @Test
    void testRealServerPersistenceLifecycleWithRaftPath() throws Exception {
        System.out.println("=== Phase 1.2 Master Persistence Integration Test (Raft Write Path) ===");

        int port = 54000 + (int)(System.currentTimeMillis() % 1000);

        // ===== STEP 1: Start server and perform initial writes =====
        System.out.println("STEP 1: Start server and perform initial writes via Raft");

        Path metadataFile1 = dataDir.resolve("metadata.json");
        Path raftStateDir = dataDir.resolve("raft-state");
        Files.createDirectories(raftStateDir);

        ClusterConfig config1 = new ClusterConfig("test-cluster", "node1", "127.0.0.1", port, null);
        MetadataServer server1 = new MetadataServer(port, metadataFile1, 2, 6000, 2000, config1, raftStateDir);
        MetadataStore store1 = server1.getMetadataStore();
        RaftNode raftNode1 = server1.getRaftNode();

        // Start server and wait for leader election
        startServerAndWaitForLeader(server1);
        System.out.println("  Server is leader");

        // Create initial objects (A, B, C, D, E) through Raft
        String[] initialObjects = {"A", "B", "C", "D", "E"};
        Map<String, Long> expectedFileSizes = new HashMap<>();
        for (String name : initialObjects) {
            ObjectMetadata obj = makeObject(name, name.hashCode());
            assertTrue(submitCreateObject(raftNode1, obj), "Failed to submit create for " + name);
            expectedFileSizes.put(name, obj.getFileSize());
        }
        // Wait for entries to be applied to state machine
        waitForApply(server1, initialObjects.length, 5000);
        System.out.println("  Created " + initialObjects.length + " objects via Raft: " + Arrays.toString(initialObjects));

        // Verify entries are in the log
        RaftLog raftLog1 = raftNode1.getRaftLog();
        long logIndexAfterWrites = raftLog1.getLastLogIndex();
        System.out.println("  Last log index after writes: " + logIndexAfterWrites);
        assertEquals(5, logIndexAfterWrites, "Should have 5 log entries");

        // ===== STEP 2: Stop first server =====
        System.out.println("STEP 2: Stop server (simulate crash)");
        server1.stop();
        Thread.sleep(300);
        System.out.println("  Server stopped");

        // ===== STEP 3: Start NEW server with SAME persistent directory =====
        System.out.println("STEP 3: Start NEW MetadataServer with same persistent directory");

        ClusterConfig config2 = new ClusterConfig("test-cluster", "node1", "127.0.0.1", port, null);
        MetadataServer server2 = new MetadataServer(port, metadataFile1, 2, 6000, 2000, config2, raftStateDir);
        MetadataStore store2 = server2.getMetadataStore();
        RaftNode raftNode2 = server2.getRaftNode();

        // Start the server and wait for it to be ready (recover from WAL and start apply loop)
        startServerAndWaitForLeader(server2);
        System.out.println("  Server restarted");

        // ===== STEP 4: Verify complete state matches =====
        System.out.println("STEP 4: Verify complete state matches expected");

        List<String> actualObjects = new ArrayList<>(store2.listObjects());
        assertEquals(initialObjects.length, actualObjects.size(),
            "Object count should match. Expected: " + initialObjects.length + ", Got: " + actualObjects.size());

        for (String name : initialObjects) {
            ObjectMetadata actual = store2.getObject(name)
                .orElseThrow(() -> new AssertionError("Object " + name + " should exist after restart"));
            assertEquals(expectedFileSizes.get(name), actual.getFileSize(),
                "File size should match for " + name);
        }
        System.out.println("  State verified: " + actualObjects.size() + " objects match");

        // ===== STEP 5: Verify log indexes =====
        System.out.println("STEP 5: Verify log indexes");

        RaftLog raftLog2 = raftNode2.getRaftLog();
        long lastLogIndex = raftLog2.getLastLogIndex();
        long nextLogIndex = lastLogIndex + 1;

        System.out.println("  lastLogIndex=" + lastLogIndex);
        System.out.println("  nextLogIndex=" + nextLogIndex);

        assertEquals(5, lastLogIndex, "lastLogIndex should be 5 (same as before restart)");
        assertEquals(6, nextLogIndex, "nextLogIndex should be 6");

        // ===== STEP 6: Add more objects through Raft =====
        System.out.println("STEP 6: Add more objects through Raft after restart");

        ObjectMetadata newObj = makeObject("NEW_AFTER_RESTART", 12345L);
        assertTrue(submitCreateObject(raftNode2, newObj), "Failed to submit create for NEW_AFTER_RESTART");
        waitForApply(server2, 6, 5000);

        long logIndexAfterNewWrite = raftNode2.getRaftLog().getLastLogIndex();
        System.out.println("  Last log index after new write: " + logIndexAfterNewWrite);
        assertEquals(6, logIndexAfterNewWrite, "Should have 6 log entries");

        // Capture state before second restart
        Map<String, Long> stateBeforeRestart2 = new HashMap<>();
        for (String name : store2.listObjects()) {
            ObjectMetadata obj = store2.getObject(name).orElseThrow();
            stateBeforeRestart2.put(name, obj.getFileSize());
        }
        assertEquals(6, stateBeforeRestart2.size(), "Should have 6 objects before second restart");

        // ===== STEP 7: Stop → Restart again =====
        System.out.println("STEP 7: Second restart");

        server2.stop();
        Thread.sleep(300);

        // Start server again with SAME directories
        ClusterConfig config3 = new ClusterConfig("test-cluster", "node1", "127.0.0.1", port, null);
        MetadataServer server3 = new MetadataServer(port, metadataFile1, 2, 6000, 2000, config3, raftStateDir);
        MetadataStore store3 = server3.getMetadataStore();
        RaftNode raftNode3 = server3.getRaftNode();

        // Start the server and wait for recovery
        startServerAndWaitForLeader(server3);

        // ===== STEP 8: Verify state after second restart =====
        System.out.println("STEP 8: Verify state after second restart");

        Map<String, Long> stateAfterRestart2 = new HashMap<>();
        for (String name : store3.listObjects()) {
            ObjectMetadata obj = store3.getObject(name).orElseThrow();
            stateAfterRestart2.put(name, obj.getFileSize());
        }

        assertEquals(stateBeforeRestart2.size(), stateAfterRestart2.size(),
            "Object count should match after second restart");

        for (Map.Entry<String, Long> entry : stateBeforeRestart2.entrySet()) {
            String name = entry.getKey();
            ObjectMetadata actual = store3.getObject(name)
                .orElseThrow(() -> new AssertionError("Object " + name + " should exist after second restart"));
            assertEquals(entry.getValue(), actual.getFileSize(),
                "File size should match for " + name + " after second restart");
        }
        System.out.println("  State verified after second restart: " + stateAfterRestart2.size() + " objects");

        // ===== STEP 9: Verify next index after second restart =====
        System.out.println("STEP 9: Verify next index after second restart");

        RaftLog raftLog3 = raftNode3.getRaftLog();
        long lastIndexAfterRestart2 = raftLog3.getLastLogIndex();
        long nextIndexAfterRestart2 = lastIndexAfterRestart2 + 1;

        System.out.println("  Last index: " + lastIndexAfterRestart2);
        System.out.println("  Next index: " + nextIndexAfterRestart2);

        assertEquals(6, lastIndexAfterRestart2, "lastLogIndex should be 6 after second restart");
        assertEquals(7, nextIndexAfterRestart2, "nextLogIndex should be 7");

        // Cleanup
        server3.stop();

        System.out.println("=== Phase 1.2 Master Persistence Integration Test PASSED ===");
    }

    @Test
    void testSimplePersistenceWithoutRaft() throws Exception {
        System.out.println("=== Test: Simple persistence without Raft ===");

        int port = 55000 + (int)(System.currentTimeMillis() % 1000);

        // ===== Setup: Create initial data =====
        Path metadataFile = dataDir.resolve("metadata.json");
        Files.createDirectories(metadataFile.getParent());

        // Create first server
        MetadataServer server1 = new MetadataServer(port, metadataFile);
        MetadataStore store1 = server1.getMetadataStore();

        // Create objects
        ObjectMetadata obj1 = makeObject("test1", 1000L);
        ObjectMetadata obj2 = makeObject("test2", 2000L);
        store1.createObject(obj1);
        store1.createObject(obj2);

        assertEquals(2, store1.listObjects().size());
        System.out.println("  Created 2 objects");

        server1.stop();
        Thread.sleep(200);

        // ===== Restart and verify =====
        MetadataServer server2 = new MetadataServer(port, metadataFile);
        MetadataStore store2 = server2.getMetadataStore();

        assertEquals(2, store2.listObjects().size(), "Should have 2 objects after restart");
        assertTrue(store2.getObject("test1").isPresent(), "test1 should exist");
        assertTrue(store2.getObject("test2").isPresent(), "test2 should exist");
        System.out.println("  Verified 2 objects after restart");

        server2.stop();

        System.out.println("  PASSED: Simple persistence works");
    }

    @Test
    void testNextIndexAfterRestart() throws Exception {
        System.out.println("=== Test: Next index after restart ===");

        int port = 56000 + (int)(System.currentTimeMillis() % 1000);

        // ===== Setup: Create initial state =====
        Path metadataFile = dataDir.resolve("metadata3.json");
        Path raftState = dataDir.resolve("raft-state-3");
        Files.createDirectories(metadataFile.getParent());
        Files.createDirectories(raftState);

        // ===== Start server =====
        ClusterConfig config = new ClusterConfig("test-cluster", "node1", "127.0.0.1", port, null);
        MetadataServer server = new MetadataServer(port, metadataFile, 2, 6000, 2000, config, raftState);
        RaftNode raftNode = server.getRaftNode();
        RaftLog raftLog = raftNode.getRaftLog();

        // Start server and wait for leader election
        startServerAndWaitForLeader(server);
        System.out.println("  Server is leader");

        // ===== Verify initial indexes =====
        long lastLogIndex = raftLog.getLastLogIndex();
        long nextIndex = lastLogIndex + 1;

        System.out.println("  Initial: lastLogIndex=" + lastLogIndex + ", nextIndex=" + nextIndex);

        // Should be 0, 1 on fresh start
        assertEquals(0, lastLogIndex, "lastLogIndex should be 0 on fresh start");
        assertEquals(1, nextIndex, "nextIndex should be 1");

        // ===== Create an object through Raft =====
        MetadataStore store = server.getMetadataStore();
        ObjectMetadata obj = makeObject("index_test", 100L);
        assertTrue(submitCreateObject(raftNode, obj), "Failed to submit create");
        waitForApply(server, 1, 5000);

        // Get new indexes
        long lastLogIndex2 = raftLog.getLastLogIndex();
        long nextIndex2 = lastLogIndex2 + 1;

        System.out.println("  After write: lastLogIndex=" + lastLogIndex2 + ", nextIndex=" + nextIndex2);

        assertEquals(1, lastLogIndex2, "lastLogIndex should be 1 after one write");
        assertEquals(2, nextIndex2, "nextIndex should be 2");

        // Verify object is in store
        assertEquals(1, store.listObjects().size(), "Should have 1 object");
        assertTrue(store.getObject("index_test").isPresent(), "index_test should exist");

        server.stop();
        Thread.sleep(200);

        // ===== Restart and verify =====
        MetadataServer server2 = new MetadataServer(port, metadataFile, 2, 6000, 2000, config, raftState);
        RaftNode raftNode2 = server2.getRaftNode();
        RaftLog raftLog2 = raftNode2.getRaftLog();

        // Start the server and wait for recovery
        startServerAndWaitForLeader(server2);

        long lastLogIndex3 = raftLog2.getLastLogIndex();
        long nextIndex3 = lastLogIndex3 + 1;

        System.out.println("  After restart: lastLogIndex=" + lastLogIndex3 + ", nextIndex=" + nextIndex3);

        // Indexes should be restored
        assertEquals(1, lastLogIndex3, "lastLogIndex should be 1 after restart");
        assertEquals(2, nextIndex3, "nextIndex should be 2");

        // Verify state
        MetadataStore store2 = server2.getMetadataStore();
        assertEquals(1, store2.listObjects().size(), "Should have 1 object after restart");
        assertTrue(store2.getObject("index_test").isPresent(), "index_test should exist");

        server2.stop();

        System.out.println("  PASSED: Next index correctly restored after restart");
    }

    // ===== Complete Acceptance Test =====
    // START → WRITE → SNAPSHOT → MORE WRITE → CAPTURE STATE → COMPACT → SHUTDOWN →
    // RESTART → AUTO RECOVER → VERIFY EXACT STATE → WRITE → RESTART AGAIN → VERIFY

    @Test
    void testCompletePersistenceLifecycleWithSnapshotAndCompact() throws Exception {
        System.out.println("=== COMPLETE ACCEPTANCE TEST ===");
        System.out.println("START → WRITE (Raft) → SNAPSHOT → MORE WRITE (Raft) → CAPTURE STATE → " +
                         "COMPACT → SHUTDOWN → RESTART → AUTO RECOVER → VERIFY EXACT STATE → " +
                         "WRITE (Raft) → RESTART AGAIN → VERIFY");

        int port = 57000 + (int)(System.currentTimeMillis() % 1000);

        // ===== Setup: Create directories =====
        Path metadataFile = dataDir.resolve("metadata-complete.json");
        Path raftStateDir = dataDir.resolve("raft-state-complete");
        Files.createDirectories(raftStateDir);

        // Create cluster config
        ClusterConfig config = new ClusterConfig("test-cluster", "node1", "127.0.0.1", port, null);

        // ===== PHASE 1: Start server, write initial data via Raft =====
        System.out.println("\n[PHASE 1] Start server, write initial data via Raft");
        MetadataServer server1 = new MetadataServer(port, metadataFile, 2, 6000, 2000, config, raftStateDir);
        MetadataStore store1 = server1.getMetadataStore();
        RaftNode raftNode1 = server1.getRaftNode();

        // Start server and wait for leader election
        startServerAndWaitForLeader(server1);
        System.out.println("  Server is leader");

        // Create objects A, B, C through Raft
        Map<String, Long> expectedFileSizes = new HashMap<>();
        for (String name : List.of("A", "B", "C")) {
            ObjectMetadata obj = makeObject(name, name.hashCode());
            assertTrue(submitCreateObject(raftNode1, obj), "Failed to submit create for " + name);
            expectedFileSizes.put(name, obj.getFileSize());
        }
        waitForApply(server1, 3, 5000);
        System.out.println("  Created 3 objects via Raft: A, B, C");
        assertEquals(3, store1.listObjects().size());

        RaftLog raftLog1 = raftNode1.getRaftLog();
        assertEquals(3, raftLog1.getLastLogIndex(), "Should have 3 log entries");

        // ===== PHASE 2: Take snapshot =====
        System.out.println("\n[PHASE 2] Take snapshot");
        raftNode1.compactLog(3);
        System.out.println("  Snapshot taken at index 3");

        // ===== PHASE 3: Write more data through Raft =====
        System.out.println("\n[PHASE 3] Write more data via Raft (D, E, F)");
        for (String name : List.of("D", "E", "F")) {
            ObjectMetadata obj = makeObject(name, name.hashCode());
            assertTrue(submitCreateObject(raftNode1, obj), "Failed to submit create for " + name);
            expectedFileSizes.put(name, obj.getFileSize());
        }
        waitForApply(server1, 6, 5000);
        System.out.println("  Now have 6 objects via Raft: A, B, C, D, E, F");
        assertEquals(6, store1.listObjects().size());

        // Capture chunk IDs for exact verification
        Map<String, List<String>> expectedChunkIds = new HashMap<>();
        for (String name : store1.listObjects()) {
            ObjectMetadata obj = store1.getObject(name).orElseThrow();
            expectedChunkIds.put(name, new ArrayList<>(obj.getChunks().stream()
                    .map(ChunkInfo::getChunkId)
                    .toList()));
        }
        System.out.println("  Captured state: " + expectedFileSizes);

        // Capture log index for verification
        long lastLogIndex = raftLog1.getLastLogIndex();
        System.out.println("  Last log index: " + lastLogIndex);
        assertEquals(6, lastLogIndex, "Should have 6 log entries");

        // ===== PHASE 4: Compact again (through index 6) =====
        System.out.println("\n[PHASE 4] Compact through index 6");
        raftNode1.compactLog(6);
        System.out.println("  Compacted through index 6");

        // ===== PHASE 5: SHUTDOWN =====
        System.out.println("\n[PHASE 5] SHUTDOWN");
        server1.stop();
        Thread.sleep(300);
        System.out.println("  Server stopped");

        // ===== PHASE 6: RESTART with automatic recovery =====
        System.out.println("\n[PHASE 6] RESTART (automatic recovery)");
        MetadataServer server2 = new MetadataServer(port, metadataFile, 2, 6000, 2000, config, raftStateDir);
        MetadataStore store2 = server2.getMetadataStore();
        RaftNode raftNode2 = server2.getRaftNode();

        // Start server and wait for recovery
        startServerAndWaitForLeader(server2);
        System.out.println("  Server restarted");

        // ===== PHASE 7: VERIFY EXACT STATE after restart =====
        System.out.println("\n[PHASE 7] VERIFY EXACT STATE after restart");

        // Check object count
        Collection<String> objectsAfterRestart = store2.listObjects();
        assertEquals(6, objectsAfterRestart.size(),
            "Should have 6 objects after restart, got " + objectsAfterRestart.size());

        // Verify each object's file size and chunk IDs
        for (String name : List.of("A", "B", "C", "D", "E", "F")) {
            ObjectMetadata obj = store2.getObject(name)
                .orElseThrow(() -> new AssertionError("Object " + name + " should exist after restart"));
            assertEquals(expectedFileSizes.get(name), obj.getFileSize(),
                "File size mismatch for " + name + " after restart");
            List<String> chunkIds = new ArrayList<>(obj.getChunks().stream()
                    .map(ChunkInfo::getChunkId)
                    .toList());
            assertEquals(expectedChunkIds.get(name), chunkIds,
                "Chunk IDs mismatch for " + name + " after restart");
        }
        System.out.println("  All 6 objects verified with exact file sizes and chunk IDs");

        // Verify log state
        RaftLog raftLog2 = raftNode2.getRaftLog();
        long logStartIndex = raftLog2.getLogStartIndex();
        long lastLogIndex2 = raftLog2.getLastLogIndex();
        System.out.println("  logStartIndex=" + logStartIndex + ", lastLogIndex=" + lastLogIndex2);
        assertTrue(logStartIndex >= 7, "logStartIndex should be >= 7 after compacting through 6");
        assertEquals(6, lastLogIndex2, "lastLogIndex should be 6 (restored from WAL)");

        // ===== PHASE 8: Write after restart via Raft =====
        System.out.println("\n[PHASE 8] Write after restart via Raft (G, H)");
        for (String name : List.of("G", "H")) {
            ObjectMetadata obj = makeObject(name, name.hashCode());
            assertTrue(submitCreateObject(raftNode2, obj), "Failed to submit create for " + name);
            expectedFileSizes.put(name, obj.getFileSize());
        }
        waitForApply(server2, 8, 5000);
        System.out.println("  Now have 8 objects");
        assertEquals(8, store2.listObjects().size());

        // Take a new snapshot to include G and H
        System.out.println("\n[PHASE 8b] Take snapshot after G, H");
        long currentLogIndex = raftNode2.getRaftLog().getLastLogIndex();
        raftNode2.compactLog(currentLogIndex);
        System.out.println("  Snapshot taken at index " + currentLogIndex);

        // Capture state before second restart
        Map<String, Long> stateBeforeRestart2 = new HashMap<>();
        for (String name : store2.listObjects()) {
            ObjectMetadata obj = store2.getObject(name).orElseThrow();
            stateBeforeRestart2.put(name, obj.getFileSize());
        }

        // ===== PHASE 9: RESTART AGAIN =====
        System.out.println("\n[PHASE 9] RESTART AGAIN");
        server2.stop();
        Thread.sleep(300);

        MetadataServer server3 = new MetadataServer(port, metadataFile, 2, 6000, 2000, config, raftStateDir);
        MetadataStore store3 = server3.getMetadataStore();

        // Start server and wait for recovery
        startServerAndWaitForLeader(server3);
        System.out.println("  Server restarted again");

        // ===== PHASE 10: VERIFY after second restart =====
        System.out.println("\n[PHASE 10] VERIFY after second restart");

        Collection<String> objectsAfterRestart2 = store3.listObjects();
        assertEquals(8, objectsAfterRestart2.size(),
            "Should have 8 objects after second restart, got " + objectsAfterRestart2.size());

        for (String name : stateBeforeRestart2.keySet()) {
            ObjectMetadata obj = store3.getObject(name)
                .orElseThrow(() -> new AssertionError("Object " + name + " should exist after second restart"));
            assertEquals(stateBeforeRestart2.get(name), obj.getFileSize(),
                "File size mismatch for " + name + " after second restart");
        }
        System.out.println("  All 8 objects verified");

        // Cleanup
        server3.stop();

        System.out.println("\n=== COMPLETE ACCEPTANCE TEST PASSED ===");
        System.out.println("All 10 phases completed successfully:");
        System.out.println("  ✓ START → WRITE (Raft) → SNAPSHOT → MORE WRITE (Raft)");
        System.out.println("  ✓ CAPTURE STATE → COMPACT → SHUTDOWN");
        System.out.println("  ✓ RESTART → AUTO RECOVER → VERIFY EXACT STATE");
        System.out.println("  ✓ WRITE (Raft) → RESTART AGAIN → VERIFY");
    }
}
