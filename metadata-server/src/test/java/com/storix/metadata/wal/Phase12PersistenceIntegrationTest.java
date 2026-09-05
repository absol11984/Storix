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
 * Tests the complete persistence lifecycle:
 * 1. Start server, perform writes via API
 * 2. Stop and restart
 * 3. Verify state is restored
 * 4. Verify log indexes are correct
 * 5. Stop and restart again
 * 6. Verify state persists
 */
class Phase12PersistenceIntegrationTest {

    @TempDir
    Path dataDir;

    @Test
    void testRealServerPersistenceLifecycle() throws Exception {
        System.out.println("=== Phase 1.2 Master Persistence Integration Test ===");

        int port = 54000 + (int)(System.currentTimeMillis() % 1000);

        // ===== STEP 1: Start server and perform initial writes =====
        System.out.println("STEP 1: Start server and perform initial writes");

        Path metadataFile1 = dataDir.resolve("metadata.json");
        Path raftStateDir = dataDir.resolve("raft-state");
        Files.createDirectories(raftStateDir);

        ClusterConfig config1 = new ClusterConfig("test-cluster", "node1", "127.0.0.1", port, null);
        MetadataServer server1 = new MetadataServer(port, metadataFile1, 2, 6000, 2000, config1, raftStateDir);
        MetadataStore store1 = server1.getMetadataStore();

        // Create initial objects (A, B, C, D, E)
        Map<String, ObjectMetadata> initialState = new HashMap<>();
        String[] initialObjects = {"A", "B", "C", "D", "E"};
        for (String name : initialObjects) {
            ObjectMetadata obj = new ObjectMetadata(name, name.hashCode(), 4096);
            obj.addChunk(new ChunkInfo("chunk-" + name, 0, 500, List.of("node1", "node2"), "hash-" + name));
            store1.createObject(obj);
            initialState.put(name, obj);
        }
        System.out.println("  Created " + initialState.size() + " initial objects: " + Arrays.toString(initialObjects));

        // Wait for leader election
        Thread.sleep(500);

        // ===== STEP 2: Stop first server =====
        System.out.println("STEP 2: Stop server (simulate crash)");

        server1.stop();
        Thread.sleep(300);

        System.out.println("  Server stopped");
        System.out.println("  Expected state: " + initialState.size() + " objects");

        // ===== STEP 3: Start NEW server with SAME persistent directory =====
        System.out.println("STEP 3: Start NEW MetadataServer with same persistent directory");

        // Use the SAME directories - the server should automatically recover
        ClusterConfig config2 = new ClusterConfig("test-cluster", "node1", "127.0.0.1", port, null);
        MetadataServer server2 = new MetadataServer(port, metadataFile1, 2, 6000, 2000, config2, raftStateDir);

        MetadataStore store2 = server2.getMetadataStore();
        RaftNode raftNode2 = server2.getRaftNode();

        Thread.sleep(500); // Wait for server to initialize

        System.out.println("  Server restarted");

        // ===== STEP 4: Verify complete state matches =====
        System.out.println("STEP 4: Verify complete state matches expected");

        Map<String, ObjectMetadata> actualState = new HashMap<>();
        for (String name : store2.listObjects()) {
            store2.getObject(name).ifPresent(obj -> actualState.put(name, obj));
        }

        assertEquals(initialState.size(), actualState.size(), "Object count should match");
        for (String name : initialObjects) {
            ObjectMetadata expected = initialState.get(name);
            ObjectMetadata actual = actualState.get(name);
            assertNotNull(actual, "Object " + name + " should exist after restart");
            assertEquals(expected.getFileSize(), actual.getFileSize(),
                "File size should match for " + name);
        }
        System.out.println("  State verified: " + actualState.size() + " objects match");

        // ===== STEP 5: Verify log indexes =====
        System.out.println("STEP 5: Verify log indexes");

        RaftLog raftLog2 = raftNode2.getRaftLog();
        long lastLogIndex = raftLog2.getLastLogIndex();
        long nextLogIndex = lastLogIndex + 1;

        System.out.println("  lastLogIndex=" + lastLogIndex);
        System.out.println("  nextLogIndex=" + nextLogIndex);

        assertTrue(lastLogIndex >= 0, "lastLogIndex should be >= 0");

        // ===== STEP 6: Add more objects =====
        System.out.println("STEP 6: Add more objects after restart");

        ObjectMetadata newObj = new ObjectMetadata("NEW_AFTER_RESTART", 12345L, 8192);
        newObj.addChunk(new ChunkInfo("chunk-new", 0, 500, List.of("node1"), "hash-new"));
        store2.createObject(newObj);

        // Capture state before second restart
        Map<String, ObjectMetadata> stateBeforeRestart2 = new HashMap<>();
        for (String name : store2.listObjects()) {
            store2.getObject(name).ifPresent(obj -> stateBeforeRestart2.put(name, obj));
        }

        // ===== STEP 7: Stop → Restart again =====
        System.out.println("STEP 7: Second restart");

        server2.stop();
        Thread.sleep(300);

        // Start server again with SAME directories
        ClusterConfig config3 = new ClusterConfig("test-cluster", "node1", "127.0.0.1", port, null);
        MetadataServer server3 = new MetadataServer(port, metadataFile1, 2, 6000, 2000, config3, raftStateDir);

        MetadataStore store3 = server3.getMetadataStore();
        RaftNode raftNode3 = server3.getRaftNode();

        Thread.sleep(500); // Wait for server to initialize

        // ===== STEP 8: Verify state after second restart =====
        System.out.println("STEP 8: Verify state after second restart");

        Map<String, ObjectMetadata> stateAfterRestart2 = new HashMap<>();
        for (String name : store3.listObjects()) {
            store3.getObject(name).ifPresent(obj -> stateAfterRestart2.put(name, obj));
        }

        assertEquals(stateBeforeRestart2.size(), stateAfterRestart2.size(), "Object count should match");
        for (Map.Entry<String, ObjectMetadata> entry : stateBeforeRestart2.entrySet()) {
            ObjectMetadata expected = entry.getValue();
            ObjectMetadata actual = stateAfterRestart2.get(entry.getKey());
            assertNotNull(actual, "Object " + entry.getKey() + " should exist after second restart");
            assertEquals(expected.getFileSize(), actual.getFileSize(),
                "File size should match for " + entry.getKey());
        }
        System.out.println("  State verified after second restart: " + stateAfterRestart2.size() + " objects");

        // ===== STEP 9: Verify next index after second restart =====
        System.out.println("STEP 9: Verify next index after second restart");

        RaftLog raftLog3 = raftNode3.getRaftLog();
        long lastIndexAfterRestart2 = raftLog3.getLastLogIndex();
        long nextIndexAfterRestart2 = lastIndexAfterRestart2 + 1;

        System.out.println("  Last index: " + lastIndexAfterRestart2);
        System.out.println("  Next index: " + nextIndexAfterRestart2);

        assertTrue(nextIndexAfterRestart2 > 0, "Next index should be > 0 after second restart");

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
        ObjectMetadata obj1 = new ObjectMetadata("test1", 1000L, 4096);
        ObjectMetadata obj2 = new ObjectMetadata("test2", 2000L, 4096);
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

        // Create initial store
        MetadataStore initialStore = new MetadataStore(metadataFile);
        initialStore.save();

        // ===== Start server =====
        ClusterConfig config = new ClusterConfig("test-cluster", "node1", "127.0.0.1", port, null);
        MetadataServer server = new MetadataServer(port, metadataFile, 2, 6000, 2000, config, raftState);

        Thread.sleep(500);

        // ===== Verify indexes =====
        RaftNode raftNode = server.getRaftNode();
        RaftLog raftLog = raftNode.getRaftLog();

        long lastLogIndex = raftLog.getLastLogIndex();
        long nextIndex = lastLogIndex + 1;

        System.out.println("  lastLogIndex=" + lastLogIndex + ", nextIndex=" + nextIndex);

        assertEquals(0, lastLogIndex, "lastLogIndex should be 0");
        assertEquals(1, nextIndex, "nextIndex should be 1");

        // ===== Create an object =====
        MetadataStore store = server.getMetadataStore();
        ObjectMetadata obj = new ObjectMetadata("index_test", 100L, 1024);
        store.createObject(obj);

        // Get new indexes
        long lastLogIndex2 = raftLog.getLastLogIndex();
        long nextIndex2 = lastLogIndex2 + 1;

        System.out.println("  After write: lastLogIndex=" + lastLogIndex2 + ", nextIndex=" + nextIndex2);

        server.stop();

        // ===== Restart and verify =====
        MetadataServer server2 = new MetadataServer(port, metadataFile, 2, 6000, 2000, config, raftState);

        Thread.sleep(500);

        RaftNode raftNode2 = server2.getRaftNode();
        RaftLog raftLog2 = raftNode2.getRaftLog();

        long lastLogIndex3 = raftLog2.getLastLogIndex();
        long nextIndex3 = lastLogIndex3 + 1;

        System.out.println("  After restart: lastLogIndex=" + lastLogIndex3 + ", nextIndex=" + nextIndex3);

        // The indexes should be at least as high as before restart
        assertTrue(lastLogIndex3 >= lastLogIndex2, "lastLogIndex should be >= previous value after restart");

        // Verify state
        MetadataStore store2 = server2.getMetadataStore();
        assertEquals(1, store2.listObjects().size(), "Should have 1 object after restart");
        assertTrue(store2.getObject("index_test").isPresent(), "index_test should exist");

        server2.stop();

        System.out.println("  PASSED: Next index correctly restored after restart");
    }
}
