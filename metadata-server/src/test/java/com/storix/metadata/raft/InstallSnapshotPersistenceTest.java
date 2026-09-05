package com.storix.metadata.raft;

import com.storix.metadata.*;
import com.storix.metadata.wal.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for InstallSnapshot persistence and durability.
 * Tests that InstallSnapshot creates real durable snapshots that survive restarts.
 */
class InstallSnapshotPersistenceTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int BASE_PORT = 60000;

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

    /**
     * TEST 1: InstallSnapshot creates a real durable snapshot
     */
    @Test
    void testInstallSnapshotCreatesDurableSnapshot() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: InstallSnapshot Creates Durable Snapshot");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 100;

        Path leaderRaftDir = tempDir.resolve("leader-raft");
        Path followerRaftDir = tempDir.resolve("follower-raft");
        Path followerSnapDir = followerRaftDir.resolve("snapshots");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerSnapDir);

        // Create leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);

        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta.json"));
        SnapshotManager leaderSnapshotMgr = new SnapshotManager(leaderRaftDir.resolve("snapshots"), leaderStore);
        WAL leaderWal = new WAL(leaderRaftDir.resolve("wal.dat"));
        RaftLog leaderLog = new RaftLog(leaderWal);
        RaftNode leader = new RaftNode(leaderConfig, leaderRaftDir, leaderLog, leaderWal);
        leader.setSnapshotManager(leaderSnapshotMgr);
        MetadataStateMachine leaderStateMachine = new MetadataStateMachine(leaderStore);
        leader.setLogEntryApplier(entry -> {
            try {
                leaderStateMachine.apply(entry);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // Create follower with SnapshotManager
        ClusterConfig followerConfig = new ClusterConfig("test", "follower", "127.0.0.1", leaderPort + 1, null);

        MetadataStore followerStore = new MetadataStore(tempDir.resolve("follower-meta.json"));
        SnapshotManager followerSnapshotMgr = new SnapshotManager(followerSnapDir, followerStore);
        WAL followerWal = new WAL(followerRaftDir.resolve("wal.dat"));
        RaftLog followerLog = new RaftLog(followerWal);
        RaftNode follower = new RaftNode(followerConfig, followerRaftDir, followerLog, followerWal);
        follower.setSnapshotManager(followerSnapshotMgr);
        MetadataStateMachine followerStateMachine = new MetadataStateMachine(followerStore);
        follower.setLogEntryApplier(entry -> {
            try {
                followerStateMachine.apply(entry);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // Start leader
        Thread leaderThread = startNode(leader);

        try {
            waitForLeader(leader, 5000);

            // Leader creates objects
            System.out.println("Creating objects on leader...");
            for (int i = 1; i <= 20; i++) {
                ObjectMetadata obj = makeObject("obj" + i, i * 1000);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            // Take snapshot on leader
            long snapshotIndex = 10;
            leader.compactLog(snapshotIndex);

            // Get the snapshot
            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            var snapshot = snapshotOpt.get();
            byte[] snapshotData = snapshot.stateData();
            int checksum = computeChecksum(snapshotData);

            // Install snapshot on follower
            System.out.println("Installing snapshot on follower...");
            RaftMessage.InstallSnapshot request = new RaftMessage.InstallSnapshot(
                leader.getCurrentTerm(), "leader",
                snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                0, snapshotData, true, checksum
            );

            RaftMessage.InstallSnapshotResponse response = follower.handleInstallSnapshot(
                request.term(), request.leaderId(),
                request.lastIncludedIndex(), request.lastIncludedTerm(),
                request.offset(), request.data(), request.done(), request.checksum()
            );

            assertTrue(response.success(), "InstallSnapshot should succeed");

            // VERIFY: Real snapshot file exists
            List<Path> snapshotFiles = Files.list(followerSnapDir)
                .filter(p -> p.getFileName().toString().startsWith("snapshot-"))
                .toList();
            assertFalse(snapshotFiles.isEmpty(), "A real snapshot file should exist in snapshot directory");

            // Find the snapshot for the index we installed
            Path durableSnapshot = followerSnapDir.resolve("snapshot-" + snapshot.lastIncludedIndex());
            assertTrue(Files.exists(durableSnapshot),
                "Durable snapshot file should exist: " + durableSnapshot);

            System.out.println("  Durable snapshot exists: " + durableSnapshot);

            // VERIFY: Snapshot can be loaded independently
            var loadedSnapshot = followerSnapshotMgr.loadLatestSnapshot();
            assertTrue(loadedSnapshot.isPresent(), "Snapshot should be loadable");
            assertEquals(snapshot.lastIncludedIndex(), loadedSnapshot.get().lastIncludedIndex());
            assertEquals(snapshot.lastIncludedTerm(), loadedSnapshot.get().lastIncludedTerm());

            // VERIFY: Follower has the objects
            int objectCount = 0;
            for (String name : followerStore.listObjects()) {
                objectCount++;
            }
            assertEquals(20, objectCount, "Follower should have 20 objects from snapshot");

            System.out.println("\n========================================");
            System.out.println("TEST: InstallSnapshot Creates Durable Snapshot - PASSED");
            System.out.println("========================================\n");

        } finally {
            leader.stop();
            leaderThread.interrupt();
        }
    }

    /**
     * TEST 2: InstallSnapshot snapshot survives restart
     */
    @Test
    void testInstallSnapshotSurvivesRestart() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: InstallSnapshot Survives Restart");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 200;

        Path leaderRaftDir = tempDir.resolve("leader-raft2");
        Path followerRaftDir = tempDir.resolve("follower-raft2");
        Path followerSnapDir = followerRaftDir.resolve("snapshots");
        Path followerMetaFile = tempDir.resolve("follower-meta2.json");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerSnapDir);

        // Create leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);

        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta2.json"));
        SnapshotManager leaderSnapshotMgr = new SnapshotManager(leaderRaftDir.resolve("snapshots"), leaderStore);
        WAL leaderWal = new WAL(leaderRaftDir.resolve("wal.dat"));
        RaftLog leaderLog = new RaftLog(leaderWal);
        RaftNode leader = new RaftNode(leaderConfig, leaderRaftDir, leaderLog, leaderWal);
        leader.setSnapshotManager(leaderSnapshotMgr);
        MetadataStateMachine leaderStateMachine = new MetadataStateMachine(leaderStore);
        leader.setLogEntryApplier(entry -> {
            try {
                leaderStateMachine.apply(entry);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // Create follower
        ClusterConfig followerConfig = new ClusterConfig("test", "follower", "127.0.0.1", leaderPort + 1, null);

        MetadataStore followerStore = new MetadataStore(followerMetaFile);
        SnapshotManager followerSnapshotMgr = new SnapshotManager(followerSnapDir, followerStore);
        WAL followerWal = new WAL(followerRaftDir.resolve("wal.dat"));
        RaftLog followerLog = new RaftLog(followerWal);
        RaftNode follower = new RaftNode(followerConfig, followerRaftDir, followerLog, followerWal);
        follower.setSnapshotManager(followerSnapshotMgr);
        MetadataStateMachine followerStateMachine = new MetadataStateMachine(followerStore);
        follower.setLogEntryApplier(entry -> {
            try {
                followerStateMachine.apply(entry);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // Start leader
        Thread leaderThread = startNode(leader);

        try {
            waitForLeader(leader, 5000);

            // Leader creates objects and takes snapshot
            System.out.println("Creating 30 objects and taking snapshot...");
            for (int i = 1; i <= 30; i++) {
                ObjectMetadata obj = makeObject("obj" + i, i * 1000);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            leader.compactLog(15);

            // Get the snapshot
            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            var snapshot = snapshotOpt.get();
            byte[] snapshotData = snapshot.stateData();
            int checksum = computeChecksum(snapshotData);

            // Install snapshot on follower
            System.out.println("Installing snapshot on follower...");
            RaftMessage.InstallSnapshot request = new RaftMessage.InstallSnapshot(
                leader.getCurrentTerm(), "leader",
                snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                0, snapshotData, true, checksum
            );

            RaftMessage.InstallSnapshotResponse response = follower.handleInstallSnapshot(
                request.term(), request.leaderId(),
                request.lastIncludedIndex(), request.lastIncludedTerm(),
                request.offset(), request.data(), request.done(), request.checksum()
            );

            assertTrue(response.success(), "InstallSnapshot should succeed");

            // Record follower state before restart
            Map<String, Long> stateBeforeRestart = new HashMap<>();
            for (String name : followerStore.listObjects()) {
                followerStore.getObject(name).ifPresent(obj -> stateBeforeRestart.put(name, obj.getFileSize()));
            }
            long logStartBefore = followerLog.getLogStartIndex();
            long commitBefore = followerLog.getCommitIndex();

            System.out.println("  Before restart: " + stateBeforeRestart.size() + " objects, logStartIndex=" + logStartBefore);

            // Stop follower
            follower.stop();
            Thread.sleep(300);

            // Create a NEW follower using the SAME data directory
            System.out.println("Creating new follower with same data directory...");
            MetadataStore newFollowerStore = new MetadataStore(followerMetaFile);
            SnapshotManager newFollowerSnapshotMgr = new SnapshotManager(followerSnapDir, newFollowerStore);
            WAL newFollowerWal = new WAL(followerRaftDir.resolve("wal.dat"));
            RaftLog newFollowerLog = new RaftLog(newFollowerWal);

            // Load snapshot to verify it exists
            var loadedSnapshot = newFollowerSnapshotMgr.loadLatestSnapshot();
            assertTrue(loadedSnapshot.isPresent(), "Snapshot should be loaded after restart");
            System.out.println("  Loaded snapshot: index=" + loadedSnapshot.get().lastIncludedIndex() +
                    ", term=" + loadedSnapshot.get().lastIncludedTerm());

            // Verify state matches
            Map<String, Long> stateAfterRestart = new HashMap<>();
            for (String name : newFollowerStore.listObjects()) {
                newFollowerStore.getObject(name).ifPresent(obj -> stateAfterRestart.put(name, obj.getFileSize()));
            }

            assertEquals(stateBeforeRestart.size(), stateAfterRestart.size(),
                "Object count should match after restart");
            for (String name : stateBeforeRestart.keySet()) {
                assertTrue(stateAfterRestart.containsKey(name),
                    "Object " + name + " should exist after restart");
                assertEquals(stateBeforeRestart.get(name), stateAfterRestart.get(name),
                    "Object " + name + " size should match");
            }

            System.out.println("  After restart: " + stateAfterRestart.size() + " objects verified");

            System.out.println("\n========================================");
            System.out.println("TEST: InstallSnapshot Survives Restart - PASSED");
            System.out.println("========================================\n");

        } finally {
            leader.stop();
            leaderThread.interrupt();
        }
    }

    /**
     * TEST 3: Multi-chunk InstallSnapshot with durable persistence
     */
    @Test
    void testMultiChunkInstallSnapshotPersistence() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Multi-Chunk InstallSnapshot Persistence");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 300;

        Path leaderRaftDir = tempDir.resolve("leader-raft3");
        Path followerRaftDir = tempDir.resolve("follower-raft3");
        Path followerSnapDir = followerRaftDir.resolve("snapshots");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerSnapDir);

        // Create leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);

        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta3.json"));
        SnapshotManager leaderSnapshotMgr = new SnapshotManager(leaderRaftDir.resolve("snapshots"), leaderStore);
        WAL leaderWal = new WAL(leaderRaftDir.resolve("wal.dat"));
        RaftLog leaderLog = new RaftLog(leaderWal);
        RaftNode leader = new RaftNode(leaderConfig, leaderRaftDir, leaderLog, leaderWal);
        leader.setSnapshotManager(leaderSnapshotMgr);
        MetadataStateMachine leaderStateMachine = new MetadataStateMachine(leaderStore);
        leader.setLogEntryApplier(entry -> {
            try {
                leaderStateMachine.apply(entry);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // Create follower
        ClusterConfig followerConfig = new ClusterConfig("test", "follower", "127.0.0.1", leaderPort + 1, null);

        MetadataStore followerStore = new MetadataStore(tempDir.resolve("follower-meta3.json"));
        SnapshotManager followerSnapshotMgr = new SnapshotManager(followerSnapDir, followerStore);
        WAL followerWal = new WAL(followerRaftDir.resolve("wal.dat"));
        RaftLog followerLog = new RaftLog(followerWal);
        RaftNode follower = new RaftNode(followerConfig, followerRaftDir, followerLog, followerWal);
        follower.setSnapshotManager(followerSnapshotMgr);
        MetadataStateMachine followerStateMachine = new MetadataStateMachine(followerStore);
        follower.setLogEntryApplier(entry -> {
            try {
                followerStateMachine.apply(entry);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // Start leader
        Thread leaderThread = startNode(leader);

        try {
            waitForLeader(leader, 5000);

            // Leader creates many objects for large snapshot
            System.out.println("Creating 50 objects for large snapshot...");
            for (int i = 1; i <= 50; i++) {
                ObjectMetadata obj = makeObject("obj" + i, i * 1000);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            leader.compactLog(25);

            // Get the snapshot
            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            var snapshot = snapshotOpt.get();
            byte[] snapshotData = snapshot.stateData();
            int checksum = computeChecksum(snapshotData);

            // Send in multiple chunks (4KB each)
            int chunkSize = 4096;
            long offset = 0;
            boolean done = false;
            int chunkCount = 0;
            RaftMessage.InstallSnapshotResponse finalResponse = null;

            while (offset < snapshotData.length || !done) {
                int len = (int) Math.min(chunkSize, snapshotData.length - offset);
                byte[] chunk = new byte[len];
                System.arraycopy(snapshotData, (int) offset, chunk, 0, len);
                done = offset + len >= snapshotData.length;

                RaftMessage.InstallSnapshot request = new RaftMessage.InstallSnapshot(
                    leader.getCurrentTerm(), "leader",
                    snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                    offset, chunk, done, done ? checksum : 0
                );

                finalResponse = follower.handleInstallSnapshot(
                    request.term(), request.leaderId(),
                    request.lastIncludedIndex(), request.lastIncludedTerm(),
                    request.offset(), request.data(), request.done(), request.checksum()
                );

                System.out.println("  Chunk " + chunkCount + ": offset=" + offset + ", len=" + len +
                    ", done=" + done + ", success=" + finalResponse.success());

                chunkCount++;
                offset += len;
            }

            assertTrue(finalResponse.success(), "Final InstallSnapshot should succeed");
            assertTrue(chunkCount > 1, "Should use multiple chunks");

            // VERIFY: Durable snapshot exists
            Path durableSnapshot = followerSnapDir.resolve("snapshot-" + snapshot.lastIncludedIndex());
            assertTrue(Files.exists(durableSnapshot),
                "Durable snapshot should exist after multi-chunk transfer");

            // VERIFY: Snapshot can be loaded
            var loadedSnapshot = followerSnapshotMgr.loadLatestSnapshot();
            assertTrue(loadedSnapshot.isPresent());

            // VERIFY: All 50 objects restored
            int objectCount = followerStore.listObjects().size();
            assertEquals(50, objectCount, "All 50 objects should be restored");

            System.out.println("  Durable snapshot verified: " + durableSnapshot);
            System.out.println("  " + objectCount + " objects restored");

            System.out.println("\n========================================");
            System.out.println("TEST: Multi-Chunk InstallSnapshot Persistence - PASSED");
            System.out.println("========================================\n");

        } finally {
            leader.stop();
            leaderThread.interrupt();
        }
    }

    /**
     * TEST 4: Failed InstallSnapshot leaves old state recoverable
     */
    @Test
    void testFailedInstallSnapshotLeavesOldStateRecoverable() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Failed InstallSnapshot Leaves Old State Recoverable");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 400;

        Path leaderRaftDir = tempDir.resolve("leader-raft4");
        Path followerRaftDir = tempDir.resolve("follower-raft4");
        Path followerSnapDir = followerRaftDir.resolve("snapshots");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerSnapDir);

        // Create leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);

        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta4.json"));
        SnapshotManager leaderSnapshotMgr = new SnapshotManager(leaderRaftDir.resolve("snapshots"), leaderStore);
        WAL leaderWal = new WAL(leaderRaftDir.resolve("wal.dat"));
        RaftLog leaderLog = new RaftLog(leaderWal);
        RaftNode leader = new RaftNode(leaderConfig, leaderRaftDir, leaderLog, leaderWal);
        leader.setSnapshotManager(leaderSnapshotMgr);
        MetadataStateMachine leaderStateMachine = new MetadataStateMachine(leaderStore);
        leader.setLogEntryApplier(entry -> {
            try {
                leaderStateMachine.apply(entry);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // Create follower with state already established
        ClusterConfig followerConfig = new ClusterConfig("test", "follower", "127.0.0.1", leaderPort + 1, null);

        MetadataStore followerStore = new MetadataStore(tempDir.resolve("follower-meta4.json"));
        SnapshotManager followerSnapshotMgr = new SnapshotManager(followerSnapDir, followerStore);
        WAL followerWal = new WAL(followerRaftDir.resolve("wal.dat"));
        RaftLog followerLog = new RaftLog(followerWal);
        RaftNode follower = new RaftNode(followerConfig, followerRaftDir, followerLog, followerWal);
        follower.setSnapshotManager(followerSnapshotMgr);
        MetadataStateMachine followerStateMachine = new MetadataStateMachine(followerStore);
        follower.setLogEntryApplier(entry -> {
            try {
                followerStateMachine.apply(entry);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // Start leader
        Thread leaderThread = startNode(leader);

        try {
            waitForLeader(leader, 5000);

            // Create initial follower state
            System.out.println("Creating initial follower state...");
            for (int i = 1; i <= 5; i++) {
                ObjectMetadata obj = makeObject("initial" + i, i * 100);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                follower.submit(entry);
            }
            Thread.sleep(300);

            // Record initial state
            Map<String, Long> initialState = new HashMap<>();
            for (String name : followerStore.listObjects()) {
                followerStore.getObject(name).ifPresent(obj -> initialState.put(name, obj.getFileSize()));
            }
            long initialLogStart = followerLog.getLogStartIndex();
            long initialCommit = followerLog.getCommitIndex();

            System.out.println("  Initial state: " + initialState.size() + " objects");
            System.out.println("  Initial logStartIndex=" + initialLogStart + ", commitIndex=" + initialCommit);

            // Get leader's snapshot
            leader.compactLog(3);
            for (int i = 6; i <= 20; i++) {
                ObjectMetadata obj = makeObject("new" + i, i * 100);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);
            leader.compactLog(10);

            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            var snapshot = snapshotOpt.get();
            byte[] snapshotData = snapshot.stateData();
            int correctChecksum = computeChecksum(snapshotData);

            // CORRUPT the checksum - this should cause failure
            System.out.println("Installing snapshot with WRONG checksum...");
            RaftMessage.InstallSnapshot request = new RaftMessage.InstallSnapshot(
                leader.getCurrentTerm(), "leader",
                snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                0, snapshotData, true, correctChecksum + 1 // WRONG checksum
            );

            RaftMessage.InstallSnapshotResponse response = follower.handleInstallSnapshot(
                request.term(), request.leaderId(),
                request.lastIncludedIndex(), request.lastIncludedTerm(),
                request.offset(), request.data(), request.done(), request.checksum()
            );

            assertFalse(response.success(), "InstallSnapshot should fail with wrong checksum");

            // VERIFY: Old state is intact
            Map<String, Long> stateAfterFail = new HashMap<>();
            for (String name : followerStore.listObjects()) {
                followerStore.getObject(name).ifPresent(obj -> stateAfterFail.put(name, obj.getFileSize()));
            }

            assertEquals(initialState.size(), stateAfterFail.size(),
                "State should be unchanged after failed InstallSnapshot");
            for (String name : initialState.keySet()) {
                assertTrue(stateAfterFail.containsKey(name),
                    "Object " + name + " should still exist after failed InstallSnapshot");
                assertEquals(initialState.get(name), stateAfterFail.get(name),
                    "Object " + name + " should have same size");
            }

            // VERIFY: Raft state unchanged
            assertEquals(initialLogStart, followerLog.getLogStartIndex(),
                "logStartIndex should be unchanged after failed InstallSnapshot");
            assertEquals(initialCommit, followerLog.getCommitIndex(),
                "commitIndex should be unchanged after failed InstallSnapshot");

            System.out.println("  Old state verified intact after failed InstallSnapshot");

            System.out.println("\n========================================");
            System.out.println("TEST: Failed InstallSnapshot Leaves Old State Recoverable - PASSED");
            System.out.println("========================================\n");

        } finally {
            leader.stop();
            leaderThread.interrupt();
        }
    }

    /**
     * Helper: Start a RaftNode in a background thread.
     */
    private Thread startNode(RaftNode node) {
        Thread thread = new Thread(() -> {
            try {
                node.start();
            } catch (IOException e) {
                // Expected on shutdown
            }
        });
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    /**
     * Helper: Wait for node to become leader.
     */
    private void waitForLeader(RaftNode node, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!node.isLeader() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        if (!node.isLeader()) {
            throw new AssertionError("Node did not become leader within timeout");
        }
    }
}
