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
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for InstallSnapshot RPC.
 * Tests multi-chunk snapshot transfer, checksum validation, failure propagation,
 * and atomic boundary update.
 */
class InstallSnapshotIntegrationTest {

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
     * TEST 1: Multi-chunk snapshot assembly
     */
    @Test
    void testMultiChunkSnapshotTransferAndChecksum() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Multi-Chunk Snapshot Transfer and Checksum");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 100;

        Path leaderRaftDir = tempDir.resolve("leader-raft");
        Path followerRaftDir = tempDir.resolve("follower-raft");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);

        // Create leader (single node for simplicity)
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

        // Create follower (single node)
        ClusterConfig followerConfig = new ClusterConfig("test", "follower", "127.0.0.1", leaderPort + 1, null);

        MetadataStore followerStore = new MetadataStore(tempDir.resolve("follower-meta.json"));
        SnapshotManager followerSnapshotMgr = new SnapshotManager(followerRaftDir.resolve("snapshots"), followerStore);
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
        Thread followerThread = startNode(follower);

        try {
            // Wait for leader to be ready
            waitForLeader(leader, 5000);

            // Leader creates many objects to create a large snapshot
            System.out.println("Creating objects on leader...");
            List<ObjectMetadata> objects = new ArrayList<>();
            for (int i = 1; i <= 50; i++) {
                ObjectMetadata obj = makeObject("obj" + i, i * 1000);
                objects.add(obj);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            // Take snapshot
            long snapshotIndex = 30;
            System.out.println("Taking snapshot at index " + snapshotIndex);
            leader.compactLog(snapshotIndex);

            // Get the snapshot
            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent(), "Leader should have a snapshot");
            var snapshot = snapshotOpt.get();
            // Use state data (JSON), not the raw file bytes (which include binary header)
            byte[] snapshotData = snapshot.stateData();

            // Compute checksum of state data
            int expectedChecksum = computeChecksum(snapshotData);
            System.out.println("Snapshot size: " + snapshotData.length + " bytes");
            System.out.println("Expected checksum: " + expectedChecksum);

            // Manually invoke InstallSnapshot on follower (simulating RPC)
            // Use 4KB chunks to ensure multi-chunk transfer
            int chunkSize = 4096;
            long offset = 0;
            boolean done = false;
            int chunkIndex = 0;
            boolean lastResponseSuccess = false;

            while (offset < snapshotData.length || !done) {
                int len = (int) Math.min(chunkSize, snapshotData.length - offset);
                byte[] chunk = new byte[len];
                System.arraycopy(snapshotData, (int) offset, chunk, 0, len);
                done = offset + len >= snapshotData.length;

                // Send checksum with FIRST chunk
                int chunkChecksum = (offset == 0) ? expectedChecksum : 0;

                // Create InstallSnapshot request
                RaftMessage.InstallSnapshot request = new RaftMessage.InstallSnapshot(
                    leader.getCurrentTerm(), "leader",
                    snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                    offset, chunk, done, chunkChecksum
                );

                // Process request
                RaftMessage.InstallSnapshotResponse response = follower.handleInstallSnapshot(
                    request.term(), request.leaderId(),
                    request.lastIncludedIndex(), request.lastIncludedTerm(),
                    request.offset(), request.data(), request.done(), request.checksum()
                );

                System.out.println("  Chunk " + chunkIndex + ": offset=" + offset + ", len=" + len +
                    ", done=" + done + ", success=" + response.success());

                if (done) {
                    lastResponseSuccess = response.success();
                }

                assertTrue(response.success() || !done,
                    "InstallSnapshot should succeed for chunk " + chunkIndex);

                offset += len;
                chunkIndex++;
            }

            System.out.println("Total chunks sent: " + chunkIndex);
            assertTrue(chunkIndex > 1, "Should use multiple chunks for large snapshot");
            assertTrue(lastResponseSuccess, "Final response should be success=true");

            // Verify follower state
            assertEquals(snapshot.lastIncludedIndex() + 1, followerLog.getLogStartIndex(),
                "Follower logStartIndex should match snapshot index + 1");

            // Verify follower has the objects
            int objectCount = 0;
            for (String name : followerStore.listObjects()) {
                objectCount++;
            }
            // Snapshot captures all objects currently in the state machine (50),
            // not just entries up to snapshotIndex
            assertEquals(50, objectCount, "Follower should have 50 objects from snapshot");

            System.out.println("\n========================================");
            System.out.println("TEST: Multi-Chunk Snapshot Transfer - PASSED");
            System.out.println("========================================\n");

        } finally {
            leader.stop();
            follower.stop();
            leaderThread.interrupt();
            followerThread.interrupt();
        }
    }

    /**
     * TEST 2: Checksum failure detection
     */
    @Test
    void testChecksumFailureDetection() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Checksum Failure Detection");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 200;

        Path leaderRaftDir = tempDir.resolve("leader-raft-cksum");
        Path followerRaftDir = tempDir.resolve("follower-raft-cksum");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);

        // Create leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);

        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta-cksum.json"));
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

        MetadataStore followerStore = new MetadataStore(tempDir.resolve("follower-meta-cksum.json"));
        SnapshotManager followerSnapshotMgr = new SnapshotManager(followerRaftDir.resolve("snapshots"), followerStore);
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

        // Start nodes
        Thread leaderThread = startNode(leader);
        Thread followerThread = startNode(follower);

        try {
            waitForLeader(leader, 5000);

            // Leader creates objects
            System.out.println("Creating objects on leader...");
            for (int i = 1; i <= 10; i++) {
                ObjectMetadata obj = makeObject("obj" + i, i * 1000);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            // Take snapshot
            long snapshotIndex = 5;
            leader.compactLog(snapshotIndex);

            // Get the snapshot
            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            var snapshot = snapshotOpt.get();
            // Use state data (JSON), not the raw file bytes (which include binary header)
            byte[] snapshotData = snapshot.stateData();

            // Compute correct checksum
            int correctChecksum = computeChecksum(snapshotData);
            System.out.println("Correct checksum: " + correctChecksum);

            // TEST 1: Send correct data with WRONG checksum - should fail
            System.out.println("Testing with correct data + WRONG checksum...");

            int wrongChecksum = correctChecksum + 1; // Wrong checksum
            RaftMessage.InstallSnapshot request = new RaftMessage.InstallSnapshot(
                leader.getCurrentTerm(), "leader",
                snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                0, snapshotData, true, wrongChecksum
            );

            RaftMessage.InstallSnapshotResponse response = follower.handleInstallSnapshot(
                request.term(), request.leaderId(),
                request.lastIncludedIndex(), request.lastIncludedTerm(),
                request.offset(), request.data(), request.done(), request.checksum()
            );

            System.out.println("Response with wrong checksum: success=" + response.success());
            assertFalse(response.success(),
                "InstallSnapshot should fail due to checksum mismatch");
            assertEquals(0, followerStore.listObjects().size(),
                "Follower should have no objects (old state preserved)");

            // TEST 2: Send CORRECT data with CORRECT checksum - should succeed
            System.out.println("\nTesting with correct data + CORRECT checksum...");

            // Clear follower state first
            for (String name : new ArrayList<>(followerStore.listObjects())) {
                followerStore.deleteObjectDirect(name);
            }
            followerStore.save();
            followerLog.truncateFrom(1);
            followerLog.setSnapshotBoundary(0, 0);

            // Send correct data with correct checksum
            RaftMessage.InstallSnapshot request2 = new RaftMessage.InstallSnapshot(
                leader.getCurrentTerm(), "leader",
                snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                0, snapshotData, true, correctChecksum
            );

            RaftMessage.InstallSnapshotResponse response2 = follower.handleInstallSnapshot(
                request2.term(), request2.leaderId(),
                request2.lastIncludedIndex(), request2.lastIncludedTerm(),
                request2.offset(), request2.data(), request2.done(), request2.checksum()
            );

            System.out.println("Response with correct checksum: success=" + response2.success());
            assertTrue(response2.success(),
                "InstallSnapshot should succeed with matching checksum");
            // Snapshot should contain all 10 objects
            assertEquals(10, followerStore.listObjects().size(),
                "Follower should have 10 objects from snapshot");

            System.out.println("\n========================================");
            System.out.println("TEST: Checksum Failure Detection - PASSED");
            System.out.println("========================================\n");

        } finally {
            leader.stop();
            follower.stop();
            leaderThread.interrupt();
            followerThread.interrupt();
        }
    }

    /**
     * TEST 3: Snapshot restore failure propagates as success=false
     */
    @Test
    void testRestoreFailurePropagation() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Restore Failure Propagation");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 300;

        Path leaderRaftDir = tempDir.resolve("leader-raft-restore");
        Path followerRaftDir = tempDir.resolve("follower-raft-restore");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);

        // Create leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);

        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta-restore.json"));
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

        // Create follower with intentional restore failure
        ClusterConfig followerConfig = new ClusterConfig("test", "follower", "127.0.0.1", leaderPort + 1, null);

        MetadataStore followerStore = new MetadataStore(tempDir.resolve("follower-meta-restore.json"));
        SnapshotManager followerSnapshotMgr = new SnapshotManager(followerRaftDir.resolve("snapshots"), followerStore);
        WAL followerWal = new WAL(followerRaftDir.resolve("wal.dat"));
        RaftLog followerLog = new RaftLog(followerWal);

        AtomicBoolean restoreFailed = new AtomicBoolean(false);
        RaftNode follower = new RaftNode(followerConfig, followerRaftDir, followerLog, followerWal);
        follower.setSnapshotManager(followerSnapshotMgr);
        follower.setLogEntryApplier(entry -> {
            if (entry.opType() == LogEntry.OpType.SNAPSHOT_RESTORE) {
                // Simulate restore failure
                restoreFailed.set(true);
                try {
                    throw new IOException("Simulated restore failure for testing");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            // Normal entry processing - no-op for this test
        });

        // Start leader
        Thread leaderThread = startNode(leader);

        try {
            waitForLeader(leader, 5000);

            // Leader creates objects and takes snapshot
            for (int i = 1; i <= 5; i++) {
                ObjectMetadata obj = makeObject("obj" + i, i * 1000);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            leader.compactLog(3);

            // Get snapshot
            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            var snapshot = snapshotOpt.get();
            // Use state data (JSON), not the raw file bytes (which include binary header)
            byte[] snapshotData = snapshot.stateData();

            // Compute checksum of state data
            int checksum = computeChecksum(snapshotData);

            // Manually invoke InstallSnapshot on follower with failing state machine
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

            System.out.println("InstallSnapshot response: success=" + response.success());
            assertTrue(restoreFailed.get(), "Restore should have been attempted and failed");
            assertFalse(response.success(), "InstallSnapshot should return success=false on restore failure");

            System.out.println("\n========================================");
            System.out.println("TEST: Restore Failure Propagation - PASSED");
            System.out.println("========================================\n");

        } finally {
            leader.stop();
            leaderThread.interrupt();
        }
    }

    /**
     * TEST 4: Atomic boundary update - boundary only changes after successful installation
     */
    @Test
    void testAtomicBoundaryUpdate() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Atomic Boundary Update");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 400;

        Path leaderRaftDir = tempDir.resolve("leader-raft-atomic");
        Path followerRaftDir = tempDir.resolve("follower-raft-atomic");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);

        // Create leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);

        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta-atomic.json"));
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

        MetadataStore followerStore = new MetadataStore(tempDir.resolve("follower-meta-atomic.json"));
        SnapshotManager followerSnapshotMgr = new SnapshotManager(followerRaftDir.resolve("snapshots"), followerStore);
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
            for (int i = 1; i <= 10; i++) {
                ObjectMetadata obj = makeObject("obj" + i, i * 1000);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            long snapshotIndex = 5;
            leader.compactLog(snapshotIndex);

            // Get snapshot
            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            var snapshot = snapshotOpt.get();
            // Use state data (JSON), not the raw file bytes (which include binary header)
            byte[] snapshotData = snapshot.stateData();

            // Record initial boundary
            long initialLogStart = followerLog.getLogStartIndex();
            System.out.println("Initial follower logStartIndex: " + initialLogStart);

            // Send corrupted snapshot with correct checksum (should fail at checksum because data doesn't match checksum)
            byte[] corruptedData = snapshotData.clone();
            if (corruptedData.length > 50) {
                corruptedData[50] = (byte) (corruptedData[50] ^ 0xFF);
            }

            // Try to install corrupted snapshot
            int correctChecksum = computeChecksum(snapshotData);
            RaftMessage.InstallSnapshot request = new RaftMessage.InstallSnapshot(
                leader.getCurrentTerm(), "leader",
                snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                0, corruptedData, true, correctChecksum
            );

            RaftMessage.InstallSnapshotResponse response = follower.handleInstallSnapshot(
                request.term(), request.leaderId(),
                request.lastIncludedIndex(), request.lastIncludedTerm(),
                request.offset(), request.data(), request.done(), request.checksum()
            );

            System.out.println("InstallSnapshot response: success=" + response.success());
            assertFalse(response.success(), "InstallSnapshot should fail due to checksum mismatch");

            // Verify boundary unchanged
            long finalLogStart = followerLog.getLogStartIndex();
            System.out.println("Final follower logStartIndex: " + finalLogStart);
            assertEquals(initialLogStart, finalLogStart,
                "Boundary should not change on failed installation");

            System.out.println("\n========================================");
            System.out.println("TEST: Atomic Boundary Update - PASSED");
            System.out.println("========================================\n");

        } finally {
            leader.stop();
            leaderThread.interrupt();
        }
    }

    /**
     * TEST 5: Post-install AppendEntries works correctly
     */
    @Test
    void testPostInstallAppendEntries() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Post-Install AppendEntries");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 500;

        Path leaderRaftDir = tempDir.resolve("leader-raft-post");
        Path followerRaftDir = tempDir.resolve("follower-raft-post");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);

        // Create leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);

        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta-post.json"));
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

        MetadataStore followerStore = new MetadataStore(tempDir.resolve("follower-meta-post.json"));
        SnapshotManager followerSnapshotMgr = new SnapshotManager(followerRaftDir.resolve("snapshots"), followerStore);
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
            for (int i = 1; i <= 20; i++) {
                ObjectMetadata obj = makeObject("obj" + i, i * 1000);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            long snapshotIndex = 10;
            leader.compactLog(snapshotIndex);

            // Get snapshot
            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            var snapshot = snapshotOpt.get();
            // Use state data (JSON), not the raw file bytes (which include binary header)
            byte[] snapshotData = snapshot.stateData();
            int checksum = computeChecksum(snapshotData);

            // Manually install snapshot on follower
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
            assertEquals(snapshot.lastIncludedIndex() + 1, followerLog.getLogStartIndex(),
                "Follower logStartIndex should be snapshotIndex + 1");

            // Now send AppendEntries for entries after snapshot
            long prevLogIndex = snapshot.lastIncludedIndex();
            long prevLogTerm = snapshot.lastIncludedTerm();

            ObjectMetadata newObj = makeObject("afterSnapshot", 9999);
            byte[] newObjData = objectMapper.writeValueAsBytes(newObj);
            LogEntry newEntry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, newObjData);
            newEntry = newEntry.withIndex(prevLogIndex + 1);

            RaftMessage.AppendEntries appendRequest = new RaftMessage.AppendEntries(
                leader.getCurrentTerm(), "leader",
                prevLogIndex, prevLogTerm,
                List.of(newEntry), prevLogIndex + 1
            );

            RaftMessage.AppendEntriesResponse appendResponse = follower.handleAppendEntries(
                appendRequest.term(), appendRequest.leaderId(),
                appendRequest.prevLogIndex(), appendRequest.prevLogTerm(),
                appendRequest.entries(), appendRequest.leaderCommit()
            );

            System.out.println("AppendEntries response: success=" + appendResponse.success());
            assertTrue(appendResponse.success(),
                "AppendEntries should succeed after snapshot installation");

            // Trigger the apply loop to apply committed entries to state machine
            follower.applyCommittedEntries();

            // Verify follower has the new object
            assertTrue(followerStore.objectExists("afterSnapshot"),
                "Follower should have the new object");

            System.out.println("\n========================================");
            System.out.println("TEST: Post-Install AppendEntries - PASSED");
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
