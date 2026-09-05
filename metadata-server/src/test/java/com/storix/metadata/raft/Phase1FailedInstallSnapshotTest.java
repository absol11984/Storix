package com.storix.metadata.raft;

import com.storix.metadata.*;
import com.storix.metadata.wal.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 1 Failed InstallSnapshot Test
 *
 * Verifies that failed InstallSnapshot:
 *   1. Returns success=false
 *   2. Live state remains old state
 *   3. Old snapshot remains authoritative
 *   4. Old state is recovered on restart
 */
class Phase1FailedInstallSnapshotTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int BASE_PORT = 61000;

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

    // ===== TEST 1: FAILED INSTALLSNAPSHOT PRESERVES OLD STATE =====

    /**
     * FAILED INSTALLSNAPSHOT TEST
     *
     * 1. Install valid snapshot (A B C) on follower via handleInstallSnapshot
     * 2. Create D on leader, compact
     * 3. Corrupt new snapshot (A B C D) and try to install
     * Expected:
     *   success = false
     *   live state = A B C (unchanged)
     */
    @Test
    void testFailedInstallSnapshotPreservesOldState() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Failed InstallSnapshot Preserves Old State");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 100;

        Path leaderRaftDir = tempDir.resolve("leader-raft-fail");
        Path followerRaftDir = tempDir.resolve("follower-raft-fail");
        Path followerSnapDir = followerRaftDir.resolve("snapshots");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);
        Files.createDirectories(followerSnapDir);

        // Setup leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);
        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta-fail.json"));
        SnapshotManager leaderSnapshotMgr = new SnapshotManager(leaderRaftDir.resolve("snapshots"), leaderStore);
        WAL leaderWal = new WAL(leaderRaftDir.resolve("wal.dat"));
        RaftLog leaderLog = new RaftLog(leaderWal);
        RaftNode leader = new RaftNode(leaderConfig, leaderRaftDir, leaderLog, leaderWal);
        leader.setSnapshotManager(leaderSnapshotMgr);
        MetadataStateMachine leaderStateMachine = new MetadataStateMachine(leaderStore);
        leader.setLogEntryApplier(entry -> {
            try { leaderStateMachine.apply(entry); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        // Setup follower
        ClusterConfig followerConfig = new ClusterConfig("test", "follower", "127.0.0.1", leaderPort + 1, null);
        Path followerMetaFile = tempDir.resolve("follower-meta-fail.json");
        MetadataStore followerStore = new MetadataStore(followerMetaFile);
        SnapshotManager followerSnapshotMgr = new SnapshotManager(followerSnapDir, followerStore);
        WAL followerWal = new WAL(followerRaftDir.resolve("wal.dat"));
        RaftLog followerLog = new RaftLog(followerWal);
        RaftNode follower = new RaftNode(followerConfig, followerRaftDir, followerLog, followerWal);
        follower.setSnapshotManager(followerSnapshotMgr);
        follower.setMetadataStore(followerStore);
        MetadataStateMachine followerStateMachine = new MetadataStateMachine(followerStore);
        follower.setLogEntryApplier(entry -> {
            try { followerStateMachine.apply(entry); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        Thread leaderThread = startNode(leader);

        try {
            waitForLeader(leader, 5000);

            // Leader creates 3 objects: A B C
            System.out.println("Creating initial state: A B C...");
            for (String name : List.of("A", "B", "C")) {
                ObjectMetadata obj = makeObject(name, 1000L);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            // Compact log to create snapshot
            leader.compactLog(2);
            Thread.sleep(100);

            // Install initial snapshot (A B C) on follower
            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent(), "Leader should have snapshot");
            var snapshot = snapshotOpt.get();
            byte[] snapshotData = snapshot.stateData();
            int checksum = computeChecksum(snapshotData);

            System.out.println("Installing initial snapshot (A B C) on follower...");
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

            assertTrue(response.success(), "Initial snapshot install should succeed");
            assertEquals(3, followerStore.listObjects().size(), "Follower should have 3 objects");
            System.out.println("Follower has initial state: A B C (3 objects)");

            // Create 4th object D on leader
            System.out.println("Creating additional state: D...");
            ObjectMetadata objD = makeObject("D", 1000L);
            byte[] dataD = objectMapper.writeValueAsBytes(objD);
            LogEntry entryD = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, dataD);
            leader.submit(entryD);
            Thread.sleep(300);

            // Compact to include D
            leader.compactLog(3);
            Thread.sleep(100);

            // Get snapshot of A B C D from leader
            snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            snapshot = snapshotOpt.get();
            snapshotData = snapshot.stateData();

            // CORRUPT the snapshot to force failure
            byte[] corruptedData = Arrays.copyOf(snapshotData, snapshotData.length);
            corruptedData[10] ^= 0xFF; // Corrupt some bytes
            int corruptedChecksum = computeChecksum(corruptedData);

            System.out.println("Sending CORRUPTED snapshot to force failure...");

            // Try to install corrupted snapshot - should fail
            request = new RaftMessage.InstallSnapshot(
                leader.getCurrentTerm(), "leader",
                snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                0, corruptedData, true, corruptedChecksum
            );

            response = follower.handleInstallSnapshot(
                request.term(), request.leaderId(),
                request.lastIncludedIndex(), request.lastIncludedTerm(),
                request.offset(), request.data(), request.done(), request.checksum()
            );

            assertFalse(response.success(), "InstallSnapshot should return success=false for corrupted data");
            System.out.println("InstallSnapshot correctly returned success=false");

            // CRITICAL: Live state must NOT have been changed
            int liveObjectCount = followerStore.listObjects().size();
            System.out.println("Live state object count after failed install: " + liveObjectCount);

            assertEquals(3, liveObjectCount,
                "Live state must be UNCHANGED (A B C) after failed install");
            assertTrue(followerStore.objectExists("A"), "Object A should exist");
            assertTrue(followerStore.objectExists("B"), "Object B should exist");
            assertTrue(followerStore.objectExists("C"), "Object C should exist");
            assertFalse(followerStore.objectExists("D"), "Object D should NOT exist");

            System.out.println("SUCCESS: Live state preserved: A B C");

            System.out.println("\n========================================");
            System.out.println("TEST: Failed InstallSnapshot Preserves Old State - PASSED");
            System.out.println("========================================\n");

        } finally {
            leader.stop();
            leaderThread.interrupt();
        }
    }

    // ===== TEST 2: FAILED INSTALL + RESTART =====

    /**
     * FAILED INSTALL + RESTART TEST
     *
     * 1. Install valid snapshot (A B C D) on follower
     * 2. Try to install corrupted snapshot - fails
     * 3. Restart follower
     * 4. Verify old state (A B C D) is recovered
     */
    @Test
    void testFailedInstallPlusRestartRecoversOldState() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Failed Install + Restart Recovers Old State");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 200;

        Path leaderRaftDir = tempDir.resolve("leader-raft-fail-restart");
        Path followerRaftDir = tempDir.resolve("follower-raft-fail-restart");
        Path followerSnapDir = followerRaftDir.resolve("snapshots");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);
        Files.createDirectories(followerSnapDir);

        // Setup leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);
        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta-fail-restart.json"));
        SnapshotManager leaderSnapshotMgr = new SnapshotManager(leaderRaftDir.resolve("snapshots"), leaderStore);
        WAL leaderWal = new WAL(leaderRaftDir.resolve("wal.dat"));
        RaftLog leaderLog = new RaftLog(leaderWal);
        RaftNode leader = new RaftNode(leaderConfig, leaderRaftDir, leaderLog, leaderWal);
        leader.setSnapshotManager(leaderSnapshotMgr);
        MetadataStateMachine leaderStateMachine = new MetadataStateMachine(leaderStore);
        leader.setLogEntryApplier(entry -> {
            try { leaderStateMachine.apply(entry); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        // Setup follower
        ClusterConfig followerConfig = new ClusterConfig("test", "follower", "127.0.0.1", leaderPort + 1, null);
        Path followerMetaFile = tempDir.resolve("follower-meta-fail-restart.json");
        MetadataStore followerStore = new MetadataStore(followerMetaFile);
        SnapshotManager followerSnapshotMgr = new SnapshotManager(followerSnapDir, followerStore);
        WAL followerWal = new WAL(followerRaftDir.resolve("wal.dat"));
        RaftLog followerLog = new RaftLog(followerWal);
        RaftNode follower = new RaftNode(followerConfig, followerRaftDir, followerLog, followerWal);
        follower.setSnapshotManager(followerSnapshotMgr);
        follower.setMetadataStore(followerStore);
        MetadataStateMachine followerStateMachine = new MetadataStateMachine(followerStore);
        follower.setLogEntryApplier(entry -> {
            try { followerStateMachine.apply(entry); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        Thread leaderThread = startNode(leader);

        try {
            waitForLeader(leader, 5000);

            // Leader creates 4 objects: A B C D
            System.out.println("Creating initial state: A B C D...");
            for (String name : List.of("A", "B", "C", "D")) {
                ObjectMetadata obj = makeObject(name, 1000L);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            // Compact to create snapshot
            leader.compactLog(2);
            Thread.sleep(100);

            // Install snapshot on follower
            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            var snapshot = snapshotOpt.get();
            byte[] snapshotData = snapshot.stateData();
            int checksum = computeChecksum(snapshotData);

            System.out.println("Installing initial snapshot (A B C D) on follower...");
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

            assertTrue(response.success(), "Initial snapshot install should succeed");
            assertEquals(4, followerStore.listObjects().size(), "Follower should have 4 objects");
            System.out.println("Follower has initial state: A B C D (4 objects)");

            // Create another object E, compact to get a new snapshot
            System.out.println("Creating additional state: E...");
            ObjectMetadata objE = makeObject("E", 1000L);
            byte[] dataE = objectMapper.writeValueAsBytes(objE);
            LogEntry entryE = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, dataE);
            leader.submit(entryE);
            Thread.sleep(300);

            leader.compactLog(3);
            Thread.sleep(100);

            // Get new snapshot with A B C D E
            snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            byte[] newSnapshotData = snapshotOpt.get().stateData();

            // CORRUPT new snapshot
            byte[] corruptedData = Arrays.copyOf(newSnapshotData, newSnapshotData.length);
            corruptedData[10] ^= 0xFF;
            int corruptedChecksum = computeChecksum(corruptedData);

            System.out.println("Sending corrupted snapshot (A B C D E)...");
            follower.handleInstallSnapshot(
                leader.getCurrentTerm(), "leader",
                snapshotOpt.get().lastIncludedIndex(), snapshotOpt.get().lastIncludedTerm(),
                0, corruptedData, true, corruptedChecksum
            );

            // Verify live state unchanged (should be 4 because initial install succeeded)
            assertEquals(4, followerStore.listObjects().size(),
                "Live state should be A B C D after failed install");

            // Stop and restart
            System.out.println("Stopping and restarting follower...");
            follower.stop();
            Thread.sleep(100);

            // Restart
            MetadataStore restartedStore = new MetadataStore(followerMetaFile);
            SnapshotManager restartedSnapshotMgr = new SnapshotManager(followerSnapDir, restartedStore);
            WAL restartedWal = new WAL(followerRaftDir.resolve("wal.dat"));
            RaftLog restartedLog = new RaftLog(restartedWal);
            RaftNode restartedFollower = new RaftNode(followerConfig, followerRaftDir, restartedLog, restartedWal);
            restartedFollower.setSnapshotManager(restartedSnapshotMgr);
            restartedFollower.setMetadataStore(restartedStore);

            // After restart, the old state (A B C D) should be recovered
            assertEquals(4, restartedStore.listObjects().size(),
                "Restarted store should have A B C D (original state)");
            assertTrue(restartedStore.objectExists("A"));
            assertTrue(restartedStore.objectExists("B"));
            assertTrue(restartedStore.objectExists("C"));
            assertTrue(restartedStore.objectExists("D"));

            System.out.println("SUCCESS: Original state A B C D recovered on restart");

            System.out.println("\n========================================");
            System.out.println("TEST: Failed Install + Restart - PASSED");
            System.out.println("========================================\n");

        } finally {
            leader.stop();
            leaderThread.interrupt();
        }
    }

    // ===== TEST 3: OLD SNAPSHOT REMAINS AUTHORITATIVE =====

    /**
     * OLD SNAPSHOT INTEGRITY TEST
     *
     * After failed InstallSnapshot with corrupted data, verify old snapshot is unchanged.
     */
    @Test
    void testOldSnapshotRemainsAuthoritativeAfterFailedInstall() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Old Snapshot Remains Authoritative");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 300;

        Path leaderRaftDir = tempDir.resolve("leader-raft-old-snap");
        Path followerRaftDir = tempDir.resolve("follower-raft-old-snap");
        Path followerSnapDir = followerRaftDir.resolve("snapshots");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);
        Files.createDirectories(followerSnapDir);

        // Setup leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);
        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta-old-snap.json"));
        SnapshotManager leaderSnapshotMgr = new SnapshotManager(leaderRaftDir.resolve("snapshots"), leaderStore);
        WAL leaderWal = new WAL(leaderRaftDir.resolve("wal.dat"));
        RaftLog leaderLog = new RaftLog(leaderWal);
        RaftNode leader = new RaftNode(leaderConfig, leaderRaftDir, leaderLog, leaderWal);
        leader.setSnapshotManager(leaderSnapshotMgr);
        MetadataStateMachine leaderStateMachine = new MetadataStateMachine(leaderStore);
        leader.setLogEntryApplier(entry -> {
            try { leaderStateMachine.apply(entry); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        // Setup follower
        ClusterConfig followerConfig = new ClusterConfig("test", "follower", "127.0.0.1", leaderPort + 1, null);
        Path followerMetaFile = tempDir.resolve("follower-meta-old-snap.json");
        MetadataStore followerStore = new MetadataStore(followerMetaFile);
        SnapshotManager followerSnapshotMgr = new SnapshotManager(followerSnapDir, followerStore);
        WAL followerWal = new WAL(followerRaftDir.resolve("wal.dat"));
        RaftLog followerLog = new RaftLog(followerWal);
        RaftNode follower = new RaftNode(followerConfig, followerRaftDir, followerLog, followerWal);
        follower.setSnapshotManager(followerSnapshotMgr);
        follower.setMetadataStore(followerStore);
        MetadataStateMachine followerStateMachine = new MetadataStateMachine(followerStore);
        follower.setLogEntryApplier(entry -> {
            try { followerStateMachine.apply(entry); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        Thread leaderThread = startNode(leader);

        try {
            waitForLeader(leader, 5000);

            // Create A B C
            System.out.println("Creating initial state: A B C...");
            for (String name : List.of("A", "B", "C")) {
                ObjectMetadata obj = makeObject(name, 1000L);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            // Compact to create first snapshot
            leader.compactLog(2);
            Thread.sleep(100);

            // Get old snapshot info
            var oldSnapOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(oldSnapOpt.isPresent());
            var oldSnap = oldSnapOpt.get();
            long oldSnapIndex = oldSnap.lastIncludedIndex();
            long oldSnapTerm = oldSnap.lastIncludedTerm();
            byte[] oldSnapData = oldSnap.stateData();
            int oldSnapChecksum = computeChecksum(oldSnapData);

            // Install old snapshot (A B C) on follower first
            System.out.println("Installing initial snapshot (A B C) on follower...");
            RaftMessage.InstallSnapshot initRequest = new RaftMessage.InstallSnapshot(
                leader.getCurrentTerm(), "leader",
                oldSnap.lastIncludedIndex(), oldSnap.lastIncludedTerm(),
                0, oldSnapData, true, oldSnapChecksum
            );
            follower.handleInstallSnapshot(
                initRequest.term(), initRequest.leaderId(),
                initRequest.lastIncludedIndex(), initRequest.lastIncludedTerm(),
                initRequest.offset(), initRequest.data(), initRequest.done(), initRequest.checksum()
            );

            // Create D
            System.out.println("Creating additional state: D...");
            ObjectMetadata objD = makeObject("D", 1000L);
            byte[] dataD = objectMapper.writeValueAsBytes(objD);
            LogEntry entryD = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, dataD);
            leader.submit(entryD);
            Thread.sleep(300);

            // Compact
            leader.compactLog(3);
            Thread.sleep(100);

            // Get new snapshot
            var newSnapOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(newSnapOpt.isPresent());

            // CORRUPT new snapshot and try to install
            byte[] corruptedData = Arrays.copyOf(newSnapOpt.get().stateData(), newSnapOpt.get().stateData().length);
            corruptedData[10] ^= 0xFF;
            int corruptedChecksum = computeChecksum(corruptedData);

            System.out.println("Sending corrupted new snapshot (A B C D)...");
            follower.handleInstallSnapshot(
                leader.getCurrentTerm(), "leader",
                newSnapOpt.get().lastIncludedIndex(), newSnapOpt.get().lastIncludedTerm(),
                0, corruptedData, true, corruptedChecksum
            );

            // Verify old snapshot still exists and is valid
            var latestSnap = followerSnapshotMgr.loadLatestSnapshot();

            assertTrue(latestSnap.isPresent(), "Old snapshot should still exist");
            assertEquals(oldSnapIndex, latestSnap.get().lastIncludedIndex(),
                "Old snapshot index should be unchanged");
            assertEquals(oldSnapTerm, latestSnap.get().lastIncludedTerm(),
                "Old snapshot term should be unchanged");

            // Verify content
            assertEquals(computeChecksum(oldSnapData),
                computeChecksum(latestSnap.get().stateData()),
                "Old snapshot checksum should be unchanged");

            System.out.println("SUCCESS: Old snapshot intact: index=" + oldSnapIndex +
                ", term=" + oldSnapTerm + ", checksum=" + oldSnapChecksum);

            System.out.println("\n========================================");
            System.out.println("TEST: Old Snapshot Remains Authoritative - PASSED");
            System.out.println("========================================\n");

        } finally {
            leader.stop();
            leaderThread.interrupt();
        }
    }

    // ===== HELPER METHODS =====

    private Thread startNode(RaftNode node) throws Exception {
        Thread thread = new Thread(() -> {
            try {
                node.start();
            } catch (IOException e) {
                // Expected on stop
            }
        });
        thread.start();
        Thread.sleep(100);
        return thread;
    }

    private void waitForLeader(RaftNode node, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (node.isLeader()) {
                return;
            }
            Thread.sleep(50);
        }
        fail("Node did not become leader within " + timeoutMs + "ms");
    }
}
