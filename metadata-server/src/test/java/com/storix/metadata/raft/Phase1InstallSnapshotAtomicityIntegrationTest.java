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
 * Phase 1 Master Integration Test for InstallSnapshot atomicity.
 *
 * Tests the core invariant:
 *   InstallSnapshot SUCCESS  → new snapshot/state becomes authoritative
 *   InstallSnapshot FAILURE  → old live state remains unchanged
 *                          AND old durable state remains recoverable
 *
 * The key regression test: failure AFTER live state publication but BEFORE
 * snapshot commit must NOT expose partially published state.
 */
class Phase1InstallSnapshotAtomicityIntegrationTest {

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
     * Helper: Compare two MetadataStores for exact equality.
     * Compares all object metadata fields: names, sizes, chunk counts, chunk IDs,
     * replica information, and checksums.
     */
    private static void assertStoresEqual(MetadataStore a, MetadataStore b, String msg) {
        Collection<String> aNames = new TreeSet<>(a.listObjects());
        Collection<String> bNames = new TreeSet<>(b.listObjects());
        assertEquals(aNames, bNames, msg + ": object names must match");

        for (String name : aNames) {
            Optional<ObjectMetadata> aMeta = a.getObject(name);
            Optional<ObjectMetadata> bMeta = b.getObject(name);
            assertTrue(aMeta.isPresent() && bMeta.isPresent(), msg + ": object must exist in both stores: " + name);
            assertObjectsEqual(aMeta.get(), bMeta.get(), msg);
        }
    }

    private static void assertObjectsEqual(ObjectMetadata a, ObjectMetadata b, String msg) {
        assertEquals(a.getObjectName(), b.getObjectName(), msg + ": object name");
        assertEquals(a.getFileSize(), b.getFileSize(), msg + ": file size for " + a.getObjectName());
        assertEquals(a.getChunks().size(), b.getChunks().size(), msg + ": chunk count for " + a.getObjectName());
        for (int i = 0; i < a.getChunks().size(); i++) {
            ChunkInfo ca = a.getChunks().get(i);
            ChunkInfo cb = b.getChunks().get(i);
            assertEquals(ca.getChunkId(), cb.getChunkId(), msg + ": chunk ID");
            assertEquals(ca.getChunkIndex(), cb.getChunkIndex(), msg + ": chunk index");
            assertEquals(ca.getChunkSize(), cb.getChunkSize(), msg + ": chunk size");
            assertEquals(new HashSet<>(ca.getReplicaNodeIds()), new HashSet<>(cb.getReplicaNodeIds()), msg + ": replicas");
            assertEquals(ca.getChecksum(), cb.getChecksum(), msg + ": chunk checksum");
        }
    }

    // ===== TEST 1: SUCCESS PATH =====

    /**
     * TEST: Successful InstallSnapshot survives restart with exact state.
     *
     * OLD STATE → valid multi-chunk snapshot → candidate validation
     * → durable publication → boundary update → success=true
     * → RESTART → installed state recovered exactly
     */
    @Test
    void testSuccessPathSurvivesRestart() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Success Path Survives Restart");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 100;

        Path leaderRaftDir = tempDir.resolve("leader-raft-success");
        Path followerRaftDir = tempDir.resolve("follower-raft-success");
        Path followerSnapDir = followerRaftDir.resolve("snapshots");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);
        Files.createDirectories(followerSnapDir);

        // Setup leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);
        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta-success.json"));
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
        Path followerMetaFile = tempDir.resolve("follower-meta-success.json");
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

            // Leader creates objects
            System.out.println("Creating 10 objects on leader...");
            for (int i = 1; i <= 10; i++) {
                ObjectMetadata obj = makeObject("obj" + i, i * 1000L);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            // Compact log to create snapshot
            leader.compactLog(5);

            // Get snapshot
            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent(), "Leader should have a snapshot");
            var snapshot = snapshotOpt.get();
            byte[] snapshotData = snapshot.stateData();
            int checksum = computeChecksum(snapshotData);

            System.out.println("Installing snapshot index=" + snapshot.lastIncludedIndex() + " on follower...");

            // Install snapshot
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
            System.out.println("InstallSnapshot succeeded, response.bytesAccepted=" + response.bytesAccepted());

            // Verify live state immediately
            assertEquals(10, followerStore.listObjects().size(), "Follower should have 10 objects after install");
            for (int i = 1; i <= 10; i++) {
                assertTrue(followerStore.objectExists("obj" + i), "obj" + i + " should exist");
            }

            // Shutdown follower and restart
            System.out.println("Shutting down follower and restarting...");
            Path followerWalPath = followerRaftDir.resolve("wal.dat");
            Path followerSnapDirPath = followerSnapDir;
            Path followerMetaFilePath = followerMetaFile;
            Path followerStateFile = followerRaftDir.resolve("raft-state.dat");

            follower.stop();

            // Create fresh follower from same directories (simulates restart)
            MetadataStore restartedStore = new MetadataStore(followerMetaFilePath);
            SnapshotManager restartedSnapshotMgr = new SnapshotManager(followerSnapDirPath, restartedStore);
            WAL restartedWal = new WAL(followerWalPath);
            RaftLog restartedLog = new RaftLog(restartedWal);
            RaftNode restartedFollower = new RaftNode(followerConfig, followerRaftDir, restartedLog, restartedWal);
            restartedFollower.setSnapshotManager(restartedSnapshotMgr);
            restartedFollower.setMetadataStore(restartedStore);
            MetadataStateMachine restartedStateMachine = new MetadataStateMachine(restartedStore);
            restartedFollower.setLogEntryApplier(entry -> {
                try { restartedStateMachine.apply(entry); }
                catch (IOException e) { throw new RuntimeException(e); }
            });

            // Verify state after restart
            System.out.println("Verifying state after restart...");
            assertEquals(10, restartedStore.listObjects().size(), "Restarted follower should have 10 objects");
            for (int i = 1; i <= 10; i++) {
                assertTrue(restartedStore.objectExists("obj" + i), "obj" + i + " should exist after restart");
            }

            // Verify exact equality with leader's state
            assertStoresEqual(leaderStore, restartedStore, "Leader and restarted follower must have identical state");

            System.out.println("\n========================================");
            System.out.println("TEST: Success Path Survives Restart - PASSED");
            System.out.println("========================================\n");

        } finally {
            leader.stop();
            leaderThread.interrupt();
        }
    }

    // ===== TEST 2: FAILURE AFTER CANDIDATE RESTORE =====

    /**
     * TEST: Failure after candidate restore does not modify live state.
     *
     * OLD STATE: A B C
     * CANDIDATE: A B C D E
     * Force failure during candidate restore → live state == A B C
     */
    @Test
    void testFailureAfterCandidateRestorePreservesLiveState() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Failure After Candidate Restore Preserves Live State");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 200;

        Path leaderRaftDir = tempDir.resolve("leader-raft-restorefail");
        Path followerRaftDir = tempDir.resolve("follower-raft-restorefail");
        Path followerSnapDir = followerRaftDir.resolve("snapshots");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);
        Files.createDirectories(followerSnapDir);

        // Setup leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);
        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta-restorefail.json"));
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

        // Setup follower with empty store
        ClusterConfig followerConfig = new ClusterConfig("test", "follower", "127.0.0.1", leaderPort + 1, null);
        Path followerMetaFile = tempDir.resolve("follower-meta-restorefail.json");
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

            // Leader creates 5 objects
            System.out.println("Creating 5 objects on leader...");
            for (int i = 1; i <= 5; i++) {
                ObjectMetadata obj = makeObject("obj" + i, i * 1000L);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            leader.compactLog(3);

            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            var snapshot = snapshotOpt.get();
            byte[] snapshotData = snapshot.stateData();
            int checksum = computeChecksum(snapshotData);

            // Corrupt the snapshot data so JSON parsing fails.
            // Strategy: truncate the JSON to cut it mid-value, which will cause a parse error.
            // Appending garbage to valid JSON won't fail (parser stops at valid end).
            int corruptLength = snapshotData.length - 10; // Remove last 10 bytes
            byte[] corruptedData = new byte[corruptLength];
            System.arraycopy(snapshotData, 0, corruptedData, 0, corruptLength);

            System.out.println("Installing CORRUPTED snapshot (should fail during restore)...");
            System.out.println("  Original size: " + snapshotData.length + ", Corrupted size: " + corruptLength);

            // Use checksum of CORRUPTED data so it passes the initial checksum check
            RaftMessage.InstallSnapshot request = new RaftMessage.InstallSnapshot(
                leader.getCurrentTerm(), "leader",
                snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                0, corruptedData, true, computeChecksum(corruptedData)
            );

            RaftMessage.InstallSnapshotResponse response = follower.handleInstallSnapshot(
                request.term(), request.leaderId(),
                request.lastIncludedIndex(), request.lastIncludedTerm(),
                request.offset(), request.data(), request.done(), request.checksum()
            );

            assertFalse(response.success(), "InstallSnapshot should return success=false");
            System.out.println("InstallSnapshot correctly returned success=false");

            // CRITICAL: Live state must be unchanged
            assertTrue(followerStore.listObjects().isEmpty(),
                "Follower store should be EMPTY after failed restore (not modified)");

            // Restart and verify
            System.out.println("Restarting follower to verify old state...");
            follower.stop();

            MetadataStore restartedStore = new MetadataStore(followerMetaFile);
            // Don't set metadataStore or snapshotManager - just load from disk
            // This simulates a node starting fresh

            assertTrue(restartedStore.listObjects().isEmpty(),
                "Restarted follower should have empty state (old state preserved)");

            System.out.println("\n========================================");
            System.out.println("TEST: Failure After Candidate Restore - PASSED");
            System.out.println("========================================\n");

        } finally {
            leader.stop();
            leaderThread.interrupt();
        }
    }

    // ===== TEST 3: FAILURE AFTER LIVE PUBLICATION (THE KEY REGRESSION TEST) =====

    /**
     * TEST: Failure AFTER live state publication but BEFORE snapshot commit
     * must NOT expose partially published state.
     *
     * This is the exact failure scenario described in the bug report:
     * 1. Candidate snapshot persisted
     * 2. Candidate state restored
     * 3. publishCandidate() modifies live store ← BUG: happens before commit
     * 4. commitCandidateSnapshot() FAILS ← The failure injection point
     * 5. Returns success=false
     *
     * Expected: Live state == OLD STATE, not candidate state.
     */
    @Test
    void testFailureAfterLivePublicationPreservesOldState() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Failure After Live Publication (KEY REGRESSION)");
        System.out.println("========================================\n");

        // This test uses a custom SnapshotManager that throws during commitCandidateSnapshot
        // to simulate failure exactly between publishCandidate and commitCandidateSnapshot.

        int leaderPort = BASE_PORT + 300;

        Path leaderRaftDir = tempDir.resolve("leader-raft-pubfail");
        Path followerRaftDir = tempDir.resolve("follower-raft-pubfail");
        Path followerSnapDir = followerRaftDir.resolve("snapshots");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);
        Files.createDirectories(followerSnapDir);

        // Setup leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);
        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta-pubfail.json"));
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

        // Setup follower with failing SnapshotManager
        ClusterConfig followerConfig = new ClusterConfig("test", "follower", "127.0.0.1", leaderPort + 1, null);
        Path followerMetaFile = tempDir.resolve("follower-meta-pubfail.json");
        MetadataStore followerStore = new MetadataStore(followerMetaFile);

        // Create a SnapshotManager that will fail on commitCandidateSnapshot
        FailingSnapshotManager followerSnapshotMgr = new FailingSnapshotManager(
            followerSnapDir, followerStore, true); // failOnCommit = true
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

            // Leader creates 5 objects
            System.out.println("Creating 5 objects on leader...");
            for (int i = 1; i <= 5; i++) {
                ObjectMetadata obj = makeObject("obj" + i, i * 1000L);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            leader.compactLog(3);

            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            var snapshot = snapshotOpt.get();
            byte[] snapshotData = snapshot.stateData();
            int checksum = computeChecksum(snapshotData);

            System.out.println("Installing snapshot (will fail during commitCandidateSnapshot)...");

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

            assertFalse(response.success(), "InstallSnapshot should return success=false after commit failure");
            System.out.println("InstallSnapshot correctly returned success=false");

            // CRITICAL CHECK: Live state must NOT have been changed
            // If the bug exists, followerStore will have 5 objects (the candidate state)
            // If fixed, followerStore will be empty (the old state)
            int liveObjectCount = followerStore.listObjects().size();
            System.out.println("Live state object count after failed install: " + liveObjectCount);

            // The old state was empty, so we expect 0 objects
            assertEquals(0, liveObjectCount,
                "Live state must be UNCHANGED (empty) after failed install, not partially modified to candidate state");

            // Restart and verify old state is recovered
            System.out.println("Restarting follower to verify old state recovery...");
            follower.stop();

            MetadataStore restartedStore = new MetadataStore(followerMetaFile);
            assertEquals(0, restartedStore.listObjects().size(),
                "Restarted follower should have empty state (old state recovered)");

            // Verify old snapshot is still valid
            var latestSnapshot = followerSnapshotMgr.loadLatestSnapshot();
            // After failed install, the old snapshot (if any) should still be loadable
            // OR there should be no new snapshot at all
            System.out.println("Old snapshot check: " + (latestSnapshot.isPresent() ? "present" : "not present"));
            // We don't assert on this - the important thing is the live state

            System.out.println("\n========================================");
            System.out.println("TEST: Failure After Live Publication - PASSED");
            System.out.println("========================================\n");

        } finally {
            leader.stop();
            leaderThread.interrupt();
        }
    }

    // ===== TEST 4: BOUNDARY NOT ADVANCED ON FAILURE =====

    /**
     * TEST: Failed InstallSnapshot must NOT advance the Raft snapshot boundary.
     */
    @Test
    void testFailedInstallDoesNotAdvanceBoundary() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Failed Install Does Not Advance Boundary");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 400;

        Path leaderRaftDir = tempDir.resolve("leader-raft-boundary");
        Path followerRaftDir = tempDir.resolve("follower-raft-boundary");
        Path followerSnapDir = followerRaftDir.resolve("snapshots");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);
        Files.createDirectories(followerSnapDir);

        // Setup leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);
        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta-boundary.json"));
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

        // Setup follower with failing SnapshotManager
        ClusterConfig followerConfig = new ClusterConfig("test", "follower", "127.0.0.1", leaderPort + 1, null);
        Path followerMetaFile = tempDir.resolve("follower-meta-boundary.json");
        MetadataStore followerStore = new MetadataStore(followerMetaFile);
        FailingSnapshotManager followerSnapshotMgr = new FailingSnapshotManager(
            followerSnapDir, followerStore, true);
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

        // Set initial boundary on follower
        long initialIndex = 20;
        long initialTerm = 5;
        followerLog.setSnapshotBoundary(initialIndex, initialTerm);
        System.out.println("Follower initial boundary: index=" + initialIndex + ", term=" + initialTerm);

        Thread leaderThread = startNode(leader);

        try {
            waitForLeader(leader, 5000);

            // Leader creates objects and takes snapshot
            for (int i = 1; i <= 5; i++) {
                ObjectMetadata obj = makeObject("obj" + i, i * 1000L);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);
            leader.compactLog(3);

            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            var snapshot = snapshotOpt.get();
            byte[] snapshotData = snapshot.stateData();
            int checksum = computeChecksum(snapshotData);

            System.out.println("Attempting install of snapshot index=" + snapshot.lastIncludedIndex() +
                " (should fail, boundary should stay at " + initialIndex + ")...");

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

            assertFalse(response.success());

            // Verify boundary is unchanged
            long afterIndex = followerLog.getLogStartIndex() - 1;
            long afterTerm = followerLog.getSnapshotTerm();
            System.out.println("Follower boundary after failed install: index=" + afterIndex + ", term=" + afterTerm);

            assertEquals(initialIndex, afterIndex, "Snapshot boundary index must NOT change on failure");
            assertEquals(initialTerm, afterTerm, "Snapshot boundary term must NOT change on failure");

            System.out.println("\n========================================");
            System.out.println("TEST: Failed Install Does Not Advance Boundary - PASSED");
            System.out.println("========================================\n");

        } finally {
            leader.stop();
            leaderThread.interrupt();
        }
    }

    // ===== TEST 5: SECOND RESTART =====

    /**
     * TEST: After successful install, two consecutive restarts must yield exact same state.
     */
    @Test
    void testSecondRestartPreservesExactState() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Second Restart Preserves Exact State");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 500;

        Path leaderRaftDir = tempDir.resolve("leader-raft-double");
        Path followerRaftDir = tempDir.resolve("follower-raft-double");
        Path followerSnapDir = followerRaftDir.resolve("snapshots");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);
        Files.createDirectories(followerSnapDir);

        // Setup leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);
        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta-double.json"));
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
        Path followerMetaFile = tempDir.resolve("follower-meta-double.json");
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

            // Leader creates objects
            for (int i = 1; i <= 8; i++) {
                ObjectMetadata obj = makeObject("restart-obj" + i, i * 500L);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);
            leader.compactLog(4);

            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            var snapshot = snapshotOpt.get();
            byte[] snapshotData = snapshot.stateData();
            int checksum = computeChecksum(snapshotData);

            // Install
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
            assertTrue(response.success());

            // First restart
            System.out.println("First restart...");
            follower.stop();
            Path snapDir = followerSnapDir;
            Path walPath = followerRaftDir.resolve("wal.dat");
            MetadataStore store1 = new MetadataStore(followerMetaFile);
            SnapshotManager snapMgr1 = new SnapshotManager(snapDir, store1);
            WAL wal1 = new WAL(walPath);
            RaftNode r1 = new RaftNode(followerConfig, followerRaftDir, new RaftLog(wal1), wal1);
            r1.setSnapshotManager(snapMgr1);
            r1.setMetadataStore(store1);
            assertEquals(8, store1.listObjects().size());

            // Second restart
            System.out.println("Second restart...");
            r1.stop();
            MetadataStore store2 = new MetadataStore(followerMetaFile);
            assertEquals(8, store2.listObjects().size(), "State must be identical after second restart");

            // Verify exact equality
            assertStoresEqual(store1, store2, "First and second restart must have identical state");

            System.out.println("\n========================================");
            System.out.println("TEST: Second Restart - PASSED");
            System.out.println("========================================\n");

        } finally {
            leader.stop();
            leaderThread.interrupt();
        }
    }

    // ===== HELPER CLASSES =====

    /**
     * A SnapshotManager wrapper that injects failures at configurable points.
     * Used to test failure scenarios deterministically.
     */
    private static class FailingSnapshotManager extends SnapshotManager {
        private final boolean failOnCommit;
        private volatile RuntimeException commitFailure;

        public FailingSnapshotManager(Path snapshotDir, MetadataStore store, boolean failOnCommit) {
            super(snapshotDir, store);
            this.failOnCommit = failOnCommit;
        }

        public void setCommitFailure(RuntimeException e) {
            this.commitFailure = e;
        }

        @Override
        public void commitCandidateSnapshot(Path candidateFile, long lastIncludedIndex, long lastIncludedTerm) throws IOException {
            if (failOnCommit) {
                throw new IOException("Simulated commit failure for atomicity test");
            }
            super.commitCandidateSnapshot(candidateFile, lastIncludedIndex, lastIncludedTerm);
        }
    }

    // ===== UTILITY METHODS =====

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
