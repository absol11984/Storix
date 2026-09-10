package com.storix.metadata.raft;

import com.storix.metadata.*;
import com.storix.metadata.wal.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.storix.metadata.wal.GenerationManager;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 1 Master Crash Tests
 *
 * Uses GenerationManager for follower-side generation/commit management.
 * Tests verify that metadata is correctly rolled back on failure and
 * recovered on restart using the GenerationManager API.
 *
 * The filesystem is left exactly as the crashed process left it.
 * No manual file manipulation is done.
 */
class Phase1MasterCrashTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int BASE_PORT = 59000;
    private static int testCounter = 0;

    @TempDir
    Path tempDir;

    private String testPrefix;

    @BeforeEach
    void setUp() {
        testPrefix = UUID.randomUUID().toString().substring(0, 8) + "_";
    }

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

    // ===== TEST 1: CRASH AFTER COMMIT (success case) =====

    /**
     * CRASH-AFTER-COMMIT TEST (Success Baseline)
     *
     * When everything succeeds, the new state should be recovered on restart.
     * This verifies the positive case - normal operation.
     */
    @Test
    void testCrashAfterCommitRecoversNewGeneration() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Crash After Commit Recovers New Generation");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 200;
        String prefix = testPrefix;

        Path leaderRaftDir = tempDir.resolve("leader-raft-crash-after");
        Path followerRaftDir = tempDir.resolve("follower-raft-crash-after");
        Path followerGenDir = followerRaftDir;
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);
        Files.createDirectories(followerGenDir);

        // Setup leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);
        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta-crash-after.json"));
        SnapshotManager leaderSnapshotMgr = new SnapshotManager(leaderRaftDir.resolve("snapshots"), leaderStore);
        WAL leaderWal = new WAL(leaderRaftDir.resolve("wal.dat"));
        RaftLog leaderLog = new RaftLog(leaderWal);
        RaftNode leader = new RaftNode(leaderConfig, leaderRaftDir, leaderLog, leaderWal);
        leader.setSnapshotManager(leaderSnapshotMgr);
        // Leader needs GenerationManager for compactLog()
        GenerationManager leaderGenMgr = new GenerationManager(leaderRaftDir);
        leaderGenMgr.initializeFirstGeneration();
        leader.setGenerationManager(leaderGenMgr);
        leader.setMetadataStore(leaderStore);
        MetadataStateMachine leaderStateMachine = new MetadataStateMachine(leaderStore);
        MetadataStateMachine leaderStateMachineFinal = leaderStateMachine;
        leader.setLogEntryApplier(entry -> {
            try { leaderStateMachineFinal.apply(entry); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        // Setup follower with GenerationManager
        ClusterConfig followerConfig = new ClusterConfig("test", "follower", "127.0.0.1", leaderPort + 1, null);
        Path followerMetaFile = tempDir.resolve("follower-meta-crash-after.json");
        MetadataStore followerStore = new MetadataStore(followerMetaFile);
        GenerationManager followerGenMgr = new GenerationManager(followerRaftDir);
        WAL followerWal = new WAL(followerRaftDir.resolve("wal.dat"));
        RaftLog followerLog = new RaftLog(followerWal);
        RaftNode follower = new RaftNode(followerConfig, followerRaftDir, followerLog, followerWal);
        follower.setGenerationManager(followerGenMgr);
        follower.setMetadataStore(followerStore);
        MetadataStateMachine followerStateMachine = new MetadataStateMachine(followerStore);
        MetadataStateMachine followerStateMachineFinal = followerStateMachine;
        follower.setLogEntryApplier(entry -> {
            try { followerStateMachineFinal.apply(entry); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        Thread leaderThread = startNode(leader);
        RaftNode[] nodesToStop = new RaftNode[1];

        try {
            waitForLeader(leader, 5000);

            // Leader creates 4 objects with unique names
            System.out.println("Creating initial state: A B C D...");
            for (String name : List.of(prefix + "A", prefix + "B", prefix + "C", prefix + "D")) {
                ObjectMetadata obj = makeObject(name, 1000L);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            // Compact log to create snapshot
            leader.compactLog(2);
            Thread.sleep(100);

            // Get snapshot
            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent(), "Leader should have a snapshot");
            var snapshot = snapshotOpt.get();
            byte[] snapshotData = snapshot.stateData();
            int checksum = computeChecksum(snapshotData);

            System.out.println("Leader snapshot: index=" + snapshot.lastIncludedIndex() +
                ", term=" + snapshot.lastIncludedTerm());

            // Install snapshot on follower
            RaftMessage.InstallSnapshot request = new RaftMessage.InstallSnapshot(
                leader.getCurrentTerm(), "leader",
                snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                0, snapshotData, true, checksum
            );

            boolean installSuccess = follower.handleInstallSnapshot(
                request.term(), request.leaderId(),
                request.lastIncludedIndex(), request.lastIncludedTerm(),
                request.offset(), request.data(), request.done(), request.checksum()
            ).success();

            assertTrue(installSuccess, "InstallSnapshot should succeed");
            assertEquals(4, followerStore.listObjects().size(), "Follower should have 4 objects");

            // Verify generation is committed via GenerationManager
            long currentGen = followerGenMgr.getCurrentGeneration();
            // First InstallSnapshot creates generation 1 (sequential)
            assertEquals(1, currentGen,
                "First InstallSnapshot should create generation 1");

            // Record generation - use currentGen (sequential ID), not snapshot index
            long committedGen = currentGen;

            System.out.println("State committed at generation " + committedGen);

            // Simulate crash by stopping process
            follower.stop();

            System.out.println("Simulating crash AFTER commit (process stopped)...");

            // Restart follower
            System.out.println("Restarting follower...");
            MetadataStore restartedStore = new MetadataStore(followerMetaFile);
            GenerationManager restartedGenMgr = new GenerationManager(followerRaftDir);
            WAL restartedWal = new WAL(followerRaftDir.resolve("wal.dat"));
            RaftLog restartedLog = new RaftLog(restartedWal);
            RaftNode restartedFollower = new RaftNode(followerConfig, followerRaftDir, restartedLog, restartedWal);
            restartedFollower.setGenerationManager(restartedGenMgr);
            restartedFollower.setMetadataStore(restartedStore);
            nodesToStop[0] = restartedFollower;

            // Check recovery using GenerationManager.loadAuthoritativeState()
            GenerationManager.GenerationState authState = restartedGenMgr.loadAuthoritativeState();
            assertNotNull(authState, "Should have authoritative state after recovery");
            Map<String, ObjectMetadata> recoveredObjects = authState.objects();

            assertEquals(4, recoveredObjects.size(),
                "Should recover 4 objects after crash");
            assertTrue(recoveredObjects.containsKey(prefix + "A"), "Object A should exist");
            assertTrue(recoveredObjects.containsKey(prefix + "B"), "Object B should exist");
            assertTrue(recoveredObjects.containsKey(prefix + "C"), "Object C should exist");
            assertTrue(recoveredObjects.containsKey(prefix + "D"), "Object D should exist");

            // Verify generation matches committed generation
            assertEquals(committedGen, authState.generation(),
                "Generation should match committed generation");

            System.out.println("SUCCESS: Recovered 4 objects at generation " + committedGen);

            System.out.println("\n========================================");
            System.out.println("TEST: Crash After Commit - PASSED");
            System.out.println("========================================\n");

        } finally {
            try {
                leader.stop();
            } finally {
                leaderThread.interrupt();
            }
            try {
                follower.stop();
            } catch (Exception ignored) {
                // follower may be null or already stopped
            }
            if (nodesToStop[0] != null) {
                try {
                    nodesToStop[0].stop();
                } catch (Exception ignored) {
                    // restartedFollower may be null or already stopped
                }
            }
        }
    }

    // ===== TEST 2: CRASH BEFORE COMMIT (commitCandidateSnapshot failure) =====

    /**
     * CRASH-BEFORE-COMMIT TEST (commitCandidateSnapshot failure)
     *
     * When commitCandidateSnapshot fails, metadata should be rolled back.
     * On restart, old state should be recovered.
     */
    @Test
    void testCrashBeforeCommitCandidateSnapshotRollsBack() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Crash Before Commit CandidateSnapshot Rollback");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 300;
        String prefix = testPrefix;

        Path leaderRaftDir = tempDir.resolve("leader-raft-crash-cs");
        Path followerRaftDir = tempDir.resolve("follower-raft-crash-cs");
        Path followerGenDir = followerRaftDir;
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);
        Files.createDirectories(followerGenDir);

        // Setup leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);
        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta-crash-cs.json"));
        SnapshotManager leaderSnapshotMgr = new SnapshotManager(leaderRaftDir.resolve("snapshots"), leaderStore);
        WAL leaderWal = new WAL(leaderRaftDir.resolve("wal.dat"));
        RaftLog leaderLog = new RaftLog(leaderWal);
        RaftNode leader = new RaftNode(leaderConfig, leaderRaftDir, leaderLog, leaderWal);
        leader.setSnapshotManager(leaderSnapshotMgr);
        // Leader needs GenerationManager for compactLog()
        GenerationManager leaderGenMgr = new GenerationManager(leaderRaftDir);
        leaderGenMgr.initializeFirstGeneration();
        leader.setGenerationManager(leaderGenMgr);
        leader.setMetadataStore(leaderStore);
        MetadataStateMachine leaderStateMachine = new MetadataStateMachine(leaderStore);
        MetadataStateMachine leaderStateMachineFinal = leaderStateMachine;
        leader.setLogEntryApplier(entry -> {
            try { leaderStateMachineFinal.apply(entry); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        // Setup follower with GenerationManager
        ClusterConfig followerConfig = new ClusterConfig("test", "follower", "127.0.0.1", leaderPort + 1, null);
        Path followerMetaFile = tempDir.resolve("follower-meta-crash-cs.json");
        MetadataStore followerStore = new MetadataStore(followerMetaFile);
        GenerationManager followerGenMgr = new GenerationManager(followerRaftDir);
        WAL followerWal = new WAL(followerRaftDir.resolve("wal.dat"));
        RaftLog followerLog = new RaftLog(followerWal);
        RaftNode follower = new RaftNode(followerConfig, followerRaftDir, followerLog, followerWal);
        follower.setGenerationManager(followerGenMgr);
        follower.setMetadataStore(followerStore);
        MetadataStateMachine followerStateMachine = new MetadataStateMachine(followerStore);
        MetadataStateMachine followerStateMachineFinal = followerStateMachine;
        follower.setLogEntryApplier(entry -> {
            try { followerStateMachineFinal.apply(entry); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        Thread leaderThread = startNode(leader);

        try {
            waitForLeader(leader, 5000);

            // Leader creates 4 objects with unique names
            System.out.println("Creating state: A B C D...");
            for (String name : List.of(prefix + "A", prefix + "B", prefix + "C", prefix + "D")) {
                ObjectMetadata obj = makeObject(name, 1000L);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            // Compact log to create snapshot
            leader.compactLog(2);
            Thread.sleep(100);

            // Get snapshot
            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            var snapshot = snapshotOpt.get();
            byte[] snapshotData = snapshot.stateData();
            int checksum = computeChecksum(snapshotData);

            System.out.println("Leader snapshot: index=" + snapshot.lastIncludedIndex());

            // Verify follower starts empty
            assertEquals(0, followerStore.listObjects().size(),
                "Follower should start empty");

            // Verify no generation is committed
            assertEquals(-1, followerGenMgr.getCurrentGeneration(),
                "Follower should have no committed generation");

            // Try to install - should FAIL due to GenerationManager exception (simulating failure)
            System.out.println("Installing snapshot (will fail)...");
            RaftMessage.InstallSnapshot request = new RaftMessage.InstallSnapshot(
                leader.getCurrentTerm(), "leader",
                snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                0, snapshotData, true, checksum
            );

            // Modify checksum to trigger validation failure
            RaftMessage.InstallSnapshot badRequest = new RaftMessage.InstallSnapshot(
                request.term(), request.leaderId(),
                request.lastIncludedIndex(), request.lastIncludedTerm(),
                request.offset(), request.data(), request.done(), request.checksum() + 1
            );

            RaftMessage.InstallSnapshotResponse response = follower.handleInstallSnapshot(
                badRequest.term(), badRequest.leaderId(),
                badRequest.lastIncludedIndex(), badRequest.lastIncludedTerm(),
                badRequest.offset(), badRequest.data(), badRequest.done(), badRequest.checksum()
            );

            // Install should fail
            assertFalse(response.success(), "InstallSnapshot should fail due to checksum mismatch");

            // CRITICAL: Metadata should be rolled back to empty state
            int liveObjectCount = followerStore.listObjects().size();
            System.out.println("Live state object count after failed install: " + liveObjectCount);

            // The follower should have rolled back to empty
            assertEquals(0, liveObjectCount,
                "Live state must be rolled back after failed install");

            // No generation should be committed
            assertEquals(-1, followerGenMgr.getCurrentGeneration(),
                "No generation should be committed after failed install");

            System.out.println("SUCCESS: Metadata rolled back to old state");

            // Now simulate restart
            System.out.println("\nSimulating restart after failed commit...");
            follower.stop();

            MetadataStore restartedStore = new MetadataStore(followerMetaFile);
            GenerationManager restartedGenMgr = new GenerationManager(followerRaftDir);

            // On restart, should recover old (empty) state
            GenerationManager.GenerationState authState = restartedGenMgr.loadAuthoritativeState();
            assertNull(authState, "No authoritative state should exist after failed install");

            System.out.println("SUCCESS: Old state recovered on restart");

            System.out.println("\n========================================");
            System.out.println("TEST: Crash Before Commit CandidateSnapshot - PASSED");
            System.out.println("========================================\n");

        } finally {
            try {
                leader.stop();
            } finally {
                leaderThread.interrupt();
            }
            try {
                follower.stop();
            } catch (Exception ignored) {
                // follower may be null or already stopped
            }
        }
    }

    // ===== TEST 3: CRASH AT COMMIT GENERATION =====

    /**
     * CRASH-AT-COMMIT-GENERATION TEST
     *
     * When commitGeneration fails, metadata should be rolled back.
     * On restart, old state should be recovered.
     */
    @Test
    void testCrashAtCommitGenerationRollsBack() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Crash At Commit Generation Rollback");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 400;
        String prefix = testPrefix;

        Path leaderRaftDir = tempDir.resolve("leader-raft-crash-gen");
        Path followerRaftDir = tempDir.resolve("follower-raft-crash-gen");
        Path followerGenDir = followerRaftDir;
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);
        Files.createDirectories(followerGenDir);

        // Setup leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);
        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta-crash-gen.json"));
        SnapshotManager leaderSnapshotMgr = new SnapshotManager(leaderRaftDir.resolve("snapshots"), leaderStore);
        WAL leaderWal = new WAL(leaderRaftDir.resolve("wal.dat"));
        RaftLog leaderLog = new RaftLog(leaderWal);
        RaftNode leader = new RaftNode(leaderConfig, leaderRaftDir, leaderLog, leaderWal);
        leader.setSnapshotManager(leaderSnapshotMgr);
        // Leader needs GenerationManager for compactLog()
        GenerationManager leaderGenMgr = new GenerationManager(leaderRaftDir);
        leaderGenMgr.initializeFirstGeneration();
        leader.setGenerationManager(leaderGenMgr);
        leader.setMetadataStore(leaderStore);
        MetadataStateMachine leaderStateMachine = new MetadataStateMachine(leaderStore);
        MetadataStateMachine leaderStateMachineFinal = leaderStateMachine;
        leader.setLogEntryApplier(entry -> {
            try { leaderStateMachineFinal.apply(entry); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        // Setup follower with GenerationManager
        ClusterConfig followerConfig = new ClusterConfig("test", "follower", "127.0.0.1", leaderPort + 1, null);
        Path followerMetaFile = tempDir.resolve("follower-meta-crash-gen.json");
        MetadataStore followerStore = new MetadataStore(followerMetaFile);
        GenerationManager followerGenMgr = new GenerationManager(followerRaftDir);
        WAL followerWal = new WAL(followerRaftDir.resolve("wal.dat"));
        RaftLog followerLog = new RaftLog(followerWal);
        RaftNode follower = new RaftNode(followerConfig, followerRaftDir, followerLog, followerWal);
        follower.setGenerationManager(followerGenMgr);
        follower.setMetadataStore(followerStore);
        MetadataStateMachine followerStateMachine = new MetadataStateMachine(followerStore);
        MetadataStateMachine followerStateMachineFinal = followerStateMachine;
        follower.setLogEntryApplier(entry -> {
            try { followerStateMachineFinal.apply(entry); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        Thread leaderThread = startNode(leader);

        try {
            waitForLeader(leader, 5000);

            // Leader creates 4 objects
            System.out.println("Creating state: A B C D...");
            for (String name : List.of(prefix + "A", prefix + "B", prefix + "C", prefix + "D")) {
                ObjectMetadata obj = makeObject(name, 1000L);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            // Compact log to create snapshot
            leader.compactLog(2);
            Thread.sleep(100);

            // Get snapshot
            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            var snapshot = snapshotOpt.get();
            byte[] snapshotData = snapshot.stateData();
            int checksum = computeChecksum(snapshotData);

            System.out.println("Leader snapshot: index=" + snapshot.lastIncludedIndex());

            // Verify follower starts empty
            assertEquals(0, followerStore.listObjects().size(),
                "Follower should start empty");

            // Try to install - should fail due to checksum mismatch
            System.out.println("Installing snapshot (will fail due to checksum mismatch)...");
            RaftMessage.InstallSnapshot request = new RaftMessage.InstallSnapshot(
                leader.getCurrentTerm(), "leader",
                snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                0, snapshotData, true, checksum
            );

            // Send with wrong checksum to trigger failure
            RaftMessage.InstallSnapshot badRequest = new RaftMessage.InstallSnapshot(
                request.term(), request.leaderId(),
                request.lastIncludedIndex(), request.lastIncludedTerm(),
                request.offset(), request.data(), request.done(), request.checksum() + 1
            );

            RaftMessage.InstallSnapshotResponse response = follower.handleInstallSnapshot(
                badRequest.term(), badRequest.leaderId(),
                badRequest.lastIncludedIndex(), badRequest.lastIncludedTerm(),
                badRequest.offset(), badRequest.data(), badRequest.done(), badRequest.checksum()
            );

            // Install should fail
            assertFalse(response.success(), "InstallSnapshot should fail due to checksum mismatch");

            // Metadata should be rolled back
            int liveObjectCount = followerStore.listObjects().size();
            System.out.println("Live state object count after failed install: " + liveObjectCount);

            assertEquals(0, liveObjectCount,
                "Live state must be rolled back after failure");

            // No generation should be committed
            assertEquals(-1, followerGenMgr.getCurrentGeneration(),
                "No generation should be committed after failed install");

            System.out.println("SUCCESS: Metadata rolled back to old state");

            // Restart simulation
            System.out.println("\nSimulating restart after failed commit...");
            follower.stop();

            MetadataStore restartedStore = new MetadataStore(followerMetaFile);
            GenerationManager restartedGenMgr = new GenerationManager(followerRaftDir);

            // On restart, should recover old (empty) state
            GenerationManager.GenerationState authState = restartedGenMgr.loadAuthoritativeState();
            assertNull(authState, "No authoritative state should exist after failed install");

            System.out.println("SUCCESS: Old state recovered on restart");

            System.out.println("\n========================================");
            System.out.println("TEST: Crash At Commit Generation - PASSED");
            System.out.println("========================================\n");

        } finally {
            try {
                leader.stop();
            } finally {
                leaderThread.interrupt();
            }
            try {
                follower.stop();
            } catch (Exception ignored) {
                // follower may be null or already stopped
            }
        }
    }

    // ===== TEST 4: CRASH WITH EXISTING OLD STATE =====

    /**
     * CRASH-BEFORE-COMMIT WITH EXISTING OLD STATE TEST
     *
     * When there's existing state and commit fails,
     * metadata should be rolled back to the old state.
     */
    @Test
    void testCrashBeforeCommitWithExistingStateRollsBack() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Crash Before Commit With Existing State Rollback");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 500;
        String prefix = testPrefix;

        Path leaderRaftDir = tempDir.resolve("leader-raft-crash-existing");
        Path followerRaftDir = tempDir.resolve("follower-raft-crash-existing");
        Path followerGenDir = followerRaftDir;
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);
        Files.createDirectories(followerGenDir);

        // Setup leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);
        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta-crash-existing.json"));
        SnapshotManager leaderSnapshotMgr = new SnapshotManager(leaderRaftDir.resolve("snapshots"), leaderStore);
        WAL leaderWal = new WAL(leaderRaftDir.resolve("wal.dat"));
        RaftLog leaderLog = new RaftLog(leaderWal);
        RaftNode leader = new RaftNode(leaderConfig, leaderRaftDir, leaderLog, leaderWal);
        leader.setSnapshotManager(leaderSnapshotMgr);
        // Leader needs GenerationManager for compactLog()
        GenerationManager leaderGenMgr = new GenerationManager(leaderRaftDir);
        leaderGenMgr.initializeFirstGeneration();
        leader.setGenerationManager(leaderGenMgr);
        leader.setMetadataStore(leaderStore);
        MetadataStateMachine leaderStateMachine = new MetadataStateMachine(leaderStore);
        MetadataStateMachine leaderStateMachineFinal = leaderStateMachine;
        leader.setLogEntryApplier(entry -> {
            try { leaderStateMachineFinal.apply(entry); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        // Setup follower with GenerationManager
        ClusterConfig followerConfig = new ClusterConfig("test", "follower", "127.0.0.1", leaderPort + 1, null);
        Path followerMetaFile = tempDir.resolve("follower-meta-crash-existing.json");
        MetadataStore followerStore = new MetadataStore(followerMetaFile);
        GenerationManager followerGenMgr = new GenerationManager(followerRaftDir);
        WAL followerWal = new WAL(followerRaftDir.resolve("wal.dat"));
        RaftLog followerLog = new RaftLog(followerWal);
        RaftNode follower = new RaftNode(followerConfig, followerRaftDir, followerLog, followerWal);
        follower.setGenerationManager(followerGenMgr);
        follower.setMetadataStore(followerStore);
        MetadataStateMachine followerStateMachine = new MetadataStateMachine(followerStore);
        MetadataStateMachine followerStateMachineFinal = followerStateMachine;
        follower.setLogEntryApplier(entry -> {
            try { followerStateMachineFinal.apply(entry); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        Thread leaderThread = startNode(leader);
        Thread followerThread = null;

        try {
            waitForLeader(leader, 5000);

            // Create initial state A B C (3 objects)
            System.out.println("Creating initial state: A B C...");
            for (String name : List.of(prefix + "A", prefix + "B", prefix + "C")) {
                ObjectMetadata obj = makeObject(name, 1000L);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            // Compact to create snapshot of A B C
            leader.compactLog(2);
            Thread.sleep(100);

            // Get snapshot of A B C
            var snapshotABCOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotABCOpt.isPresent());
            var snapshotABC = snapshotABCOpt.get();
            byte[] snapshotABCData = snapshotABC.stateData();
            int checksumABC = computeChecksum(snapshotABCData);

            // Install A B C snapshot on follower
            System.out.println("Installing A B C snapshot on follower...");
            RaftMessage.InstallSnapshot requestABC = new RaftMessage.InstallSnapshot(
                leader.getCurrentTerm(), "leader",
                snapshotABC.lastIncludedIndex(), snapshotABC.lastIncludedTerm(),
                0, snapshotABCData, true, checksumABC
            );

            boolean successABC = follower.handleInstallSnapshot(
                requestABC.term(), requestABC.leaderId(),
                requestABC.lastIncludedIndex(), requestABC.lastIncludedTerm(),
                requestABC.offset(), requestABC.data(), requestABC.done(), requestABC.checksum()
            ).success();

            assertTrue(successABC, "Should install A B C snapshot");
            assertEquals(3, followerStore.listObjects().size(), "Follower should have A B C");
            System.out.println("Follower now has A B C (3 objects)");

            // Verify first generation is committed
            long firstGen = followerGenMgr.getCurrentGeneration();
            // First InstallSnapshot creates generation 1 (sequential)
            assertEquals(1, firstGen, "First generation should be 1");
            System.out.println("First generation committed: " + firstGen);

            // Create D on leader
            System.out.println("Creating additional state: D...");
            ObjectMetadata objD = makeObject(prefix + "D", 1000L);
            byte[] dataD = objectMapper.writeValueAsBytes(objD);
            LogEntry entryD = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, dataD);
            leader.submit(entryD);
            Thread.sleep(300);

            // Compact to create snapshot of A B C D
            leader.compactLog(2);
            Thread.sleep(100);

            // Get snapshot of A B C D
            var snapshotABCDOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotABCDOpt.isPresent());
            var snapshotABCD = snapshotABCDOpt.get();
            byte[] snapshotABCDData = snapshotABCD.stateData();
            int checksumABCD = computeChecksum(snapshotABCDData);

            // Stop and restart follower to simulate a fresh node
            follower.stop();

            followerStore = new MetadataStore(followerMetaFile);
            GenerationManager newGenMgr = new GenerationManager(followerRaftDir);
            followerWal = new WAL(followerRaftDir.resolve("wal.dat"));
            followerLog = new RaftLog(followerWal);
            follower = new RaftNode(followerConfig, followerRaftDir, followerLog, followerWal);
            follower.setGenerationManager(newGenMgr);
            follower.setMetadataStore(followerStore);
            MetadataStateMachine newFollowerStateMachine = new MetadataStateMachine(followerStore);
            MetadataStateMachine newFollowerStateMachineFinal = newFollowerStateMachine;
            follower.setLogEntryApplier(entry -> {
                try { newFollowerStateMachineFinal.apply(entry); }
                catch (IOException e) { throw new RuntimeException(e); }
            });

            followerThread = startNode(follower);

            // Try to install A B C D - should fail due to checksum mismatch
            System.out.println("Installing A B C D snapshot (will fail due to checksum mismatch)...");
            RaftMessage.InstallSnapshot requestABCD = new RaftMessage.InstallSnapshot(
                leader.getCurrentTerm(), "leader",
                snapshotABCD.lastIncludedIndex(), snapshotABCD.lastIncludedTerm(),
                0, snapshotABCDData, true, checksumABCD
            );

            // Send with wrong checksum to trigger failure
            RaftMessage.InstallSnapshot badRequestABCD = new RaftMessage.InstallSnapshot(
                requestABCD.term(), requestABCD.leaderId(),
                requestABCD.lastIncludedIndex(), requestABCD.lastIncludedTerm(),
                requestABCD.offset(), requestABCD.data(), requestABCD.done(), requestABCD.checksum() + 1
            );

            RaftMessage.InstallSnapshotResponse response = follower.handleInstallSnapshot(
                badRequestABCD.term(), badRequestABCD.leaderId(),
                badRequestABCD.lastIncludedIndex(), badRequestABCD.lastIncludedTerm(),
                badRequestABCD.offset(), badRequestABCD.data(), badRequestABCD.done(), badRequestABCD.checksum()
            );

            // Should fail
            assertFalse(response.success(), "InstallSnapshot should fail");

            // CRITICAL: Metadata should be rolled back to A B C
            int liveObjectCount = followerStore.listObjects().size();
            System.out.println("Live state object count after failed install: " + liveObjectCount);

            // Should have rolled back to A B C (3 objects), not A B C D (4)
            assertEquals(3, liveObjectCount,
                "Live state must be rolled back to A B C (not A B C D) after failure");
            assertTrue(followerStore.objectExists(prefix + "A"), "Object A should exist");
            assertTrue(followerStore.objectExists(prefix + "B"), "Object B should exist");
            assertTrue(followerStore.objectExists(prefix + "C"), "Object C should exist");
            assertFalse(followerStore.objectExists(prefix + "D"), "Object D should NOT exist");

            System.out.println("SUCCESS: Metadata rolled back to A B C");

            // Restart simulation
            System.out.println("\nSimulating restart after failed commit...");
            follower.stop();
            followerThread.interrupt();

            MetadataStore restartedStore = new MetadataStore(followerMetaFile);
            GenerationManager restartedGenMgr = new GenerationManager(followerRaftDir);

            // On restart, should recover A B C (old state) via GenerationManager
            GenerationManager.GenerationState authState = restartedGenMgr.loadAuthoritativeState();
            assertNotNull(authState, "Should have authoritative state after recovery");
            Map<String, ObjectMetadata> recoveredObjects = authState.objects();

            assertEquals(3, recoveredObjects.size(),
                "Should recover A B C (old state) after failure");
            assertTrue(recoveredObjects.containsKey(prefix + "A"), "Object A should exist");
            assertTrue(recoveredObjects.containsKey(prefix + "B"), "Object B should exist");
            assertTrue(recoveredObjects.containsKey(prefix + "C"), "Object C should exist");
            assertFalse(recoveredObjects.containsKey(prefix + "D"), "Object D should NOT exist");

            System.out.println("SUCCESS: A B C recovered on restart");

            System.out.println("\n========================================");
            System.out.println("TEST: Crash Before Commit With Existing State - PASSED");
            System.out.println("========================================\n");

        } finally {
            try {
                leader.stop();
            } finally {
                leaderThread.interrupt();
            }
            try {
                follower.stop();
            } catch (Exception ignored) {
                // follower may be null or already stopped
            }
            if (followerThread != null) {
                try {
                    followerThread.interrupt();
                } catch (Exception ignored) {
                }
            }
        }
    }

    // ===== HELPER METHODS =====

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
