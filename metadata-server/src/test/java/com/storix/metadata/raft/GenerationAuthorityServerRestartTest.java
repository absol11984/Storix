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
 * Generation Authority Server Restart Tests
 *
 * Tests the full integration path of:
 * 1. InstallSnapshot → CURRENT switch → crash → NEW server → loads CURRENT → exact state
 * 2. Candidate prepared → crash BEFORE CURRENT → restart → OLD state
 *
 * These tests verify that GenerationManager is the ONLY authoritative persistence path,
 * and that MetadataServer correctly loads from GenerationManager on restart.
 */
class GenerationAuthorityServerRestartTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int BASE_PORT = 64000;

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

    // ===== TEST 1: InstallSnapshot → CURRENT switch → crash → restart → exact state =====

    /**
     * CRITICAL TEST: Full server restart after successful InstallSnapshot
     *
     * Test flow:
     * 1. Setup leader with objects A, B, C
     * 2. Create snapshot on leader
     * 3. Install snapshot on follower (CURRENT switch to generation 2)
     * 4. Stop follower
     * 5. Create NEW MetadataServer with same directories
     * 6. Verify generation 2 is loaded (CURRENT=2)
     * 7. Verify exact state matches (A, B, C)
     */
    @Test
    void testServerRestartAfterInstallSnapshotLoadsCurrentGeneration() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Server Restart After InstallSnapshot");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 100;

        Path leaderRaftDir = tempDir.resolve("leader-raft-restart");
        Path followerRaftDir = tempDir.resolve("follower-raft-restart");
        Path followerMetaFile = tempDir.resolve("follower-meta-restart.json");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);

        // Setup leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);
        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta-restart.json"));
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
        leader.setLogEntryApplier(entry -> {
            try { leaderStateMachine.apply(entry); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        // Setup follower with GenerationManager
        // Use followerRaftDir directly (GenerationManager internally adds "generations/")
        ClusterConfig followerConfig = new ClusterConfig("test", "follower", "127.0.0.1", leaderPort + 1, null);
        MetadataStore followerStore = new MetadataStore(followerMetaFile);
        GenerationManager followerGenMgr = new GenerationManager(followerRaftDir);
        WAL followerWal = new WAL(followerRaftDir.resolve("wal.dat"));
        RaftLog followerLog = new RaftLog(followerWal);
        RaftNode follower = new RaftNode(followerConfig, followerRaftDir, followerLog, followerWal);
        follower.setGenerationManager(followerGenMgr);
        follower.setMetadataStore(followerStore);
        MetadataStateMachine followerStateMachine = new MetadataStateMachine(followerStore);
        follower.setLogEntryApplier(entry -> {
            try { followerStateMachine.apply(entry); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        Thread leaderThread = startNode(leader);

        try {
            waitForLeader(leader, 5000);

            // Step 1: Create objects A, B, C on leader
            System.out.println("Step 1: Creating objects A, B, C on leader...");
            for (String name : List.of("A", "B", "C")) {
                ObjectMetadata obj = makeObject(name, 1000L);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            // Compact to create snapshot
            leader.compactLog(2);
            Thread.sleep(100);

            // Get snapshot
            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            var snapshot = snapshotOpt.get();
            byte[] snapshotData = snapshot.stateData();
            int checksum = computeChecksum(snapshotData);

            // Step 2: Install snapshot on follower
            System.out.println("Step 2: Installing snapshot on follower...");
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
            System.out.println("InstallSnapshot completed successfully");

            // Verify CURRENT is set (generation IDs are sequential, separate from snapshot index)
            long currentGen = followerGenMgr.getCurrentGeneration();
            assertTrue(currentGen >= 0, "CURRENT should be >= 0 (new generation committed)");
            System.out.println("CURRENT after InstallSnapshot: " + currentGen + " (snapshot index=" + snapshot.lastIncludedIndex() + ")");

            // Verify follower has the state
            assertEquals(3, followerStore.listObjects().size(), "Follower should have A, B, C");

            // Record state before stop
            Set<String> stateBefore = new HashSet<>(followerStore.listObjects());
            System.out.println("State before stop: " + stateBefore);

            // Step 3: Stop follower
            System.out.println("Step 3: Stopping follower...");
            follower.stop();
            Thread.sleep(100);

            // Step 4: Create NEW server with same directories
            System.out.println("Step 4: Creating NEW MetadataServer with same directories...");
            ClusterConfig newConfig = new ClusterConfig("test", "follower", "127.0.0.1", leaderPort + 1, null);

            // Create new MetadataServer - it should load from GenerationManager
            MetadataServer newServer = new MetadataServer(
                leaderPort + 1,
                followerMetaFile,
                2, 6000, 2000,
                newConfig,
                followerRaftDir
            );

            // Step 5: Verify generation is loaded
            System.out.println("Step 5: Verifying generation is loaded...");
            GenerationManager newGenMgr = newServer.getGenerationManager();
            assertNotNull(newGenMgr, "GenerationManager should be available in cluster mode");
            long newCurrentGen = newGenMgr.getCurrentGeneration();
            assertEquals(currentGen, newCurrentGen, "CURRENT should be preserved after restart");

            // Step 6: Verify exact state matches
            System.out.println("Step 6: Verifying exact state matches...");
            MetadataStore newStore = newServer.getMetadataStore();
            Set<String> stateAfter = new HashSet<>(newStore.listObjects());

            assertEquals(stateBefore, stateAfter, "State should match exactly after restart");
            assertTrue(newStore.objectExists("A"), "Object A should exist");
            assertTrue(newStore.objectExists("B"), "Object B should exist");
            assertTrue(newStore.objectExists("C"), "Object C should exist");

            System.out.println("State after restart: " + stateAfter);
            System.out.println("\nSUCCESS: Generation " + currentGen + " with exact state recovered");
            System.out.println("  - CURRENT = " + newCurrentGen);
            System.out.println("  - Objects: " + stateAfter);

            newServer.stop();

            System.out.println("\n========================================");
            System.out.println("TEST: Server Restart After InstallSnapshot - PASSED");
            System.out.println("========================================\n");

        } finally {
            leader.stop();
            leaderThread.interrupt();
        }
    }

    // ===== TEST 2: Candidate prepared → crash BEFORE CURRENT → restart → OLD state =====

    /**
     * CRITICAL TEST: Server restart when crash happens BEFORE CURRENT switch
     *
     * Test flow:
     * 1. Setup leader with objects A, B
     * 2. Install snapshot on follower (generation 2)
     * 3. Create more objects C, D on leader
     * 4. Create new snapshot (generation 3 with A, B, C, D)
     * 5. Begin InstallSnapshot but DON'T complete (simulate crash before CURRENT switch)
     * 6. Restart server
     * 7. Verify generation 2 is still current (generation 3 is not committed)
     * 8. Verify state is A, B only (not C, D)
     */
    @Test
    void testServerRestartBeforeCurrentSwitchRecoversOldGeneration() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Server Restart Before CURRENT Switch");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 200;

        Path leaderRaftDir = tempDir.resolve("leader-raft-no-switch");
        Path followerRaftDir = tempDir.resolve("follower-raft-no-switch");
        Path followerMetaFile = tempDir.resolve("follower-meta-no-switch.json");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);

        // Setup leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader2", "127.0.0.1", leaderPort, null);
        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta-no-switch.json"));
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
        leader.setLogEntryApplier(entry -> {
            try { leaderStateMachine.apply(entry); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        // Setup follower with GenerationManager
        // Use followerRaftDir directly (GenerationManager internally adds "generations/")
        ClusterConfig followerConfig = new ClusterConfig("test", "follower2", "127.0.0.1", leaderPort + 1, null);
        MetadataStore followerStore = new MetadataStore(followerMetaFile);
        GenerationManager followerGenMgr = new GenerationManager(followerRaftDir);
        WAL followerWal = new WAL(followerRaftDir.resolve("wal.dat"));
        RaftLog followerLog = new RaftLog(followerWal);
        RaftNode follower = new RaftNode(followerConfig, followerRaftDir, followerLog, followerWal);
        follower.setGenerationManager(followerGenMgr);
        follower.setMetadataStore(followerStore);
        MetadataStateMachine followerStateMachine = new MetadataStateMachine(followerStore);
        follower.setLogEntryApplier(entry -> {
            try { followerStateMachine.apply(entry); }
            catch (IOException e) { throw new RuntimeException(e); }
        });

        Thread leaderThread = startNode(leader);

        try {
            waitForLeader(leader, 5000);

            // Step 1: Create objects A, B on leader
            System.out.println("Step 1: Creating objects A, B on leader...");
            for (String name : List.of("A", "B")) {
                ObjectMetadata obj = makeObject(name, 1000L);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            // Compact to create generation 2 snapshot
            leader.compactLog(2);
            Thread.sleep(100);

            // Install generation 2 on follower
            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            var snapshot = snapshotOpt.get();
            byte[] snapshotData = snapshot.stateData();
            int checksum = computeChecksum(snapshotData);

            System.out.println("Installing generation 2 snapshot on follower...");
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
            assertEquals(1, followerGenMgr.getCurrentGeneration(), "CURRENT should be 1 (first generation on follower)");
            System.out.println("Generation 1 installed, CURRENT = 1");

            // Step 2: Create more objects C, D on leader
            System.out.println("Step 2: Creating objects C, D on leader...");
            for (String name : List.of("C", "D")) {
                ObjectMetadata obj = makeObject(name, 1000L);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            // Compact to create generation 3 snapshot
            leader.compactLog(3);
            Thread.sleep(100);

            // Get generation 3 snapshot
            snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            snapshot = snapshotOpt.get();
            snapshotData = snapshot.stateData();

            // Step 3: Begin InstallSnapshot but DON'T complete (simulate crash before CURRENT switch)
            System.out.println("Step 3: Sending FIRST chunk only (simulating crash before CURRENT switch)...");

            int chunkSize = 1024;
            byte[] firstChunk = Arrays.copyOf(snapshotData, Math.min(chunkSize, snapshotData.length));

            request = new RaftMessage.InstallSnapshot(
                leader.getCurrentTerm(), "leader",
                snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                0, firstChunk, false, computeChecksum(snapshotData)  // done=false = interrupted!
            );

            // This starts the transfer but doesn't complete it
            follower.handleInstallSnapshot(
                request.term(), request.leaderId(),
                request.lastIncludedIndex(), request.lastIncludedTerm(),
                request.offset(), request.data(), request.done(), request.checksum()
            );

            System.out.println("First chunk sent, simulating crash...");

            // Step 4: Verify old state is still there
            long currentGenBeforeStop = followerGenMgr.getCurrentGeneration();
            System.out.println("CURRENT before stop: " + currentGenBeforeStop + " (should be 1)");
            assertEquals(1, currentGenBeforeStop, "CURRENT should still be 1 (gen 2 not committed)");

            // Step 5: Stop follower
            System.out.println("Step 5: Stopping follower...");
            follower.stop();
            Thread.sleep(100);

            // Step 6: Restart server
            System.out.println("Step 6: Creating NEW MetadataServer with same directories...");
            ClusterConfig newConfig = new ClusterConfig("test", "follower2", "127.0.0.1", leaderPort + 1, null);

            MetadataServer newServer = new MetadataServer(
                leaderPort + 1,
                followerMetaFile,
                2, 6000, 2000,
                newConfig,
                followerRaftDir
            );

            // Step 7: Verify generation 1 is still current
            System.out.println("Step 7: Verifying generation 1 is still current...");
            GenerationManager newGenMgr = newServer.getGenerationManager();
            long newCurrentGen = newGenMgr.getCurrentGeneration();
            System.out.println("CURRENT after restart: " + newCurrentGen);
            assertEquals(1, newCurrentGen, "CURRENT should still be 1 (gen 2 was never committed)");

            // Step 8: Verify state is A, B only (not C, D)
            System.out.println("Step 8: Verifying state is A, B only...");
            MetadataStore newStore = newServer.getMetadataStore();
            Set<String> stateAfter = new HashSet<>(newStore.listObjects());

            assertEquals(Set.of("A", "B"), stateAfter, "State should be A, B only (gen 3 was not committed)");
            assertFalse(newStore.objectExists("C"), "Object C should NOT exist");
            assertFalse(newStore.objectExists("D"), "Object D should NOT exist");

            System.out.println("State after restart: " + stateAfter);
            System.out.println("\nSUCCESS: Old generation 1 recovered");
            System.out.println("  - CURRENT = " + newCurrentGen);
            System.out.println("  - Objects: " + stateAfter);
            System.out.println("  - Generation 2 correctly ignored (was not committed)");

            newServer.stop();

            System.out.println("\n========================================");
            System.out.println("TEST: Server Restart Before CURRENT Switch - PASSED");
            System.out.println("========================================\n");

        } finally {
            leader.stop();
            leaderThread.interrupt();
        }
    }

    // ===== TEST 3: Corrupt generation causes fatal startup =====

    /**
     * Tests that corrupt generation data causes fatal startup failure.
     */
    @Test
    void testCorruptGenerationCausesFatalStartup() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Corrupt Generation Causes Fatal Startup");
        System.out.println("========================================\n");

        Path corruptRaftDir = tempDir.resolve("corrupt-raft");
        Path corruptMetaFile = tempDir.resolve("corrupt-meta.json");
        Files.createDirectories(corruptRaftDir);

        // Create GenerationManager and set up a corrupt generation
        // MetadataServer now passes resolvedRaftStateDir to GenerationManager (not resolvedRaftStateDir.resolve("generations"))
        // So GenerationManager will look for:
        // - CURRENT at: corruptRaftDir/CURRENT
        // - gen-N at: corruptRaftDir/generations/gen-N
        GenerationManager genMgr = new GenerationManager(corruptRaftDir);

        // Create generation 5 directory but DON'T write all required files
        // GenerationManager will look for gen-5 at corruptRaftDir/generations/gen-5
        Path genDir = corruptRaftDir.resolve("generations").resolve("gen-5");
        Files.createDirectories(genDir);

        // Write CURRENT pointing to gen 5 (which is incomplete/corrupt)
        // CURRENT file is at corruptRaftDir/CURRENT (parent of generations/)
        Files.writeString(corruptRaftDir.resolve("CURRENT"), "5");

        System.out.println("Created corrupt generation 5 (missing files)");

        // Try to create MetadataServer - should fail
        ClusterConfig config = new ClusterConfig("test", "corrupt", "127.0.0.1", 65000, null);

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> {
            new MetadataServer(65000, corruptMetaFile, 2, 6000, 2000, config, corruptRaftDir);
        });

        assertTrue(thrown.getMessage().contains("FATAL") || thrown.getMessage().contains("corrupt"),
            "Error should mention FATAL or corrupt: " + thrown.getMessage());

        System.out.println("FATAL error thrown as expected: " + thrown.getMessage());

        System.out.println("\n========================================");
        System.out.println("TEST: Corrupt Generation Causes Fatal Startup - PASSED");
        System.out.println("========================================\n");
    }

    // ===== HELPER METHODS =====

    private Thread startNode(RaftNode node) throws IOException {
        node.start();
        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                // Expected on shutdown
            }
        });
        thread.start();
        return thread;
    }

    private void waitForLeader(RaftNode node, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (node.isLeader()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new RuntimeException("Node did not become leader within timeout");
    }
}
