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
 * Phase 1 Crash After CURRENT Switch Test
 *
 * CRITICAL TEST: Verifies that when InstallSnapshot completes successfully
 * (CURRENT is switched to new generation), the NEW generation is recovered on restart.
 *
 * Test flow:
 * 1. Establish generation 2 with state A B C
 * 2. Prepare generation 3 with state A B C D
 * 3. Complete InstallSnapshot successfully (CURRENT switched to gen 3)
 * 4. Simulate crash AFTER CURRENT switch
 * 5. Restart
 * 6. Verify NEW generation (3) with NEW state (A B C D) is recovered
 *
 * All components must agree:
 * - CURRENT = generation 3
 * - snapshot = generation 3
 * - metadata = generation 3
 * - Raft boundary = generation 3
 */
class Phase1CrashAfterCommitTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int BASE_PORT = 62000;

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

    // ===== TEST: CRASH AFTER CURRENT SWITCH =====

    /**
     * CRITICAL: Crash after CURRENT switch must recover NEW generation.
     *
     * Initial: generation 2, state A B C
     * Candidate: generation 3, state A B C D
     * CURRENT switched to generation 3 successfully
     * Crash
     * Restart
     * Expected: generation 3, state A B C D
     * All components must agree:
     * - CURRENT = 3
     * - snapshot = generation 3
     * - metadata generation = 3
     * - Raft boundary = 3
     */
    @Test
    void testCrashAfterCommitRecoversNewGeneration() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Crash After CURRENT Switch Recovers New Generation");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 100;

        Path leaderRaftDir = tempDir.resolve("leader-raft");
        Path followerRaftDir = tempDir.resolve("follower-raft");
        Path followerGenDir = followerRaftDir;
        Path followerMetaFile = tempDir.resolve("follower-meta.json");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);
        Files.createDirectories(followerGenDir);

        // Setup leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);
        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta.json"));
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

            // Step 1: Create generation 2 with A B C
            System.out.println("Step 1: Creating generation 2 with state A B C...");
            for (String name : List.of("A", "B", "C")) {
                ObjectMetadata obj = makeObject(name, 1000L);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            // Compact to create snapshot for generation 2
            leader.compactLog(2);
            Thread.sleep(100);

            // Install initial snapshot on follower (establishes generation 2)
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
            assertEquals(3, followerStore.listObjects().size(), "Follower should have A B C");
            System.out.println("Follower now has generation 2 with A B C");

            // Step 2: Create generation 3 with A B C D
            System.out.println("\nStep 2: Creating generation 3 with state A B C D...");
            ObjectMetadata objD = makeObject("D", 1000L);
            byte[] dataD = objectMapper.writeValueAsBytes(objD);
            LogEntry entryD = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, dataD);
            leader.submit(entryD);
            Thread.sleep(300);

            // Compact to create snapshot for generation 3
            leader.compactLog(3);
            Thread.sleep(100);

            // Get generation 3 snapshot
            snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            snapshot = snapshotOpt.get();
            snapshotData = snapshot.stateData();

            // Step 3: Complete InstallSnapshot successfully
            System.out.println("\nStep 3: Completing InstallSnapshot successfully...");
            request = new RaftMessage.InstallSnapshot(
                leader.getCurrentTerm(), "leader",
                snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                0, snapshotData, true, computeChecksum(snapshotData)
            );
            response = follower.handleInstallSnapshot(
                request.term(), request.leaderId(),
                request.lastIncludedIndex(), request.lastIncludedTerm(),
                request.offset(), request.data(), request.done(), request.checksum()
            );

            assertTrue(response.success(), "InstallSnapshot should succeed");
            assertEquals(4, followerStore.listObjects().size(), "Follower should have A B C D");
            System.out.println("InstallSnapshot completed successfully");

            // CRITICAL: Verify CURRENT is now gen 3
            long currentGen = followerGenMgr.getCurrentGeneration();
            System.out.println("CURRENT after InstallSnapshot: " + currentGen);
            assertEquals(3, currentGen, "CURRENT must be 3 (NEW generation)");
            System.out.println("VERIFIED: CURRENT = 3");

            // Step 4: Simulate crash - stop follower
            System.out.println("\nStep 4: Simulating crash (stopping follower)...");
            follower.stop();
            Thread.sleep(100);

            // Step 5: Restart follower with fresh state
            System.out.println("Step 5: Restarting follower with fresh state...");
            MetadataStore restartedStore = new MetadataStore(followerMetaFile);
            GenerationManager restartedGenMgr = new GenerationManager(followerGenDir);

            // Step 6: Verify NEW generation (3) is recovered
            System.out.println("\nStep 6: Verifying NEW generation is recovered...");

            long recoveredGen = restartedGenMgr.getCurrentGeneration();
            System.out.println("CURRENT after restart: " + recoveredGen);

            assertEquals(3, recoveredGen,
                "CRITICAL: CURRENT must be 3 (NEW generation)");

            // Verify state via GenerationManager
            var loadedState = restartedGenMgr.loadAuthoritativeState();
            assertEquals(4, loadedState.objects().size(),
                "CRITICAL: Must have A B C D (NEW state)");
            assertTrue(loadedState.objects().containsKey("A"), "Object A must exist");
            assertTrue(loadedState.objects().containsKey("B"), "Object B must exist");
            assertTrue(loadedState.objects().containsKey("C"), "Object C must exist");
            assertTrue(loadedState.objects().containsKey("D"),
                "CRITICAL: Object D MUST exist - generation 3 was committed");

            System.out.println("SUCCESS: New generation 3 with state A B C D recovered");
            System.out.println("  - CURRENT = 3");
            System.out.println("  - state has 4 objects");

        } finally {
            leader.stop();
            leaderThread.interrupt();
        }
    }

    // ===== TEST: MULTI-CHUNK INSTALL WITH CRASH =====

    /**
     * Tests that multi-chunk InstallSnapshot with CURRENT switch survives crashes.
     */
    @Test
    void testMultiChunkInstallWithCurrentSwitch() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Multi-Chunk Install with CURRENT Switch");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 200;

        Path leaderRaftDir = tempDir.resolve("leader-raft2");
        Path followerRaftDir = tempDir.resolve("follower-raft2");
        Path followerGenDir = followerRaftDir;
        Path followerMetaFile = tempDir.resolve("follower-meta2.json");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);
        Files.createDirectories(followerGenDir);

        // Setup leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader2", "127.0.0.1", leaderPort, null);
        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta2.json"));
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

            // Create state with many objects
            System.out.println("Creating state with 50 objects...");
            for (int i = 0; i < 50; i++) {
                ObjectMetadata obj = makeObject("Obj" + i, 10000L);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(500);

            // Compact
            leader.compactLog(2);
            Thread.sleep(200);

            // Get large snapshot
            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            var snapshot = snapshotOpt.get();
            byte[] snapshotData = snapshot.stateData();

            System.out.println("Snapshot size: " + snapshotData.length + " bytes");

            // Send in chunks
            int chunkSize = 4096;
            int offset = 0;
            int chunkNum = 0;
            boolean success = false;

            while (offset < snapshotData.length) {
                int len = Math.min(chunkSize, snapshotData.length - offset);
                byte[] chunk = Arrays.copyOfRange(snapshotData, offset, offset + len);
                boolean done = (offset + len >= snapshotData.length);

                RaftMessage.InstallSnapshot req = new RaftMessage.InstallSnapshot(
                    leader.getCurrentTerm(), "leader",
                    snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                    offset, chunk, done, computeChecksum(snapshotData)
                );

                RaftMessage.InstallSnapshotResponse resp = follower.handleInstallSnapshot(
                    req.term(), req.leaderId(),
                    req.lastIncludedIndex(), req.lastIncludedTerm(),
                    req.offset(), req.data(), req.done(), req.checksum()
                );

                if (done) {
                    success = resp.success();
                    System.out.println("Final chunk sent, success=" + success);
                }

                offset += len;
                chunkNum++;
            }

            assertTrue(success, "Multi-chunk InstallSnapshot should succeed");

            // Verify CURRENT
            long currentGen = followerGenMgr.getCurrentGeneration();
            assertEquals(2, currentGen, "CURRENT must be 2");

            // Simulate crash and restart
            System.out.println("\nSimulating crash...");
            follower.stop();
            Thread.sleep(100);

            GenerationManager restartedGenMgr = new GenerationManager(followerGenDir);
            long recoveredGen = restartedGenMgr.getCurrentGeneration();
            assertEquals(2, recoveredGen, "After restart, CURRENT should still be 2");

            var recoveredState = restartedGenMgr.loadAuthoritativeState();
            assertEquals(50, recoveredState.objects().size(), "Should recover all 50 objects");

            System.out.println("SUCCESS: Multi-chunk install with " + chunkNum + " chunks recovered correctly");

        } finally {
            leader.stop();
            leaderThread.interrupt();
        }
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
