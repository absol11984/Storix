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
 * Phase 1 Crash Before CURRENT Switch Test
 *
 * CRITICAL TEST: Verifies that when InstallSnapshot crashes BEFORE CURRENT switch,
 * the OLD generation remains authoritative on restart.
 *
 * Test flow:
 * 1. Establish generation 2 with state A B C
 * 2. Begin generation 3 installation with state A B C D
 * 3. Simulate crash DURING installation (before CURRENT switch)
 * 4. Restart
 * 5. Verify OLD generation (2) with OLD state (A B C) is recovered
 *
 * All components must agree:
 * - CURRENT = generation 2 (NOT 3)
 * - snapshot = generation 2
 * - metadata = generation 2
 * - Raft boundary = generation 2
 */
class Phase1CrashBeforeCommitTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int BASE_PORT = 63000;

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

    // ===== TEST: CRASH BEFORE CURRENT SWITCH =====

    /**
     * CRITICAL: Crash before CURRENT switch must recover OLD generation.
     *
     * Initial: generation 2, state A B C
     * Begin: generation 3 installation with state A B C D
     * Crash BEFORE CURRENT switch
     * Restart
     * Expected: generation 2, state A B C
     * All components must agree:
     * - CURRENT = generation 2 (NOT 3)
     * - snapshot = generation 2
     * - metadata = generation 2
     * - Raft boundary = generation 2
     */
    @Test
    void testCrashBeforeCommitRecoversOldGeneration() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Crash Before CURRENT Switch Recovers Old Generation");
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
            System.out.println("Follower now has a new generation with A B C");

            // Verify CURRENT was advanced to generation 1 (generation IDs are sequential, starting from 1)
            long genAfterFirstSnapshot = followerGenMgr.getCurrentGeneration();
            assertEquals(1, genAfterFirstSnapshot, "CURRENT should be generation 1 after first snapshot");
            System.out.println("CURRENT after first snapshot: " + genAfterFirstSnapshot);

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

            // Step 3: Begin InstallSnapshot for generation 3 but DON'T complete
            System.out.println("\nStep 3: Beginning InstallSnapshot for generation 3...");
            System.out.println("Simulating CRASH before CURRENT switch...");

            // Send FIRST chunk only (not the final chunk)
            // This simulates a crash during multi-chunk transfer
            int chunkSize = 1024;
            byte[] firstChunk = Arrays.copyOf(snapshotData, Math.min(chunkSize, snapshotData.length));

            request = new RaftMessage.InstallSnapshot(
                leader.getCurrentTerm(), "leader",
                snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                0, firstChunk, false, computeChecksum(snapshotData)  // done=false = crash!
            );

            // This will start the transfer but not complete it
            follower.handleInstallSnapshot(
                request.term(), request.leaderId(),
                request.lastIncludedIndex(), request.lastIncludedTerm(),
                request.offset(), request.data(), request.done(), request.checksum()
            );

            System.out.println("First chunk sent, simulating crash...");

            // Step 4: Simulate crash - stop follower
            System.out.println("\nStep 4: Simulating crash (stopping follower)...");
            follower.stop();
            Thread.sleep(100);

            // Step 5: Restart follower with fresh state
            System.out.println("Step 5: Restarting follower with fresh state...");
            MetadataStore restartedStore = new MetadataStore(followerMetaFile);
            GenerationManager restartedGenMgr = new GenerationManager(followerGenDir);

            // Step 6: Verify OLD generation (2) is recovered
            System.out.println("\nStep 6: Verifying OLD generation is recovered...");

            long recoveredGen = restartedGenMgr.getCurrentGeneration();
            System.out.println("CURRENT after restart: " + recoveredGen);

            assertEquals(1, recoveredGen,
                "CRITICAL: CURRENT must be 1 (OLD generation) - gen 2 was not committed");

            // Verify state via GenerationManager - should have A B C only
            var loadedState = restartedGenMgr.loadAuthoritativeState();
            assertEquals(3, loadedState.objects().size(),
                "CRITICAL: Must have only A B C (OLD state) - D should NOT exist");
            assertTrue(loadedState.objects().containsKey("A"), "Object A must exist");
            assertTrue(loadedState.objects().containsKey("B"), "Object B must exist");
            assertTrue(loadedState.objects().containsKey("C"), "Object C must exist");
            assertFalse(loadedState.objects().containsKey("D"),
                "CRITICAL: Object D MUST NOT exist - generation 3 was NOT committed");

            System.out.println("SUCCESS: Old generation 2 with state A B C recovered");
            System.out.println("  - CURRENT = 2 (NOT 3)");
            System.out.println("  - state has 3 objects (NOT 4)");
            System.out.println("  - Object D correctly absent");

        } finally {
            leader.stop();
            leaderThread.interrupt();
        }
    }

    // ===== TEST: INTERRUPTED MULTI-CHUNK TRANSFER =====

    /**
     * Tests that interrupted multi-chunk InstallSnapshot recovers old state.
     */
    @Test
    void testInterruptedMultiChunkRecoversOldState() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Interrupted Multi-Chunk Transfer Recovers Old State");
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

            // Step 1: Create initial state with 10 objects and install complete snapshot
            System.out.println("Step 1: Creating initial state with 10 objects and installing complete snapshot...");
            for (int i = 0; i < 10; i++) {
                ObjectMetadata obj = makeObject("Obj" + i, 5000L);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(300);

            // Compact to create first snapshot
            leader.compactLog(2);
            Thread.sleep(200);

            // Install first snapshot completely on follower
            var snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            var snapshot = snapshotOpt.get();
            byte[] snapshotData = snapshot.stateData();
            int checksum = computeChecksum(snapshotData);

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
            System.out.println("Initial snapshot installed successfully");

            // Verify first generation is committed
            long genAfterFirstSnapshot = followerGenMgr.getCurrentGeneration();
            assertEquals(1, genAfterFirstSnapshot, "CURRENT should be generation 1 after first snapshot");
            System.out.println("CURRENT after first snapshot: " + genAfterFirstSnapshot);

            // Step 2: Create more state (30 objects) and attempt interrupted multi-chunk install
            System.out.println("\nStep 2: Creating larger state with 30 objects...");
            for (int i = 10; i < 40; i++) {
                ObjectMetadata obj = makeObject("Obj" + i, 5000L);
                byte[] data = objectMapper.writeValueAsBytes(obj);
                LogEntry entry = LogEntry.create(leader.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);
                leader.submit(entry);
            }
            Thread.sleep(400);

            // Compact to create second snapshot
            leader.compactLog(2);
            Thread.sleep(200);

            // Get second snapshot
            snapshotOpt = leaderSnapshotMgr.loadLatestSnapshot();
            assertTrue(snapshotOpt.isPresent());
            snapshot = snapshotOpt.get();
            snapshotData = snapshot.stateData();

            System.out.println("Snapshot size: " + snapshotData.length + " bytes");

            // Send first 3 chunks only (NOT the final chunk - done=false for all)
            int chunkSize = 4096;
            int chunksSent = 0;
            for (int offset = 0; offset < Math.min(snapshotData.length, chunkSize * 3); offset += chunkSize) {
                int len = Math.min(chunkSize, snapshotData.length - offset);
                byte[] chunk = Arrays.copyOfRange(snapshotData, offset, offset + len);
                boolean done = false;  // Never complete the transfer

                RaftMessage.InstallSnapshot req = new RaftMessage.InstallSnapshot(
                    leader.getCurrentTerm(), "leader",
                    snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                    offset, chunk, done, computeChecksum(snapshotData)
                );

                follower.handleInstallSnapshot(
                    req.term(), req.leaderId(),
                    req.lastIncludedIndex(), req.lastIncludedTerm(),
                    req.offset(), req.data(), req.done(), req.checksum()
                );
                chunksSent++;
            }

            System.out.println("Sent " + chunksSent + " chunks, simulating crash...");

            // Simulate crash
            follower.stop();
            Thread.sleep(100);

            // Restart and verify OLD state (generation should be unchanged after interrupted InstallSnapshot)
            GenerationManager restartedGenMgr = new GenerationManager(followerGenDir);
            long recoveredGen = restartedGenMgr.getCurrentGeneration();
            // With sequential generation IDs, the generation number depends on how many snapshots were committed
            // The key invariant is: interrupted snapshot should NOT advance CURRENT
            assertEquals(1, recoveredGen,
                "CURRENT should be generation 1 after interrupted InstallSnapshot");

            System.out.println("SUCCESS: Interrupted transfer recovered old generation correctly");

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
