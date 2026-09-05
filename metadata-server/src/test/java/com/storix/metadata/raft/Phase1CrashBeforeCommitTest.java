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
 * Phase 1 Crash Before Commit Test
 *
 * CRITICAL TEST: Verifies that when InstallSnapshot crashes BEFORE the commit marker
 * is written, the OLD generation is recovered on restart.
 *
 * Test flow:
 * 1. Establish generation 2 with state A B C
 * 2. Prepare generation 3 with state A B C D (candidate)
 * 3. Simulate crash BEFORE commit marker write
 * 4. Restart
 * 5. Verify OLD generation (2) with OLD state (A B C) is recovered
 *
 * This is NOT a simulated crash - we use deterministic failure injection.
 */
class Phase1CrashBeforeCommitTest {

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

    // ===== FAILING SNAPSHOT MANAGER FOR TEST =====

    /**
     * Test-only SnapshotManager that can fail at specific points.
     * FAIL_BEFORE_COMMIT = fail BEFORE writing commit marker
     * This means: candidate is prepared, but commit marker is never written.
     */
    private static class FailingSnapshotManager extends SnapshotManager {
        public enum FailurePoint {
            NONE,
            FAIL_BEFORE_COMMIT
        }

        private volatile FailurePoint failurePoint = FailurePoint.NONE;

        public FailingSnapshotManager(Path snapshotDir, MetadataStore store) {
            super(snapshotDir, store);
        }

        public void setFailurePoint(FailurePoint point) {
            this.failurePoint = point;
        }

        @Override
        public void commitGeneration(long generationIndex, long generationTerm) throws IOException {
            if (failurePoint == FailurePoint.FAIL_BEFORE_COMMIT) {
                System.out.println("[FAILING] commitGeneration - throwing IOException BEFORE writing marker");
                throw new IOException("Simulated crash BEFORE commit marker write");
            }
            super.commitGeneration(generationIndex, generationTerm);
        }
    }

    // ===== TEST: CRASH BEFORE COMMIT =====

    /**
     * CRITICAL: Crash before commit marker must recover OLD generation.
     *
     * Initial: generation 2, state A B C
     * Candidate: generation 3, state A B C D
     * FAIL at commitGeneration (before marker written)
     * Restart
     * Expected: generation 2, state A B C
     */
    @Test
    void testCrashBeforeCommitRecoversOldGeneration() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Crash Before Commit Recovers Old Generation");
        System.out.println("========================================\n");

        int leaderPort = BASE_PORT + 100;

        Path leaderRaftDir = tempDir.resolve("leader-raft");
        Path followerRaftDir = tempDir.resolve("follower-raft");
        Path followerSnapDir = followerRaftDir.resolve("snapshots");
        Path followerMetaFile = tempDir.resolve("follower-meta.json");
        Files.createDirectories(leaderRaftDir);
        Files.createDirectories(followerRaftDir);
        Files.createDirectories(followerSnapDir);

        // Setup leader
        ClusterConfig leaderConfig = new ClusterConfig("test", "leader", "127.0.0.1", leaderPort, null);
        MetadataStore leaderStore = new MetadataStore(tempDir.resolve("leader-meta.json"));
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

        // Setup follower with FAILING SnapshotManager
        ClusterConfig followerConfig = new ClusterConfig("test", "follower", "127.0.0.1", leaderPort + 1, null);
        MetadataStore followerStore = new MetadataStore(followerMetaFile);
        FailingSnapshotManager followerSnapshotMgr = new FailingSnapshotManager(followerSnapDir, followerStore);
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

            // Step 3: Enable FAILURE BEFORE COMMIT
            System.out.println("\nStep 3: Enabling FAILURE_BEFORE_COMMIT...");
            followerSnapshotMgr.setFailurePoint(FailingSnapshotManager.FailurePoint.FAIL_BEFORE_COMMIT);

            // Step 4: Attempt install - should FAIL at commitGeneration
            System.out.println("Step 4: Attempting InstallSnapshot (will fail before commit marker)...");
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

            // Install should fail
            assertFalse(response.success(), "InstallSnapshot should fail at commitGeneration");
            System.out.println("InstallSnapshot correctly returned success=false");

            // CRITICAL: Verify commit marker does NOT exist
            Path commitMarker3 = followerSnapDir.resolve("generation-3.committed");
            assertFalse(Files.exists(commitMarker3),
                "COMMIT MARKER MUST NOT EXIST - generation 3 is NOT committed");
            System.out.println("VERIFIED: No generation-3.committed marker exists");

            // Step 5: Simulate crash - stop follower
            System.out.println("\nStep 5: Simulating crash (stopping follower)...");
            follower.stop();
            Thread.sleep(100);

            // Step 6: Restart follower
            System.out.println("Step 6: Restarting follower with fresh state...");
            MetadataStore restartedStore = new MetadataStore(followerMetaFile);
            SnapshotManager restartedSnapshotMgr = new SnapshotManager(followerSnapDir, restartedStore);

            // Step 7: Verify OLD generation (2) is recovered
            System.out.println("\nStep 7: Verifying OLD generation is recovered...");

            long committedGen = restartedSnapshotMgr.getCurrentCommittedGeneration();
            System.out.println("Committed generation after restart: " + committedGen);

            assertEquals(2, committedGen,
                "CRITICAL: Committed generation must be 2 (OLD), not 3");

            // Verify state
            assertEquals(3, restartedStore.listObjects().size(),
                "CRITICAL: Must have A B C (OLD state), not A B C D");
            assertTrue(restartedStore.objectExists("A"), "Object A must exist");
            assertTrue(restartedStore.objectExists("B"), "Object B must exist");
            assertTrue(restartedStore.objectExists("C"), "Object C must exist");
            assertFalse(restartedStore.objectExists("D"),
                "CRITICAL: Object D must NOT exist - generation 3 was never committed");

            System.out.println("SUCCESS: Old generation 2 with state A B C recovered");

            System.out.println("\n========================================");
            System.out.println("TEST: Crash Before Commit - PASSED");
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
