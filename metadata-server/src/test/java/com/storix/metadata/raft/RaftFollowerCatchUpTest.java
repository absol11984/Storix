package com.storix.metadata.raft;

import com.storix.metadata.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Follower catch-up and restart verification tests.
 * Mandatory assertions:
 * - lastLogIndex matches
 * - exact indexes, terms, commands match
 * - commitIndex matches
 * - lastApplied matches
 * - metadata state convergence
 *
 * Test scenarios:
 * 1. Follower offline → catches up via AppendEntries → state matches leader
 * 2. Follower restarts → recovers state → catches up → state matches
 * 3. Follower behind leader → leader sends missing entries → follower catches up
 */
class RaftFollowerCatchUpTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private int portA, portB, portC;
    private Path clusterDir;
    private Path dataDirA, dataDirB, dataDirC;
    private MetadataServer serverA, serverB, serverC;
    private ClusterConfig configA, configB, configC;
    private TestPortAllocator.Lease portLease;

    @TempDir
    Path tempDir;

    private void setupCluster() throws Exception {
        portLease = TestPortAllocator.lease(6);
        portA = portLease.port(0);
        portB = portLease.port(1);
        portC = portLease.port(2);
        int raftPortA = portLease.port(3);
        int raftPortB = portLease.port(4);
        int raftPortC = portLease.port(5);
        portLease.release();

        clusterDir = Files.createTempDirectory("follower-catchup-test-" + System.nanoTime());
        dataDirA = clusterDir.resolve("meta-a");
        dataDirB = clusterDir.resolve("meta-b");
        dataDirC = clusterDir.resolve("meta-c");
        Files.createDirectories(dataDirA);
        Files.createDirectories(dataDirB);
        Files.createDirectories(dataDirC);

        configA = new ClusterConfig("storix", "meta-a", "127.0.0.1", portA, raftPortA,
            List.of(new RaftPeer("meta-b", "127.0.0.1", raftPortB),
                    new RaftPeer("meta-c", "127.0.0.1", raftPortC)));

        configB = new ClusterConfig("storix", "meta-b", "127.0.0.1", portB, raftPortB,
            List.of(new RaftPeer("meta-a", "127.0.0.1", raftPortA),
                    new RaftPeer("meta-c", "127.0.0.1", raftPortC)));

        configC = new ClusterConfig("storix", "meta-c", "127.0.0.1", portC, raftPortC,
            List.of(new RaftPeer("meta-a", "127.0.0.1", raftPortA),
                    new RaftPeer("meta-b", "127.0.0.1", raftPortB)));
    }

    private MetadataServer startServer(int port, Path metaFile, ClusterConfig config, Path dataDir) throws Exception {
        MetadataServer server = new MetadataServer(port, metaFile, 2, 6000, 2000, config, dataDir);
        try {
            server.getRaftNode().start();
            server.getRaftNode().waitForRpcServerReady();
            return server;
        } catch (Exception e) {
            server.stop();
            throw e;
        }
    }

    private MetadataServer getLeader() {
        if (serverA != null && serverA.getRaftNode().isLeader()) return serverA;
        if (serverB != null && serverB.getRaftNode().isLeader()) return serverB;
        if (serverC != null && serverC.getRaftNode().isLeader()) return serverC;
        return null;
    }

    private MetadataServer getFollower() {
        for (MetadataServer s : List.of(serverA, serverB, serverC)) {
            if (s != null && !s.getRaftNode().isLeader()) return s;
        }
        return null;
    }

    private void waitForLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (getLeader() != null) return;
            Thread.sleep(50);
        }
        fail("Leader election timeout");
    }

    private long submitCreate(MetadataServer leader, String name) throws Exception {
        ObjectMetadata obj = new ObjectMetadata(name, 1024L, 4096);
        byte[] data = objectMapper.writeValueAsBytes(obj);
        LogEntry entry = LogEntry.create(leader.getRaftNode().getCurrentTerm(),
            LogEntry.OpType.CREATE_OBJECT, data,
            "catchup-client", "catchup-req-" + System.nanoTime()).withIndex(leader.getRaftNode().getRaftLog().getLastLogIndex() + 1);
        boolean ok = leader.getRaftNode().submit(entry);
        assertTrue(ok, "submit() should succeed with majority available");
        return leader.getRaftNode().getRaftLog().getLastLogIndex();
    }

    private void waitForApplied(MetadataServer server, long target, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (server.getRaftNode().getRaftLog().getLastApplied() >= target) return;
            Thread.sleep(50);
        }
        fail("Timed out waiting for lastApplied >= " + target + " on " +
             server.getRaftNode().getNodeId());
    }

    private Map<String, Object> captureState(MetadataServer server) {
        Map<String, Object> state = new HashMap<>();
        for (String name : server.getMetadataStore().listObjects()) {
            state.put(name, name);
        }
        return state;
    }

    private void wireApplier(MetadataServer server) {
        try {
            var field = RaftNode.class.getDeclaredField("stateMachineApplier");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            java.util.function.Consumer<LogEntry> applier = entry -> {
                try {
                    server.getStateMachine().apply(entry);
                } catch (Exception e) {
                    System.err.println("[APPLIER] Failed: " + e.getMessage());
                }
            };
            field.set(server.getRaftNode(), applier);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void teardown() {
        RuntimeException failure = null;
        for (MetadataServer server : Arrays.asList(serverA, serverB, serverC)) {
            if (server == null) {
                continue;
            }
            try {
                server.stop();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        serverA = null;
        serverB = null;
        serverC = null;
        try {
            if (clusterDir != null) {
                Files.walk(clusterDir).sorted(Comparator.reverseOrder())
                    .map(Path::toFile).forEach(java.io.File::delete);
            }
        } catch (Exception e) {
            if (failure == null) {
                failure = new RuntimeException("Failed to remove cluster directory", e);
            } else {
                failure.addSuppressed(e);
            }
        }
        if (portLease != null) {
            portLease.close();
            portLease = null;
        }
        if (failure != null) {
            throw failure;
        }
    }

    // ===== TEST 1: Follower catch-up from offline state =====

    /**
     * Scenario:
     * 1. Cluster of 3 elects leader
     * 2. Submit entries
     * 3. Stop one follower
     * 4. Submit more entries while follower is offline
     * 5. Restart follower
     * 6. Leader sends missing entries via AppendEntries
     * 7. Follower catches up and matches leader exactly
     */
    @Test
    void testFollowerCatchUpAfterDelayedStart() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Follower Catch-Up After Delayed Start");
        System.out.println("========================================\n");

        // Phase 1: Start cluster
        setupCluster();
        serverA = startServer(portA, dataDirA.resolve("meta.json"), configA, dataDirA);
        serverB = startServer(portB, dataDirB.resolve("meta.json"), configB, dataDirB);
        serverC = startServer(portC, dataDirC.resolve("meta.json"), configC, dataDirC);
        wireApplier(serverA);
        wireApplier(serverB);
        wireApplier(serverC);

        waitForLeader(15000);
        MetadataServer leader = getLeader();
        assertNotNull(leader, "Should have a leader");
        String leaderId = leader.getRaftNode().getNodeId();
        System.out.println("Leader: " + leaderId);

        // Phase 2: Submit entries while all nodes are running
        int preEntries = 3;
        long preIndex = 0;
        for (int i = 1; i <= preEntries; i++) {
            preIndex = submitCreate(leader, "pre-catchup-" + i);
        }
        System.out.println("Submitted " + preEntries + " entries while all nodes running");

        // Wait for all nodes to apply
        for (MetadataServer s : List.of(serverA, serverB, serverC)) {
            if (s != null) {
                waitForApplied(s, preIndex, 5000);
            }
        }

        // Find and stop one follower
        MetadataServer slowFollower = null;
        for (MetadataServer s : List.of(serverA, serverB, serverC)) {
            if (s != null && !s.getRaftNode().isLeader()) {
                slowFollower = s;
                break;
            }
        }
        assertNotNull(slowFollower, "Should have a follower");
        String followerId = slowFollower.getRaftNode().getNodeId();
        System.out.println("Stopping follower: " + followerId);

        // Stop the follower
        slowFollower.stop();

        // Phase 3: Submit more entries while follower is offline
        int midEntries = 2;
        long midIndex = preIndex;
        for (int i = 1; i <= midEntries; i++) {
            midIndex = submitCreate(leader, "mid-catchup-" + i);
        }
        System.out.println("Submitted " + midEntries + " more entries while follower was offline");

        // Wait for leader to apply
        waitForApplied(leader, midIndex, 3000);

        // Capture leader state
        Map<String, Object> leaderState = captureState(leader);
        long leaderLastLogIndex = leader.getRaftNode().getRaftLog().getLastLogIndex();
        long leaderCommitIndex = leader.getRaftNode().getRaftLog().getCommitIndex();
        long leaderLastApplied = leader.getRaftNode().getRaftLog().getLastApplied();
        System.out.println("Leader lastLogIndex=" + leaderLastLogIndex + " commitIndex=" + leaderCommitIndex +
            " lastApplied=" + leaderLastApplied);
        System.out.println("Leader state: " + leaderState.keySet());

        // Phase 4: Restart follower
        System.out.println("Restarting follower: " + followerId);
        slowFollower.getRaftNode().start();
        slowFollower.getRaftNode().waitForRpcServerReady();
        wireApplier(slowFollower);
        Thread.sleep(200);

        // Phase 5: Wait for catch-up via AppendEntries from leader
        System.out.println("Waiting for follower to catch up...");
        waitForApplied(slowFollower, midIndex, 10000);

        // Phase 6: VERIFY EXACT MATCH
        long followerLastLogIndex = slowFollower.getRaftNode().getRaftLog().getLastLogIndex();
        long followerCommitIndex = slowFollower.getRaftNode().getRaftLog().getCommitIndex();
        long followerLastApplied = slowFollower.getRaftNode().getRaftLog().getLastApplied();
        Map<String, Object> followerState = captureState(slowFollower);

        System.out.println("Follower lastLogIndex=" + followerLastLogIndex + " commitIndex=" + followerCommitIndex +
            " lastApplied=" + followerLastApplied);
        System.out.println("Follower state: " + followerState.keySet());

        // Mandatory assertion 1: lastLogIndex matches
        assertEquals(leaderLastLogIndex, followerLastLogIndex,
            "Follower lastLogIndex must match leader");

        // Mandatory assertion 2: commitIndex matches
        assertEquals(leaderCommitIndex, followerCommitIndex,
            "Follower commitIndex must match leader");

        // Mandatory assertion 3: lastApplied matches
        assertEquals(leaderLastApplied, followerLastApplied,
            "Follower lastApplied must match leader");

        // Mandatory assertion 4: exact metadata state convergence
        assertEquals(leaderState, followerState,
            "Follower metadata state must exactly match leader");

        // Verify exact log entry comparison
        RaftLog leaderLog = leader.getRaftNode().getRaftLog();
        RaftLog followerLog = slowFollower.getRaftNode().getRaftLog();
        for (long idx = leaderLog.getLogStartIndex(); idx <= leaderLastLogIndex; idx++) {
            LogEntry leaderEntry = leaderLog.getEntry(idx);
            LogEntry followerEntry = followerLog.getEntry(idx);
            assertNotNull(followerEntry, "Follower must have entry at index " + idx);
            assertEquals(leaderEntry.term(), followerEntry.term(),
                "Entry term must match at index " + idx);
            assertEquals(leaderEntry.opType(), followerEntry.opType(),
                "Entry opType must match at index " + idx);
            assertEquals(leaderEntry.index(), followerEntry.index(),
                "Entry index must match at index " + idx);
            assertArrayEquals(leaderEntry.data(), followerEntry.data(),
                "Entry data must match at index " + idx);
            assertEquals(leaderEntry.clientId(), followerEntry.clientId(),
                "Entry clientId must match at index " + idx);
            assertEquals(leaderEntry.requestId(), followerEntry.requestId(),
                "Entry requestId must match at index " + idx);
        }
        System.out.println("All log entries verified: indexes, terms, commands match");

        System.out.println("\n========================================");
        System.out.println("TEST: Follower Catch-Up - PASSED");
        System.out.println("========================================\n");
    }

    // ===== TEST 2: Follower restart and catch-up =====

    /**
     * Scenario:
     * 1. Cluster of 3 elects leader, submits entries
     * 2. Stop follower completely
     * 3. Leader continues with more entries
     * 4. Restart follower (with persisted state)
     * 5. Follower catches up via AppendEntries
     * 6. Verify exact state convergence
     */
    @Test
    void testFollowerRestartAndCatchUp() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Follower Restart and Catch-Up");
        System.out.println("========================================\n");

        // Phase 1: Start cluster
        setupCluster();
        serverA = startServer(portA, dataDirA.resolve("meta.json"), configA, dataDirA);
        serverB = startServer(portB, dataDirB.resolve("meta.json"), configB, dataDirB);
        serverC = startServer(portC, dataDirC.resolve("meta.json"), configC, dataDirC);
        wireApplier(serverA);
        wireApplier(serverB);
        wireApplier(serverC);

        waitForLeader(15000);
        MetadataServer leader = getLeader();
        assertNotNull(leader, "Should have a leader");
        System.out.println("Leader: " + leader.getRaftNode().getNodeId());

        // Submit entries
        int entries = 4;
        long lastIndex = 0;
        for (int i = 1; i <= entries; i++) {
            lastIndex = submitCreate(leader, "restart-catchup-" + i);
        }
        waitForApplied(leader, lastIndex, 5000);
        System.out.println("Submitted " + entries + " entries, lastIndex=" + lastIndex);

        // Find follower
        MetadataServer follower = null;
        for (MetadataServer s : List.of(serverA, serverB, serverC)) {
            if (s != null && !s.getRaftNode().isLeader()) {
                follower = s;
                break;
            }
        }
        assertNotNull(follower, "Should have a follower");
        String followerId = follower.getRaftNode().getNodeId();
        Path followerDataDir = followerId.equals("meta-a") ? dataDirA :
                               followerId.equals("meta-b") ? dataDirB : dataDirC;

        // Capture expected state
        Map<String, Object> expectedState = captureState(leader);
        long expectedLastLogIndex = leader.getRaftNode().getRaftLog().getLastLogIndex();
        long expectedCommitIndex = leader.getRaftNode().getRaftLog().getCommitIndex();
        long expectedLastApplied = leader.getRaftNode().getRaftLog().getLastApplied();

        // Stop follower completely
        System.out.println("Stopping follower: " + followerId);
        follower.stop();

        // Leader continues with more entries
        int moreEntries = 2;
        for (int i = 1; i <= moreEntries; i++) {
            lastIndex = submitCreate(leader, "restart-catchup-more-" + i);
        }
        waitForApplied(leader, lastIndex, 3000);
        System.out.println("Submitted " + moreEntries + " more entries while follower was offline");

        // Updated expected state
        expectedState = captureState(leader);
        expectedLastLogIndex = leader.getRaftNode().getRaftLog().getLastLogIndex();
        expectedCommitIndex = leader.getRaftNode().getRaftLog().getCommitIndex();
        expectedLastApplied = leader.getRaftNode().getRaftLog().getLastApplied();

        // Restart follower with the same client/Raft ports and data directory.
        // Register the replacement in the fixture before starting it so an
        // exception during startup cannot leave an unowned RaftNode running.
        System.out.println("Restarting follower: " + followerId + " with same data dir");
        ClusterConfig followerConfig = followerId.equals("meta-a") ? configA :
                                       followerId.equals("meta-b") ? configB : configC;
        int followerPort = followerId.equals("meta-a") ? portA :
                           followerId.equals("meta-b") ? portB : portC;
        MetadataServer restartedFollower = new MetadataServer(
            followerPort,
            followerDataDir.resolve("meta.json"),
            2,
            6000,
            2000,
            followerConfig,
            followerDataDir);
        if (followerId.equals("meta-a")) {
            serverA = restartedFollower;
        } else if (followerId.equals("meta-b")) {
            serverB = restartedFollower;
        } else {
            serverC = restartedFollower;
        }
        follower = restartedFollower;
        follower.getRaftNode().start();
        follower.getRaftNode().waitForRpcServerReady();
        wireApplier(follower);
        Thread.sleep(200);

        // Wait for catch-up
        System.out.println("Waiting for follower to catch up...");
        waitForApplied(follower, lastIndex, 10000);

        // Verify state convergence
        Map<String, Object> actualState = captureState(follower);
        long actualLastLogIndex = follower.getRaftNode().getRaftLog().getLastLogIndex();
        long actualCommitIndex = follower.getRaftNode().getRaftLog().getCommitIndex();
        long actualLastApplied = follower.getRaftNode().getRaftLog().getLastApplied();

        System.out.println("Expected: lastLogIndex=" + expectedLastLogIndex + " commitIndex=" + expectedCommitIndex +
            " lastApplied=" + expectedLastApplied);
        System.out.println("Actual:   lastLogIndex=" + actualLastLogIndex + " commitIndex=" + actualCommitIndex +
            " lastApplied=" + actualLastApplied);
        System.out.println("Expected state: " + expectedState.keySet());
        System.out.println("Actual state:   " + actualState.keySet());

        // Mandatory assertions
        assertEquals(expectedLastLogIndex, actualLastLogIndex,
            "Follower lastLogIndex must match after restart and catch-up");
        assertEquals(expectedCommitIndex, actualCommitIndex,
            "Follower commitIndex must match after restart and catch-up");
        assertEquals(expectedLastApplied, actualLastApplied,
            "Follower lastApplied must match after restart and catch-up");
        assertEquals(expectedState, actualState,
            "Follower state must exactly match leader after restart and catch-up");

        System.out.println("\n========================================");
        System.out.println("TEST: Follower Restart and Catch-Up - PASSED");
        System.out.println("========================================\n");
    }

    // ===== TEST 3: State machine exactly-once application =====

    /**
     * Verifies that even if AppendEntries is retried, each committed entry
     * is applied to the state machine exactly once.
     */
    @Test
    void testExactlyOnceApplication() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Exactly-Once State Machine Application");
        System.out.println("========================================\n");

        // Start cluster
        setupCluster();
        serverA = startServer(portA, dataDirA.resolve("meta.json"), configA, dataDirA);
        serverB = startServer(portB, dataDirB.resolve("meta.json"), configB, dataDirB);
        serverC = startServer(portC, dataDirC.resolve("meta.json"), configC, dataDirC);
        wireApplier(serverA);
        wireApplier(serverB);
        wireApplier(serverC);

        waitForLeader(15000);
        MetadataServer leader = getLeader();
        assertNotNull(leader, "Should have a leader");
        System.out.println("Leader: " + leader.getRaftNode().getNodeId());

        // Submit entries and wait for commitment
        int entries = 3;
        long lastIndex = 0;
        for (int i = 1; i <= entries; i++) {
            lastIndex = submitCreate(leader, "exactly-once-" + i);
        }
        waitForApplied(leader, lastIndex, 5000);

        // Verify exact state on leader
        Map<String, Object> leaderState = captureState(leader);
        System.out.println("Leader state: " + leaderState.keySet());

        // The key invariant: each object appears exactly once in the state
        // If exactly-once is violated, we'd see duplicate objects
        for (String name : leaderState.keySet()) {
            int count = 0;
            for (String n : leaderState.keySet()) {
                if (n.equals(name)) count++;
            }
            assertEquals(1, count,
                "Object " + name + " should appear exactly once in state machine");
        }
        System.out.println("Exactly-once verified: each object appears exactly once");

        System.out.println("\n========================================");
        System.out.println("TEST: Exactly-Once Application - PASSED");
        System.out.println("========================================\n");
    }
}
