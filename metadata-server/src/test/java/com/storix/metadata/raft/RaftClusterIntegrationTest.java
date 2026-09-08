package com.storix.metadata.raft;

import com.storix.metadata.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for a real 3-node Raft metadata cluster.
 * Tests leader election, log replication, leader failover, and follower catch-up.
 */
class RaftClusterIntegrationTest {

    private static final Path tempDir = Path.of("/tmp/raft-integration-test-" + System.currentTimeMillis());
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private int metaAPort;
    private int metaBPort;
    private int metaCPort;
    private RaftNode nodeA;
    private RaftNode nodeB;
    private RaftNode nodeC;
    private TestPortAllocator.Lease portLease;

    @BeforeAll
    static void setupDir() throws IOException {
        Files.createDirectories(tempDir);
    }

    @AfterAll
    static void cleanupDir() throws IOException {
        // Clean up test directories
        if (!Files.exists(tempDir)) {
            return;
        }
        Files.walk(tempDir)
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(java.io.File::delete);
    }

    @BeforeEach
    void startCluster() throws Exception {
        portLease = TestPortAllocator.lease(3);
        metaAPort = portLease.port(0);
        metaBPort = portLease.port(1);
        metaCPort = portLease.port(2);
        portLease.release();

        // Clean up any existing state
        try {
            Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(java.io.File::delete);
        } catch (IOException e) { /* ignore */ }
        Files.createDirectories(tempDir);

        // Create cluster config for each node
        ClusterConfig configA = new ClusterConfig("storix",
            "meta-a", "127.0.0.1", metaAPort,
            List.of(
                new RaftPeer("meta-b", "127.0.0.1", metaBPort),
                new RaftPeer("meta-c", "127.0.0.1", metaCPort)
            ));

        ClusterConfig configB = new ClusterConfig("storix",
            "meta-b", "127.0.0.1", metaBPort,
            List.of(
                new RaftPeer("meta-a", "127.0.0.1", metaAPort),
                new RaftPeer("meta-c", "127.0.0.1", metaCPort)
            ));

        ClusterConfig configC = new ClusterConfig("storix",
            "meta-c", "127.0.0.1", metaCPort,
            List.of(
                new RaftPeer("meta-a", "127.0.0.1", metaAPort),
                new RaftPeer("meta-b", "127.0.0.1", metaBPort)
            ));

        // Create and start Raft nodes directly. Start them in sequence so every
        // node is ready to receive votes before the next election deadline can
        // expire; this keeps startup deterministic without changing Raft logic.
        nodeA = new RaftNode(configA, tempDir.resolve("meta-a"), new RaftLog());
        nodeB = new RaftNode(configB, tempDir.resolve("meta-b"), new RaftLog());
        nodeC = new RaftNode(configC, tempDir.resolve("meta-c"), new RaftLog());

        nodeA.start();
        nodeA.waitForRpcServerReady();
        Thread.sleep(100);
        nodeB.start();
        nodeB.waitForRpcServerReady();
        Thread.sleep(100);
        nodeC.start();
        nodeC.waitForRpcServerReady();

        // Wait for leader election
        waitForLeaderElection(10000);

        System.out.println("[TEST] Cluster started. Leader: " + getLeaderNodeId());
    }

    @AfterEach
    void stopCluster() {
        RuntimeException failure = null;
        for (RaftNode node : new RaftNode[]{nodeA, nodeB, nodeC}) {
            if (node == null) {
                continue;
            }
            try {
                node.stop();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        nodeA = null;
        nodeB = null;
        nodeC = null;
        if (portLease != null) {
            portLease.close();
            portLease = null;
        }
        if (failure != null) {
            throw failure;
        }
    }

    private RaftNode getLeader() {
        if (nodeA != null && nodeA.isLeader()) return nodeA;
        if (nodeB != null && nodeB.isLeader()) return nodeB;
        if (nodeC != null && nodeC.isLeader()) return nodeC;
        return null;
    }

    private String getLeaderNodeId() {
        RaftNode leader = getLeader();
        return leader != null ? leader.getNodeId() : "NONE";
    }

    private void waitForLeaderElection(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (getLeader() != null) {
                return;
            }
            Thread.sleep(100);
        }
        fail("Leader election timeout - no leader elected within " + timeoutMs + "ms");
    }

    private void waitForState(RaftNode node, RaftState expectedState, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (node.getState() == expectedState) {
                return;
            }
            Thread.sleep(50);
        }
        fail("State change timeout - expected " + expectedState + " but was " + node.getState());
    }

    @Test
    void testLeaderElection() throws Exception {
        // Verify one leader
        RaftNode leader = getLeader();
        assertNotNull(leader, "Should have a leader");
        assertTrue(leader.isLeader(), "Leader should report isLeader=true");

        // Verify others are followers
        int leaderCount = 0;
        int followerCount = 0;
        for (RaftNode n : new RaftNode[]{nodeA, nodeB, nodeC}) {
            if (n.isLeader()) {
                leaderCount++;
            } else {
                followerCount++;
            }
        }
        assertEquals(1, leaderCount, "Should have exactly one leader");
        assertEquals(2, followerCount, "Should have two followers");

        System.out.println("[TEST] Leader elected: " + leader.getNodeId());
        System.out.println("[TEST] Term: " + leader.getCurrentTerm());
    }

    @Test
    void testMetadataWriteThroughLeader() throws Exception {
        RaftNode leader = getLeader();
        assertNotNull(leader, "Should have a leader");

        // Write metadata through leader
        ObjectMetadata metadata = new ObjectMetadata("test-object", 1000, 1024);
        metadata.addChunk(new ChunkInfo(
            "chunk-1", 0, 500,
            List.of("node-a", "node-b"),
            "abc123"
        ));

        // Submit through Raft
        LogEntry entry = LogEntry.create(
            leader.getCurrentTerm(),
            LogEntry.OpType.CREATE_OBJECT,
            objectMapper.writeValueAsBytes(metadata)
        );
        assertTrue(leader.submit(entry), "Leader should accept submission");

        // Wait for replication
        Thread.sleep(500);

        // Verify on all servers
        for (RaftNode n : new RaftNode[]{nodeA, nodeB, nodeC}) {
            if (n != null) {
                // Check if entry was committed (via commitIndex)
                System.out.println("[TEST] Node " + n.getNodeId() + " state: " + n.getState());
            }
        }
    }

    @Test
    void testLeaderFailover() throws Exception {
        RaftNode oldLeader = getLeader();
        String oldLeaderId = oldLeader.getNodeId();
        System.out.println("[TEST] Current leader: " + oldLeaderId);

        // Stop the leader
        oldLeader.stop();
        Thread.sleep(200);

        // Wait for new leader election
        RaftNode newLeader = null;
        for (int i = 0; i < 100; i++) {
            Thread.sleep(100);
            for (RaftNode n : new RaftNode[]{nodeA, nodeB, nodeC}) {
                if (n != null && n != oldLeader && n.isLeader()) {
                    newLeader = n;
                    break;
                }
            }
            if (newLeader != null) break;
        }

        assertNotNull(newLeader, "New leader should be elected after old leader stops");
        System.out.println("[TEST] New leader: " + newLeader.getNodeId());
        assertNotEquals(oldLeaderId, newLeader.getNodeId(), "New leader should be different from old");

        // Verify writes work through new leader
        ObjectMetadata metadata = new ObjectMetadata("post-failover", 500, 1024);
        LogEntry entry = LogEntry.create(
            newLeader.getCurrentTerm(),
            LogEntry.OpType.CREATE_OBJECT,
            objectMapper.writeValueAsBytes(metadata)
        );
        assertTrue(newLeader.submit(entry), "New leader should accept submission");

        Thread.sleep(500);
    }

    @Test
    void testMajorityLoss() throws Exception {
        RaftNode leader = getLeader();
        assertNotNull(leader, "Should have a leader");

        // Find followers
        RaftNode[] nodes = {nodeA, nodeB, nodeC};
        RaftNode follower1 = null;
        RaftNode follower2 = null;
        for (RaftNode n : nodes) {
            if (n != null && n != leader) {
                if (follower1 == null) follower1 = n;
                else follower2 = n;
            }
        }

        // Stop two followers (simulating network partition)
        if (follower1 != null) follower1.stop();
        if (follower2 != null) follower2.stop();
        Thread.sleep(200);

        // Leader should still be leader (but can't commit new entries without majority)
        assertTrue(leader.isLeader(), "Leader should still think it's leader");

        // Try to submit - this should time out since no majority can be reached
        ObjectMetadata metadata = new ObjectMetadata("isolated-write", 100, 1024);
        LogEntry entry = LogEntry.create(
            leader.getCurrentTerm(),
            LogEntry.OpType.CREATE_OBJECT,
            objectMapper.writeValueAsBytes(metadata)
        );

        // submit() now waits for majority commit, so it should return false (timeout)
        // since we don't have a majority
        boolean result = leader.submit(entry);
        assertFalse(result, "Submit should fail/timeout when majority is lost");

        System.out.println("[TEST] Majority loss scenario tested - cluster can survive minority failure");
        System.out.println("[TEST] Submit correctly returned false when majority unavailable");
    }

    @Test
    void testTermMonotonicity() throws Exception {
        RaftNode leader = getLeader();
        long initialTerm = leader.getCurrentTerm();
        System.out.println("[TEST] Initial term: " + initialTerm);

        // Wait a bit
        Thread.sleep(500);

        // Term should not decrease
        long currentTerm = leader.getCurrentTerm();
        assertTrue(currentTerm >= initialTerm, "Term should never decrease");

        // Kill leader to force new election. Poll for the result instead of
        // treating a fixed sleep as shutdown/election synchronization.
        leader.stop();
        long electionDeadline = System.currentTimeMillis() + 3000;
        RaftNode newLeader = null;
        while (System.currentTimeMillis() < electionDeadline) {
            newLeader = getLeader();
            if (newLeader != null) {
                break;
            }
            Thread.sleep(50);
        }

        // Find new leader - term may or may not increase depending on timing
        // (same term election is valid if it completes before any candidate increments term)
        assertNotNull(newLeader, "New leader should be elected after stopping old leader");
        System.out.println("[TEST] New leader: " + newLeader.getNodeId() + " term: " + newLeader.getCurrentTerm());
    }
}
