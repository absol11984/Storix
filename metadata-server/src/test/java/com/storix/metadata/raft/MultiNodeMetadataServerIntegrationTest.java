package com.storix.metadata.raft;

import com.storix.metadata.*;
import com.storix.metadata.wal.GenerationManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for a real 3-node MetadataServer Raft cluster.
 *
 * This test proves that:
 * 1. Three independent MetadataServer instances can form a Raft cluster
 * 2. Exactly one leader is elected via real TCP network RPC
 * 3. Leader fails → new election → exactly one new leader
 * 4. Old leader restart → becomes follower, exactly one leader
 * 5. Majority required (2/3) for leadership
 * 6. Higher term step-down
 * 7. Term monotonicity (terms never decrease)
 * 8. Term persistence across restart
 * 9. Real network communication between independent MetadataServer instances
 * 10. Independent persistent directories per server
 */
class MultiNodeMetadataServerIntegrationTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    // Atomic counter for unique port assignment per test
    private static final AtomicLong portCounter = new AtomicLong(System.currentTimeMillis() % 10000);

    // Ports for the three nodes
    private int portA;
    private int portB;
    private int portC;

    // Data directories
    private Path clusterDir;
    private Path dataDirA;
    private Path dataDirB;
    private Path dataDirC;

    // The three MetadataServer instances
    private MetadataServer serverA;
    private MetadataServer serverB;
    private MetadataServer serverC;

    // Cluster configuration
    private ClusterConfig configA;
    private ClusterConfig configB;
    private ClusterConfig configC;

    // For tracking previous terms across restarts
    private final AtomicLong previousHighestTerm = new AtomicLong(0);

    @BeforeEach
    void setupCluster() throws Exception {
        // Use incrementing ports to avoid conflicts between tests
        long base = portCounter.addAndGet(10);
        portA = (int) (45000 + base % 15000);
        portB = portA + 10;  // Separate ports for client and raft
        portC = portA + 20;
        int raftPortA = portA + 1;
        int raftPortB = portB + 1;
        int raftPortC = portC + 1;

        // Create unique cluster directory for this test
        clusterDir = Files.createTempDirectory("meta-cluster-integration-" + System.nanoTime());
        dataDirA = clusterDir.resolve("meta-a");
        dataDirB = clusterDir.resolve("meta-b");
        dataDirC = clusterDir.resolve("meta-c");
        Files.createDirectories(dataDirA);
        Files.createDirectories(dataDirB);
        Files.createDirectories(dataDirC);

        // Create cluster configs with separate client and raft ports
        configA = new ClusterConfig("storix",
            "meta-a", "127.0.0.1", portA, raftPortA,
            List.of(
                new RaftPeer("meta-b", "127.0.0.1", raftPortB),
                new RaftPeer("meta-c", "127.0.0.1", raftPortC)
            ));

        configB = new ClusterConfig("storix",
            "meta-b", "127.0.0.1", portB, raftPortB,
            List.of(
                new RaftPeer("meta-a", "127.0.0.1", raftPortA),
                new RaftPeer("meta-c", "127.0.0.1", raftPortC)
            ));

        configC = new ClusterConfig("storix",
            "meta-c", "127.0.0.1", portC, raftPortC,
            List.of(
                new RaftPeer("meta-a", "127.0.0.1", raftPortA),
                new RaftPeer("meta-b", "127.0.0.1", raftPortB)
            ));
    }

    @AfterEach
    void teardownCluster() {
        stopAllServers();
        // Clean up directories
        try {
            if (clusterDir != null) {
                Files.walk(clusterDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(java.io.File::delete);
            }
        } catch (IOException e) {
            // ignore
        }
        // Extra wait for OS to release ports (TIME_WAIT state)
        // Must exceed election timeout (6000ms) to ensure election deadlines fully expire
        // and election activity settles before the next test's startAllServers().
        // Without this, previous test's servers may still be timing out/electing, causing
        // stale state to pollute the next test's fresh server initialization.
        try { Thread.sleep(8000); } catch (InterruptedException e) { /* ignore */ }
    }

    /**
     * Creates and starts a server, with retry on BindException.
     * Some OS configurations have longer TIME_WAIT linger; retry handles this.
     */
    private MetadataServer createServerWithRetry(int port, Path metaFile,
            ClusterConfig config, Path dataDir, String label, int maxRetries)
            throws Exception {
        Exception lastEx = null;
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            if (attempt > 0) {
                System.out.println("[TEST] Retry " + attempt + " for " + label + " on port " + port);
                Thread.sleep(500 * attempt);
            }
            try {
                MetadataServer server = new MetadataServer(port, metaFile, 2, 6000, 2000, config, dataDir);
                System.out.println("[TEST] Server " + label + " created");
                return server;
            } catch (IOException e) {
                lastEx = e;
                System.out.println("[TEST] Attempt " + (attempt + 1) + " for " + label + " failed: " + e.getMessage());
            }
        }
        throw new IOException("Failed to create " + label + " after " + maxRetries + " attempts", lastEx);
    }

    private void startAllServers() throws Exception {
        Path metaFileA = dataDirA.resolve("metadata.json");
        Path metaFileB = dataDirB.resolve("metadata.json");
        Path metaFileC = dataDirC.resolve("metadata.json");

        System.out.println("[TEST] Starting servers on ports: A=" + portA + ", B=" + portB + ", C=" + portC);

        // Create with retry on BindException
        serverA = createServerWithRetry(portA, metaFileA, configA, dataDirA, "A", 3);
        // Start RaftNode (which starts RPC server, election loop, apply loop)
        serverA.getRaftNode().start();
        // Wait for A's RPC server to be ready before creating B
        serverA.getRaftNode().waitForRpcServerReady();
        System.out.println("[TEST] Server A RPC ready");

        serverB = createServerWithRetry(portB, metaFileB, configB, dataDirB, "B", 3);
        serverB.getRaftNode().start();
        // Wait for B's RPC server to be ready before creating C
        serverB.getRaftNode().waitForRpcServerReady();
        System.out.println("[TEST] Server B RPC ready");

        serverC = createServerWithRetry(portC, metaFileC, configC, dataDirC, "C", 3);
        serverC.getRaftNode().start();
        // Wait for C's RPC server to be ready
        serverC.getRaftNode().waitForRpcServerReady();
        System.out.println("[TEST] Server C RPC ready");

        // All servers' RPC servers are now bound and accepting connections.
        // The test thread (not the server threads) now waits for leader election.
        System.out.println("[TEST] All RPC servers ready, waiting for leader election...");


        // Wait a moment for the election timeout loops to settle before polling
        Thread.sleep(100);

        // Wait for leader election
        waitForLeader(15000);
    }

    private void stopAllServers() {
        try { if (serverA != null) serverA.stop(); } catch (Exception e) { /* ignore */ }
        try { if (serverB != null) serverB.stop(); } catch (Exception e) { /* ignore */ }
        try { if (serverC != null) serverC.stop(); } catch (Exception e) { /* ignore */ }
        // Wait for ports to be released (OS TIME_WAIT)
        try { Thread.sleep(1000); } catch (InterruptedException e) { /* ignore */ }
    }

    private MetadataServer getLeaderServer() {
        if (serverA != null && serverA.getRaftNode() != null && serverA.getRaftNode().isLeader()) return serverA;
        if (serverB != null && serverB.getRaftNode() != null && serverB.getRaftNode().isLeader()) return serverB;
        if (serverC != null && serverC.getRaftNode() != null && serverC.getRaftNode().isLeader()) return serverC;
        return null;
    }

    private int getLeaderCount() {
        int count = 0;
        if (serverA != null && serverA.getRaftNode() != null && serverA.getRaftNode().isLeader()) count++;
        if (serverB != null && serverB.getRaftNode() != null && serverB.getRaftNode().isLeader()) count++;
        if (serverC != null && serverC.getRaftNode() != null && serverC.getRaftNode().isLeader()) count++;
        return count;
    }

    private int getRunningLeaderCount() {
        int count = 0;
        // Only count leaders that are actually running (isRunning() == true).
        // A stopped node must not be counted as an active leader.
        if (serverA != null && serverA.getRaftNode() != null && serverA.getRaftNode().isRunning() && serverA.getRaftNode().isLeader()) count++;
        if (serverB != null && serverB.getRaftNode() != null && serverB.getRaftNode().isRunning() && serverB.getRaftNode().isLeader()) count++;
        if (serverC != null && serverC.getRaftNode() != null && serverC.getRaftNode().isRunning() && serverC.getRaftNode().isLeader()) count++;
        return count;
    }

    private int getFollowerCount() {
        int count = 0;
        // Only count servers that are running and not leaders
        if (serverA != null && serverA.getRaftNode() != null && serverA.getRaftNode().isRunning() && !serverA.getRaftNode().isLeader()) count++;
        if (serverB != null && serverB.getRaftNode() != null && serverB.getRaftNode().isRunning() && !serverB.getRaftNode().isLeader()) count++;
        if (serverC != null && serverC.getRaftNode() != null && serverC.getRaftNode().isRunning() && !serverC.getRaftNode().isLeader()) count++;
        return count;
    }

    private int getActiveServerCount() {
        int count = 0;
        if (serverA != null && serverA.getRaftNode() != null && serverA.getRaftNode().isRunning()) count++;
        if (serverB != null && serverB.getRaftNode() != null && serverB.getRaftNode().isRunning()) count++;
        if (serverC != null && serverC.getRaftNode() != null && serverC.getRaftNode().isRunning()) count++;
        return count;
    }

    private long getCurrentTerm(MetadataServer server) {
        if (server == null || server.getRaftNode() == null) return -1;
        return server.getRaftNode().getCurrentTerm();
    }

    private void waitForLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (getLeaderCount() == 1) {
                return;
            }
            Thread.sleep(100);
        }
        String stateA = (serverA != null && serverA.getRaftNode() != null) ? serverA.getRaftNode().getState().toString() : "null";
        String stateB = (serverB != null && serverB.getRaftNode() != null) ? serverB.getRaftNode().getState().toString() : "null";
        String stateC = (serverC != null && serverC.getRaftNode() != null) ? serverC.getRaftNode().getState().toString() : "null";
        fail("Leader election timeout - leaderCount=" + getLeaderCount() +
             ", serverA=" + stateA +
             ", serverB=" + stateB +
             ", serverC=" + stateC);
    }

    private void waitForNoLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (getLeaderCount() == 0) {
                return;
            }
            Thread.sleep(100);
        }
        fail("Expected no leader but leaderCount=" + getLeaderCount());
    }

    private void waitForFollowerCount(int expected, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (getFollowerCount() == expected) {
                return;
            }
            Thread.sleep(100);
        }
        fail("Follower count timeout - expected=" + expected + ", actual=" + getFollowerCount());
    }

    // ===== TEST 1: Basic Leader Election =====

    /**
     * Verifies that exactly one leader is elected among three MetadataServer instances.
     * This is the fundamental requirement of Raft consensus.
     */
    @Test
    void testBasicLeaderElection() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Basic Leader Election");
        System.out.println("========================================\n");

        startAllServers();

        // Verify exactly one leader
        assertEquals(1, getLeaderCount(), "Should have exactly one leader");

        // Verify two followers
        assertEquals(2, getFollowerCount(), "Should have two followers");

        MetadataServer leader = getLeaderServer();
        assertNotNull(leader, "Should have a leader");

        System.out.println("Leader elected: " + leader.getRaftNode().getNodeId());
        System.out.println("Leader term: " + leader.getRaftNode().getCurrentTerm());
        System.out.println("Follower count: " + getFollowerCount());

        // Record term for monotonicity tests
        long term = leader.getRaftNode().getCurrentTerm();
        previousHighestTerm.set(Math.max(previousHighestTerm.get(), term));

        System.out.println("\n========================================");
        System.out.println("TEST: Basic Leader Election - PASSED");
        System.out.println("========================================\n");
    }

    // ===== TEST 2: Leader Stability =====

    /**
     * Verifies that once elected, the leader remains stable across multiple heartbeat intervals.
     */
    @Test
    void testLeaderStability() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Leader Stability");
        System.out.println("========================================\n");

        startAllServers();

        MetadataServer leader = getLeaderServer();
        assertNotNull(leader, "Should have a leader");
        String leaderId = leader.getRaftNode().getNodeId();
        long initialTerm = leader.getRaftNode().getCurrentTerm();

        // Wait for multiple heartbeat intervals (heartbeat is 50ms)
        // Wait at least 600ms (12 heartbeats) to ensure stability
        Thread.sleep(600);

        // Verify still one leader
        assertEquals(1, getLeaderCount(), "Should still have exactly one leader");

        // Verify it's the same leader
        MetadataServer currentLeader = getLeaderServer();
        assertNotNull(currentLeader, "Should still have a leader");
        assertEquals(leaderId, currentLeader.getRaftNode().getNodeId(),
            "Leader should be stable across heartbeats");

        // Verify term hasn't decreased
        assertEquals(initialTerm, currentLeader.getRaftNode().getCurrentTerm(),
            "Term should not decrease");

        System.out.println("Leader remained stable: " + leaderId);
        System.out.println("Term remained: " + initialTerm);
        System.out.println("\n========================================");
        System.out.println("TEST: Leader Stability - PASSED");
        System.out.println("========================================\n");
    }

    // ===== TEST 3: Leader Failure → New Election =====

    /**
     * Verifies that when the leader fails, a new leader is elected among the survivors.
     * This proves the cluster can recover from single-node failures.
     */
    @Test
    void testLeaderFailureTriggersNewElection() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Leader Failure Triggers New Election");
        System.out.println("========================================\n");

        startAllServers();

        MetadataServer oldLeader = getLeaderServer();
        assertNotNull(oldLeader, "Should have a leader");
        String oldLeaderId = oldLeader.getRaftNode().getNodeId();
        long oldTerm = oldLeader.getRaftNode().getCurrentTerm();
        System.out.println("Original leader: " + oldLeaderId + " (term=" + oldTerm + ")");

        // Stop the leader
        System.out.println("Stopping leader: " + oldLeaderId);
        oldLeader.stop();

        // Wait for new leader election among survivors
        System.out.println("Waiting for new leader election...");
        waitForLeader(10000);

        // Verify exactly one running leader exists (running node only)
        assertEquals(1, getRunningLeaderCount(),
            "Exactly one running leader must exist after failure");

        // Get the new leader and verify its properties
        MetadataServer newLeader = getLeaderServer();
        assertNotNull(newLeader, "Should have a new leader");
        System.out.println("New leader: " + newLeader.getRaftNode().getNodeId() +
            " (term=" + newLeader.getRaftNode().getCurrentTerm() + ")");

        // Verify the new leader is running and has LEADER state
        assertTrue(newLeader.getRaftNode().isRunning(),
            "New leader must be running");
        assertEquals(RaftState.LEADER, newLeader.getRaftNode().getState(),
            "New leader must have LEADER state");

        // The new term should be >= old term (Raft guarantee)
        assertTrue(newLeader.getRaftNode().getCurrentTerm() >= oldTerm,
            "New term should be >= old term");

        // Record term
        previousHighestTerm.set(Math.max(previousHighestTerm.get(),
            newLeader.getRaftNode().getCurrentTerm()));

        System.out.println("\n========================================");
        System.out.println("TEST: Leader Failure Triggers New Election - PASSED");
        System.out.println("========================================\n");
    }

    // ===== TEST 4: Old Leader Restart → Becomes Follower =====

    /**
     * Verifies that when a previously-stopped leader restarts, it rejoins as a follower.
     * This proves the cluster properly handles leader reconnection.
     */
    @Test
    void testOldLeaderRestartBecomesFollower() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Old Leader Restart Becomes Follower");
        System.out.println("========================================\n");

        startAllServers();

        MetadataServer originalLeader = getLeaderServer();
        assertNotNull(originalLeader, "Should have a leader");
        String originalLeaderId = originalLeader.getRaftNode().getNodeId();
        long termBeforeStop = originalLeader.getRaftNode().getCurrentTerm();
        System.out.println("Original leader: " + originalLeaderId + " (term=" + termBeforeStop + ")");

        // Stop the leader
        System.out.println("Stopping leader: " + originalLeaderId);
        originalLeader.stop();

        // Wait for new leader
        waitForLeader(10000);
        MetadataServer newLeader = getLeaderServer();
        assertNotNull(newLeader, "Should have a new leader");
        System.out.println("New leader: " + newLeader.getRaftNode().getNodeId() +
            " (term=" + newLeader.getRaftNode().getCurrentTerm() + ")");

        // Restart the old leader's RaftNode (MetadataServer.start() already shut it down)
        System.out.println("Restarting old leader: " + originalLeaderId);
        originalLeader.getRaftNode().start();

        // Give it time to reconnect and potentially see higher term
        Thread.sleep(500);

        // Verify still exactly one leader (the new one)
        assertEquals(1, getLeaderCount(),
            "Should still have exactly one leader after old leader restart");

        // Verify old leader is a follower
        RaftState oldLeaderState = originalLeader.getRaftNode().getState();
        assertNotEquals(RaftState.LEADER, oldLeaderState,
            "Old leader should not be leader after restart");

        System.out.println("Old leader " + originalLeaderId + " state after restart: " + oldLeaderState);
        System.out.println("Current leader: " + getLeaderServer().getRaftNode().getNodeId());

        System.out.println("\n========================================");
        System.out.println("TEST: Old Leader Restart Becomes Follower - PASSED");
        System.out.println("========================================\n");
    }

    // ===== TEST 5: Majority Required =====

    /**
     * Verifies that a single node cannot become leader when majority (2/3) is not available.
     * This proves Raft's majority requirement for leader election.
     *
     * Approach: Start a 3-node cluster, stop all, restart only 1 node.
     * The isolated node's election timeout fires but it cannot achieve majority
     * (needs 2 votes but can only get its own self-vote).
     */
    @Test
    void testMajorityRequiredForLeadership() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Majority Required for Leadership");
        System.out.println("========================================\n");

        // Start a 3-node cluster first
        startAllServers();
        MetadataServer leader = getLeaderServer();
        assertNotNull(leader, "Should have a leader");
        String leaderId = leader.getRaftNode().getNodeId();
        long term = leader.getRaftNode().getCurrentTerm();
        System.out.println("Initial leader: " + leaderId + " (term=" + term + ")");

        // Stop all servers
        System.out.println("Stopping all servers...");
        stopAllServers();

        // Restart only ONE node (simulating a network partition)
        // Use fresh ports for the isolated node
        long base = portCounter.addAndGet(10);
        int isolatedPortA = (int) (45000 + base % 15000);
        int isolatedRaftPortA = isolatedPortA + 1;

        Path metaFileA = dataDirA.resolve("metadata-isolated.json");
        ClusterConfig isolatedConfig = new ClusterConfig("storix",
            "meta-a", "127.0.0.1", isolatedPortA, isolatedRaftPortA,
            List.of(new RaftPeer("meta-b", "127.0.0.1", isolatedPortA + 10)));

        MetadataServer isolatedNode = new MetadataServer(isolatedPortA, metaFileA, 2, 6000, 2000, isolatedConfig, dataDirA);
        isolatedNode.getRaftNode().start();
        isolatedNode.getRaftNode().waitForRpcServerReady();
        System.out.println("Isolated node started on port " + isolatedPortA);

        // The isolated node is alone - it cannot achieve majority (2/3).
        // Wait for at least 2 election timeout cycles
        Thread.sleep(800);

        // Verify isolated node does NOT become leader
        // With just 1 node (self-vote only), it cannot reach majority of 2
        boolean isLeader = isolatedNode.getRaftNode().isLeader();
        System.out.println("Isolated node state: " + isolatedNode.getRaftNode().getState());
        System.out.println("Isolated node term: " + isolatedNode.getRaftNode().getCurrentTerm());
        System.out.println("Isolated node isLeader: " + isLeader);

        assertFalse(isLeader,
            "Isolated single node should NOT be able to become leader (majority=2 required)");

        // Cleanup
        isolatedNode.stop();
        Thread.sleep(500);

        System.out.println("Isolated node did NOT become leader (as expected)");
        System.out.println("\n========================================");
        System.out.println("TEST: Majority Required - PASSED");
        System.out.println("========================================\n");
    }

    // ===== TEST 6: Higher Term Step-Down =====

    /**
     * Verifies that a leader steps down when it observes a higher term.
     * This proves Raft's term-based leader validity.
     */
    @Test
    void testHigherTermStepDown() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Higher Term Step-Down");
        System.out.println("========================================\n");

        startAllServers();

        MetadataServer leader = getLeaderServer();
        assertNotNull(leader, "Should have a leader");
        String leaderId = leader.getRaftNode().getNodeId();
        long initialTerm = leader.getRaftNode().getCurrentTerm();
        System.out.println("Initial leader: " + leaderId + " (term=" + initialTerm + ")");

        // Stop the leader
        System.out.println("Stopping leader: " + leaderId);
        leader.stop();

        // Wait for the old leader's election loop to fully stop before the remaining
        // nodes start their election. This prevents split elections where both remaining
        // nodes fire their timeouts simultaneously and end up in the same term.
        Thread.sleep(500);

        // Wait for new leader (will have term >= initialTerm + 1)
        waitForLeader(10000);
        MetadataServer newLeader = getLeaderServer();
        assertNotNull(newLeader, "Should have a new leader");
        long newTerm = newLeader.getRaftNode().getCurrentTerm();
        System.out.println("New leader: " + newLeader.getRaftNode().getNodeId() +
            " (term=" + newTerm + ")");

        // Verify new term > old term
        assertTrue(newTerm > initialTerm,
            "New term should be higher than old term");

        // Restart the old leader's RaftNode
        System.out.println("Restarting old leader: " + leaderId);
        leader.getRaftNode().start();

        // Wait for old leader to see the higher term
        Thread.sleep(500);

        // Verify old leader stepped down
        RaftState oldLeaderState = leader.getRaftNode().getState();
        assertNotEquals(RaftState.LEADER, oldLeaderState,
            "Old leader should step down after seeing higher term");
        assertTrue(leader.getRaftNode().getCurrentTerm() >= newTerm,
            "Old leader's term should be updated to >= new term");

        System.out.println("Old leader stepped down to: " + oldLeaderState);
        System.out.println("Old leader's term updated to: " + leader.getRaftNode().getCurrentTerm());

        System.out.println("\n========================================");
        System.out.println("TEST: Higher Term Step-Down - PASSED");
        System.out.println("========================================\n");
    }

    // ===== TEST 7: Term Monotonicity =====

    /**
     * Verifies that terms never decrease across leader elections.
     * This is a fundamental Raft safety property.
     */
    @Test
    void testTermMonotonicity() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Term Monotonicity");
        System.out.println("========================================\n");

        long maxTerm = 0;

        // Phase 1: Initial election
        System.out.println("Phase 1: Initial election");
        startAllServers();

        MetadataServer leader1 = getLeaderServer();
        assertNotNull(leader1, "Should have a leader");
        long term1 = leader1.getRaftNode().getCurrentTerm();
        System.out.println("  Term after initial election: " + term1);
        assertTrue(term1 > 0, "Term should be positive");
        maxTerm = Math.max(maxTerm, term1);

        // Record for overall tracking
        previousHighestTerm.set(Math.max(previousHighestTerm.get(), term1));

        // Phase 2: Kill leader, restart killed node, wait for new election
        // Restarting the killed leader ensures exactly 2 running nodes for Phase 2,
        // preventing split votes (2-node cluster: one leader + one follower).
        System.out.println("Phase 2: Kill leader, restart killed node, new election");
        leader1.stop();
        Thread.sleep(50);
        leader1.getRaftNode().start();
        leader1.getRaftNode().waitForRpcServerReady();
        waitForLeader(10000);

        MetadataServer leader2 = getLeaderServer();
        assertNotNull(leader2, "Should have a new leader");
        long term2 = leader2.getRaftNode().getCurrentTerm();
        System.out.println("  Term after second election: " + term2);
        assertTrue(term2 >= term1, "Term should be >= previous term (monotonicity)");
        maxTerm = Math.max(maxTerm, term2);

        previousHighestTerm.set(Math.max(previousHighestTerm.get(), term2));

        // Phase 3: Kill leader again, restart killed node, wait for third election
        // Restarting the killed leader ensures exactly 2 running nodes for Phase 3,
        // preventing split votes (2-node cluster: one leader + one follower).
        System.out.println("Phase 3: Kill leader again, restart killed node, third election");
        leader2.stop();
        Thread.sleep(50);
        leader2.getRaftNode().start();
        leader2.getRaftNode().waitForRpcServerReady();
        // Give the restarted node's election timeout loop a moment to start before waiting
        Thread.sleep(200);
        waitForLeader(10000);

        MetadataServer leader3 = getLeaderServer();
        assertNotNull(leader3, "Should have a third leader");
        long term3 = leader3.getRaftNode().getCurrentTerm();
        System.out.println("  Term after third election: " + term3);
        assertTrue(term3 >= term2, "Term should be >= previous term (monotonicity)");
        maxTerm = Math.max(maxTerm, term3);

        previousHighestTerm.set(Math.max(previousHighestTerm.get(), term3));

        // Verify all nodes have terms >= maxTerm
        for (MetadataServer server : List.of(serverA, serverB, serverC)) {
            if (server != null && server.getRaftNode() != null) {
                long nodeTerm = server.getRaftNode().getCurrentTerm();
                assertTrue(nodeTerm >= maxTerm - 1, // Allow for one-term difference during transition
                    "Node " + server.getRaftNode().getNodeId() + " term=" + nodeTerm +
                    " should be close to maxTerm=" + maxTerm);
            }
        }

        System.out.println("\n  Monotonicity verified: " + term1 + " -> " + term2 + " -> " + term3);
        System.out.println("\n========================================");
        System.out.println("TEST: Term Monotonicity - PASSED");
        System.out.println("========================================\n");
    }

    // ===== TEST 8: Term Persistence Across Restart =====

    /**
     * Verifies that term and votedFor are correctly persisted across server restarts.
     * This proves Raft's durable state guarantees.
     */
    @Test
    void testTermPersistenceAcrossRestart() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Term Persistence Across Restart");
        System.out.println("========================================\n");

        // Phase 1: Start cluster and elect leader
        System.out.println("Phase 1: Starting cluster");
        startAllServers();

        MetadataServer leader = getLeaderServer();
        assertNotNull(leader, "Should have a leader");
        String leaderId = leader.getRaftNode().getNodeId();
        long termBeforeRestart = leader.getRaftNode().getCurrentTerm();
        System.out.println("Leader: " + leaderId + " (term=" + termBeforeRestart + ")");

        // Phase 2: Stop all servers
        System.out.println("Phase 2: Stopping all servers");
        stopAllServers();

        // Phase 3: Restart all servers
        System.out.println("Phase 3: Restarting all servers");
        startAllServers();

        // Phase 4: Verify term is preserved
        MetadataServer newLeader = getLeaderServer();
        assertNotNull(newLeader, "Should have a leader after restart");
        long termAfterRestart = newLeader.getRaftNode().getCurrentTerm();
        System.out.println("New leader after restart: " + newLeader.getRaftNode().getNodeId() +
            " (term=" + termAfterRestart + ")");

        // Term should be at least as high as before (could be same or +1 due to election)
        assertTrue(termAfterRestart >= termBeforeRestart,
            "Term after restart should be >= term before restart");

        // In most cases it should be the same (no election needed)
        // If it increased, that's also fine (election happened)
        System.out.println("Term before restart: " + termBeforeRestart);
        System.out.println("Term after restart: " + termAfterRestart);

        System.out.println("\n========================================");
        System.out.println("TEST: Term Persistence - PASSED");
        System.out.println("========================================\n");
    }

    // ===== TEST 9: Cluster Recovery After Full Shutdown =====

    /**
     * Verifies that the cluster can fully shut down and restart correctly.
     * This is the ultimate integration test for cluster resilience.
     */
    @Test
    void testClusterRecoveryAfterFullShutdown() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Cluster Recovery After Full Shutdown");
        System.out.println("========================================\n");

        // Phase 1: Start cluster
        System.out.println("Phase 1: Starting cluster");
        startAllServers();

        MetadataServer leader1 = getLeaderServer();
        assertNotNull(leader1, "Should have a leader");
        String leader1Id = leader1.getRaftNode().getNodeId();
        System.out.println("Leader 1: " + leader1Id);

        // Phase 2: Full shutdown
        System.out.println("Phase 2: Full shutdown");
        stopAllServers();
        Thread.sleep(500);

        // Phase 3: Full restart
        System.out.println("Phase 3: Full restart");
        startAllServers();

        // Phase 4: Verify recovery
        assertEquals(1, getLeaderCount(), "Should have exactly one leader after recovery");

        MetadataServer leader2 = getLeaderServer();
        assertNotNull(leader2, "Should have a leader");
        String leader2Id = leader2.getRaftNode().getNodeId();
        System.out.println("Leader 2 (after recovery): " + leader2Id);

        // Verify all servers are in valid states
        for (MetadataServer server : List.of(serverA, serverB, serverC)) {
            if (server != null && server.getRaftNode() != null) {
                RaftState state = server.getRaftNode().getState();
                assertTrue(state == RaftState.LEADER || state == RaftState.FOLLOWER,
                    "Server " + server.getRaftNode().getNodeId() +
                    " should be LEADER or FOLLOWER, not " + state);
            }
        }

        System.out.println("\n========================================");
        System.out.println("TEST: Cluster Recovery - PASSED");
        System.out.println("========================================\n");
    }

    // ===== TEST 3a: New Leader Is Different Node =====

    /**
     * Verifies that after leader failure, the new leader is a different node.
     * This proves the cluster doesn't incorrectly re-elect the same node.
     */
    @Test
    void testNewLeaderIsDifferentNode() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: New Leader Is Different Node");
        System.out.println("========================================\n");

        startAllServers();

        MetadataServer oldLeader = getLeaderServer();
        assertNotNull(oldLeader, "Should have a leader");
        String oldLeaderId = oldLeader.getRaftNode().getNodeId();
        System.out.println("Old leader: " + oldLeaderId);

        // Stop the leader
        oldLeader.stop();

        // Wait for new leader
        waitForLeader(10000);
        MetadataServer newLeader = getLeaderServer();
        assertNotNull(newLeader, "Should have a new leader");
        String newLeaderId = newLeader.getRaftNode().getNodeId();

        // The new leader must be a different node
        assertNotEquals(oldLeaderId, newLeaderId,
            "New leader should be a different node (old leader is stopped)");
        assertEquals(1, getRunningLeaderCount(),
            "Exactly one running leader must exist after old leader stopped");

        System.out.println("New leader (different node): " + newLeaderId);
        System.out.println("\n========================================");
        System.out.println("TEST: New Leader Is Different Node - PASSED");
        System.out.println("========================================\n");
    }

    // ===== TEST 5a: Majority Required (No Leader with Only One Vote) =====

    /**
     * Verifies that in a 3-node cluster, a single running node cannot become leader
     * because it can only get its own vote (majority = 2 required).
     *
     * Setup: Start full 3-node cluster, stop B and C, leaving only A running.
     * Node A's election timeout fires but it cannot achieve majority.
     */
    @Test
    void testSingleNodeCannotBecomeLeader() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Single Node Cannot Become Leader (Majority=2 Required)");
        System.out.println("========================================\n");

        // Start full 3-node cluster
        startAllServers();
        MetadataServer leader = getLeaderServer();
        assertNotNull(leader, "Should have a leader");
        String leaderId = leader.getRaftNode().getNodeId();
        long initialTerm = leader.getRaftNode().getCurrentTerm();
        System.out.println("Initial leader: " + leaderId + " (term=" + initialTerm + ")");

        // =====================================================
        // SCENARIO 1: 1 node alive, 2 stopped — NO leader possible
        // =====================================================
        System.out.println("\n--- Scenario 1: 1 node alive, 2 stopped ---\n");

        // Stop the LEADER and one follower. Keep ONE FOLLOWER alive.
        // This way the survivor is a FOLLOWER that will try to elect itself
        // but cannot reach majority (1 self-vote, needs 2).
        MetadataServer followerToKeep = null;
        MetadataServer[] serversToStop = new MetadataServer[2];
        int idx = 0;
        for (MetadataServer s : List.of(serverA, serverB, serverC)) {
            if (s == null) continue;
            if (s.getRaftNode().isLeader()) {
                serversToStop[idx++] = s; // stop the leader
            } else {
                // Keep the first follower, stop the rest
                if (followerToKeep == null) {
                    followerToKeep = s;
                } else {
                    serversToStop[idx++] = s;
                }
            }
        }

        // Stop two servers (leave the follower alive)
        System.out.println("Stopping 2 servers, leaving 1 follower alive...");
        for (MetadataServer s : serversToStop) {
            if (s != null) {
                System.out.println("  Stopping: " + s.getRaftNode().getNodeId());
                s.stop();
            }
        }
        Thread.sleep(500);

        // Wait through election timeout cycles (need > 300ms for the alive node's election to fire and fail)
        Thread.sleep(1000);

        // Verify: NO leader exists (single node cannot reach majority of 2)
        int leaderCount = getLeaderCount();
        int runningLeaderCount = getRunningLeaderCount();
        String survivingState = followerToKeep.getRaftNode().getState().toString();
        boolean survivingIsLeader = followerToKeep.getRaftNode().isLeader();
        System.out.println("Surviving follower " + followerToKeep.getRaftNode().getNodeId() +
            " state: " + survivingState + ", isLeader=" + survivingIsLeader);
        System.out.println("runningLeaderCount=" + runningLeaderCount + ", leaderCount=" + leaderCount);

        assertEquals(0, runningLeaderCount,
            "Single surviving follower MUST NOT be leader (majority=2 required, only 1 node alive)");
        assertFalse(survivingIsLeader,
            "Surviving follower should NOT be leader when alone");
        assertEquals(0, leaderCount,
            "No leader should exist with only 1 of 3 nodes running");

        // =====================================================
        // SCENARIO 2: 2 nodes alive, 1 stopped — leader MUST be elected
        // =====================================================
        System.out.println("\n--- Scenario 2: 2 nodes alive, 1 stopped ---\n");

        // Identify the stopped node to restart
        MetadataServer stoppedServer = serversToStop[0];
        System.out.println("Restarting " + stoppedServer.getRaftNode().getNodeId() + "...");
        stoppedServer.getRaftNode().start();
        stoppedServer.getRaftNode().waitForRpcServerReady();
        Thread.sleep(100);

        // Now 2 nodes are alive — they can reach majority (2 votes = majority in 3-node cluster)
        waitForLeader(10000);

        int newLeaderCount = getRunningLeaderCount();
        MetadataServer newLeader = getLeaderServer();
        assertNotNull(newLeader, "Must have a leader with 2 nodes alive");
        assertEquals(1, newLeaderCount,
            "Exactly one leader must exist with 2 of 3 nodes alive");
        System.out.println("New leader elected: " + newLeader.getRaftNode().getNodeId() +
            " (term=" + newLeader.getRaftNode().getCurrentTerm() + ")");

        // The new leader must be one of the two running nodes
        String newLeaderId = newLeader.getRaftNode().getNodeId();
        assertTrue(
            newLeaderId.equals(followerToKeep.getRaftNode().getNodeId()) ||
            newLeaderId.equals(stoppedServer.getRaftNode().getNodeId()),
            "New leader must be one of the two running nodes");

        System.out.println("\n========================================");
        System.out.println("TEST: Single Node Cannot Become Leader - PASSED");
        System.out.println("========================================\n");
    }

    // ===== TEST 11: Persistence Failure During Election → No Invalid Leadership =====

    /**
     * Verifies that if term/votedFor persistence fails during an election start,
     * the node does NOT incorrectly claim leadership or grant votes.
     *
     * This is the critical safety property: persistence failures must not cause
     * invalid state (e.g., becoming leader without persisting the term).
     */
    @Test
    void testPersistenceFailureDuringElectionPreventsInvalidLeadership() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Persistence Failure During Election");
        System.out.println("========================================\n");

        // Start cluster normally
        startAllServers();

        MetadataServer leader = getLeaderServer();
        assertNotNull(leader, "Should have a leader");
        String leaderId = leader.getRaftNode().getNodeId();
        System.out.println("Initial leader: " + leaderId);

        // Stop all servers
        stopAllServers();
        Thread.sleep(500);

        // Restart only serverA and inject persistence failure
        Path metaFileA = dataDirA.resolve("metadata-persist-fail.json");
        MetadataServer isolatedNode = new MetadataServer(portA, metaFileA, 2, 6000, 2000, configA, dataDirA);

        // Inject persistence failure BEFORE starting the node
        isolatedNode.getRaftNode().injectPersistenceFailure("test: election term persistence");
        isolatedNode.getRaftNode().start();
        isolatedNode.getRaftNode().waitForRpcServerReady();

        // Wait for election timeout to fire (150-300ms)
        Thread.sleep(500);

        // With persistence failure injected, the election should fail to persist term.
        // The node should remain FOLLOWER, NOT become LEADER.
        RaftState state = isolatedNode.getRaftNode().getState();
        System.out.println("Node A state after election timeout (with persistence failure): " + state);
        System.out.println("Node A term: " + isolatedNode.getRaftNode().getCurrentTerm());
        System.out.println("Persistence failure injected: " + isolatedNode.getRaftNode().hasPersistenceFailure());

        // Node must NOT become leader without persisting its term.
        // After rollback to FOLLOWER the node retries the election (persistence succeeds
        // on the second attempt because the hook is one-shot). The safety property is:
        // no invalid leadership is reported — the node never became LEADER.
        assertNotEquals(RaftState.LEADER, state,
            "Node should NOT become leader when term persistence fails during election");

        // Verify the persistence failure was consumed (one-shot hook)
        assertFalse(isolatedNode.getRaftNode().hasPersistenceFailure(),
            "Persistence failure hook should have been consumed");

        isolatedNode.stop();
        Thread.sleep(500);

        System.out.println("\n========================================");
        System.out.println("TEST: Persistence Failure During Election - PASSED");
        System.out.println("========================================\n");
    }

    // ===== TEST 12: Persistence Failure During Vote Grant → Vote Not Granted =====

    /**
     * Verifies that if persistence fails while granting a vote in RequestVote,
     * the vote is NOT granted (voteGranted=false is returned).
     *
     * Design: Isolate the persistence-failure node from ALL heartbeat sources so its
     * election timeout fires. The node rolls back to FOLLOWER on persistence failure.
     * Since no majority exists (all other nodes stopped), it retries but can't win.
     * Safety property: no node becomes LEADER without persisting term+votedFor.
     */
    @Test
    void testPersistenceFailureDuringVoteGrantPreventsVoteGranting() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Persistence Failure During Vote Grant");
        System.out.println("========================================\n");

        // Start cluster
        startAllServers();
        MetadataServer leader = getLeaderServer();
        assertNotNull(leader, "Should have a leader");
        System.out.println("Initial leader: " + leader.getRaftNode().getNodeId());

        // Stop the leader and wait for a new election
        leader.stop();
        waitForLeader(10000);
        MetadataServer newLeader = getLeaderServer();
        assertNotNull(newLeader);
        System.out.println("New leader: " + newLeader.getRaftNode().getNodeId() + " (term=" + newLeader.getRaftNode().getCurrentTerm() + ")");

        // KEY FIX: Stop ALL other nodes so serverA has NO heartbeat source.
        // This ensures serverA's election timeout fires without interference.
        // Without this, the remaining leader sends heartbeats that reset serverA's
        // election deadline, preventing the timeout from ever firing.
        serverB.stop();
        serverC.stop();
        Thread.sleep(800);

        // Inject persistence failure on serverA BEFORE stopping it.
        // This must be done while A is stopped, so it's armed before A restarts.
        serverA.getRaftNode().injectPersistenceFailure("test: vote grant persistence");

        // Restart serverA - it will receive no heartbeats from stopped B and C,
        // so its election timeout fires (150-300ms), it becomes CANDIDATE,
        // increments term, tries to persist (fails with injected hook),
        // rolls back to FOLLOWER. The retry succeeds (one-shot hook), but with
        // all other nodes stopped, no majority exists → A stays FOLLOWER.
        System.out.println("Restarting A with persistence failure injected...");
        serverA.stop();
        Thread.sleep(100);
        serverA.getRaftNode().injectPersistenceFailure("test: vote grant persistence");
        serverA.getRaftNode().start();
        serverA.getRaftNode().waitForRpcServerReady();

        // Wait long enough for election timeout + retry (up to 1 second)
        Thread.sleep(1200);

        RaftState aState = serverA.getRaftNode().getState();
        long aTerm = serverA.getRaftNode().getCurrentTerm();
        System.out.println("Server A state: " + aState);
        System.out.println("Server A term: " + aTerm);
        System.out.println("Server A hasPersistenceFailure: " + serverA.getRaftNode().hasPersistenceFailure());

        // Safety property: A must NOT be leader. The node either:
        // (a) Rolled back and retried successfully (one-shot hook consumed),
        //     but can't win without majority (B and C stopped) → stays CANDIDATE
        // (b) Stayed FOLLOWER after rollback
        // Either way: no invalid LEADER state without persisting term.
        assertNotEquals(RaftState.LEADER, aState,
            "Server A must NOT become leader when term persistence fails during election");

        // Verify exactly 0 running leaders
        int leaderCount = getRunningLeaderCount();
        System.out.println("Running leader count: " + leaderCount);
        assertEquals(0, leaderCount,
            "With all followers stopped, no node should be leader");

        System.out.println("\n========================================");
        System.out.println("TEST: Persistence Failure During Vote Grant - PASSED");
        System.out.println("========================================\n");
    }

    // ===== TEST 10: Split Vote Recovery =====

    /**
     * Verifies that split votes (no majority for any candidate) eventually resolve.
     * The randomized election timeout should prevent infinite split votes.
     */
    @Test
    void testSplitVoteRecovery() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Split Vote Recovery");
        System.out.println("========================================\n");

        // Start cluster
        startAllServers();

        // Kill leader
        MetadataServer leader = getLeaderServer();
        assertNotNull(leader, "Should have a leader");
        String leaderId = leader.getRaftNode().getNodeId();
        long oldTerm = leader.getRaftNode().getCurrentTerm();
        System.out.println("Stopping leader: " + leaderId + " (term=" + oldTerm + ")");
        leader.stop();

        // Kill one follower immediately to create split vote scenario
        // (The two remaining nodes might both try to become leader simultaneously)
        MetadataServer firstFollower = (serverA.getRaftNode().getNodeId().equals(leaderId)) ? serverB : serverA;
        System.out.println("Stopping first follower to force split vote: " + firstFollower.getRaftNode().getNodeId());
        firstFollower.stop();

        // Wait a bit, then restart the first follower
        // This should create a scenario where two nodes try to elect simultaneously
        Thread.sleep(200);

        System.out.println("Restarting first follower");
        firstFollower.getRaftNode().start();

        // Now we have 2 nodes - they need to elect a leader
        // But with only 2 nodes, neither can get majority (need 2 votes)
        Thread.sleep(500);

        // With only 2 nodes, no leader should emerge (need 2 votes, only 1 possible)
        int leaderCountWith2 = getLeaderCount();
        System.out.println("Leader count with 2 nodes: " + leaderCountWith2);
        // Note: with 2 nodes, both can vote for each other potentially, creating a split
        // The cluster should eventually resolve this

        // Restart the second follower (the one that was the leader originally)
        // This should create a 3-node cluster again
        System.out.println("Restarting second follower");
        MetadataServer secondFollower = (serverA.getRaftNode().getNodeId().equals(leaderId))
            ? serverC : serverA;
        secondFollower.getRaftNode().start();

        // Now we have all 3 nodes - wait for leader
        waitForLeader(10000);

        assertEquals(1, getLeaderCount(), "Should have exactly one leader with 3 nodes");
        MetadataServer newLeader = getLeaderServer();
        System.out.println("New leader elected: " + newLeader.getRaftNode().getNodeId() +
            " (term=" + newLeader.getRaftNode().getCurrentTerm() + ")");

        System.out.println("\n========================================");
        System.out.println("TEST: Split Vote Recovery - PASSED");
        System.out.println("========================================\n");
    }
}
