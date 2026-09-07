package com.storix.client;

import com.storix.metadata.*;
import com.storix.metadata.raft.ClusterConfig;
import com.storix.metadata.raft.RaftPeer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests client-side leader failover when the current leader fails.
 *
 * Scenario:
 * - meta-a is leader
 * - meta-b and meta-c are followers
 * - Client connects and writes successfully to leader
 * - Leader (meta-a) is stopped
 * - Client should discover new leader (meta-b or meta-c)
 * - Client should continue writing successfully
 * - Old leader (meta-a) restarts as follower
 */
class MetadataClientLeaderFailoverTest {

    @TempDir
    Path tempDir;

    private int metaAPort;
    private int metaBPort;
    private int metaCPort;
    private MetadataServer metaA;
    private MetadataServer metaB;
    private MetadataServer metaC;
    private Path metaAData;
    private Path metaBData;
    private Path metaCData;

    @BeforeEach
    void setupCluster() throws Exception {
        // Wait for previous ports to be released
        Thread.sleep(300);

        // Allocate ports
        metaAPort = findFreePort();
        metaBPort = findFreePort();
        metaCPort = findFreePort();

        // Clean and create data directories
        metaAData = tempDir.resolve("meta-a");
        metaBData = tempDir.resolve("meta-b");
        metaCData = tempDir.resolve("meta-c");
        Files.createDirectories(metaAData);
        Files.createDirectories(metaBData);
        Files.createDirectories(metaCData);

        // Create cluster configs
        ClusterConfig configA = createClusterConfig("meta-a", metaAPort);
        ClusterConfig configB = createClusterConfig("meta-b", metaBPort);
        ClusterConfig configC = createClusterConfig("meta-c", metaCPort);

        // Create servers
        metaA = createMetadataServer(metaAPort, metaAData, configA);
        metaB = createMetadataServer(metaBPort, metaBData, configB);
        metaC = createMetadataServer(metaCPort, metaCData, configC);

        // Start servers
        startServer(metaA, "meta-a");
        startServer(metaB, "meta-b");
        startServer(metaC, "meta-c");

        // Wait for leader election
        waitForLeader();
    }

    @AfterEach
    void stopCluster() {
        stopServer(metaA, "meta-a");
        stopServer(metaB, "meta-b");
        stopServer(metaC, "meta-c");
        try { Thread.sleep(300); } catch (InterruptedException ignored) {}
    }

    private ClusterConfig createClusterConfig(String nodeId, int port) {
        List<RaftPeer> peers = new ArrayList<>(Arrays.asList(
            new RaftPeer("meta-a", "127.0.0.1", metaAPort + 10000),
            new RaftPeer("meta-b", "127.0.0.1", metaBPort + 10000),
            new RaftPeer("meta-c", "127.0.0.1", metaCPort + 10000)
        ));
        peers.removeIf(p -> p.nodeId().equals(nodeId));
        return new ClusterConfig("storix", nodeId, "127.0.0.1", port, port + 10000, peers);
    }

    private MetadataServer createMetadataServer(int port, Path dataDir, ClusterConfig config) throws IOException {
        Path metaFile = dataDir.resolve("metadata.json");
        return new MetadataServer(port, metaFile, 2, 2000, 500, config, dataDir);
    }

    private void startServer(MetadataServer server, String name) {
        new Thread(() -> {
            try {
                server.start();
            } catch (IOException e) {
                System.err.println(name + " server error: " + e.getMessage());
            }
        }, "server-" + name).start();
    }

    private void stopServer(MetadataServer server, String name) {
        if (server != null) {
            try {
                server.stop();
            } catch (Exception e) {
                System.err.println("Error stopping " + name + ": " + e.getMessage());
            }
        }
    }

    private int findFreePort() {
        try ( var ss = new java.net.ServerSocket(0)) {
            return ss.getLocalPort();
        } catch (IOException e) {
            return 50000 + new Random().nextInt(10000);
        }
    }

    private void waitForLeader() throws InterruptedException {
        Thread.sleep(2000);
    }

    private void waitForNewLeader(MetadataServer oldLeader, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (metaA != oldLeader && metaA.isLeader()) return;
            if (metaB != oldLeader && metaB.isLeader()) return;
            if (metaC != oldLeader && metaC.isLeader()) return;
            Thread.sleep(200);
        }
        fail("New leader not elected within " + timeoutMs + "ms after stopping " + oldLeader.getRaftNode().getNodeId());
    }

    private MetadataServer getLeader() {
        if (metaA.isLeader()) return metaA;
        if (metaB.isLeader()) return metaB;
        if (metaC.isLeader()) return metaC;
        return null;
    }

    /**
     * Test: Leader fails, client discovers new leader and continues writing.
     */
    @Test
    void testClientFailoverToNewLeader() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Client failover to new leader after leader failure");
        System.out.println("========================================\n");

        MetadataServer initialLeader = getLeader();
        assertNotNull(initialLeader, "Should have a leader");
        String initialLeaderName = initialLeader.getRaftNode().getNodeId();
        System.out.println("Initial leader: " + initialLeaderName);

        // Create client with all endpoints
        List<InetSocketAddress> endpoints = Arrays.asList(
            new InetSocketAddress("127.0.0.1", metaAPort),
            new InetSocketAddress("127.0.0.1", metaBPort),
            new InetSocketAddress("127.0.0.1", metaCPort)
        );

        try (ClusterMetadataClient client = new ClusterMetadataClient(endpoints, 10)) {
            // Connect
            client.connect();
            System.out.println("Connected to: " + client.getCurrentServer());

            // Write object 1 to initial leader
            String objectName1 = "test-failover-1-" + System.currentTimeMillis();
            ObjectMetadataDTO metadata1 = new ObjectMetadataDTO(objectName1, 1024, 512);
            metadata1.addChunk(new ChunkInfoDTO("chunk-1", 0, 256, List.of("n1", "n2"), "abc"));
            client.createObject(metadata1);
            System.out.println("Object 1 created: " + objectName1);

            // Wait for replication to quorum
            Thread.sleep(1000);

            // Stop the initial leader
            System.out.println("Stopping leader: " + initialLeaderName);
            stopServer(initialLeader, initialLeaderName);

            // Wait for new leader election
            waitForNewLeader(initialLeader, 20000);

            MetadataServer newLeader = getLeader();
            assertNotNull(newLeader, "Should have a new leader");
            String newLeaderName = newLeader.getRaftNode().getNodeId();
            System.out.println("New leader elected: " + newLeaderName);
            assertNotEquals(initialLeaderName, newLeaderName, "New leader should be different from initial");

            // Client should automatically discover new leader and write
            String objectName2 = "test-failover-2-" + System.currentTimeMillis();
            ObjectMetadataDTO metadata2 = new ObjectMetadataDTO(objectName2, 2048, 512);
            metadata2.addChunk(new ChunkInfoDTO("chunk-2", 0, 512, List.of("n1", "n2"), "def"));
            client.createObject(metadata2);
            System.out.println("Object 2 created after failover: " + objectName2);

            // Verify both objects exist
            ObjectMetadataDTO retrieved1 = client.getObject(objectName1);
            assertNotNull(retrieved1, "Object 1 should still exist");
            assertEquals(objectName1, retrieved1.getObjectName());

            ObjectMetadataDTO retrieved2 = client.getObject(objectName2);
            assertNotNull(retrieved2, "Object 2 should exist");
            assertEquals(objectName2, retrieved2.getObjectName());

            System.out.println("\n========================================");
            System.out.println("TEST: Client failover to new leader - PASSED");
            System.out.println("========================================\n");
        }
    }

    /**
     * Test: Old leader restarts as follower.
     */
    @Test
    void testOldLeaderRestartsAsFollower() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Old leader restarts as follower");
        System.out.println("========================================\n");

        MetadataServer initialLeader = getLeader();
        String initialLeaderName = initialLeader.getRaftNode().getNodeId();
        System.out.println("Initial leader: " + initialLeaderName);

        // Create client
        List<InetSocketAddress> endpoints = Arrays.asList(
            new InetSocketAddress("127.0.0.1", metaAPort),
            new InetSocketAddress("127.0.0.1", metaBPort),
            new InetSocketAddress("127.0.0.1", metaCPort)
        );

        try (ClusterMetadataClient client = new ClusterMetadataClient(endpoints, 10)) {
            client.connect();

            // Write before failure
            String objectName1 = "test-rejoin-1-" + System.currentTimeMillis();
            ObjectMetadataDTO metadata1 = new ObjectMetadataDTO(objectName1, 1024, 512);
            metadata1.addChunk(new ChunkInfoDTO("chunk-1", 0, 256, List.of("n1", "n2"), "abc"));
            client.createObject(metadata1);
            System.out.println("Object 1 created before failover: " + objectName1);

            // Stop leader
            stopServer(initialLeader, initialLeaderName);
            waitForNewLeader(initialLeader, 15000);
            MetadataServer newLeader = getLeader();
            System.out.println("New leader: " + newLeader.getRaftNode().getNodeId());

            // Write after failover
            String objectName2 = "test-rejoin-2-" + System.currentTimeMillis();
            ObjectMetadataDTO metadata2 = new ObjectMetadataDTO(objectName2, 2048, 512);
            metadata2.addChunk(new ChunkInfoDTO("chunk-2", 0, 512, List.of("n1", "n2"), "def"));
            client.createObject(metadata2);
            System.out.println("Object 2 created after failover: " + objectName2);

            // Restart old leader
            System.out.println("Restarting old leader: " + initialLeaderName);
            Path dataDir = getDataDirForServer(initialLeader);
            ClusterConfig config = createClusterConfig(initialLeaderName, getPortForServer(initialLeader));
            MetadataServer restartedServer = createMetadataServer(getPortForServer(initialLeader), dataDir, config);
            startServer(restartedServer, initialLeaderName);

            // Wait for it to become a follower
            Thread.sleep(3000);

            // Verify old leader is now a follower
            assertFalse(restartedServer.isLeader(), "Old leader should be follower after restart");
            System.out.println("Old leader is now a follower");

            // Verify old leader has the committed data
            // (Note: it may take some time to catch up via Raft replication)
            Thread.sleep(1000);

            // Verify all data is still accessible
            ObjectMetadataDTO retrieved1 = client.getObject(objectName1);
            assertNotNull(retrieved1, "Object 1 should be accessible");
            ObjectMetadataDTO retrieved2 = client.getObject(objectName2);
            assertNotNull(retrieved2, "Object 2 should be accessible");

            System.out.println("\n========================================");
            System.out.println("TEST: Old leader restarts as follower - PASSED");
            System.out.println("========================================\n");
        }
    }

    /**
     * Test: Connection failure to dead leader triggers failover.
     */
    @Test
    void testConnectionFailureTriggersFailover() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Connection failure triggers failover");
        System.out.println("========================================\n");

        MetadataServer leader = getLeader();
        String leaderName = leader.getRaftNode().getNodeId();
        System.out.println("Leader: " + leaderName);

        // Create client and connect to leader
        List<InetSocketAddress> endpoints = Arrays.asList(
            new InetSocketAddress("127.0.0.1", metaAPort),
            new InetSocketAddress("127.0.0.1", metaBPort),
            new InetSocketAddress("127.0.0.1", metaCPort)
        );

        try (ClusterMetadataClient client = new ClusterMetadataClient(endpoints, 10)) {
            client.connect();
            System.out.println("Connected to: " + client.getCurrentServer());

            // Write object
            String objectName = "test-conn-fail-" + System.currentTimeMillis();
            ObjectMetadataDTO metadata = new ObjectMetadataDTO(objectName, 1024, 512);
            metadata.addChunk(new ChunkInfoDTO("chunk-1", 0, 256, List.of("n1", "n2"), "abc"));
            client.createObject(metadata);
            System.out.println("Object created: " + objectName);

            // Wait for replication to quorum (at least 500ms + network latency)
            Thread.sleep(1000);

            // Verify the object exists (it's been committed to majority)
            assertNotNull(client.getObject(objectName), "Object should be committed before failover");

            // Stop leader
            System.out.println("Stopping leader: " + leaderName);
            stopServer(leader, leaderName);

            // Wait for new leader
            waitForNewLeader(leader, 20000);
            System.out.println("New leader: " + getLeader().getRaftNode().getNodeId());

            // Give client time to discover the new leader
            Thread.sleep(500);

            // The client's cached connection should fail, triggering discovery
            // We test this by writing again - it should succeed
            String objectName2 = "test-conn-fail-2-" + System.currentTimeMillis();
            ObjectMetadataDTO metadata2 = new ObjectMetadataDTO(objectName2, 2048, 512);
            metadata2.addChunk(new ChunkInfoDTO("chunk-2", 0, 512, List.of("n1", "n2"), "def"));

            // This should trigger connection failure detection and failover
            client.createObject(metadata2);
            System.out.println("Object 2 created after connection failure: " + objectName2);

            // Verify both objects exist - need to invalidate cache and retry
            // since the follower might not have the data yet
            client.invalidateLeaderCache();
            assertNotNull(client.getObject(objectName), "Object 1 should be accessible");
            assertNotNull(client.getObject(objectName2), "Object 2 should be accessible");

            System.out.println("\n========================================");
            System.out.println("TEST: Connection failure triggers failover - PASSED");
            System.out.println("========================================\n");
        }
    }

    private Path getDataDirForServer(MetadataServer server) {
        if (server == metaA) return metaAData;
        if (server == metaB) return metaBData;
        return metaCData;
    }

    private int getPortForServer(MetadataServer server) {
        if (server == metaA) return metaAPort;
        if (server == metaB) return metaBPort;
        return metaCPort;
    }
}
