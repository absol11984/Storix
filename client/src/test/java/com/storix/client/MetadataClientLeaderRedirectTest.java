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
 * Tests client-side leader routing when initially connecting to a follower.
 *
 * Scenario:
 * - meta-a and meta-c are followers
 * - meta-b is the leader
 * - Client initially connects to a follower (meta-a or meta-c)
 * - Client should receive NOT_LEADER and discover the leader (meta-b)
 * - Client should retry and succeed with the leader
 */
class MetadataClientLeaderRedirectTest {

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

        // Create and start servers
        metaA = createMetadataServer(metaAPort, metaAData, configA);
        metaB = createMetadataServer(metaBPort, metaBData, configB);
        metaC = createMetadataServer(metaCPort, metaCData, configC);

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
        List<RaftPeer> peers = Arrays.asList(
            new RaftPeer("meta-a", "127.0.0.1", metaAPort + 10000),
            new RaftPeer("meta-b", "127.0.0.1", metaBPort + 10000),
            new RaftPeer("meta-c", "127.0.0.1", metaCPort + 10000)
        );
        // Remove self from peers list
        peers = new ArrayList<>(peers);
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
            server.stop();
        }
    }

    private int findFreePort() {
        try (var ss = new java.net.ServerSocket(0)) {
            return ss.getLocalPort();
        } catch (IOException e) {
            return 50000 + new Random().nextInt(10000);
        }
    }

    private void waitForLeader() throws InterruptedException {
        // Wait for leader election
        Thread.sleep(2000);
    }

    private MetadataServer getLeader() {
        if (metaA.isLeader()) return metaA;
        if (metaB.isLeader()) return metaB;
        if (metaC.isLeader()) return metaC;
        return null;
    }

    private MetadataServer getFollower() {
        if (!metaA.isLeader()) return metaA;
        if (!metaB.isLeader()) return metaB;
        if (!metaC.isLeader()) return metaC;
        return null;
    }

    /**
     * Test: Client connects to follower, receives NOT_LEADER, routes to leader.
     *
     * Steps:
     * 1. Identify leader and follower
     * 2. Create client with multiple endpoints including the follower
     * 3. Connect to follower first
     * 4. Send a mutation (should get NOT_LEADER)
     * 5. Verify client routes to leader and succeeds
     */
    @Test
    void testClientRoutesFromFollowerToLeader() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Client routes from follower to leader");
        System.out.println("========================================\n");

        // Identify leader and follower
        MetadataServer leader = getLeader();
        MetadataServer follower = getFollower();
        assertNotNull(leader, "Should have a leader");
        assertNotNull(follower, "Should have a follower");

        String leaderName = leader.getRaftNode().getNodeId();
        String followerName = follower.getRaftNode().getNodeId();
        System.out.println("Leader: " + leaderName + ", Follower: " + followerName);

        // Create client with all endpoints, but try follower first
        List<InetSocketAddress> endpoints = Arrays.asList(
            new InetSocketAddress("127.0.0.1", getFollowerPort(follower)),
            new InetSocketAddress("127.0.0.1", getLeaderPort(leader)),
            new InetSocketAddress("127.0.0.1", getOtherPort())
        );

        try (ClusterMetadataClient client = new ClusterMetadataClient(endpoints, 5, 1000, 30000)) {
            // Connect (should connect to first endpoint - the follower)
            client.connect();

            // Verify connected to follower initially
            System.out.println("Connected to: " + client.getCurrentServer());

            // Send a mutation - should be routed to leader
            ObjectMetadataDTO metadata = new ObjectMetadataDTO(
                "test-object-" + System.currentTimeMillis(),
                1024,
                512
            );
            metadata.addChunk(new ChunkInfoDTO(
                "chunk-1", 0, 256, List.of("node-1", "node-2"), "abc123"
            ));

            // This should succeed - client should route to leader
            client.createObject(metadata);

            System.out.println("Object created successfully after redirect");

            // Give Raft time to replicate
            Thread.sleep(500);

            // Verify the object exists in the metadata store
            ObjectMetadataDTO retrieved = client.getObject(metadata.getObjectName());
            assertNotNull(retrieved, "Object should exist after creation");
            assertEquals(metadata.getObjectName(), retrieved.getObjectName());

            System.out.println("\n========================================");
            System.out.println("TEST: Client routes from follower to leader - PASSED");
            System.out.println("========================================\n");
        }
    }

    /**
     * Test: Client's cached leader is invalidated when it becomes stale.
     */
    @Test
    void testClientInvalidatesCachedLeaderOnStaleResponse() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Client invalidates cached leader on stale response");
        System.out.println("========================================\n");

        MetadataServer leader = getLeader();
        MetadataServer follower = getFollower();

        List<InetSocketAddress> endpoints = Arrays.asList(
            new InetSocketAddress("127.0.0.1", getLeaderPort(leader)),
            new InetSocketAddress("127.0.0.1", getFollowerPort(follower))
        );

        try (ClusterMetadataClient client = new ClusterMetadataClient(endpoints, 3)) {
            client.connect();

            // First mutation should work
            ObjectMetadataDTO metadata1 = new ObjectMetadataDTO(
                "test-obj-1-" + System.currentTimeMillis(),
                1024, 512
            );
            metadata1.addChunk(new ChunkInfoDTO("chunk-1", 0, 256, List.of("n1", "n2"), "abc"));

            client.createObject(metadata1);
            System.out.println("First object created with leader: " + client.getCachedLeader());

            // Invalidate the cached leader
            client.invalidateLeaderCache();
            System.out.println("Leader cache invalidated");

            // Second mutation should still work (even with cache invalidated)
            ObjectMetadataDTO metadata2 = new ObjectMetadataDTO(
                "test-obj-2-" + System.currentTimeMillis(),
                2048, 512
            );
            metadata2.addChunk(new ChunkInfoDTO("chunk-2", 0, 512, List.of("n1", "n2"), "def"));

            client.createObject(metadata2);
            System.out.println("Second object created after cache invalidation");

            System.out.println("\n========================================");
            System.out.println("TEST: Client invalidates cached leader - PASSED");
            System.out.println("========================================\n");
        }
    }

    private int getLeaderPort(MetadataServer server) {
        if (server == metaA) return metaAPort;
        if (server == metaB) return metaBPort;
        if (server == metaC) return metaCPort;
        return metaAPort;
    }

    private int getFollowerPort(MetadataServer server) {
        return getLeaderPort(server);
    }

    private int getOtherPort() {
        if (metaA.isLeader()) return metaCPort;
        if (metaB.isLeader()) return metaAPort;
        return metaBPort;
    }
}
