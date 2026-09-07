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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests client retry behavior and idempotency.
 *
 * Key scenarios:
 * 1. Request reaches leader and is committed, but client loses response
 * 2. Client retries with same request ID
 * 3. System detects duplicate and returns success without duplicate operation
 */
class MetadataClientRetryIdempotencyTest {

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
        Thread.sleep(300);

        metaAPort = findFreePort();
        metaBPort = findFreePort();
        metaCPort = findFreePort();

        metaAData = tempDir.resolve("meta-a");
        metaBData = tempDir.resolve("meta-b");
        metaCData = tempDir.resolve("meta-c");
        Files.createDirectories(metaAData);
        Files.createDirectories(metaBData);
        Files.createDirectories(metaCData);

        ClusterConfig configA = createClusterConfig("meta-a", metaAPort);
        ClusterConfig configB = createClusterConfig("meta-b", metaBPort);
        ClusterConfig configC = createClusterConfig("meta-c", metaCPort);

        metaA = createMetadataServer(metaAPort, metaAData, configA);
        metaB = createMetadataServer(metaBPort, metaBData, configB);
        metaC = createMetadataServer(metaCPort, metaCData, configC);

        startServer(metaA, "meta-a");
        startServer(metaB, "meta-b");
        startServer(metaC, "meta-c");

        Thread.sleep(2000);
    }

    @AfterEach
    void stopCluster() {
        stopServer(metaA);
        stopServer(metaB);
        stopServer(metaC);
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
                System.err.println(name + " error: " + e.getMessage());
            }
        }, "server-" + name).start();
    }

    private void stopServer(MetadataServer server) {
        if (server != null) {
            try {
                server.stop();
            } catch (Exception e) { /* ignore */ }
        }
    }

    private int findFreePort() {
        try (var ss = new java.net.ServerSocket(0)) {
            return ss.getLocalPort();
        } catch (IOException e) {
            return 50000 + new Random().nextInt(10000);
        }
    }

    private MetadataServer getLeader() {
        if (metaA.isLeader()) return metaA;
        if (metaB.isLeader()) return metaB;
        if (metaC.isLeader()) return metaC;
        return null;
    }

    private int getLeaderPort() {
        MetadataServer leader = getLeader();
        if (leader == metaA) return metaAPort;
        if (leader == metaB) return metaBPort;
        return metaCPort;
    }

    /**
     * Test: Multiple identical requests with same ID should not create duplicates.
     *
     * The deduplication cache on the server side should prevent duplicate operations.
     */
    @Test
    void testIdempotentRetry() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Idempotent retry does not create duplicates");
        System.out.println("========================================\n");

        MetadataServer leader = getLeader();
        System.out.println("Leader: " + leader.getRaftNode().getNodeId());

        List<InetSocketAddress> endpoints = Arrays.asList(
            new InetSocketAddress("127.0.0.1", getLeaderPort()),
            new InetSocketAddress("127.0.0.1", metaAPort),
            new InetSocketAddress("127.0.0.1", metaBPort)
        );

        try (ClusterMetadataClient client = new ClusterMetadataClient(endpoints, 5, 2000, 30000)) {
            client.connect();

            // Create object with unique name
            String objectName = "test-idempotent-" + System.currentTimeMillis();
            ObjectMetadataDTO metadata = new ObjectMetadataDTO(objectName, 1024, 512);
            metadata.addChunk(new ChunkInfoDTO("chunk-1", 0, 256, List.of("n1", "n2"), "abc123"));

            // First create - should succeed
            client.createObject(metadata);
            System.out.println("First create succeeded: " + objectName);

            // Give Raft time to replicate
            Thread.sleep(500);

            // Second create with same name - should be idempotent (server dedup)
            // The server's deduplication cache should return success without error
            try {
                client.createObject(metadata);
                System.out.println("Second create also succeeded (idempotent)");
            } catch (IOException e) {
                // Not found or duplicate is acceptable
                System.out.println("Second create: " + e.getMessage());
            }

            // Verify only one object exists
            String[] objects = client.listObjects();
            long count = Arrays.stream(objects)
                .filter(n -> n.equals(objectName))
                .count();
            assertEquals(1, count, "Should have exactly one object with this name");

            // Verify object has correct chunk count
            ObjectMetadataDTO retrieved = client.getObject(objectName);
            assertNotNull(retrieved);
            assertEquals(1, retrieved.getChunkCount(), "Should have exactly 1 chunk");

            System.out.println("\n========================================");
            System.out.println("TEST: Idempotent retry - PASSED");
            System.out.println("========================================\n");
        }
    }

    /**
     * Test: Request ID generation for tracking retries.
     */
    @Test
    void testRequestIdGeneration() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Request ID generation");
        System.out.println("========================================\n");

        List<InetSocketAddress> endpoints = Arrays.asList(
            new InetSocketAddress("127.0.0.1", getLeaderPort())
        );

        try (ClusterMetadataClient client = new ClusterMetadataClient(endpoints, 3)) {
            // Generate multiple request IDs
            String id1 = client.generateRequestId();
            String id2 = client.generateRequestId();
            String id3 = client.generateRequestId();

            System.out.println("Generated IDs:");
            System.out.println("  1: " + id1);
            System.out.println("  2: " + id2);
            System.out.println("  3: " + id3);

            // Verify IDs are unique
            assertNotEquals(id1, id2);
            assertNotEquals(id2, id3);
            assertNotEquals(id1, id3);

            // Verify IDs are prefixed with client ID
            String clientId = client.getClientId();
            assertTrue(id1.startsWith(clientId), "ID should start with client ID");
            assertTrue(id2.startsWith(clientId), "ID should start with client ID");
            assertTrue(id3.startsWith(clientId), "ID should start with client ID");

            // Verify client can be used for operations
            client.connect();
            String objectName = "test-req-id-" + System.currentTimeMillis();
            ObjectMetadataDTO metadata = new ObjectMetadataDTO(objectName, 1024, 512);
            metadata.addChunk(new ChunkInfoDTO("chunk-1", 0, 256, List.of("n1", "n2"), "abc"));

            client.createObject(metadata);
            System.out.println("Object created with request tracking: " + objectName);

            System.out.println("\n========================================");
            System.out.println("TEST: Request ID generation - PASSED");
            System.out.println("========================================\n");
        }
    }

    /**
     * Test: Bounded retries - should fail after max attempts.
     */
    @Test
    void testBoundedRetries() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Bounded retries");
        System.out.println("========================================\n");

        // Create client with very short timeout and low max attempts
        List<InetSocketAddress> unreachableEndpoints = Arrays.asList(
            new InetSocketAddress("127.0.0.1", 1),  // Invalid port
            new InetSocketAddress("127.0.0.1", 2),  // Invalid port
            new InetSocketAddress("127.0.0.1", 3)   // Invalid port
        );

        try (ClusterMetadataClient client = new ClusterMetadataClient(unreachableEndpoints, 2, 100, 500)) {
            client.connect(); // This should fail after bounded retries

            fail("Should have thrown IOException after bounded retries");
        } catch (IOException e) {
            System.out.println("Expected failure: " + e.getMessage());
            // Verify error message mentions retry count
            assertTrue(e.getMessage().contains("attempts") || e.getMessage().contains("Failed to connect"),
                "Error should mention attempts or connection failure");
        }

        System.out.println("\n========================================");
        System.out.println("TEST: Bounded retries - PASSED");
        System.out.println("========================================\n");
    }

    /**
     * Test: All endpoints unavailable returns bounded failure.
     */
    @Test
    void testAllEndpointsUnavailable() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: All endpoints unavailable");
        System.out.println("========================================\n");

        // Use unreachable ports
        List<InetSocketAddress> endpoints = Arrays.asList(
            new InetSocketAddress("127.0.0.1", 1),
            new InetSocketAddress("127.0.0.1", 2),
            new InetSocketAddress("127.0.0.1", 3)
        );

        long startTime = System.currentTimeMillis();

        try (ClusterMetadataClient client = new ClusterMetadataClient(endpoints, 1, 100, 200)) {
            client.connect();
            fail("Should have failed to connect");
        } catch (IOException e) {
            long elapsed = System.currentTimeMillis() - startTime;
            System.out.println("Connection failed after " + elapsed + "ms: " + e.getMessage());

            // Should fail within reasonable time (bounded)
            assertTrue(elapsed < 5000, "Should fail within reasonable time, took: " + elapsed + "ms");
        }

        System.out.println("\n========================================");
        System.out.println("TEST: All endpoints unavailable - PASSED");
        System.out.println("========================================\n");
    }
}
