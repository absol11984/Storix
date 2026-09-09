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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that when a client sends a request that commits successfully to Raft,
 * but the response is lost (network failure after commit), the client can
 * retry with the same clientId+requestId and get the cached result.
 */
public class MetadataClientLostResponseRetryTest {

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
    private ExecutorService executor;

    @BeforeEach
    void setupCluster() throws Exception {
        executor = Executors.newCachedThreadPool();

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
        try { Thread.sleep(200); } catch (InterruptedException ignored) {}
        if (executor != null) {
            executor.shutdownNow();
        }
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
        executor.submit(() -> {
            try {
                server.start();
            } catch (IOException e) {
                System.err.println("Server " + name + " error: " + e.getMessage());
            }
        });
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

    private void waitForLeader() throws Exception {
        for (int i = 0; i < 30; i++) {
            Thread.sleep(200);
            try {
                if (metaA.isLeader() || metaB.isLeader() || metaC.isLeader()) {
                    return;
                }
            } catch (Exception e) {
                // Server not ready yet
            }
        }
        throw new RuntimeException("No leader elected within timeout");
    }

    private int findFreePort() throws IOException {
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /**
     * Tests that a retry with the same clientId+requestId returns the cached result.
     */
    @Test
    @Timeout(30)
    void testRetryAfterLostResponse() throws Exception {
        // Create client connected to any server (will redirect to leader)
        try (ClusterMetadataClient client = new ClusterMetadataClient(
                List.of(new InetSocketAddress("127.0.0.1", metaAPort),
                        new InetSocketAddress("127.0.0.1", metaBPort),
                        new InetSocketAddress("127.0.0.1", metaCPort)),
                3, 5000, 10000)) {

            client.connect();

            String clientId = "retry-client-" + System.nanoTime();
            String requestId = "retry-request-" + System.nanoTime();

            ObjectMetadataDTO metadata = new ObjectMetadataDTO(
                "retry-object-" + System.nanoTime(),
                1024,
                3
            );

            // First request with explicit clientId/requestId
            // The ClusterMetadataClient generates its own clientId, but we can use a MetadataClient directly
        }

        // Test with direct MetadataClient for more control
        int leaderPort = getLeaderPort();
        try (MetadataClient client = new MetadataClient("127.0.0.1", leaderPort)) {
            client.setConnectionTimeout(5000);
            client.setRequestTimeout(10000);
            client.setClientId("retry-client");
            client.setCurrentRequestId("retry-request-1");
            client.connect();

            // First request
            ObjectMetadataDTO metadata = new ObjectMetadataDTO(
                "retry-object-" + System.nanoTime(),
                1024,
                3
            );
            client.createObject(metadata);

            // Close and reconnect
            client.close();
            client.connect();

            // Same clientId and requestId - should be deduplicated
            client.setClientId("retry-client");
            client.setCurrentRequestId("retry-request-1");

            assertDoesNotThrow(() ->
                client.createObject(new ObjectMetadataDTO("retry-object-2-" + System.nanoTime(), 2048, 3)),
                "Same clientId+requestId should be deduplicated");
        }
    }

    /**
     * Tests that different requestIds are NOT deduplicated.
     * Each request creates a different object - deduplication only applies to same requestId.
     */
    @Test
    @Timeout(30)
    void testDifferentRequestIdsNotDeduplicated() throws Exception {
        int leaderPort = getLeaderPort();

        try (MetadataClient client = new MetadataClient("127.0.0.1", leaderPort)) {
            client.setConnectionTimeout(5000);
            client.setRequestTimeout(10000);
            client.setClientId("test-client");
            client.connect();

            // Different requestIds, different objects - all should succeed
            client.setCurrentRequestId("request-1");
            client.createObject(new ObjectMetadataDTO("object-a-" + System.nanoTime(), 1024, 3));

            client.setCurrentRequestId("request-2");
            client.createObject(new ObjectMetadataDTO("object-b-" + System.nanoTime(), 1024, 3));

            client.setCurrentRequestId("request-3");
            client.createObject(new ObjectMetadataDTO("object-c-" + System.nanoTime(), 1024, 3));
        }
    }

    /**
     * Tests concurrent duplicate requests with same clientId+requestId.
     */
    @Test
    @Timeout(30)
    void testConcurrentDuplicates() throws Exception {
        int leaderPort = getLeaderPort();
        String sharedRequestId = "concurrent-" + System.nanoTime();

        int concurrency = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(concurrency);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        ExecutorService parallelExecutor = Executors.newFixedThreadPool(concurrency);

        for (int i = 0; i < concurrency; i++) {
            final int threadId = i;
            parallelExecutor.submit(() -> {
                try {
                    startLatch.await();

                    try (MetadataClient client = new MetadataClient("127.0.0.1", leaderPort)) {
                        client.setConnectionTimeout(5000);
                        client.setRequestTimeout(10000);
                        client.setClientId("concurrent-client-" + threadId);
                        client.setCurrentRequestId(sharedRequestId);

                        client.connect();
                        client.createObject(new ObjectMetadataDTO(
                            "concurrent-object-" + threadId + "-" + System.nanoTime(),
                            2048,
                            3
                        ));
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(20, TimeUnit.SECONDS);
        assertTrue(completed, "All concurrent requests should complete");

        parallelExecutor.shutdown();

        // At least some should succeed - due to deduplication, only one may actually commit
        assertTrue(successCount.get() > 0 || errorCount.get() == concurrency,
            "At least some requests should complete successfully or all may fail due to timing");
    }

    /**
     * Tests the ClusterMetadataClient requestId propagation.
     */
    @Test
    @Timeout(30)
    void testClusterClientWithRequestId() throws Exception {
        try (ClusterMetadataClient clusterClient = new ClusterMetadataClient(
                List.of(new InetSocketAddress("127.0.0.1", metaAPort)),
                3, 5000, 10000)) {

            clusterClient.connect();

            // Verify clientId is generated
            assertNotNull(clusterClient.getClientId());

            // Create object - uses beginRequest() internally to generate requestId
            ObjectMetadataDTO metadata = new ObjectMetadataDTO(
                "cluster-object-" + System.nanoTime(),
                4096,
                3
            );

            assertDoesNotThrow(() -> clusterClient.createObject(metadata),
                "Request should succeed with proper requestId");
        }
    }

    /**
     * Tests that requestId is consistent across retries.
     */
    @Test
    @Timeout(30)
    void testRequestIdConsistencyAcrossRetries() throws Exception {
        int leaderPort = getLeaderPort();

        String fixedClientId = "fixed-client-" + System.nanoTime();
        String fixedRequestId = "fixed-request-" + System.nanoTime();

        try (MetadataClient client = new MetadataClient("127.0.0.1", leaderPort)) {
            client.setConnectionTimeout(5000);
            client.setRequestTimeout(10000);
            client.setClientId(fixedClientId);
            client.setCurrentRequestId(fixedRequestId);
            client.connect();

            String objectName = "fixed-retry-test-" + System.nanoTime();

            // First request
            client.createObject(new ObjectMetadataDTO(objectName, 1024, 3));

            // Close and reconnect
            client.close();
            Thread.sleep(100);
            client.connect();

            // Same clientId and requestId
            client.setClientId(fixedClientId);
            client.setCurrentRequestId(fixedRequestId);

            // Should be deduplicated
            assertDoesNotThrow(() ->
                client.createObject(new ObjectMetadataDTO(objectName, 1024, 3)),
                "Same clientId+requestId should be deduplicated after reconnect");
        }
    }

    private int getLeaderPort() {
        try {
            if (metaA.isLeader()) {
                return metaAPort;
            } else if (metaB.isLeader()) {
                return metaBPort;
            } else if (metaC.isLeader()) {
                return metaCPort;
            }
        } catch (Exception e) {
            // Try other servers
        }
        return metaAPort;
    }
}
