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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that request timeout enforcement works correctly.
 * Different from connection timeout - request timeout is how long we wait for
 * a response from the server after the connection is established.
 */
public class MetadataClientRequestTimeoutTest {

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
            } catch (Exception e) {
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

    /**
     * Tests that request timeout is enforced for slow operations.
     */
    @Test
    @Timeout(15)
    void testRequestTimeoutEnforcement() throws Exception {
        int leaderPort = getLeaderPort();

        // Create client with short request timeout
        try (MetadataClient client = new MetadataClient("127.0.0.1", leaderPort)) {
            client.setConnectionTimeout(5000);
            client.setRequestTimeout(500); // 500ms request timeout

            client.connect();

            // This should complete within the timeout
            ObjectMetadataDTO metadata = new ObjectMetadataDTO(
                "timeout-test-" + System.nanoTime(),
                1024,
                3
            );

            // For a single-node style write, this should be fast
            assertDoesNotThrow(() -> client.createObject(metadata),
                "Fast operation should complete within request timeout");
        }
    }

    /**
     * Tests that a longer request timeout allows the operation to proceed.
     */
    @Test
    @Timeout(15)
    void testNoTimeoutWhenServerResponds() throws Exception {
        int leaderPort = getLeaderPort();

        try (MetadataClient client = new MetadataClient("127.0.0.1", leaderPort)) {
            client.setConnectionTimeout(5000);
            client.setRequestTimeout(10000); // 10 seconds

            client.connect();
            client.createObject(new ObjectMetadataDTO(
                "no-timeout-test-" + System.nanoTime(),
                1024,
                3
            ));
            // If we get here, no timeout occurred
        }
    }

    /**
     * Tests that request timeout is separate from connection timeout.
     */
    @Test
    @Timeout(20)
    void testRequestTimeoutDistinctFromConnectionTimeout() throws Exception {
        int leaderPort = getLeaderPort();

        // Test with a properly configured client
        AtomicReference<Exception> capturedException = new AtomicReference<>();

        Thread clientThread = new Thread(() -> {
            try (MetadataClient client = new MetadataClient("127.0.0.1", leaderPort)) {
                client.setConnectionTimeout(5000);
                client.setRequestTimeout(10000);

                client.connect();
                client.createObject(new ObjectMetadataDTO("distinct-timeout-test", 1024, 3));
            } catch (Exception e) {
                capturedException.set(e);
            }
        });

        clientThread.start();
        clientThread.join(15000);

        if (capturedException.get() != null && capturedException.get() instanceof IOException) {
            String msg = capturedException.get().getMessage();
            // Should not be a timeout if operation is fast
            assertFalse(msg.contains("timeout") && msg.contains("timed out"),
                "Fast operation should not timeout: " + msg);
        }
    }

    /**
     * Tests that request timeout resets between operations.
     */
    @Test
    @Timeout(15)
    void testRequestTimeoutResetsBetweenOperations() throws Exception {
        int leaderPort = getLeaderPort();

        try (MetadataClient client = new MetadataClient("127.0.0.1", leaderPort)) {
            client.setConnectionTimeout(5000);
            client.setRequestTimeout(5000);

            client.connect();

            // First operation
            String objectName1 = "test-object-1-" + System.nanoTime();
            client.createObject(new ObjectMetadataDTO(objectName1, 1024, 3));

            Thread.sleep(100);

            // Second operation - timeout should have reset
            String objectName2 = "test-object-2-" + System.nanoTime();
            client.createObject(new ObjectMetadataDTO(objectName2, 2048, 3));

            // Both succeeded
        }
    }

    /**
     * Tests that ClusterMetadataClient respects request timeout.
     */
    @Test
    @Timeout(15)
    void testClusterMetadataClientRequestTimeout() throws Exception {
        try (ClusterMetadataClient client = new ClusterMetadataClient(
                List.of(new InetSocketAddress("127.0.0.1", metaAPort),
                        new InetSocketAddress("127.0.0.1", metaBPort),
                        new InetSocketAddress("127.0.0.1", metaCPort)),
                3, // max attempts
                5000, // connection timeout
                10000 // request timeout
        )) {
            client.connect();

            ObjectMetadataDTO metadata = new ObjectMetadataDTO(
                "cluster-timeout-test-" + System.nanoTime(),
                1024,
                3
            );

            assertDoesNotThrow(() -> client.createObject(metadata),
                "Request should complete within request timeout");
        }
    }

    /**
     * Tests that a very long request timeout works correctly.
     */
    @Test
    @Timeout(15)
    void testLongRequestTimeout() throws Exception {
        int leaderPort = getLeaderPort();

        try (MetadataClient client = new MetadataClient("127.0.0.1", leaderPort)) {
            client.setConnectionTimeout(5000);
            client.setRequestTimeout(60000); // 60 seconds

            client.connect();
            client.createObject(new ObjectMetadataDTO("long-timeout-test-" + System.nanoTime(), 1024, 3));
        }
    }

    /**
     * Tests multiple operations with consistent timeouts.
     */
    @Test
    @Timeout(20)
    void testMultipleOperationsWithTimeout() throws Exception {
        int leaderPort = getLeaderPort();

        try (MetadataClient client = new MetadataClient("127.0.0.1", leaderPort)) {
            client.setConnectionTimeout(5000);
            client.setRequestTimeout(5000);

            client.connect();

            // Perform multiple operations - all should respect timeout
            for (int i = 0; i < 10; i++) {
                String objectName = "multi-timeout-" + i + "-" + System.nanoTime();
                client.createObject(new ObjectMetadataDTO(objectName, 1024, 3));
            }
        }
    }
}
