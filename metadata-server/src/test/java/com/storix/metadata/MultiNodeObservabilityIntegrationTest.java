package com.storix.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration test verifying that observability metrics (Requests,
 * ManagerMetrics, Lifecycle State, and Health State) are accurately updated,
 * snapshotted, and serialized into the cluster status response JSON.
 */
class MultiNodeObservabilityIntegrationTest {

    @TempDir
    Path tempDir;

    private MetadataServer server;
    private int serverPort;
    private ObjectMapper mapper = new ObjectMapper();

    private static int findFreePort() throws Exception {
        try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        serverPort = findFreePort();
        server = new MetadataServer(serverPort, tempDir.resolve("metadata.json"));

        Thread serverThread = new Thread(() -> {
            try {
                server.start();
            } catch (Exception e) {
                // Expected on stop
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();

        // Wait for server to bind
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 5000) {
            try (Socket s = new Socket("127.0.0.1", serverPort)) {
                break; // connected successfully!
            } catch (Exception e) {
                Thread.sleep(50);
            }
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void clusterStatusIncludesObservabilitySnapshot() throws Exception {
        NodeRegistry registry = server.getNodeRegistry();
        registry.registerNode("nodeA", "127.0.0.1", 9091);
        registry.registerNode("nodeB", "127.0.0.1", 9092);

        Observability.Registry obs = server.getObservabilityRegistry();
        obs.requests().record("GET_OBJECT", true, 42, "success");
        obs.requests().record("GET_OBJECT", false, 110, "timeout");
        obs.managers().repairSuccess();
        obs.managers().repairFailure();
        obs.managers().repairChunks(5);
        obs.managers().recoverySuccess();
        obs.managers().rebalanceAttempt();

        try (Socket socket = new Socket("127.0.0.1", serverPort);
             OutputStream out = socket.getOutputStream();
             InputStream in = socket.getInputStream()) {

            // Frame: [requestLength: 4 bytes = 5] [opcode: 1 byte] [payloadLength: 4 bytes = 0]
            ByteBuffer request = ByteBuffer.allocate(9);
            request.putInt(5); // total request body length (1 opcode + 4 payload length)
            request.put(MetadataProtocol.GET_CLUSTER_STATUS);
            request.putInt(0); // 0 bytes payload
            out.write(request.array());
            out.flush();

            // Wire format response: [responseLength: 4 bytes] [status: 1 byte] [payloadLength: 4 bytes] [payload: N bytes]
            byte[] respLenBytes = in.readNBytes(4);
            assertEquals(4, respLenBytes.length, "Should read 4 bytes for response length");

            int status = in.read();
            assertEquals(MetadataProtocol.OK, (byte) status, "Response should be OK (0)");

            byte[] payloadLenBytes = in.readNBytes(4);
            assertEquals(4, payloadLenBytes.length, "Should read 4 bytes for payload length");
            int len = ByteBuffer.wrap(payloadLenBytes).getInt();

            byte[] payloadBytes = in.readNBytes(len);
            assertEquals(len, payloadBytes.length, "Should read full payload");

            JsonNode root = mapper.readTree(payloadBytes);

            // --- General Health / Lifecycle ---
            assertTrue(root.has("lifecycleState"), "Should have lifecycleState");
            assertEquals("RUNNING", root.get("lifecycleState").asText());

            assertTrue(root.has("healthState"), "Should have healthState");
            assertEquals("HEALTHY", root.get("healthState").asText());

            // --- Metrics Object ---
            assertTrue(root.has("metrics"), "Should have metrics object");
            JsonNode metrics = root.get("metrics");

            // Requests subsystem
            assertTrue(metrics.has("requests"), "Should have requests metrics");
            JsonNode requests = metrics.get("requests");
            assertEquals(2, requests.get("totalRequests").asLong());
            assertEquals(1, requests.get("successfulRequests").asLong());
            assertEquals(1, requests.get("failedRequests").asLong());
            assertTrue(requests.has("byFailureCategory"));
            assertEquals(1, requests.get("byFailureCategory").get("timeout").asLong());

            // Managers subsystem
            assertTrue(metrics.has("managers"), "Should have managers metrics");
            JsonNode managers = metrics.get("managers");

            assertTrue(managers.has("repair"), "Should have repair metrics");
            JsonNode repair = managers.get("repair");
            // Check alias names required by CLI
            assertEquals(0, repair.get("activeRepairs").asLong());
            assertEquals(5, repair.get("chunksRepaired").asLong());
            assertEquals(1, repair.get("failedRepairs").asLong());
            // Check native names
            assertEquals(1, repair.get("successes").asLong());
            assertEquals(5, repair.get("chunksProcessed").asLong());

            assertTrue(managers.has("recovery"), "Should have recovery metrics");
            JsonNode recovery = managers.get("recovery");
            assertEquals(1, recovery.get("successes").asLong());

            assertTrue(managers.has("rebalance"), "Should have rebalance metrics");
            JsonNode rebalance = managers.get("rebalance");
            assertEquals(1, rebalance.get("attempts").asLong());
        }
    }
}
