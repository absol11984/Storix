package com.storix.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers active-request tracking through real metadata request execution.
 * The tests verify:
 * - activeRequests starts at 0
 * - becomes >0 while requests are executing
 * - decrements to 0 even on exception/error responses
 * - status/metrics payload includes the live activeRequests value
 */
class ActiveRequestCliIntegrationTest {

    @TempDir
    Path tempDir;

    private MetadataServer server;
    private int port;

    private static int findFreePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    private void startServer() throws Exception {
        port = findFreePort();
        Path metadataFile = tempDir.resolve("metadata.json");
        server = new MetadataServer(port, metadataFile);

        Thread serverThread = new Thread(() -> {
            try {
                server.start();
            } catch (Exception ignored) {
                // Expected on stop
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();

        // Wait for server to bind
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 5000) {
            try (Socket s = new Socket("127.0.0.1", port)) {
                return;
            } catch (Exception e) {
                Thread.sleep(50);
            }
        }
        fail("Metadata server did not bind within 5s");
    }

    private void stopServer() {
        if (server != null) {
            server.stop();
        }
    }

    @AfterEach
    void tearDown() {
        stopServer();
    }

    private JsonNode sendGetClusterStatus() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        try (Socket socket = new Socket("127.0.0.1", port);
             OutputStream out = socket.getOutputStream();
             InputStream in = socket.getInputStream()) {

            // Frame: [requestLength:4][opcode:1][payloadLen:4][payload...]
            // For GET_CLUSTER_STATUS: opcode=13 and payloadLen=0.
            int payloadLen = 0;
            int requestLen = 1 + 4 + payloadLen;

            ByteBuffer request = ByteBuffer.allocate(4 + requestLen);
            request.putInt(requestLen);
            request.put(MetadataProtocol.GET_CLUSTER_STATUS);
            request.putInt(payloadLen);
            // payload omitted (0 bytes)
            out.write(request.array());
            out.flush();

            // Response: [responseLen:4][status:1][payloadLen:4][payload...]
            byte[] respLenBytes = in.readNBytes(4);
            assertEquals(4, respLenBytes.length);
            int status = in.read();
            assertEquals(MetadataProtocol.OK, status);
            byte[] payloadLenBytes = in.readNBytes(4);
            int len = ByteBuffer.wrap(payloadLenBytes).getInt();
            byte[] payloadBytes = in.readNBytes(len);

            return mapper.readTree(payloadBytes);
        }
    }

    private void populateHeavyStatusDataset() throws Exception {
        // Ensure nodeRegistry has ACTIVE nodes so GET_CLUSTER_STATUS has
        // work proportional to replica-set size.
        NodeRegistry registry = server.getNodeRegistry();
        int nodeCount = 8;
        for (int i = 0; i < nodeCount; i++) {
            registry.registerNode("node-" + i, "127.0.0.1", 9000 + i);
        }

        MetadataStore store = server.getMetadataStore();

        int objectCount = 120;
        int chunksPerObject = 80;
        int replicasPerChunk = 4;

        List<String> replicas = List.of("node-0", "node-1", "node-2", "node-3");

        for (int o = 0; o < objectCount; o++) {
            ObjectMetadata meta = new ObjectMetadata("obj-" + o, 1024L, 4096);
            for (int c = 0; c < chunksPerObject; c++) {
                String chunkId = "chunk-" + o + "-" + c;
                meta.addChunk(new ChunkInfo(chunkId, c, 128, replicas, ""));
            }
            store.createObjectDirect(meta);
        }
    }

    @Test
    void basicLifecycleActiveRequestsInStatusResponse() throws Exception {
        startServer();

        // Initial
        assertEquals(0, server.getObservabilityRegistry().requests().activeRequests());

        // Single request should show active=1 while it's executing.
        JsonNode root = sendGetClusterStatus();
        JsonNode reqMetrics = root.get("metrics").get("requests");
        assertEquals(1L, reqMetrics.get("activeRequests").asLong());

        assertEquals(0, server.getObservabilityRegistry().requests().activeRequests());
    }

    @Test
    void exceptionPathDoesNotLeakActiveRequests() throws Exception {
        startServer();

        assertEquals(0, server.getObservabilityRegistry().requests().activeRequests());

        // Send malformed CREATE_OBJECT payload so handler throws
        // and processRequest returns an error response.
        try (Socket socket = new Socket("127.0.0.1", port);
             OutputStream out = socket.getOutputStream();
             InputStream in = socket.getInputStream()) {

            byte opcode = MetadataProtocol.CREATE_OBJECT;
            byte[] badPayload = new byte[] { 'n', 'o', 't', '-', 'j', 's', 'o', 'n' };
            int payloadLen = badPayload.length;
            int requestLen = 1 + 4 + payloadLen;

            ByteBuffer request = ByteBuffer.allocate(4 + requestLen);
            request.putInt(requestLen);
            request.put(opcode);
            request.putInt(payloadLen);
            request.put(badPayload);
            out.write(request.array());
            out.flush();

            byte[] respLenBytes = in.readNBytes(4);
            assertEquals(4, respLenBytes.length);
            int status = in.read();
            assertEquals(MetadataProtocol.ERROR, status);

            // Drain response payload.
            byte[] payloadLenBytes = in.readNBytes(4);
            int len = ByteBuffer.wrap(payloadLenBytes).getInt();
            in.readNBytes(len);
        }

        assertEquals(0, server.getObservabilityRegistry().requests().activeRequests());
    }

    @Test
    void concurrencyActiveRequestsAndStatusPayloadReflectInFlightCount() throws Exception {
        startServer();
        populateHeavyStatusDataset();

        int concurrent = 10;
        ExecutorService executor = Executors.newFixedThreadPool(concurrent);
        CountDownLatch allSent = new CountDownLatch(concurrent);
        CountDownLatch allowRead = new CountDownLatch(1);

        Future<?>[] futures = new Future<?>[concurrent];

        for (int i = 0; i < concurrent; i++) {
            futures[i] = executor.submit(() -> {
                try (Socket socket = new Socket("127.0.0.1", port);
                     OutputStream out = socket.getOutputStream();
                     InputStream in = socket.getInputStream()) {

                    int payloadLen = 0;
                    int requestLen = 1 + 4 + payloadLen;

                    ByteBuffer request = ByteBuffer.allocate(4 + requestLen);
                    request.putInt(requestLen);
                    request.put(MetadataProtocol.GET_CLUSTER_STATUS);
                    request.putInt(payloadLen);
                    out.write(request.array());
                    out.flush();

                    allSent.countDown();

                    // Keep the request "in-flight" on the server side by
                    // delaying response reads.
                    assertTrue(allowRead.await(10, TimeUnit.SECONDS));

                    // Read response fully.
                    byte[] respLenBytes = in.readNBytes(4);
                    assertEquals(4, respLenBytes.length);
                    int status = in.read();
                    assertEquals(MetadataProtocol.OK, status);
                    byte[] payloadLenBytes = in.readNBytes(4);
                    int len = ByteBuffer.wrap(payloadLenBytes).getInt();
                    in.readNBytes(len);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        assertTrue(allSent.await(5, TimeUnit.SECONDS));

        // Wait until all concurrent requests are actively executing.
        long deadline = System.currentTimeMillis() + 5000;
        int observed = 0;
        while (System.currentTimeMillis() < deadline) {
            observed = server.getObservabilityRegistry().requests().activeRequests();
            if (observed >= concurrent) {
                break;
            }
            Thread.sleep(10);
        }

        assertEquals(concurrent, observed,
                "Expected activeRequests=" + concurrent + " while GET_CLUSTER_STATUS requests are in-flight");

        // Send one extra status request and verify the payload reflects
        // the live activeRequests counter.
        JsonNode root = sendGetClusterStatus();
        long activeFromResponse = root.get("metrics").get("requests").get("activeRequests").asLong();
        assertTrue(activeFromResponse >= concurrent, "activeRequests should reflect in-flight count");

        allowRead.countDown();

        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }

        executor.shutdownNow();
        assertEquals(0, server.getObservabilityRegistry().requests().activeRequests());
    }
}
