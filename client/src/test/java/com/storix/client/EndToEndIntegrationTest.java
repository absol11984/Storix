package com.storix.client;

import com.storix.metadata.MetadataServer;
import com.storix.storage.ChunkServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EndToEndIntegrationTest {

    @TempDir
    Path tempDir;

    private MetadataServer metadataServer;
    private ChunkServer nodeA, nodeB, nodeC;
    private Path metadataFile;
    private int metadataPort;
    private int nodeAPort, nodeBPort, nodeCPort;

    private int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        // Use dynamic ports to avoid conflicts
        metadataPort = findFreePort();
        nodeAPort = findFreePort();
        nodeBPort = findFreePort();
        nodeCPort = findFreePort();

        metadataFile = tempDir.resolve("metadata.json");
        // Start Metadata Server with Factor 2, short timeout/interval
        metadataServer = new MetadataServer(metadataPort, metadataFile, 2, 2000, 500);
        new Thread(() -> {
            try {
                metadataServer.start();
            } catch (IOException ignored) {}
        }).start();
        Thread.sleep(500); // give it time to bind

        // Start Nodes
        nodeA = startNode("node-a", nodeAPort);
        nodeB = startNode("node-b", nodeBPort);
        nodeC = startNode("node-c", nodeCPort);
        Thread.sleep(500); // give nodes time to start and register
    }

    private ChunkServer startNode(String id, int port) throws IOException {
        ChunkServer node = new ChunkServer(id, "127.0.0.1", port,
                tempDir.resolve(id), "127.0.0.1", metadataPort);
        new Thread(() -> {
            try {
                node.start();
            } catch (IOException ignored) {}
        }).start();
        return node;
    }

    @AfterEach
    void tearDown() {
        if (nodeA != null) nodeA.stop();
        if (nodeB != null) nodeB.stop();
        if (nodeC != null) nodeC.stop();
        if (metadataServer != null) metadataServer.stop();
    }

    @Test
    void testUploadDownloadAndFailover() throws Exception {
        // Step 1: Create a test file
        Path originalFile = tempDir.resolve("test.bin");
        byte[] data = new byte[1024 * 1024 * 5]; // 5 MB
        for (int i = 0; i < data.length; i++) data[i] = (byte) (i % 256);
        Files.write(originalFile, data);

        // Step 2: Put file with chunk size 1MB using StorixClient
        StorixClient client = new StorixClient("127.0.0.1", metadataPort, 1024 * 1024);
        client.putFile(originalFile);

        // Verify info
        ObjectMetadataDTO info = client.getInfo("test.bin");
        assertNotNull(info);
        assertEquals(5, info.getChunkCount());
        assertEquals(2, info.getChunks().get(0).getReplicaNodeIds().size());

        // Step 3: Get the file
        Path recoveredFile = tempDir.resolve("recovered.bin");
        client.getFile("test.bin", recoveredFile);
        assertArrayEquals(data, Files.readAllBytes(recoveredFile));

        // Step 4: Simulate Node B failure (Stop node-b)
        nodeB.stop();
        System.out.println("Node B stopped for failover test");

        // Wait for health monitor to mark it unhealthy
        Thread.sleep(3000);

        // Check status to ensure it's marked unhealthy
        Map<String, Object> status = client.getMetadataClient().getClusterStatus();
        assertEquals(3, status.get("totalNodes"));
        assertEquals(2, status.get("healthyNodes"));

        // Step 5: Get the file again, it should failover to healthy replicas
        Path recoveredFile2 = tempDir.resolve("recovered2.bin");
        client.getFile("test.bin", recoveredFile2);
        assertArrayEquals(data, Files.readAllBytes(recoveredFile2));

        // Step 6: Automatic repair should trigger (via health monitor or manually here)
        client.getMetadataClient().repair();

        // Check if replicas are restored to 2 for all chunks
        ObjectMetadataDTO infoAfterRepair = client.getInfo("test.bin");
        for (ChunkInfoDTO chunk : infoAfterRepair.getChunks()) {
            // Count healthy replicas
            long healthy = chunk.getReplicaNodeIds().stream()
                    .filter(id -> !id.equals("node-b"))
                    .count();
            assertEquals(2, healthy, "Chunk " + chunk.getChunkId() + " not repaired");
        }

        // Step 7: Restart Node B and verify it rejoins
        nodeB = startNode("node-b", nodeBPort);
        Thread.sleep(1000);
        status = client.getMetadataClient().getClusterStatus();
        assertEquals(3, status.get("healthyNodes"));

        client.close();
    }
}
