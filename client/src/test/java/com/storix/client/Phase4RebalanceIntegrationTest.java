package com.storix.client;

import com.storix.metadata.*;
import com.storix.metadata.raft.ClusterConfig;
import com.storix.storage.ChunkServer;
import com.storix.storage.ChunkStorage;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class Phase4RebalanceIntegrationTest {

    @TempDir
    Path tempDir;

    private MetadataServer metadataServer;
    private ChunkServer[] storageNodes;
    private int metaPort;
    private int[] storagePorts;
    private ExecutorService executor;

    // Node 0/1 small, node 2 large
    private final long[] capacities = {100L, 100L, 1_000L};

    @BeforeEach
    void setUp() throws Exception {
        executor = Executors.newCachedThreadPool();
        storageNodes = new ChunkServer[3];
        storagePorts = new int[3];

        metaPort = findFreePort();
        for (int i = 0; i < 3; i++) {
            storagePorts[i] = findFreePort();
        }

        Path metaFile = tempDir.resolve("metadata.json");
        Path raftStateDir = tempDir.resolve("raft-state");
        Files.createDirectories(raftStateDir);

        ClusterConfig config = new ClusterConfig("test", "meta-1", "127.0.0.1", metaPort, null);
        metadataServer = new MetadataServer(metaPort, metaFile, 2, 20_000, 500, config, raftStateDir);

        executor.submit(() -> {
            try {
                metadataServer.start();
            } catch (IOException e) {
                System.err.println("Metadata server error: " + e.getMessage());
            }
        });

        // Wait for leader
        for (int i = 0; i < 50; i++) {
            if (metadataServer.isLeader()) break;
            Thread.sleep(100);
        }

        // Start storage nodes
        for (int i = 0; i < 3; i++) {
            int port = storagePorts[i];
            Path nodeDir = tempDir.resolve("node" + i);
            Files.createDirectories(nodeDir);

            ChunkServer server = new ChunkServer(
                    "node-" + i,
                    "127.0.0.1",
                    port,
                    nodeDir,
                    "127.0.0.1",
                    metaPort,
                    capacities[i],
                    200L
            );
            storageNodes[i] = server;
            executor.submit(() -> {
                try {
                    server.start();
                } catch (IOException e) {
                    System.err.println("Storage error: " + e.getMessage());
                }
            });
        }

        // Wait for all nodes to be registered and ACTIVE
        NodeRegistry registry = metadataServer.getNodeRegistry();
        for (int i = 0; i < 50; i++) {
            if (registry.healthyCount() == 3) {
                break;
            }
            Thread.sleep(100);
        }
        assertEquals(3, registry.healthyCount());
    }

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    @Test
    @Order(1)
    void testRebalanceMovesChunkAndReadsSuccessfully() throws Exception {
        StorixClient client = new StorixClient("127.0.0.1", metaPort, 64 * 1024);
        String objectName = "my-data.txt";

        // 85 bytes => when placed on 100-byte nodes => used ratio 0.85 (> 0.80 highThreshold)
        byte[] payload = new byte[85];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 256);
        }

        Path inFile = tempDir.resolve(objectName);
        Files.write(inFile, payload);
        client.putFile(inFile);

        // Upload should place the first chunk on node-0 and node-1 (least used ratio, deterministic).
        ObjectMetadata metaBefore = metadataServer.getMetadataStore().getObject(objectName).orElseThrow();
        assertEquals(1, metaBefore.getChunks().size());
        ChunkInfo chunkBefore = metaBefore.getChunks().get(0);

        Set<String> replicasBefore = new HashSet<>(chunkBefore.getReplicaNodeIds());
        assertEquals(Set.of("node-0", "node-1"), replicasBefore);

        assertEquals(85, storageNodes[0].getUsedCapacityBytes());
        assertEquals(85, storageNodes[1].getUsedCapacityBytes());
        assertEquals(0, storageNodes[2].getUsedCapacityBytes());

        // Allow heartbeats to update used capacity telemetry for overload/eligibility.
        Thread.sleep(1000);

        RebalanceManager rebalancer = metadataServer.getRebalanceManager();
        RebalanceManager.RebalanceResult result = rebalancer.rebalanceOnce();

        assertEquals(1, result.chunksMoved(), "Exactly one overloaded replica move should succeed");
        assertEquals(0, result.chunksFailed(), "No injected failures in this test");

        // Metadata: node-0 -> node-2 (source with smallest nodeId under tie)
        ObjectMetadata metaAfter = metadataServer.getMetadataStore().getObject(objectName).orElseThrow();
        ChunkInfo chunkAfter = metaAfter.getChunks().get(0);

        Set<String> replicasAfter = new HashSet<>(chunkAfter.getReplicaNodeIds());
        assertEquals(Set.of("node-1", "node-2"), replicasAfter);

        assertFalse(replicasAfter.contains("node-0"));
        assertTrue(replicasAfter.contains("node-2"));

        // Storage bytes: source deleted, destination has replica
        assertEquals(0, storageNodes[0].getUsedCapacityBytes());
        assertEquals(85, storageNodes[1].getUsedCapacityBytes());
        assertEquals(85, storageNodes[2].getUsedCapacityBytes());

        double underloadedRatio = storageNodes[2].getUsedCapacityBytes() / (double) storageNodes[2].getTotalCapacityBytes();
        assertTrue(underloadedRatio < 0.30, "Destination should remain underloaded after receiving the replica");

        // File still readable from remaining replicas
        Path outFile = tempDir.resolve("out-success.bin");
        client.getFile(objectName, outFile);
        assertArrayEquals(payload, Files.readAllBytes(outFile));

        client.close();
    }

    @Test
    @Order(2)
    void testRebalanceDestPutFailureLeavesMetadataUnchanged() throws Exception {
        StorixClient client = new StorixClient("127.0.0.1", metaPort, 64 * 1024);
        String objectName = "my-data-fail.txt";

        byte[] payload = new byte[85];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 251);
        }

        Path inFile = tempDir.resolve(objectName);
        Files.write(inFile, payload);
        client.putFile(inFile);

        ObjectMetadata metaBefore = metadataServer.getMetadataStore().getObject(objectName).orElseThrow();
        assertEquals(1, metaBefore.getChunks().size());
        ChunkInfo chunkBefore = metaBefore.getChunks().get(0);

        Set<String> replicasBefore = new HashSet<>(chunkBefore.getReplicaNodeIds());
        assertEquals(Set.of("node-0", "node-1"), replicasBefore);

        // Inject a deterministic failure on the destination node's PUT.
        ChunkStorage destStorage = storageNodes[2].getStorage();
        destStorage.setPutFailureInjector(cid -> {
            if (cid.equals(chunkBefore.getChunkId())) {
                throw new IOException("Injected dest PUT failure for chunk " + cid);
            }
        });

        try {
            // Allow heartbeats to update used capacity telemetry.
            Thread.sleep(1000);

            RebalanceManager rebalancer = metadataServer.getRebalanceManager();
            RebalanceManager.RebalanceResult result = rebalancer.rebalanceOnce();

            assertEquals(0, result.chunksMoved(), "Move must not commit when destination PUT fails");

            ObjectMetadata metaAfter = metadataServer.getMetadataStore().getObject(objectName).orElseThrow();
            ChunkInfo chunkAfter = metaAfter.getChunks().get(0);

            Set<String> replicasAfter = new HashSet<>(chunkAfter.getReplicaNodeIds());
            assertEquals(Set.of("node-0", "node-1"), replicasAfter);

            assertFalse(replicasAfter.contains("node-2"), "Destination replica must not be added to metadata");

            // Destination node should not have the chunk committed.
            assertEquals(0, storageNodes[2].getUsedCapacityBytes());
            assertFalse(destStorage.chunkExists(chunkBefore.getChunkId()));

            // Source replicas remain intact.
            assertEquals(85, storageNodes[0].getUsedCapacityBytes());
            assertEquals(85, storageNodes[1].getUsedCapacityBytes());

            // File is still readable via the original replicas.
            Path outFile = tempDir.resolve("out-failure.bin");
            client.getFile(objectName, outFile);
            assertArrayEquals(payload, Files.readAllBytes(outFile));

        } finally {
            destStorage.setPutFailureInjector(null);
            client.close();
        }
    }
}
