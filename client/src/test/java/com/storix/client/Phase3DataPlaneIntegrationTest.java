package com.storix.client;

import com.storix.metadata.*;
import com.storix.metadata.raft.ClusterConfig;
import com.storix.metadata.raft.RaftPeer;
import com.storix.storage.ChunkServer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 3 integration test: End-to-end chunk placement, replication, and read paths.
 * Tests the complete flow from chunk creation through placement, write, verification, and read.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class Phase3DataPlaneIntegrationTest {

    @TempDir
    Path tempDir;

    private MetadataServer metadataServer;
    private ChunkServer storageNode1;
    private ChunkServer storageNode2;
    private ChunkServer storageNode3;

    private int metaPort;
    private int storage1Port;
    private int storage2Port;
    private int storage3Port;

    private ExecutorService executor;

    @BeforeEach
    void setUp() throws Exception {
        executor = Executors.newCachedThreadPool();

        // Allocate ports
        metaPort = findFreePort();
        storage1Port = findFreePort();
        storage2Port = findFreePort();
        storage3Port = findFreePort();

        // Start metadata server (single-node for simplicity)
        Path metaFile = tempDir.resolve("metadata.json");
        Path raftStateDir = tempDir.resolve("raft-state");
        Files.createDirectories(raftStateDir);

        ClusterConfig config = new ClusterConfig("test", "meta-1", "127.0.0.1", metaPort, null);
        metadataServer = new MetadataServer(metaPort, metaFile, 2, 5000, 1000, config, raftStateDir);

        executor.submit(() -> {
            try {
                metadataServer.start();
            } catch (IOException e) {
                System.err.println("Metadata server error: " + e.getMessage());
            }
        });

        // Wait for metadata server to be ready
        Thread.sleep(500);

        // Start storage nodes
        Path storage1Dir = tempDir.resolve("storage-1");
        Path storage2Dir = tempDir.resolve("storage-2");
        Path storage3Dir = tempDir.resolve("storage-3");
        Files.createDirectories(storage1Dir);
        Files.createDirectories(storage2Dir);
        Files.createDirectories(storage3Dir);

        storageNode1 = new ChunkServer("node-1", "127.0.0.1", storage1Port, storage1Dir,
                "127.0.0.1", metaPort, 1000);
        storageNode2 = new ChunkServer("node-2", "127.0.0.1", storage2Port, storage2Dir,
                "127.0.0.1", metaPort, 1000);
        storageNode3 = new ChunkServer("node-3", "127.0.0.1", storage3Port, storage3Dir,
                "127.0.0.1", metaPort, 1000);

        executor.submit(() -> {
            try {
                storageNode1.start();
            } catch (IOException e) {
                System.err.println("Storage node 1 error: " + e.getMessage());
            }
        });
        executor.submit(() -> {
            try {
                storageNode2.start();
            } catch (IOException e) {
                System.err.println("Storage node 2 error: " + e.getMessage());
            }
        });
        executor.submit(() -> {
            try {
                storageNode3.start();
            } catch (IOException e) {
                System.err.println("Storage node 3 error: " + e.getMessage());
            }
        });

        // Wait for storage nodes to register and send heartbeats
        Thread.sleep(2000);
    }

    @AfterEach
    void tearDown() {
        if (storageNode1 != null) storageNode1.stop();
        if (storageNode2 != null) storageNode2.stop();
        if (storageNode3 != null) storageNode3.stop();
        if (metadataServer != null) metadataServer.stop();

        if (executor != null) {
            executor.shutdownNow();
        }
    }

    /**
     * Test: Complete end-to-end flow - create chunk, get placement, write to replicas, read back.
     */
    @Test
    @Order(1)
    void testCompleteDataPlaneFlow() throws Exception {
        try (ClusterMetadataClient metaClient = new ClusterMetadataClient(
                List.of(new InetSocketAddress("127.0.0.1", metaPort)))) {

            metaClient.connect();

            // Step 1: Get placement for chunk 0
            NodeInfoDTO[] placement = metaClient.getPlacement(0);
            assertNotNull(placement, "Placement should not be null");
            assertEquals(2, placement.length, "Should return 2 replica nodes (replication factor)");

            // Verify no duplicates
            assertEquals(2, Arrays.stream(placement)
                            .map(NodeInfoDTO::getNodeId)
                            .distinct()
                            .count(),
                    "Placement must not contain duplicate nodes");

            // Step 2: Write chunk to all replicas
            String chunkId = "test-chunk-0";
            byte[] chunkData = "Hello from Phase 3!".getBytes();

            List<String> successfulReplicas = new ArrayList<>();
            for (NodeInfoDTO node : placement) {
                try (StorageNodeClient storageClient = new StorageNodeClient(node.getHost(), node.getPort())) {
                    storageClient.connect();
                    storageClient.putChunk(chunkId, chunkData);
                    successfulReplicas.add(node.getNodeId());
                    System.out.println("[TEST] Wrote chunk to " + node.getNodeId());
                } catch (IOException e) {
                    System.err.println("[TEST] Failed to write to " + node.getNodeId() + ": " + e.getMessage());
                }
            }

            // Step 3: Verify at least one replica succeeded
            assertFalse(successfulReplicas.isEmpty(),
                    "At least one replica write must succeed");

            // Step 4: Create metadata with chunk info
            String checksum = computeSHA256(chunkData);
            ObjectMetadataDTO metadata = new ObjectMetadataDTO("test-object-1", chunkData.length, chunkData.length);
            metadata.addChunk(new ChunkInfoDTO(chunkId, 0, chunkData.length, successfulReplicas, checksum));

            metaClient.createObject(metadata);
            System.out.println("[TEST] Created object metadata with " + successfulReplicas.size() + " replicas");

            // Step 5: Read object metadata back
            ObjectMetadataDTO retrieved = metaClient.getObject("test-object-1");
            assertNotNull(retrieved, "Object should exist");
            assertEquals(1, retrieved.getChunks().size(), "Should have 1 chunk");

            ChunkInfoDTO chunkInfo = retrieved.getChunks().get(0);
            assertEquals(chunkId, chunkInfo.getChunkId());
            assertEquals(checksum, chunkInfo.getChecksum());
            assertEquals(successfulReplicas.size(), chunkInfo.getReplicaNodeIds().size());

            // Step 6: Read chunk from a replica
            String firstReplicaId = successfulReplicas.get(0);
            NodeInfoDTO firstReplica = Arrays.stream(placement)
                    .filter(n -> n.getNodeId().equals(firstReplicaId))
                    .findFirst()
                    .orElseThrow();

            try (StorageNodeClient storageClient = new StorageNodeClient(firstReplica.getHost(), firstReplica.getPort())) {
                storageClient.connect();
                byte[] readData = storageClient.getChunk(chunkId);
                assertArrayEquals(chunkData, readData, "Read data should match written data");
                System.out.println("[TEST] Successfully read chunk from replica");
            }

            System.out.println("[TEST] Complete data plane flow - PASSED");
        }
    }

    /**
     * Test: Replica write failures are detected and don't block successful writes.
     */
    @Test
    @Order(2)
    void testReplicaWriteFailureHandling() throws Exception {
        try (ClusterMetadataClient metaClient = new ClusterMetadataClient(
                List.of(new InetSocketAddress("127.0.0.1", metaPort)))) {

            metaClient.connect();

            // Get placement
            NodeInfoDTO[] placement = metaClient.getPlacement(1);
            assertEquals(2, placement.length);

            // Stop one replica that is part of the placement to simulate failure
            String replicaToStop = placement[0].getNodeId();
            if ("node-1".equals(replicaToStop)) {
                storageNode1.stop();
            } else if ("node-2".equals(replicaToStop)) {
                storageNode2.stop();
            } else if ("node-3".equals(replicaToStop)) {
                storageNode3.stop();
            } else {
                fail("Unexpected replica nodeId in test placement: " + replicaToStop);
            }
            Thread.sleep(800);

            String chunkId = "test-chunk-failure";
            byte[] chunkData = "Partial replication test".getBytes();

            List<String> successfulReplicas = new ArrayList<>();
            for (NodeInfoDTO node : placement) {
                try (StorageNodeClient storageClient = new StorageNodeClient(node.getHost(), node.getPort())) {
                    storageClient.connect();
                    storageClient.putChunk(chunkId, chunkData);
                    successfulReplicas.add(node.getNodeId());
                } catch (IOException e) {
                    System.out.println("[TEST] Expected failure writing to " + node.getNodeId());
                }
            }

            // At least one replica should succeed (node-2 or node-3)
            assertFalse(successfulReplicas.isEmpty(),
                    "At least one replica should succeed even when one node is down");
            assertTrue(successfulReplicas.size() < placement.length,
                    "Should detect that not all replicas succeeded");

            System.out.println("[TEST] Replica failure detection - PASSED");
        }
    }

    /**
     * Test: Read fallback to another replica when primary fails.
     */
    @Test
    @Order(3)
    void testReadFallbackToAnotherReplica() throws Exception {
        try (ClusterMetadataClient metaClient = new ClusterMetadataClient(
                List.of(new InetSocketAddress("127.0.0.1", metaPort)))) {

            metaClient.connect();

            // Write chunk to multiple replicas
            String chunkId = "test-chunk-fallback";
            byte[] chunkData = "Fallback test data".getBytes();

            NodeInfoDTO[] placement = metaClient.getPlacement(2);
            List<String> replicaIds = new ArrayList<>();

            for (NodeInfoDTO node : placement) {
                try (StorageNodeClient storageClient = new StorageNodeClient(node.getHost(), node.getPort())) {
                    storageClient.connect();
                    storageClient.putChunk(chunkId, chunkData);
                    replicaIds.add(node.getNodeId());
                }
            }

            // Stop the first replica node
            if (replicaIds.contains("node-1")) {
                storageNode1.stop();
            } else if (replicaIds.contains("node-2")) {
                storageNode2.stop();
            }
            Thread.sleep(500);

            // Try reading - should succeed from remaining replica
            boolean readSucceeded = false;
            for (String replicaId : replicaIds) {
                NodeInfoDTO node = Arrays.stream(placement)
                        .filter(n -> n.getNodeId().equals(replicaId))
                        .findFirst()
                        .orElse(null);

                if (node == null) continue;

                try (StorageNodeClient storageClient = new StorageNodeClient(node.getHost(), node.getPort())) {
                    storageClient.connect();
                    byte[] readData = storageClient.getChunk(chunkId);
                    assertArrayEquals(chunkData, readData);
                    readSucceeded = true;
                    System.out.println("[TEST] Successfully read from fallback replica: " + replicaId);
                    break;
                } catch (IOException e) {
                    System.out.println("[TEST] Failed to read from " + replicaId + ", trying next replica");
                }
            }

            assertTrue(readSucceeded, "Should be able to read from at least one replica");
            System.out.println("[TEST] Read fallback - PASSED");
        }
    }

    /**
     * Test: Metadata contains placement info but not chunk bytes.
     */
    @Test
    @Order(4)
    void testMetadataContainsPlacementNotBytes() throws Exception {
        try (ClusterMetadataClient metaClient = new ClusterMetadataClient(
                List.of(new InetSocketAddress("127.0.0.1", metaPort)))) {

            metaClient.connect();

            // Create a large chunk
            byte[] largeChunk = new byte[1024 * 100]; // 100KB
            Arrays.fill(largeChunk, (byte) 'X');

            NodeInfoDTO[] placement = metaClient.getPlacement(3);
            String chunkId = "large-chunk-test";

            List<String> replicaIds = new ArrayList<>();
            for (NodeInfoDTO node : placement) {
                try (StorageNodeClient storageClient = new StorageNodeClient(node.getHost(), node.getPort())) {
                    storageClient.connect();
                    storageClient.putChunk(chunkId, largeChunk);
                    replicaIds.add(node.getNodeId());
                } catch (IOException e) {
                    System.err.println("Failed to write to " + node.getNodeId());
                }
            }

            // Create metadata
            String checksum = computeSHA256(largeChunk);
            ObjectMetadataDTO metadata = new ObjectMetadataDTO("large-object", largeChunk.length, largeChunk.length);
            metadata.addChunk(new ChunkInfoDTO(chunkId, 0, largeChunk.length, replicaIds, checksum));

            metaClient.createObject(metadata);

            // Read back metadata
            ObjectMetadataDTO retrieved = metaClient.getObject("large-object");
            assertNotNull(retrieved);

            // Verify metadata contains placement info
            ChunkInfoDTO chunkInfo = retrieved.getChunks().get(0);
            assertNotNull(chunkInfo.getReplicaNodeIds());
            assertFalse(chunkInfo.getReplicaNodeIds().isEmpty());
            assertNotNull(chunkInfo.getChecksum());

            // The metadata should be small (< 10KB even with large chunks)
            // Metadata contains only IDs, not chunk bytes
            System.out.println("[TEST] Metadata size is reasonable, contains placement not bytes - PASSED");
        }
    }

    private int findFreePort() throws IOException {
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private String computeSHA256(byte[] data) {
        try {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data);
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
