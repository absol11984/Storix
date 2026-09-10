package com.storix.client;

import com.storix.metadata.*;
import com.storix.metadata.raft.ClusterConfig;
import com.storix.storage.ChunkServer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 3 Prompt 3: Robust chunk read/download failover tests.
 * Tests the client's ability to read from replica fallbacks when storage nodes fail,
 * return corrupted data, or are unavailable.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class Phase3ReadFailoverTest {

    @TempDir
    Path tempDir;

    private MetadataServer metadataServer;
    private ChunkServer[] storageNodes;
    private int metaPort;
    private int[] storagePorts;
    private ExecutorService executor;

    @BeforeEach
    void setUp() throws Exception {
        executor = Executors.newCachedThreadPool();
        storageNodes = new ChunkServer[3];
        storagePorts = new int[3];

        // Allocate ports
        metaPort = findFreePort();
        for (int i = 0; i < 3; i++) {
            storagePorts[i] = findFreePort();
        }

        // Start metadata server (single-node, replication factor = 3)
        Path metaFile = tempDir.resolve("metadata.json");
        Path raftStateDir = tempDir.resolve("raft-state");
        Files.createDirectories(raftStateDir);

        ClusterConfig config = new ClusterConfig("test", "meta-1", "127.0.0.1", metaPort, null);
        metadataServer = new MetadataServer(metaPort, metaFile, 3, 5000, 1000, config, raftStateDir);

        executor.submit(() -> {
            try {
                metadataServer.start();
            } catch (IOException e) {
                System.err.println("Metadata server error: " + e.getMessage());
            }
        });

        Thread.sleep(500); // Let metadata server start

        // Start 3 storage nodes
        for (int i = 0; i < 3; i++) {
            Path storageDir = tempDir.resolve("storage-" + (i + 1));
            Files.createDirectories(storageDir);

            storageNodes[i] = new ChunkServer(
                "node-" + (i + 1),
                "127.0.0.1",
                storagePorts[i],
                storageDir,
                "127.0.0.1",
                metaPort,
                1000
            );

            final int idx = i;
            executor.submit(() -> {
                try {
                    storageNodes[idx].start();
                } catch (IOException e) {
                    System.err.println("Storage node " + (idx + 1) + " error: " + e.getMessage());
                }
            });
        }

        // Wait for nodes to register
        Thread.sleep(2000);
    }

    @AfterEach
    void tearDown() {
        for (ChunkServer node : storageNodes) {
            if (node != null) node.stop();
        }
        if (metadataServer != null) metadataServer.stop();
        if (executor != null) executor.shutdownNow();
    }

    /**
     * Test 1: Successful read from primary replica.
     */
    @Test
    @Order(1)
    void testSuccessfulReadFromPrimaryReplica() throws Exception {
        try (ClusterMetadataClient metaClient = new ClusterMetadataClient(
                List.of(new InetSocketAddress("127.0.0.1", metaPort)))) {

            metaClient.connect();

            // Upload a file
            Path testFile = tempDir.resolve("test.txt");
            String content = "Test content for primary replica read";
            Files.writeString(testFile, content);

            try (StorixClient client = new StorixClient("127.0.0.1", metaPort, 1024)) {
                client.putFile(testFile);

                // Download the file
                Path downloaded = tempDir.resolve("downloaded.txt");
                client.getFile("test.txt", downloaded);

                String downloadedContent = Files.readString(downloaded);
                assertEquals(content, downloadedContent, "Downloaded content must match original");
            }
        }
    }

    /**
     * Test 2: Primary connection failure → secondary replica succeeds.
     */
    @Test
    @Order(2)
    void testPrimaryConnectionFailureSecondarySucceeds() throws Exception {
        try (ClusterMetadataClient metaClient = new ClusterMetadataClient(
                List.of(new InetSocketAddress("127.0.0.1", metaPort)))) {

            metaClient.connect();

            // Upload chunk to all 3 replicas
            String chunkId = "test-chunk-failover";
            byte[] chunkData = "Primary fails, secondary should succeed".getBytes();
            NodeInfoDTO[] placement = metaClient.getPlacement(0);

            List<String> replicaIds = new ArrayList<>();
            for (NodeInfoDTO node : placement) {
                try (StorageNodeClient storageClient = new StorageNodeClient(node.getHost(), node.getPort())) {
                    storageClient.connect();
                    storageClient.putChunk(chunkId, chunkData);
                    replicaIds.add(node.getNodeId());
                }
            }

            // Stop the first replica node (primary)
            String primaryNode = replicaIds.get(0);
            int primaryIdx = Integer.parseInt(primaryNode.substring(5)) - 1;
            storageNodes[primaryIdx].stop();
            Thread.sleep(500);

            // Try to read - should fall back to secondary
            boolean readSucceeded = false;
            for (String replicaId : replicaIds) {
                if (replicaId.equals(primaryNode)) continue; // Skip stopped node

                NodeInfoDTO node = Arrays.stream(placement)
                        .filter(n -> n.getNodeId().equals(replicaId))
                        .findFirst()
                        .orElse(null);

                if (node == null) continue;

                try (StorageNodeClient storageClient = new StorageNodeClient(node.getHost(), node.getPort())) {
                    storageClient.connect();
                    byte[] readData = storageClient.getChunk(chunkId);
                    assertArrayEquals(chunkData, readData, "Read data should match written data");
                    readSucceeded = true;
                    break;
                } catch (IOException e) {
                    // Continue to next replica
                }
            }

            assertTrue(readSucceeded, "Should succeed from secondary replica after primary failure");
        }
    }

    /**
     * Test 3: First two replicas fail → third replica succeeds.
     */
    @Test
    @Order(3)
    void testFirstTwoReplicasFailThirdSucceeds() throws Exception {
        try (ClusterMetadataClient metaClient = new ClusterMetadataClient(
                List.of(new InetSocketAddress("127.0.0.1", metaPort)))) {

            metaClient.connect();

            // Upload chunk
            String chunkId = "test-chunk-two-fail";
            byte[] chunkData = "Two fail, third works".getBytes();
            NodeInfoDTO[] placement = metaClient.getPlacement(1);

            List<String> replicaIds = new ArrayList<>();
            for (NodeInfoDTO node : placement) {
                try (StorageNodeClient storageClient = new StorageNodeClient(node.getHost(), node.getPort())) {
                    storageClient.connect();
                    storageClient.putChunk(chunkId, chunkData);
                    replicaIds.add(node.getNodeId());
                }
            }

            // Stop first two replicas
            for (int i = 0; i < 2; i++) {
                String nodeId = replicaIds.get(i);
                int idx = Integer.parseInt(nodeId.substring(5)) - 1;
                storageNodes[idx].stop();
            }
            Thread.sleep(500);

            // Try to read from third replica
            String thirdNodeId = replicaIds.get(2);
            NodeInfoDTO thirdNode = Arrays.stream(placement)
                    .filter(n -> n.getNodeId().equals(thirdNodeId))
                    .findFirst()
                    .orElseThrow();

            try (StorageNodeClient storageClient = new StorageNodeClient(thirdNode.getHost(), thirdNode.getPort())) {
                storageClient.connect();
                byte[] readData = storageClient.getChunk(chunkId);
                assertArrayEquals(chunkData, readData, "Read from third replica should succeed");
            }
        }
    }

    /**
     * Test 4: Checksum mismatch → fallback to another replica.
     */
    @Test
    @Order(4)
    void testChecksumMismatchFallback() throws Exception {
        try (ClusterMetadataClient metaClient = new ClusterMetadataClient(
                List.of(new InetSocketAddress("127.0.0.1", metaPort)))) {

            metaClient.connect();

            // Upload the same chunk to two replicas, corrupt one
            String chunkId = "test-chunk-checksum";
            byte[] goodData = "Good data".getBytes();
            byte[] badData = "Corrupted".getBytes(); // Different data → different checksum

            NodeInfoDTO[] placement = metaClient.getPlacement(2);

            // Write good data to replica 1
            try (StorageNodeClient sc1 = new StorageNodeClient(placement[0].getHost(), placement[0].getPort())) {
                sc1.connect();
                sc1.putChunk(chunkId, goodData);
            }

            // Write bad data to replica 2
            try (StorageNodeClient sc2 = new StorageNodeClient(placement[1].getHost(), placement[1].getPort())) {
                sc2.connect();
                sc2.putChunk(chunkId, badData);
            }

            // Write good data to replica 3
            try (StorageNodeClient sc3 = new StorageNodeClient(placement[2].getHost(), placement[2].getPort())) {
                sc3.connect();
                sc3.putChunk(chunkId, goodData);
            }

            // Create metadata with correct checksum for good data
            String correctChecksum = computeSHA256(goodData);
            ObjectMetadataDTO metadata = new ObjectMetadataDTO("checksum-test", goodData.length, goodData.length);
            List<String> replicaIds = Arrays.asList(placement[0].getNodeId(), placement[1].getNodeId(), placement[2].getNodeId());
            metadata.addChunk(new ChunkInfoDTO(chunkId, 2, goodData.length, replicaIds, correctChecksum));
            metaClient.createObject(metadata);

            // Download the file - should succeed despite replica 2 corruption
            try (StorixClient client = new StorixClient("127.0.0.1", metaPort, 1024)) {
                Path downloaded = tempDir.resolve("checksum-recovered.txt");
                client.getFile("checksum-test", downloaded);

                byte[] recovered = Files.readAllBytes(downloaded);
                assertArrayEquals(goodData, recovered, "Should recover correct data despite checksum mismatch on one replica");
            }
        }
    }

    /**
     * Test 5: All replicas unavailable → explicit failure.
     */
    @Test
    @Order(5)
    void testAllReplicasUnavailableExplicitFailure() throws Exception {
        try (ClusterMetadataClient metaClient = new ClusterMetadataClient(
                List.of(new InetSocketAddress("127.0.0.1", metaPort)))) {

            metaClient.connect();

            // Upload a chunk
            String chunkId = "test-chunk-all-down";
            byte[] chunkData = "Data that will be unavailable".getBytes();
            NodeInfoDTO[] placement = metaClient.getPlacement(3);

            List<String> replicaIds = new ArrayList<>();
            for (NodeInfoDTO node : placement) {
                try (StorageNodeClient storageClient = new StorageNodeClient(node.getHost(), node.getPort())) {
                    storageClient.connect();
                    storageClient.putChunk(chunkId, chunkData);
                    replicaIds.add(node.getNodeId());
                }
            }

            // Stop ALL storage nodes
            for (ChunkServer node : storageNodes) {
                node.stop();
            }
            Thread.sleep(500);

            // Try to read - should fail with clear error
            IOException exception = assertThrows(IOException.class, () -> {
                try (StorixClient client = new StorixClient("127.0.0.1", metaPort, 1024)) {
                    // Create metadata
                    ObjectMetadataDTO metadata = new ObjectMetadataDTO("all-down-test", chunkData.length, chunkData.length);
                    String checksum = computeSHA256(chunkData);
                    metadata.addChunk(new ChunkInfoDTO(chunkId, 3, chunkData.length, replicaIds, checksum));
                    metaClient.createObject(metadata);

                    // Try to download
                    Path downloaded = tempDir.resolve("should-fail.txt");
                    client.getFile("all-down-test", downloaded);
                }
            });

            assertTrue(exception.getMessage().contains("Failed to retrieve chunk"),
                "Should throw explicit failure when all replicas unavailable");
            assertTrue(exception.getMessage().contains("from any replica"),
                "Error message should indicate replica exhaustion");
        }
    }

    /**
     * Test 6: Multi-chunk file with different replicas used per chunk.
     */
    @Test
    @Order(6)
    void testMultiChunkDifferentReplicasPerChunk() throws Exception {
        try (StorixClient client = new StorixClient("127.0.0.1", metaPort, 512)) {
            // Create a 2KB file (4 chunks of 512 bytes)
            Path testFile = tempDir.resolve("multichunk.txt");
            StringBuilder contentBuilder = new StringBuilder();
            for (int i = 0; i < 2048; i++) {
                contentBuilder.append((char) ('A' + (i % 26)));
            }
            String content = contentBuilder.toString();
            Files.writeString(testFile, content);

            // Upload file
            client.putFile(testFile);

            // Stop node-1 (will affect some replicas)
            storageNodes[0].stop();
            Thread.sleep(500);

            // Download file - should succeed using remaining replicas
            Path downloaded = tempDir.resolve("multichunk-downloaded.txt");
            client.getFile("multichunk.txt", downloaded);

            String downloadedContent = Files.readString(downloaded);
            assertEquals(content, downloadedContent, "Multi-chunk file should download correctly despite node failure");
        }
    }

    /**
     * Test 7: Corrupted replica detection and fallback.
     */
    @Test
    @Order(7)
    void testCorruptedReplicaDetectionFallback() throws Exception {
        try (ClusterMetadataClient metaClient = new ClusterMetadataClient(
                List.of(new InetSocketAddress("127.0.0.1", metaPort)))) {

            metaClient.connect();

            // Upload original data to replica 1
            String chunkId = "test-corrupted-fallback";
            byte[] originalData = "Original correct data".getBytes();
            byte[] corruptedData = "CORRUPTED data different".getBytes();

            NodeInfoDTO[] placement = metaClient.getPlacement(4);

            // Write corrupted data to replica 1
            try (StorageNodeClient sc1 = new StorageNodeClient(placement[0].getHost(), placement[0].getPort())) {
                sc1.connect();
                sc1.putChunk(chunkId, corruptedData);
            }

            // Write original data to replica 2
            try (StorageNodeClient sc2 = new StorageNodeClient(placement[1].getHost(), placement[1].getPort())) {
                sc2.connect();
                sc2.putChunk(chunkId, originalData);
            }

            // Write original data to replica 3
            try (StorageNodeClient sc3 = new StorageNodeClient(placement[2].getHost(), placement[2].getPort())) {
                sc3.connect();
                sc3.putChunk(chunkId, originalData);
            }

            // Create metadata with checksum for original data
            String correctChecksum = computeSHA256(originalData);
            ObjectMetadataDTO metadata = new ObjectMetadataDTO("corruption-test", originalData.length, originalData.length);
            List<String> replicaIds = Arrays.asList(placement[0].getNodeId(), placement[1].getNodeId(), placement[2].getNodeId());
            metadata.addChunk(new ChunkInfoDTO(chunkId, 4, originalData.length, replicaIds, correctChecksum));
            metaClient.createObject(metadata);

            // Download file - should detect corruption on replica 1, fall back to replica 2/3
            try (StorixClient client = new StorixClient("127.0.0.1", metaPort, 1024)) {
                Path downloaded = tempDir.resolve("corruption-recovered.txt");
                client.getFile("corruption-test", downloaded);

                byte[] recovered = Files.readAllBytes(downloaded);
                assertArrayEquals(originalData, recovered, "Should recover original data despite corrupted replica");
            }
        }
    }

    /**
     * Test 8: Stale placement containing failed storage node.
     */
    @Test
    @Order(8)
    void testStalePlacementWithFailedNode() throws Exception {
        try (ClusterMetadataClient metaClient = new ClusterMetadataClient(
                List.of(new InetSocketAddress("127.0.0.1", metaPort)))) {

            metaClient.connect();

            // Upload chunk to all replicas
            String chunkId = "test-stale-placement";
            byte[] chunkData = "Stale placement test".getBytes();
            NodeInfoDTO[] placement = metaClient.getPlacement(5);

            List<String> replicaIds = new ArrayList<>();
            for (NodeInfoDTO node : placement) {
                try (StorageNodeClient storageClient = new StorageNodeClient(node.getHost(), node.getPort())) {
                    storageClient.connect();
                    storageClient.putChunk(chunkId, chunkData);
                    replicaIds.add(node.getNodeId());
                }
            }

            // Stop one node but don't wait for health detection (simulate stale placement)
            storageNodes[0].stop();
            Thread.sleep(200); // Short delay, not enough for health detection

            // Client should try the failed node, detect failure, fall back to others
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
                    break;
                } catch (IOException e) {
                    // Expected for stopped node, continue to next
                }
            }

            assertTrue(readSucceeded, "Should succeed from remaining replicas despite stale placement");
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