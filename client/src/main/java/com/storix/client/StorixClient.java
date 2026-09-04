package com.storix.client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * High-level client for Storix operations.
 * Orchestrates chunking, metadata management, and multi-node chunk storage.
 */
public class StorixClient implements AutoCloseable {

    private final MetadataClient metadataClient;
    private final Chunker chunker;

    public StorixClient(String metadataHost, int metadataPort, int chunkSize) {
        this.metadataClient = new MetadataClient(metadataHost, metadataPort);
        this.chunker = new Chunker(chunkSize);
    }

    /**
     * Uploads a file to Storix using chunk-level replication.
     */
    public void putFile(Path filePath) throws IOException {
        String objectName = filePath.getFileName().toString();
        long fileSize = Files.size(filePath);

        System.out.println("Uploading " + objectName);
        System.out.println("File size: " + fileSize + " bytes");

        ObjectMetadataDTO metadata = new ObjectMetadataDTO(objectName, fileSize, chunker.getChunkSize());
        metadataClient.connect();

        final int[] chunkCount = {0};
        final List<String> uploadedChunkIds = new ArrayList<>();
        final List<NodeInfoDTO> failedUploadNodes = new ArrayList<>();

        try {
            int totalChunks = chunker.processChunks(filePath, chunk -> {
                chunkCount[0]++;
                System.out.println("Uploading chunk " + chunkCount[0]);

                try {
                    // Ask Metadata Server for placement nodes for this chunk (replicas)
                    NodeInfoDTO[] replicaNodes = metadataClient.getPlacement(chunk.chunkIndex());
                    if (replicaNodes == null || replicaNodes.length == 0) {
                        throw new IOException("No healthy storage nodes available for placement");
                    }

                    List<String> successfulReplicaIds = new ArrayList<>();
                    String checksum = computeSha256(chunk.data());

                    // Upload to each replica node
                    for (NodeInfoDTO node : replicaNodes) {
                        try (StorageNodeClient storageClient = new StorageNodeClient(node.getHost(), node.getPort())) {
                            storageClient.putChunk(chunk.chunkId(), chunk.data());
                            successfulReplicaIds.add(node.getNodeId());
                        } catch (IOException e) {
                            System.err.println("Warning: Failed to upload chunk " + chunk.chunkId() +
                                    " to node " + node.getNodeId() + ": " + e.getMessage());
                            failedUploadNodes.add(node);
                        }
                    }

                    if (successfulReplicaIds.isEmpty()) {
                        throw new IOException("Failed to store chunk " + chunk.chunkId() + " on any replica node");
                    }

                    ChunkInfoDTO chunkInfo = new ChunkInfoDTO(
                            chunk.chunkId(),
                            chunk.chunkIndex(),
                            chunk.data().length,
                            successfulReplicaIds,
                            checksum
                    );
                    metadata.addChunk(chunkInfo);
                    uploadedChunkIds.add(chunk.chunkId());

                } catch (IOException e) {
                    throw new RuntimeException("Failed to upload chunk " + chunk.chunkId() + ": " + e.getMessage(), e);
                }
            });

            System.out.println("Total chunks: " + totalChunks);

            // Save metadata
            metadataClient.createObject(metadata);

            System.out.println("Upload successful");
            if (!failedUploadNodes.isEmpty()) {
                System.out.println("Note: Upload succeeded but with degraded replication for some chunks.");
            }

        } catch (RuntimeException e) {
            System.err.println("Upload failed, cleaning up...");
            // Cleanup partial upload
            for (String chunkId : uploadedChunkIds) {
                try {
                    NodeInfoDTO[] allNodes = metadataClient.getNodes();
                    for (NodeInfoDTO node : allNodes) {
                        try (StorageNodeClient sc = new StorageNodeClient(node.getHost(), node.getPort())) {
                            sc.deleteChunk(chunkId);
                        } catch (Exception ignored) {}
                    }
                } catch (Exception ignored) {}
            }
            throw new IOException("Upload failed: " + e.getMessage(), e);
        }
    }

    /**
     * Downloads a file from Storix with replica failover and checksum verification.
     */
    public void getFile(String objectName, Path outputPath) throws IOException {
        System.out.println("Downloading " + objectName);

        metadataClient.connect();
        ObjectMetadataDTO metadata = metadataClient.getObject(objectName);
        if (metadata == null) {
            throw new IOException("Object not found: " + objectName);
        }

        System.out.println("Chunks to download: " + metadata.getChunkCount());

        List<ChunkInfoDTO> sortedChunks = new ArrayList<>(metadata.getChunks());
        sortedChunks.sort((a, b) -> Integer.compare(a.getChunkIndex(), b.getChunkIndex()));

        NodeInfoDTO[] activeNodes = metadataClient.getNodes();

        try (var out = Files.newOutputStream(outputPath)) {
            int chunkNum = 1;
            for (ChunkInfoDTO chunkInfo : sortedChunks) {
                System.out.println("Downloading chunk " + chunkNum + "/" + metadata.getChunkCount());

                byte[] data = null;
                boolean chunkSuccess = false;

                for (String replicaNodeId : chunkInfo.getReplicaNodeIds()) {
                    NodeInfoDTO targetNode = findNode(activeNodes, replicaNodeId);
                    if (targetNode == null) {
                        System.out.println("[GET] " + replicaNodeId + " unavailable (not in active nodes)");
                        continue;
                    }

                    try (StorageNodeClient sc = new StorageNodeClient(targetNode.getHost(), targetNode.getPort())) {
                        data = sc.getChunk(chunkInfo.getChunkId());

                        // Verify checksum
                        if (chunkInfo.getChecksum() != null && !chunkInfo.getChecksum().isEmpty()) {
                            String expected = chunkInfo.getChecksum();
                            String actual = computeSha256(data);
                            if (!expected.equals(actual)) {
                                System.out.println("[GET] " + replicaNodeId + " chunk corrupted (checksum mismatch)");
                                continue;
                            }
                        }

                        chunkSuccess = true;
                        break;
                    } catch (IOException e) {
                        System.out.println("[GET] " + replicaNodeId + " unavailable: " + e.getMessage());
                        System.out.println("[GET] Falling back to next replica...");
                    }
                }

                if (!chunkSuccess) {
                    throw new IOException("Failed to retrieve chunk " + chunkInfo.getChunkId() + " from any replica");
                }

                out.write(data);
                chunkNum++;
            }
        }

        System.out.println("Download successful: " + outputPath);
    }

    /**
     * Deletes an object from Storix. Removes chunks from all replicas.
     */
    public void deleteObject(String objectName) throws IOException {
        System.out.println("Deleting " + objectName);

        metadataClient.connect();
        ObjectMetadataDTO metadata = metadataClient.getObject(objectName);
        if (metadata == null) {
            throw new IOException("Object not found: " + objectName);
        }

        NodeInfoDTO[] activeNodes = metadataClient.getNodes();
        List<String> failedChunks = new ArrayList<>();

        for (ChunkInfoDTO chunkInfo : metadata.getChunks()) {
            for (String replicaNodeId : chunkInfo.getReplicaNodeIds()) {
                NodeInfoDTO targetNode = findNode(activeNodes, replicaNodeId);
                // Also attempt to connect and delete on nodes even if not currently "ACTIVE" if possible,
                // but for simplicity we only delete from active nodes or known hosts.
                // It's safest to just try the host/port if we know it.
                // In Prompt 1, chunk metadata had host/post. Now we look it up.
                if (targetNode != null) {
                    try (StorageNodeClient sc = new StorageNodeClient(targetNode.getHost(), targetNode.getPort())) {
                        sc.deleteChunk(chunkInfo.getChunkId());
                    } catch (IOException e) {
                        System.err.println("Failed to delete chunk " + chunkInfo.getChunkId() + " from " + replicaNodeId);
                        failedChunks.add(chunkInfo.getChunkId());
                    }
                }
            }
        }

        if (!failedChunks.isEmpty()) {
            throw new IOException("Failed to delete " + failedChunks.size() + " chunk(s). Metadata preserved.");
        }

        if (!metadataClient.deleteObject(objectName)) {
            throw new IOException("Failed to delete metadata for " + objectName);
        }

        System.out.println("Delete successful");
    }

    public ObjectMetadataDTO getInfo(String objectName) throws IOException {
        metadataClient.connect();
        return metadataClient.getObject(objectName);
    }

    public String[] listObjects() throws IOException {
        metadataClient.connect();
        return metadataClient.listObjects();
    }

    public MetadataClient getMetadataClient() {
        return metadataClient;
    }

    private NodeInfoDTO findNode(NodeInfoDTO[] nodes, String nodeId) {
        if (nodes == null) return null;
        for (NodeInfoDTO n : nodes) {
            if (nodeId.equals(n.getNodeId())) {
                return n;
            }
        }
        return null;
    }

    private String computeSha256(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data);
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    @Override
    public void close() throws IOException {
        metadataClient.close();
    }
}
