package com.storix.metadata;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Manages automatic repair of under-replicated chunks.
 * Detects degraded chunks and re-replicates from a healthy source to a healthy destination.
 */
public class RepairManager {

    private final MetadataStore metadataStore;
    private final NodeRegistry nodeRegistry;
    private final PlacementManager placementManager;

    public RepairManager(MetadataStore metadataStore, NodeRegistry nodeRegistry, PlacementManager placementManager) {
        this.metadataStore = metadataStore;
        this.nodeRegistry = nodeRegistry;
        this.placementManager = placementManager;
    }

    /**
     * Scans all objects for under-replicated chunks and repairs them.
     * @return summary of repair actions
     */
    public RepairResult repairAll() {
        int chunksScanned = 0;
        int chunksRepaired = 0;
        int chunksFailed = 0;
        int chunksAlreadyHealthy = 0;

        for (String objectName : metadataStore.listObjects()) {
            ObjectMetadata metadata = metadataStore.getObject(objectName).orElse(null);
            if (metadata == null) continue;

            boolean objectModified = false;

            for (ChunkInfo chunk : metadata.getChunks()) {
                chunksScanned++;

                // Filter replica nodes to only those currently healthy
                List<String> healthyReplicas = new ArrayList<>();
                for (String nodeId : chunk.getReplicaNodeIds()) {
                    NodeInfo node = nodeRegistry.getNode(nodeId).orElse(null);
                    if (node != null && node.getStatus() == NodeStatus.ACTIVE) {
                        healthyReplicas.add(nodeId);
                    }
                }

                if (healthyReplicas.size() >= placementManager.getReplicationFactor()) {
                    chunksAlreadyHealthy++;
                    continue;
                }

                if (healthyReplicas.isEmpty()) {
                    System.out.println("[REPAIR] chunk " + chunk.getChunkId() +
                            " has NO healthy replicas — cannot repair");
                    chunksFailed++;
                    continue;
                }

                // Need to add replicas
                int needed = placementManager.getReplicationFactor() - healthyReplicas.size();

                for (int i = 0; i < needed; i++) {
                    // All existing replicas (healthy + unhealthy) to avoid placing on any of them
                    List<String> allExisting = new ArrayList<>(chunk.getReplicaNodeIds());
                    NodeInfo target = placementManager.selectRepairTarget(allExisting);

                    if (target == null) {
                        System.out.println("[REPAIR] chunk " + chunk.getChunkId() +
                                " — no available target node for additional replica");
                        chunksFailed++;
                        break;
                    }

                    // Pick a healthy source
                    String sourceNodeId = healthyReplicas.get(0);
                    NodeInfo sourceNode = nodeRegistry.getNode(sourceNodeId).orElse(null);
                    if (sourceNode == null) {
                        chunksFailed++;
                        continue;
                    }

                    System.out.println("[REPAIR] chunk " + chunk.getChunkId() +
                            " source=" + sourceNodeId +
                            " destination=" + target.getNodeId());

                    try {
                        // GET from source
                        byte[] data = getChunkFromNode(sourceNode, chunk.getChunkId());

                        // Verify checksum if present
                        if (chunk.getChecksum() != null && !chunk.getChecksum().isEmpty()) {
                            String actualChecksum = computeSha256(data);
                            if (!actualChecksum.equals(chunk.getChecksum())) {
                                System.out.println("[REPAIR] FAILED — source data checksum mismatch for " +
                                        chunk.getChunkId());
                                chunksFailed++;
                                continue;
                            }
                        }

                        // PUT to destination
                        putChunkToNode(target, chunk.getChunkId(), data);

                        // Update metadata only after successful copy
                        chunk.getReplicaNodeIds().add(target.getNodeId());
                        allExisting.add(target.getNodeId());
                        objectModified = true;
                        chunksRepaired++;

                        System.out.println("[REPAIR] chunk " + chunk.getChunkId() +
                                " SUCCESS — replicated to " + target.getNodeId());
                    } catch (IOException e) {
                        System.out.println("[REPAIR] FAILED — chunk " + chunk.getChunkId() +
                                " error: " + e.getMessage());
                        chunksFailed++;
                    }
                }
            }

            if (objectModified) {
                metadataStore.updateObject(metadata);
            }
        }

        return new RepairResult(chunksScanned, chunksRepaired, chunksFailed, chunksAlreadyHealthy);
    }

    /**
     * Removes a node ID from all chunk replica lists (called when a node is permanently removed).
     */
    public void removeNodeFromReplicas(String nodeId) {
        for (String objectName : metadataStore.listObjects()) {
            ObjectMetadata metadata = metadataStore.getObject(objectName).orElse(null);
            if (metadata == null) continue;

            boolean modified = false;
            for (ChunkInfo chunk : metadata.getChunks()) {
                if (chunk.getReplicaNodeIds().remove(nodeId)) {
                    modified = true;
                }
            }
            if (modified) {
                metadataStore.updateObject(metadata);
            }
        }
    }

    private byte[] getChunkFromNode(NodeInfo node, String chunkId) throws IOException {
        try (SocketChannel channel = SocketChannel.open(
                new InetSocketAddress(node.getHost(), node.getPort()))) {
            // Build GET_CHUNK request
            byte[] chunkIdBytes = chunkId.getBytes();
            int requestSize = 1 + 4 + chunkIdBytes.length;

            ByteBuffer request = ByteBuffer.allocate(4 + requestSize);
            request.putInt(requestSize);
            request.put((byte) 2); // GET_CHUNK
            request.putInt(chunkIdBytes.length);
            request.put(chunkIdBytes);
            request.flip();
            writeFully(channel, request);

            // Read response
            ByteBuffer lengthBuf = ByteBuffer.allocate(4);
            readFully(channel, lengthBuf);
            lengthBuf.flip();
            int responseSize = lengthBuf.getInt();

            ByteBuffer responseBuf = ByteBuffer.allocate(responseSize);
            readFully(channel, responseBuf);
            responseBuf.flip();

            byte status = responseBuf.get();
            int dataLen = responseBuf.getInt();
            byte[] data = new byte[dataLen];
            if (dataLen > 0) {
                responseBuf.get(data);
            }

            if (status != 0) {
                throw new IOException("GET_CHUNK failed on " + node.getNodeId() + ": " + new String(data));
            }
            return data;
        }
    }

    private void putChunkToNode(NodeInfo node, String chunkId, byte[] chunkData) throws IOException {
        try (SocketChannel channel = SocketChannel.open(
                new InetSocketAddress(node.getHost(), node.getPort()))) {
            byte[] chunkIdBytes = chunkId.getBytes();
            int requestSize = 1 + 4 + chunkIdBytes.length + 4 + chunkData.length;

            ByteBuffer request = ByteBuffer.allocate(4 + requestSize);
            request.putInt(requestSize);
            request.put((byte) 1); // PUT_CHUNK
            request.putInt(chunkIdBytes.length);
            request.put(chunkIdBytes);
            request.putInt(chunkData.length);
            request.put(chunkData);
            request.flip();
            writeFully(channel, request);

            // Read response
            ByteBuffer lengthBuf = ByteBuffer.allocate(4);
            readFully(channel, lengthBuf);
            lengthBuf.flip();
            int responseSize = lengthBuf.getInt();

            ByteBuffer responseBuf = ByteBuffer.allocate(responseSize);
            readFully(channel, responseBuf);
            responseBuf.flip();

            byte status = responseBuf.get();
            int dataLen = responseBuf.getInt();
            byte[] data = new byte[dataLen];
            if (dataLen > 0) {
                responseBuf.get(data);
            }

            if (status != 0) {
                throw new IOException("PUT_CHUNK failed on " + node.getNodeId() + ": " + new String(data));
            }
        }
    }

    private String computeSha256(byte[] data) {
        try {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data);
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    private void readFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read == -1) {
                throw new IOException("Connection closed prematurely");
            }
        }
    }

    private void writeFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    /**
     * Summary of a repair run.
     */
    public record RepairResult(int chunksScanned, int chunksRepaired, int chunksFailed, int chunksAlreadyHealthy) {}
}
