package com.storix.metadata;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Manages automatic repair of under-replicated chunks.
 * Detects degraded chunks and re-replicates from a healthy source to a healthy destination.
 * Uses bounded concurrency to limit the number of simultaneous repair operations.
 */
public class RepairManager {

    private static final int DEFAULT_MAX_CONCURRENT_REPAIRS = 4;

    private final MetadataStore metadataStore;
    private final NodeRegistry nodeRegistry;
    private final PlacementManager placementManager;
    private final Semaphore repairSemaphore;
    private final ExecutorService repairExecutor;

    /**
     * Creates a RepairManager with default max concurrent repairs (4).
     */
    public RepairManager(MetadataStore metadataStore, NodeRegistry nodeRegistry, PlacementManager placementManager) {
        this(metadataStore, nodeRegistry, placementManager, DEFAULT_MAX_CONCURRENT_REPAIRS);
    }

    /**
     * Creates a RepairManager with custom max concurrent repairs.
     */
    public RepairManager(MetadataStore metadataStore, NodeRegistry nodeRegistry,
                        PlacementManager placementManager, int maxConcurrentRepairs) {
        this.metadataStore = metadataStore;
        this.nodeRegistry = nodeRegistry;
        this.placementManager = placementManager;
        this.repairSemaphore = new Semaphore(maxConcurrentRepairs);
        this.repairExecutor = Executors.newFixedThreadPool(maxConcurrentRepairs,
                r -> {
                    Thread t = new Thread(r, "repair-worker");
                    t.setDaemon(true);
                    return t;
                });
    }

    /**
     * Returns the max concurrent repairs limit.
     */
    public int getMaxConcurrentRepairs() {
        return repairSemaphore.availablePermits();
    }

    /**
     * Scans all objects for under-replicated chunks and repairs them.
     * Uses bounded concurrency to limit simultaneous repairs.
     * @return summary of repair actions
     */
    public RepairResult repairAll() {
        // First, collect all repair tasks
        List<RepairTask> tasks = collectRepairTasks();

        if (tasks.isEmpty()) {
            return new RepairResult(0, 0, 0, 0);
        }

        // Execute repairs with bounded concurrency
        int chunksScanned = tasks.size();
        int chunksRepaired = 0;
        int chunksFailed = 0;

        List<Future<RepairOutcome>> futures = new ArrayList<>();
        for (RepairTask task : tasks) {
            Future<RepairOutcome> future = repairExecutor.submit(() -> repairChunk(task));
            futures.add(future);
        }

        // Wait for all repairs to complete
        for (Future<RepairOutcome> future : futures) {
            try {
                RepairOutcome outcome = future.get(30, TimeUnit.SECONDS);
                if (outcome == RepairOutcome.SUCCESS) {
                    chunksRepaired++;
                } else if (outcome == RepairOutcome.FAILED) {
                    chunksFailed++;
                }
            } catch (ExecutionException e) {
                chunksFailed++;
            } catch (TimeoutException e) {
                chunksFailed++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        return new RepairResult(chunksScanned, chunksRepaired, chunksFailed, 0);
    }

    /**
     * Collects all repair tasks from metadata.
     */
    private List<RepairTask> collectRepairTasks() {
        List<RepairTask> tasks = new ArrayList<>();

        for (String objectName : metadataStore.listObjects()) {
            ObjectMetadata metadata = metadataStore.getObject(objectName).orElse(null);
            if (metadata == null) continue;

            for (ChunkInfo chunk : metadata.getChunks()) {
                // Filter replica nodes to only those currently healthy
                List<String> healthyReplicas = new ArrayList<>();
                for (String nodeId : chunk.getReplicaNodeIds()) {
                    NodeInfo node = nodeRegistry.getNode(nodeId).orElse(null);
                    if (node != null && node.getStatus() == NodeStatus.ACTIVE) {
                        healthyReplicas.add(nodeId);
                    }
                }

                if (healthyReplicas.size() >= placementManager.getReplicationFactor()) {
                    // Already healthy
                    continue;
                }

                if (healthyReplicas.isEmpty()) {
                    // Cannot repair - no healthy source
                    continue;
                }

                // Add repair task
                int needed = placementManager.getReplicationFactor() - healthyReplicas.size();
                tasks.add(new RepairTask(chunk.getChunkId(), objectName, healthyReplicas.get(0),
                        chunk.getChecksum(), needed));
            }
        }

        return tasks;
    }

    /**
     * Repairs a single chunk with semaphore-based concurrency control.
     */
    private RepairOutcome repairChunk(RepairTask task) {
        // Acquire semaphore permit
        try {
            repairSemaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return RepairOutcome.FAILED;
        }

        try {
            // Get source and target nodes
            NodeInfo sourceNode = nodeRegistry.getNode(task.sourceNodeId).orElse(null);
            if (sourceNode == null) {
                return RepairOutcome.FAILED;
            }

            // Find the chunk metadata to get existing replicas
            ObjectMetadata metadata = metadataStore.getObject(task.objectName).orElse(null);
            if (metadata == null) {
                return RepairOutcome.FAILED;
            }

            List<String> existingReplicas = new ArrayList<>();
            for (ChunkInfo chunk : metadata.getChunks()) {
                if (chunk.getChunkId().equals(task.chunkId)) {
                    existingReplicas.addAll(chunk.getReplicaNodeIds());
                    break;
                }
            }

            // Find a target node (not currently hosting this chunk)
            NodeInfo targetNode = placementManager.selectRepairTarget(existingReplicas);
            if (targetNode == null) {
                System.out.println("[REPAIR] FAILED — no available target node for " + task.chunkId);
                return RepairOutcome.FAILED;
            }

            System.out.println("[REPAIR] chunk " + task.chunkId +
                    " source=" + task.sourceNodeId +
                    " destination=" + targetNode.getNodeId());

            // GET from source
            byte[] data = getChunkFromNode(sourceNode, task.chunkId);

            // Verify checksum if present
            if (task.checksum != null && !task.checksum.isEmpty()) {
                String actualChecksum = computeSha256(data);
                if (!actualChecksum.equals(task.checksum)) {
                    System.out.println("[REPAIR] FAILED — source data checksum mismatch for " +
                            task.chunkId);
                    return RepairOutcome.FAILED;
                }
            }

            // PUT to destination
            putChunkToNode(targetNode, task.chunkId, data);

            System.out.println("[REPAIR] chunk " + task.chunkId +
                    " SUCCESS — replicated to " + targetNode.getNodeId());

            // Update metadata to include the new replica
            metadataStore.updateChunkReplica(task.objectName, task.chunkId, targetNode.getNodeId());

            return RepairOutcome.SUCCESS;

        } catch (IOException e) {
            System.out.println("[REPAIR] FAILED — chunk " + task.chunkId +
                    " error: " + e.getMessage());
            return RepairOutcome.FAILED;
        } finally {
            repairSemaphore.release();
        }
    }

    /**
     * Removes a node ID from all chunk replica lists (called when a node is permanently removed).
     */
    public void removeNodeFromReplicas(String nodeId) throws IOException {
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

    private enum RepairOutcome { SUCCESS, FAILED }

    private record RepairTask(String chunkId, String objectName, String sourceNodeId,
                              String checksum, int replicasNeeded) {}
}
