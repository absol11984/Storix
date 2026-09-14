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
    private static final int IO_TIMEOUT_MILLIS = 5000;

    private final MetadataStore metadataStore;
    private final NodeRegistry nodeRegistry;
    private final PlacementManager placementManager;
    private final int maxConcurrentRepairs;
    private final ChunkOperationLock chunkOperationLock;

    private final Object lifecycleLock = new Object();
    private volatile boolean stopped = false;

    private volatile Semaphore repairSemaphore;
    private volatile ExecutorService repairExecutor;

    /**
     * Creates a RepairManager with default max concurrent repairs (4).
     */
    public RepairManager(MetadataStore metadataStore,
                         NodeRegistry nodeRegistry,
                         PlacementManager placementManager) {
        this(metadataStore, nodeRegistry, placementManager, DEFAULT_MAX_CONCURRENT_REPAIRS, new ChunkOperationLock());
    }

    /**
     * Creates a RepairManager with custom max concurrent repairs.
     */
    public RepairManager(MetadataStore metadataStore,
                         NodeRegistry nodeRegistry,
                         PlacementManager placementManager,
                         int maxConcurrentRepairs) {
        this(metadataStore, nodeRegistry, placementManager, maxConcurrentRepairs, new ChunkOperationLock());
    }

    /**
     * Creates a RepairManager with explicit chunk operation lock.
     */
    public RepairManager(MetadataStore metadataStore,
                         NodeRegistry nodeRegistry,
                         PlacementManager placementManager,
                         int maxConcurrentRepairs,
                         ChunkOperationLock chunkOperationLock) {
        this.metadataStore = metadataStore;
        this.nodeRegistry = nodeRegistry;
        this.placementManager = placementManager;
        this.maxConcurrentRepairs = maxConcurrentRepairs;
        this.chunkOperationLock = chunkOperationLock;

        // Default is “started” so existing call sites that never call start() still work.
        this.repairSemaphore = new Semaphore(maxConcurrentRepairs);
        this.repairExecutor = createRepairExecutor(maxConcurrentRepairs);
    }

    private ExecutorService createRepairExecutor(int threads) {
        return Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "repair-worker");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Starts the repair executor if it was stopped.
     * This method is idempotent.
     */
    public void start() {
        synchronized (lifecycleLock) {
            if (!stopped && repairExecutor != null && !repairExecutor.isShutdown()) {
                return;
            }
            stopped = false;
            repairSemaphore = new Semaphore(maxConcurrentRepairs);
            repairExecutor = createRepairExecutor(maxConcurrentRepairs);
        }
    }

    /**
     * Stops the repair executor.
     * Idempotent; safe to call multiple times.
     */
    public void stop() {
        ExecutorService execToStop;
        synchronized (lifecycleLock) {
            if (stopped) {
                return;
            }
            stopped = true;
            execToStop = repairExecutor;
            repairExecutor = null;
            repairSemaphore = null;
        }

        if (execToStop == null) {
            return;
        }

        execToStop.shutdownNow();
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        boolean interrupted = false;
        try {
            while (!execToStop.isTerminated() && System.nanoTime() < deadlineNanos) {
                long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
                if (remainingMs <= 0) {
                    break;
                }
                try {
                    execToStop.awaitTermination(Math.min(2_000, remainingMs), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    interrupted = true;
                    execToStop.shutdownNow();
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Compatibility alias.
     */
    public void close() {
        stop();
    }

    /**
     * Returns the max concurrent repairs limit (bounded concurrency).
     */
    public int getMaxConcurrentRepairs() {
        return maxConcurrentRepairs;
    }

    /**
     * Scans all objects for under-replicated chunks and repairs them.
     * Uses bounded concurrency to limit simultaneous repairs.
     */
    public RepairResult repairAll() {
        ExecutorService exec;
        Semaphore sem;
        synchronized (lifecycleLock) {
            if (stopped) {
                return new RepairResult(0, 0, 0, 0);
            }
            exec = repairExecutor;
            sem = repairSemaphore;
            if (exec == null || sem == null || exec.isShutdown()) {
                return new RepairResult(0, 0, 0, 0);
            }
        }

        // First, collect all repair tasks
        List<RepairTask> tasks = collectRepairTasks();
        if (tasks.isEmpty()) {
            return new RepairResult(0, 0, 0, 0);
        }

        int chunksScanned = tasks.size();
        int chunksRepaired = 0;
        int chunksFailed = 0;
        int chunksSkipped = 0;

        List<Future<RepairOutcome>> futures = new ArrayList<>();

        for (RepairTask task : tasks) {
            Future<RepairOutcome> future = exec.submit(() -> {
                if (stopped || Thread.currentThread().isInterrupted()) {
                    return RepairOutcome.SKIPPED;
                }
                try {
                    return repairChunk(task, sem);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return RepairOutcome.SKIPPED;
                }
            });
            futures.add(future);
        }

        // Wait for all repairs to complete
        long repairDeadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        for (Future<RepairOutcome> future : futures) {
            if (stopped || Thread.currentThread().isInterrupted()) {
                for (Future<RepairOutcome> f : futures) {
                    f.cancel(true);
                }
                break;
            }

            long remainingNanos = repairDeadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                for (Future<RepairOutcome> f : futures) {
                    f.cancel(true);
                }
                break;
            }

            try {
                RepairOutcome outcome = future.get(
                        Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNanos)),
                        TimeUnit.MILLISECONDS);
                if (outcome == RepairOutcome.SUCCESS) {
                    chunksRepaired++;
                } else if (outcome == RepairOutcome.FAILED) {
                    chunksFailed++;
                } else {
                    chunksSkipped++;
                }
            } catch (CancellationException e) {
                chunksFailed++;
            } catch (ExecutionException e) {
                chunksFailed++;
            } catch (TimeoutException e) {
                chunksFailed++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        return new RepairResult(chunksScanned, chunksRepaired, chunksFailed, chunksSkipped);
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

                int needed = placementManager.getReplicationFactor() - healthyReplicas.size();
                tasks.add(new RepairTask(
                        chunk.getChunkId(),
                        objectName,
                        healthyReplicas.get(0),
                        chunk.getChecksum(),
                        needed,
                        chunk.getChunkSize()));
            }
        }

        return tasks;
    }

    /**
     * Repairs a single chunk with semaphore-based concurrency control.
     */
    private RepairOutcome repairChunk(RepairTask task, Semaphore repairSemaphore) throws InterruptedException {
        if (stopped || Thread.currentThread().isInterrupted()) {
            return RepairOutcome.SKIPPED;
        }

        if (!chunkOperationLock.tryAcquire(task.chunkId)) {
            return RepairOutcome.SKIPPED;
        }

        boolean permitAcquired = false;
        try {
            try {
                repairSemaphore.acquire();
                permitAcquired = true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return RepairOutcome.FAILED;
            }

            if (stopped || Thread.currentThread().isInterrupted()) {
                return RepairOutcome.SKIPPED;
            }

            NodeInfo sourceNode = nodeRegistry.getNode(task.sourceNodeId).orElse(null);
            if (sourceNode == null) {
                return RepairOutcome.FAILED;
            }

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

            NodeInfo targetNode = placementManager.selectRepairTarget(existingReplicas, (long) task.chunkSizeBytes);
            if (targetNode == null) {
                return RepairOutcome.FAILED;
            }

            byte[] data;
            try {
                data = getChunkFromNode(sourceNode, task.chunkId);
            } catch (IOException e) {
                return RepairOutcome.FAILED;
            }

            if (task.checksum != null && !task.checksum.isEmpty()) {
                String actualChecksum = computeSha256(data);
                if (!actualChecksum.equals(task.checksum)) {
                    return RepairOutcome.FAILED;
                }
            }

            try {
                putChunkToNode(targetNode, task.chunkId, data);
            } catch (IOException e) {
                return RepairOutcome.FAILED;
            }

            try {
                metadataStore.updateChunkReplica(task.objectName, task.chunkId, targetNode.getNodeId());
            } catch (IOException e) {
                return RepairOutcome.FAILED;
            }

            return RepairOutcome.SUCCESS;
        } catch (RuntimeException e) {
            return RepairOutcome.FAILED;
        } finally {
            if (permitAcquired) {
                repairSemaphore.release();
            }
            chunkOperationLock.release(task.chunkId);
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
            channel.socket().setSoTimeout(IO_TIMEOUT_MILLIS);

            byte[] chunkIdBytes = chunkId.getBytes();
            int requestSize = 1 + 4 + chunkIdBytes.length;

            ByteBuffer request = ByteBuffer.allocate(4 + requestSize);
            request.putInt(requestSize);
            request.put((byte) 2); // GET_CHUNK
            request.putInt(chunkIdBytes.length);
            request.put(chunkIdBytes);
            request.flip();
            writeFully(channel, request);

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
            channel.socket().setSoTimeout(IO_TIMEOUT_MILLIS);

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

    private enum RepairOutcome { SUCCESS, FAILED, SKIPPED }

    private record RepairTask(String chunkId, String objectName, String sourceNodeId,
                                String checksum, int replicasNeeded, int chunkSizeBytes) {}
}
