package com.storix.metadata;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Controlled storage-node recovery + safe reintegration (Phase 4 Prompt 3).
 *
 * Lifecycle:
 *   FAILED  --(node reconnects, heartbeat arrives)--> RECONNECTING
 *   RECONNECTING -> HEALTH_CHECK -> (if health ok) RECOVERING -> (if reconcile ok) READY
 *   ACTIVE  --(heartbeat stops, health monitor times out)--> FAILED
 *   RECOVERING/HEALTH_CHECK/RECONNECTING --(heartbeat stops)--> FAILED
 *   UNHEALTHY status from the registry is treated as terminal for ongoing recovery;
 *   on next heartbeat the node re-enters RECONNECTING.
 *
 * During HEALTH_CHECK and RECOVERING, the node is kept UNHEALTHY in the registry
 * so placement/repair/rebalance selectors exclude it. A `recoveryHold` flag on
 * {@link NodeInfo} prevents {@code recordHeartbeat()} from flipping the status
 * back to ACTIVE until recovery completes.
 */
public class NodeRecoveryManager {

    // Storage-node protocol constants (mirrors storage-node/com.storix.storage.Protocol)
    private static final byte PUT_CHUNK = 1;
    private static final byte GET_CHUNK = 2;
    private static final byte DELETE_CHUNK = 3;
    private static final byte VERIFY_CHUNK = 4;
    private static final byte LIST_NODE_STATE = 5;

    private static final byte OK = 0;
    private static final byte ERROR = 1;

    private static final long DEFAULT_SCAN_INTERVAL_MILLIS = 2000L;
    private static final int IO_TIMEOUT_MILLIS = 5000;

    private final NodeRegistry nodeRegistry;
    private final MetadataStore metadataStore;
    private final ChunkOperationLock chunkOperationLock;
    private final long scanIntervalMillis;
    private final Observability.Registry observability;

    private final Map<String, NodeRecoveryState> recoveryStates = new ConcurrentHashMap<>();
    private final Map<String, ReentrantLock> nodeLocks = new ConcurrentHashMap<>();

    private final Object lifecycleLock = new Object();
    private volatile ScheduledExecutorService executor;

    private volatile boolean started = false;

    public NodeRecoveryManager(NodeRegistry nodeRegistry,
                               MetadataStore metadataStore,
                               ChunkOperationLock chunkOperationLock) {
        this(nodeRegistry, metadataStore, chunkOperationLock, DEFAULT_SCAN_INTERVAL_MILLIS, null);
    }

    public NodeRecoveryManager(NodeRegistry nodeRegistry,
                               MetadataStore metadataStore,
                               ChunkOperationLock chunkOperationLock,
                               long scanIntervalMillis) {
        this(nodeRegistry, metadataStore, chunkOperationLock, scanIntervalMillis, null);
    }

    public NodeRecoveryManager(NodeRegistry nodeRegistry,
                               MetadataStore metadataStore,
                               ChunkOperationLock chunkOperationLock,
                               long scanIntervalMillis,
                               Observability.Registry observability) {
        this.nodeRegistry = nodeRegistry;
        this.metadataStore = metadataStore;
        this.chunkOperationLock = chunkOperationLock;
        this.scanIntervalMillis = scanIntervalMillis;
        this.observability = observability != null ? observability : new Observability.Registry();
        this.executor = createExecutor();
    }

    private ScheduledExecutorService createExecutor() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "node-recovery");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Starts the periodic recovery scan. Call once after construction.
     * Idempotent: repeated start() calls won't create duplicate schedulers.
     */
    public void start() {
        ScheduledExecutorService execToSchedule;
        boolean shouldSchedule;
        synchronized (lifecycleLock) {
            shouldSchedule = !started || executor == null || executor.isShutdown() || executor.isTerminated();
            if (!shouldSchedule) {
                return;
            }
            started = true;
            if (executor == null || executor.isShutdown() || executor.isTerminated()) {
                executor = createExecutor();
            }
            execToSchedule = executor;
        }

        try {
            execToSchedule.scheduleAtFixedRate(this::recoverAll,
                    scanIntervalMillis,
                    scanIntervalMillis,
                    TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // If we were concurrently stopped during restart, ignore.
            return;
        }

        // Run an immediate first pass so the initial state stabilizes quickly.
        recoverAll();
    }

    /**
     * Stops the periodic recovery scan.
     * Idempotent: safe to call stop() multiple times and to restart on the same instance.
     */
    public void stop() {
        ScheduledExecutorService execToStop;
        synchronized (lifecycleLock) {
            started = false;
            execToStop = executor;
            executor = null;
        }
        if (execToStop == null) {
            return;
        }

        execToStop.shutdownNow();
        try {
            execToStop.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Submits an asynchronous full recovery scan.
     * <p>
     * Lifecycle semantics:
     * <ul>
     *   <li>If {@link #stop()} has been called (i.e., the manager is stopped), this method
     *       throws {@link IllegalStateException} and will <b>not</b> recreate/start a new executor.</li>
     *   <li>If the manager is (re)started via {@link #start()}, this method submits work
     *       to the active executor.</li>
     * </ul>
     */
    public Future<Integer> recoverAllAsync() {
        ScheduledExecutorService exec;
        synchronized (lifecycleLock) {
            exec = executor;
            if (exec == null) {
                throw new IllegalStateException("NodeRecoveryManager is stopped; call start() before recoverAllAsync()");
            }
            if (exec.isShutdown() || exec.isTerminated()) {
                throw new IllegalStateException("NodeRecoveryManager executor is not running; call start() before recoverAllAsync()");
            }
        }
        return exec.submit(this::recoverAll);
    }

    /**
     * Returns the configured scan interval in milliseconds.
     */
    public long getScanIntervalMillis() {
        return scanIntervalMillis;
    }

    /**
     * Returns the current recovery state for the given node, or null if unknown.
     */
    public NodeRecoveryState getRecoveryState(String nodeId) {
        return recoveryStates.get(nodeId);
    }

    /**
     * Deterministic periodic scan.
     *
     * Rules:
     * - Nodes without an entry in recoveryStates are treated as READY when ACTIVE,
     *   or FAILED when UNHEALTHY (initializing their state).
     * - Heartbeats arriving while a node is FAILED move it to RECONNECTING.
     * - Nodes in RECONNECTING/HEALTH_CHECK/RECOVERING are driven toward READY.
     * - If a recovery-holding node stops heartbeating, it is moved back to FAILED
     *   so the next heartbeat can restart the process.
     *
     * @return number of nodes that reached READY in this scan
     */
    public int recoverAll() {
        if (Thread.currentThread().isInterrupted()) {
            return 0;
        }

        List<String> nodeIds = new ArrayList<>();
        for (NodeInfo n : nodeRegistry.getAllNodes()) {
            nodeIds.add(n.getNodeId());
        }
        nodeIds.sort(String::compareTo);

        int readyNow = 0;

        for (String nodeId : nodeIds) {
            if (Thread.currentThread().isInterrupted()) {
                return readyNow;
            }

            NodeInfo node = nodeRegistry.getNode(nodeId).orElse(null);
            if (node == null) continue;

            NodeRecoveryState state = recoveryStates.get(nodeId);

            if (state == null) {
                // First time we see this node: initialize to its current status.
                if (node.getStatus() == NodeStatus.ACTIVE) {
                    recoveryStates.put(nodeId, NodeRecoveryState.READY);
                    node.setRecoveryHold(false);
                } else {
                    recoveryStates.put(nodeId, NodeRecoveryState.FAILED);
                    node.setRecoveryHold(true);
                }
                continue;
            }

            if (state == NodeRecoveryState.READY) {
                // When a READY node becomes UNHEALTHY (heartbeat stops / timeout),
                // flip it back to FAILED so the next heartbeat transition can
                // re-run health+chunk reconciliation before reintegration.
                if (node.getStatus() == NodeStatus.UNHEALTHY) {
                    recoveryStates.put(nodeId, NodeRecoveryState.FAILED);
                    node.setRecoveryHold(true);
                }
                continue;
            }

            // Nodes in FAILED / RECONNECTING / HEALTH_CHECK / RECOVERING:
            //
            // We need to distinguish between:
            // - a node that is truly dead (no heartbeats)
            // - a node that is heartbeating but cannot flip status ACTIVE because
            //   recoveryHold is set (status stays UNHEALTHY during recovery)
            //
            // So we use a short timeout based on lastHeartbeat (via isTimedOut)
            // to detect heartbeat resumption and (re)start recovery.
            final long RECOVERY_HEARTBEAT_TIMEOUT_MILLIS = 5000L;
            boolean hasRecentHeartbeat = !node.isTimedOut(RECOVERY_HEARTBEAT_TIMEOUT_MILLIS);

            if (node.getStatus() == NodeStatus.UNHEALTHY) {
                // If we're waiting for reintegration (FAILED) but heartbeats resumed,
                // start recovery even though recoveryHold keeps the registry status
                // UNHEALTHY.
                if (state == NodeRecoveryState.FAILED && hasRecentHeartbeat) {
                    recoveryStates.put(nodeId, NodeRecoveryState.RECONNECTING);
                    node.setRecoveryHold(true);
                    state = NodeRecoveryState.RECONNECTING;
                } else {
                    // No recent heartbeat: keep it in its current failure state.
                    // If we were mid-recovery, reset to FAILED so the next
                    // heartbeat resumption can re-run health+reconcile.
                    if (!hasRecentHeartbeat && state != NodeRecoveryState.FAILED) {
                        recoveryStates.put(nodeId, NodeRecoveryState.FAILED);
                        node.setRecoveryHold(true);
                    }
                    continue;
                }
            }

            // node is ACTIVE
            if (node.getStatus() == NodeStatus.ACTIVE && state == NodeRecoveryState.FAILED) {
                // Heartbeat resumed: start recovery.
                recoveryStates.put(nodeId, NodeRecoveryState.RECONNECTING);
                node.setRecoveryHold(true);
                state = NodeRecoveryState.RECONNECTING;
            }

            if (state == NodeRecoveryState.RECONNECTING
                    || state == NodeRecoveryState.HEALTH_CHECK
                    || state == NodeRecoveryState.RECOVERING) {
                boolean becameReady = recoverNodeWithLock(node);
                if (becameReady) {
                    readyNow++;
                }
            }
        }

        return readyNow;
    }

    private ReentrantLock lockFor(String nodeId) {
        return nodeLocks.computeIfAbsent(nodeId, id -> new ReentrantLock());
    }

    private boolean recoverNodeWithLock(NodeInfo node) {
        if (Thread.currentThread().isInterrupted()) {
            return false;
        }

        String nodeId = node.getNodeId();
        ReentrantLock lock = lockFor(nodeId);
        if (!lock.tryLock()) {
            return false;
        }

        observability.managers().recoveryAttempt();
        observability.managers().recoveryActiveStart();
        try {
            NodeRecoveryState state = recoveryStates.getOrDefault(nodeId, NodeRecoveryState.FAILED);

            // Keep the node excluded from placement/repair/rebalance while we reconcile.
            if (state == NodeRecoveryState.RECONNECTING
                    || state == NodeRecoveryState.HEALTH_CHECK
                    || state == NodeRecoveryState.RECOVERING) {
                node.setStatus(NodeStatus.UNHEALTHY);
            }

            switch (state) {
                case RECONNECTING:
                    recoveryStates.put(nodeId, NodeRecoveryState.HEALTH_CHECK);
                    // fallthrough
                case HEALTH_CHECK:
                    node.setRecoveryHold(true);
                    if (!verifyHealth(node)) {
                        recoveryStates.put(nodeId, NodeRecoveryState.FAILED);
                        node.setRecoveryHold(false);
                        observability.managers().recoveryFailure();
                        return false;
                    }
                    recoveryStates.put(nodeId, NodeRecoveryState.RECOVERING);
                    // fallthrough
                case RECOVERING:
                    node.setRecoveryHold(true);
                    if (!reconcileNodeChunks(node)) {
                        recoveryStates.put(nodeId, NodeRecoveryState.FAILED);
                        node.setRecoveryHold(false);
                        observability.managers().recoveryFailure();
                        return false;
                    }
                    // Mark eligible again.
                    node.setRecoveryHold(false);
                    node.setStatus(NodeStatus.ACTIVE);
                    recoveryStates.put(nodeId, NodeRecoveryState.READY);
                    observability.managers().recoverySuccess();
                    return true;
                case READY:
                case FAILED:
                default:
                    return false;
            }
        } finally {
            lock.unlock();
            observability.managers().recoveryActiveEnd();
        }
    }

    private boolean verifyHealth(NodeInfo node) {
        try {
            ChunkInventory inventory = chunkInventory(node);
            // Refresh capacity telemetry while also forcing a heartbeat timestamp.
            nodeRegistry.heartbeat(node.getNodeId(), inventory.totalCapacityBytes, inventory.usedCapacityBytes);
            // NodeRegistry.heartbeat would set status ACTIVE; we keep it UNHEALTHY during recovery.
            node.setStatus(NodeStatus.UNHEALTHY);
            return true;
        } catch (Exception e) {
            LogHandler.error("[RECOVERY] health check failed for " + node.getNodeId() + ": " + e.getMessage());
            return false;
        }
    }

    private record ChunkInventory(long totalCapacityBytes, long usedCapacityBytes, Set<String> chunkIds) {}

    private ChunkInventory chunkInventory(NodeInfo node) throws IOException {
        try (SocketChannel channel = SocketChannel.open()) {
            channel.socket().connect(new InetSocketAddress(node.getHost(), node.getPort()), IO_TIMEOUT_MILLIS);
            channel.socket().setSoTimeout(IO_TIMEOUT_MILLIS);

            byte[] emptyChunkId = new byte[0];
            int requestSize = 1 + 4 + emptyChunkId.length;
            ByteBuffer request = ByteBuffer.allocate(4 + requestSize);
            request.putInt(requestSize);
            request.put(LIST_NODE_STATE);
            request.putInt(emptyChunkId.length);
            if (emptyChunkId.length > 0) request.put(emptyChunkId);
            request.flip();

            while (request.hasRemaining()) {
                channel.write(request);
            }

            ByteBuffer lengthBuf = ByteBuffer.allocate(4);
            readFully(channel, lengthBuf);
            lengthBuf.flip();
            int responseSize = lengthBuf.getInt();

            ByteBuffer responseBuf = ByteBuffer.allocate(responseSize);
            readFully(channel, responseBuf);
            responseBuf.flip();

            byte status = responseBuf.get();
            int dataLen = responseBuf.getInt();
            if (dataLen < 0) throw new IOException("Invalid inventory response length: " + dataLen);
            byte[] payload = new byte[dataLen];
            if (dataLen > 0) responseBuf.get(payload);

            if (status != OK) {
                throw new IOException("LIST_NODE_STATE failed: " + new String(payload, StandardCharsets.UTF_8));
            }

            // Parse payload according to storage-node ChunkHandler.buildNodeStatePayload():
            // [nodeIdLen:4][nodeId:N][total:8][used:8][chunkCount:4][each:chunkIdLen:4][chunkId:N]
            ByteBuffer buf = ByteBuffer.wrap(payload);
            int nodeIdLen = buf.getInt();
            if (nodeIdLen < 0 || nodeIdLen > 4096) {
                throw new IOException("Invalid nodeIdLen in inventory: " + nodeIdLen);
            }
            if (buf.remaining() < nodeIdLen) {
                throw new IOException("Inventory payload truncated");
            }
            buf.position(buf.position() + nodeIdLen); // skip nodeId bytes

            long total = buf.getLong();
            long used = buf.getLong();
            int chunkCount = buf.getInt();

            Set<String> ids = new HashSet<>();
            for (int i = 0; i < chunkCount; i++) {
                int cidLen = buf.getInt();
                if (cidLen < 0 || cidLen > 4096) {
                    throw new IOException("Invalid chunkId length: " + cidLen);
                }
                byte[] cidBytes = new byte[cidLen];
                buf.get(cidBytes);
                ids.add(new String(cidBytes, StandardCharsets.UTF_8));
            }

            return new ChunkInventory(total, used, ids);
        }
    }

    private void readFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            if (Thread.currentThread().isInterrupted()) {
                throw new IOException("Interrupted");
            }
            int read = channel.read(buffer);
            if (read == -1) throw new IOException("Connection closed prematurely");
        }
    }

    /**
     * Reconcile local chunks on this node against authoritative metadata.
     *
     * Idempotency:
     * - verify before overwrite
     * - missing/corrupt chunks are overwritten deterministically from a healthy source
     * - orphan/unexpected chunks are never auto-deleted
     */
    private boolean reconcileNodeChunks(NodeInfo node) {
        try {
            if (Thread.currentThread().isInterrupted()) {
                return false;
            }

            ChunkInventory inventory = chunkInventory(node);
            // Keep node excluded while reconciling; capacity refresh already happened in verifyHealth.
            node.setStatus(NodeStatus.UNHEALTHY);

            // Build expected-chunk map for this node from authoritative metadata.
            Map<String, String> expectedChecksums = new HashMap<>();
            for (String objectName : metadataStore.listObjects()) {
                if (Thread.currentThread().isInterrupted()) {
                    return false;
                }

                ObjectMetadata obj = metadataStore.getObject(objectName).orElse(null);
                if (obj == null) continue;
                for (ChunkInfo chunk : obj.getChunks()) {
                    if (chunk.getReplicaNodeIds() != null && chunk.getReplicaNodeIds().contains(node.getNodeId())) {
                        expectedChecksums.put(chunk.getChunkId(), chunk.getChecksum());
                    }
                }
            }

            for (Map.Entry<String, String> e : expectedChecksums.entrySet()) {
                if (Thread.currentThread().isInterrupted()) {
                    return false;
                }

                String chunkId = e.getKey();
                String expectedChecksum = e.getValue();

                boolean lockAcquired = chunkOperationLock.tryAcquire(chunkId);
                if (!lockAcquired) {
                    // Another repair/rebalance is in-flight; retry later.
                    return false;
                }

                try {
                    boolean hasLocal = inventory.chunkIds.contains(chunkId);
                    if (!hasLocal) {
                        if (!repairChunkBytes(node, chunkId, expectedChecksum)) {
                            return false;
                        }
                        continue;
                    }

                    // If we know the checksum, verify on-node. If invalid, repair.
                    if (expectedChecksum != null && !expectedChecksum.isEmpty()) {
                        if (!verifyChunkOnNode(node, chunkId, expectedChecksum)) {
                            if (!repairChunkBytes(node, chunkId, expectedChecksum)) {
                                return false;
                            }
                        }
                    }

                } finally {
                    chunkOperationLock.release(chunkId);
                }
            }

            return true;
        } catch (Exception e) {
            LogHandler.error("[RECOVERY] reconcile failed for " + node.getNodeId() + ": " + e.getMessage());
            return false;
        }
    }

    private boolean verifyChunkOnNode(NodeInfo node, String chunkId, String checksum) {
        try (SocketChannel channel = SocketChannel.open()) {
            channel.socket().connect(new InetSocketAddress(node.getHost(), node.getPort()), IO_TIMEOUT_MILLIS);
            channel.socket().setSoTimeout(IO_TIMEOUT_MILLIS);

            byte[] cidBytes = chunkId.getBytes(StandardCharsets.UTF_8);
            byte[] checksumBytes = checksum.getBytes(StandardCharsets.UTF_8);

            int requestSize = 1 + 4 + cidBytes.length + 4 + checksumBytes.length;
            ByteBuffer request = ByteBuffer.allocate(4 + requestSize);
            request.putInt(requestSize);
            request.put(VERIFY_CHUNK);
            request.putInt(cidBytes.length);
            request.put(cidBytes);
            request.putInt(checksumBytes.length);
            request.put(checksumBytes);
            request.flip();

            while (request.hasRemaining()) channel.write(request);

            ByteBuffer lengthBuf = ByteBuffer.allocate(4);
            readFully(channel, lengthBuf);
            lengthBuf.flip();
            int responseSize = lengthBuf.getInt();

            ByteBuffer responseBuf = ByteBuffer.allocate(responseSize);
            readFully(channel, responseBuf);
            responseBuf.flip();

            byte status = responseBuf.get();
            int dataLen = responseBuf.getInt();
            if (dataLen > 0) {
                // drain payload
                responseBuf.position(responseBuf.position() + Math.min(dataLen, responseBuf.remaining()));
            }

            return status == OK;
        } catch (IOException e) {
            return false;
        }
    }

    private boolean repairChunkBytes(NodeInfo targetNode, String chunkId, String expectedChecksum) {
        // Find a healthy source from authoritative metadata.
        List<String> replicaNodeIds = findExpectedReplicasForChunk(chunkId);
        NodeInfo sourceNode = null;
        for (String replicaId : replicaNodeIds) {
            if (replicaId.equals(targetNode.getNodeId())) continue;
            NodeInfo n = nodeRegistry.getNode(replicaId).orElse(null);
            if (n != null && n.getStatus() == NodeStatus.ACTIVE) {
                sourceNode = n;
                break;
            }
        }

        if (sourceNode == null) {
            return false;
        }

        try {
            byte[] data = getChunkFromNode(sourceNode, chunkId);
            if (expectedChecksum != null && !expectedChecksum.isEmpty()) {
                String actual = computeSha256Hex(data);
                if (!expectedChecksum.equalsIgnoreCase(actual)) {
                    return false;
                }
            }

            putChunkToNode(targetNode, chunkId, data);

            if (expectedChecksum != null && !expectedChecksum.isEmpty()) {
                if (verifyChunkOnNode(targetNode, chunkId, expectedChecksum)) {
                    observability.managers().recoveryChunks(1);
                    return true;
                }
                return false;
            }
            observability.managers().recoveryChunks(1);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private List<String> findExpectedReplicasForChunk(String chunkId) {
        for (String objectName : metadataStore.listObjects()) {
            ObjectMetadata obj = metadataStore.getObject(objectName).orElse(null);
            if (obj == null) continue;
            for (ChunkInfo c : obj.getChunks()) {
                if (chunkId.equals(c.getChunkId())) {
                    return c.getReplicaNodeIds() == null ? List.of() : c.getReplicaNodeIds();
                }
            }
        }
        return List.of();
    }

    private byte[] getChunkFromNode(NodeInfo node, String chunkId) throws IOException {
        try (SocketChannel channel = SocketChannel.open()) {
            channel.socket().connect(new InetSocketAddress(node.getHost(), node.getPort()), IO_TIMEOUT_MILLIS);
            channel.socket().setSoTimeout(IO_TIMEOUT_MILLIS);

            byte[] cidBytes = chunkId.getBytes(StandardCharsets.UTF_8);
            int requestSize = 1 + 4 + cidBytes.length;

            ByteBuffer request = ByteBuffer.allocate(4 + requestSize);
            request.putInt(requestSize);
            request.put(GET_CHUNK);
            request.putInt(cidBytes.length);
            request.put(cidBytes);
            request.flip();

            while (request.hasRemaining()) channel.write(request);

            ByteBuffer lengthBuf = ByteBuffer.allocate(4);
            readFully(channel, lengthBuf);
            lengthBuf.flip();
            int responseSize = lengthBuf.getInt();

            ByteBuffer responseBuf = ByteBuffer.allocate(responseSize);
            readFully(channel, responseBuf);
            responseBuf.flip();

            byte status = responseBuf.get();
            int dataLen = responseBuf.getInt();
            byte[] data = new byte[Math.max(0, dataLen)];
            if (dataLen > 0) responseBuf.get(data);

            if (status != OK) {
                throw new IOException("GET_CHUNK failed on " + node.getNodeId() + ": " + new String(data, StandardCharsets.UTF_8));
            }
            return data;
        }
    }

    private void putChunkToNode(NodeInfo node, String chunkId, byte[] chunkData) throws IOException {
        try (SocketChannel channel = SocketChannel.open()) {
            channel.socket().connect(new InetSocketAddress(node.getHost(), node.getPort()), IO_TIMEOUT_MILLIS);
            channel.socket().setSoTimeout(IO_TIMEOUT_MILLIS);

            byte[] cidBytes = chunkId.getBytes(StandardCharsets.UTF_8);
            int requestSize = 1 + 4 + cidBytes.length + 4 + chunkData.length;

            ByteBuffer request = ByteBuffer.allocate(4 + requestSize);
            request.putInt(requestSize);
            request.put(PUT_CHUNK);
            request.putInt(cidBytes.length);
            request.put(cidBytes);
            request.putInt(chunkData.length);
            request.put(chunkData);
            request.flip();

            while (request.hasRemaining()) channel.write(request);

            ByteBuffer lengthBuf = ByteBuffer.allocate(4);
            readFully(channel, lengthBuf);
            lengthBuf.flip();
            int responseSize = lengthBuf.getInt();

            ByteBuffer responseBuf = ByteBuffer.allocate(responseSize);
            readFully(channel, responseBuf);
            responseBuf.flip();

            byte status = responseBuf.get();
            int dataLen = responseBuf.getInt();
            if (dataLen > 0) {
                // drain payload
                responseBuf.position(responseBuf.position() + Math.min(dataLen, responseBuf.remaining()));
            }

            if (status != OK) {
                throw new IOException("PUT_CHUNK failed on " + node.getNodeId());
            }
        }
    }

    private String computeSha256Hex(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data);
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
