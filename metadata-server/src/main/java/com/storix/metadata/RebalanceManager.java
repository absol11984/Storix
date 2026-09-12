package com.storix.metadata;

import com.storix.metadata.raft.LogEntry;
import com.storix.metadata.raft.RaftNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Controlled replica movement based on used/total capacity ratio.
 *
 * For each chunk, if an ACTIVE replica is overloaded and there exists an ACTIVE underloaded
 * destination with enough available capacity, the manager performs a safe copy-then-swap move:
 * GET from source -> PUT to destination -> verify destination checksum -> persist metadata swap
 * -> delete source bytes.
 *
 * When a RaftNode is present (cluster mode), the manager:
 * - Skips movement when the local server is not the leader (runOnceSafely short-circuits).
 * - Submits the metadata mutation through Raft via {@code raftNode.submit(UPDATE_OBJECT)}
 *   instead of calling {@code metadataStore.moveChunkReplica()} directly.
 * - Deletes source bytes only after the Raft submit returns {@code true} (majority-commit).
 *   If the Raft submit fails, the dest replica is cleaned up and the source is preserved.
 *
 * Repair and rebalance are coordinated via {@link ChunkOperationLock} to prevent concurrent
 * operations on the same chunk.
 */
public class RebalanceManager {

    private static final int DEFAULT_REBALANCE_INTERVAL_MILLIS = 30_000;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final MetadataStore metadataStore;
    private final NodeRegistry nodeRegistry;
    private final PlacementManager placementManager;
    private final ChunkOperationLock chunkOperationLock;
    /** May be null in single-node mode; non-null in cluster mode. */
    private final RaftNode raftNode;

    private final double highThreshold;
    private final double lowThreshold;

    private final Object lifecycleLock = new Object();
    private ScheduledExecutorService scheduler;

    private final ChunkTransfer chunkTransfer;
    private final FailureInjector failureInjector;

    /**
     * Production constructor (TCP transfer, default thresholds, default interval).
     */
    public RebalanceManager(MetadataStore metadataStore,
                             NodeRegistry nodeRegistry,
                             PlacementManager placementManager,
                             ChunkOperationLock chunkOperationLock) {
        this(metadataStore,
                nodeRegistry,
                placementManager,
                chunkOperationLock,
                null,
                0.80,
                0.30,
                DEFAULT_REBALANCE_INTERVAL_MILLIS,
                new TcpChunkTransfer(),
                FailureInjector.noop());
    }

    /**
     * Cluster-aware constructor: accepts an optional {@link RaftNode} so rebalance
     * can submit metadata mutations through Raft rather than writing directly to the
     * store. When raftNode is null (single-node mode), behaviour is unchanged.
     */
    public RebalanceManager(MetadataStore metadataStore,
                             NodeRegistry nodeRegistry,
                             PlacementManager placementManager,
                             ChunkOperationLock chunkOperationLock,
                             RaftNode raftNode) {
        this(metadataStore,
                nodeRegistry,
                placementManager,
                chunkOperationLock,
                raftNode,
                0.80,
                0.30,
                DEFAULT_REBALANCE_INTERVAL_MILLIS,
                new TcpChunkTransfer(),
                FailureInjector.noop());
    }

    /**
     * Test constructor.
     */
    public RebalanceManager(MetadataStore metadataStore,
                             NodeRegistry nodeRegistry,
                             PlacementManager placementManager,
                             ChunkOperationLock chunkOperationLock,
                             double highThreshold,
                             double lowThreshold,
                             long rebalanceIntervalMillis,
                             ChunkTransfer chunkTransfer,
                             FailureInjector failureInjector) {
        this(metadataStore,
                nodeRegistry,
                placementManager,
                chunkOperationLock,
                null,
                highThreshold,
                lowThreshold,
                rebalanceIntervalMillis,
                chunkTransfer,
                failureInjector);
    }

    /**
     * Full test constructor (with raftNode parameter).
     */
    public RebalanceManager(MetadataStore metadataStore,
                             NodeRegistry nodeRegistry,
                             PlacementManager placementManager,
                             ChunkOperationLock chunkOperationLock,
                             RaftNode raftNode,
                             double highThreshold,
                             double lowThreshold,
                             long rebalanceIntervalMillis,
                             ChunkTransfer chunkTransfer,
                             FailureInjector failureInjector) {
        this.metadataStore = Objects.requireNonNull(metadataStore);
        this.nodeRegistry = Objects.requireNonNull(nodeRegistry);
        this.placementManager = Objects.requireNonNull(placementManager);
        this.chunkOperationLock = Objects.requireNonNull(chunkOperationLock);
        this.raftNode = raftNode;
        this.highThreshold = highThreshold;
        this.lowThreshold = lowThreshold;
        this.rebalanceIntervalMillis = rebalanceIntervalMillis;
        this.chunkTransfer = Objects.requireNonNull(chunkTransfer);
        this.failureInjector = Objects.requireNonNull(failureInjector);
    }

    private final long rebalanceIntervalMillis;

    public long getRebalanceIntervalMillis() {
        return rebalanceIntervalMillis;
    }

    /**
     * Returns true when this manager runs in cluster mode with a RaftNode.
     * Used by tests to gate expectations.
     */
    public boolean hasRaftNode() {
        return raftNode != null;
    }

    /**
     * Starts periodic rebalancing.
     */
    public void start() {
        synchronized (lifecycleLock) {
            if (scheduler != null && !scheduler.isShutdown()) {
                return;
            }
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "rebalance-manager");
                t.setDaemon(true);
                return t;
            });

            scheduler.scheduleAtFixedRate(this::runOnceSafely,
                    rebalanceIntervalMillis,
                    rebalanceIntervalMillis,
                    TimeUnit.MILLISECONDS);

            System.out.println("[REBALANCE] Manager started (interval=" + rebalanceIntervalMillis + "ms, " +
                    "high=" + highThreshold + ", low=" + lowThreshold + ", raft=" + raftNode + ")");
        }
    }

    /**
     * Stops periodic rebalancing.
     */
    public void stop() {
        synchronized (lifecycleLock) {
            if (scheduler == null) {
                return;
            }
            ScheduledExecutorService exec = scheduler;
            scheduler = null;
            exec.shutdownNow();
        }
    }

    /**
     * Scheduled entry point. When in cluster mode and this node is not the leader,
     * the scan is skipped entirely — followers must never independently mutate
     * durable placement metadata.
     */
    private void runOnceSafely() {
        if (raftNode != null && !raftNode.isLeader()) {
            return;
        }
        try {
            RebalanceResult result = rebalanceOnce();
            if (result.chunksMoved() > 0 || result.chunksFailed() > 0) {
                System.out.println("[REBALANCE] Complete: scanned=" + result.chunksScanned() +
                        " moved=" + result.chunksMoved() +
                        " skipped=" + result.chunksSkipped() +
                        " failed=" + result.chunksFailed());
            }
        } catch (Exception e) {
            System.err.println("[REBALANCE] Error during rebalance: " + e.getMessage());
        }
    }

    /**
     * Runs one deterministic rebalance scan.
     */
    public RebalanceResult rebalanceOnce() {
        List<String> objects = new ArrayList<>(metadataStore.listObjects());
        objects.sort(String::compareTo);

        int chunksScanned = 0;
        int chunksMoved = 0;
        int chunksSkipped = 0;
        int chunksFailed = 0;

        Set<String> movedChunkIds = new HashSet<>();

        for (String objectName : objects) {
            Optional<ObjectMetadata> metaOpt = metadataStore.getObject(objectName);
            if (metaOpt.isEmpty()) {
                continue;
            }
            ObjectMetadata metadata = metaOpt.get();

            for (ChunkInfo chunk : metadata.getChunks()) {
                String chunkId = chunk.getChunkId();
                chunksScanned++;

                if (movedChunkIds.contains(chunkId)) {
                    chunksSkipped++;
                    continue;
                }

                boolean moved = attemptMoveOneChunk(objectName, chunk);
                if (moved) {
                    movedChunkIds.add(chunkId);
                    chunksMoved++;
                } else {
                    chunksSkipped++;
                }
            }
        }

        return new RebalanceResult(chunksScanned, chunksMoved, chunksSkipped, chunksFailed);
    }

    /**
     * Attempts a copy-then-swap move for a single chunk.
     *
     * <p>In cluster mode (raftNode != null) the metadata mutation is submitted
     * through Raft via {@link #trySubmitReplicaMove}. In single-node mode the
     * legacy direct-store path is used for backward compatibility.
     */
    private boolean attemptMoveOneChunk(String objectName, ChunkInfo chunk) {
        String chunkId = chunk.getChunkId();

        // Skip if another move/repair is in flight for this chunk.
        if (!chunkOperationLock.tryAcquire(chunkId)) {
            System.out.println("[REBALANCE] SKIPPED — chunk lock held for " + chunkId);
            return false;
        }

        try {
            int rf = placementManager.getReplicationFactor();
            long chunkSizeBytes = chunk.getChunkSize();

            // Re-read metadata to validate current replica placement.
            Optional<ObjectMetadata> metaOpt = metadataStore.getObject(objectName);
            if (metaOpt.isEmpty()) {
                return false;
            }
            ObjectMetadata metadata = metaOpt.get();

            ChunkInfo currentChunk = findChunk(metadata, chunkId);
            if (currentChunk == null) {
                return false;
            }

            List<String> replicaIds = new ArrayList<>(currentChunk.getReplicaNodeIds());
            if (replicaIds.isEmpty()) {
                return false;
            }

            // Skip under-replicated chunks (repair owns those).
            long healthyReplicas = replicaIds.stream()
                    .map(nodeRegistry::getNode)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .filter(n -> n.getStatus() == NodeStatus.ACTIVE)
                    .count();

            if (healthyReplicas < rf) {
                return false;
            }

            // Find an overloaded ACTIVE source replica.
            List<NodeInfo> overloadedSources = replicaIds.stream()
                    .map(nodeRegistry::getNode)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .filter(n -> n.getStatus() == NodeStatus.ACTIVE)
                    .filter(this::isOverloaded)
                    .toList();

            if (overloadedSources.isEmpty()) {
                return false;
            }

            NodeInfo source = overloadedSources.stream()
                    .sorted(overloadedSourceComparator())
                    .findFirst()
                    .orElse(null);

            if (source == null) {
                return false;
            }

            // Determine destination candidates (underloaded, capacity-eligible, not already a replica).
            List<NodeInfo> eligibleDestinations = eligibleDestinations(replicaIds, currentChunk);
            if (eligibleDestinations.isEmpty()) {
                return false;
            }

            NodeInfo dest = eligibleDestinations.stream().min(usedRatioComparator()).orElse(null);
            if (dest == null) {
                return false;
            }

            // Validate source still has the replica and dest is not already in metadata.
            List<String> latestReplicas = currentChunk.getReplicaNodeIds();
            if (!latestReplicas.contains(source.getNodeId()) || latestReplicas.contains(dest.getNodeId())) {
                return false;
            }

            byte[] sourceData;
            boolean destPut = false;
            try {
                // Step 1: GET bytes from source.
                sourceData = chunkTransfer.getChunk(source, chunkId);

                failureInjector.maybeFail(FailureStep.AFTER_GET, objectName, chunkId);

                // Step 2: Verify source checksum (best-effort).
                String expectedChecksum = currentChunk.getChecksum();
                if (expectedChecksum != null && !expectedChecksum.isEmpty()) {
                    String actual = computeSha256Hex(sourceData);
                    if (!expectedChecksum.equalsIgnoreCase(actual)) {
                        System.out.println("[REBALANCE] FAILED — source checksum mismatch for " + chunkId);
                        return false;
                    }
                }

                // Step 3: PUT bytes to destination.
                chunkTransfer.putChunk(dest, chunkId, sourceData);
                destPut = true;

                failureInjector.maybeFail(FailureStep.AFTER_PUT, objectName, chunkId);

                // Step 4: Verify destination bytes.
                byte[] destData = chunkTransfer.getChunk(dest, chunkId);
                String destActualChecksum = computeSha256Hex(destData);
                if (expectedChecksum != null && !expectedChecksum.isEmpty()) {
                    if (!expectedChecksum.equalsIgnoreCase(destActualChecksum)) {
                        System.out.println("[REBALANCE] FAILED — dest checksum mismatch for " + chunkId);
                        // Best-effort cleanup of bad dest data.
                        try {
                            chunkTransfer.deleteChunk(dest, chunkId);
                        } catch (IOException ignored) {
                        }
                        return false;
                    }
                }

                failureInjector.maybeFail(FailureStep.AFTER_VERIFY_DEST, objectName, chunkId);

                if (raftNode != null) {
                    // Cluster mode: submit metadata mutation through Raft.
                    // Delete source bytes only after Raft commit confirms the swap.
                    boolean raftSuccess = trySubmitReplicaMove(objectName, chunkId, source, dest,
                            expectedChecksum, currentChunk, metadata, destPut);
                    if (raftSuccess) {
                        // Step 6: DELETE bytes from source (only after Raft commit).
                        try {
                            failureInjector.maybeFail(FailureStep.AFTER_SOURCE_DELETE, objectName, chunkId);
                            chunkTransfer.deleteChunk(source, chunkId);
                        } catch (IOException e) {
                            // Per spec: if delete fails, do NOT revert metadata.
                            System.err.println("[REBALANCE] WARNING — source delete failed for " + chunkId +
                                    ": " + e.getMessage());
                        }
                        // Step 7: Local used-capacity adjustment for hysteresis.
                        adjustLocalUsedCapacity(source, dest, chunk.getChunkSize());
                        System.out.println("[REBALANCE] SUCCESS — moved chunk=" + chunkId +
                                " from " + source.getNodeId() + " to " + dest.getNodeId() + " (via Raft)");
                        return true;
                    }
                    // Raft submit failed — destPut is still true, cleanup handled inside trySubmitReplicaMove.
                    System.err.println("[REBALANCE] FAILED — Raft submit failed for chunk " + chunkId);
                    return false;
                } else {
                    // Single-node mode: fall back to direct store mutation (legacy path).
                    failureInjector.maybeFail(FailureStep.BEFORE_METADATA_PERSIST, objectName, chunkId);

                    boolean moved = metadataStore.moveChunkReplica(objectName, chunkId,
                            source.getNodeId(), dest.getNodeId());

                    if (!moved) {
                        // Metadata did not change; clean up only if dest is not now a replica.
                        boolean destIsReplica = metadataStore.getObject(objectName)
                                .flatMap(m -> Optional.ofNullable(findChunk(m, chunkId)))
                                .map(c -> c.getReplicaNodeIds().contains(dest.getNodeId()))
                                .orElse(false);

                        if (!destIsReplica) {
                            try {
                                chunkTransfer.deleteChunk(dest, chunkId);
                            } catch (IOException ignored) {
                            }
                        }
                        return false;
                    }

                    // Step 6: DELETE bytes from source (best-effort).
                    try {
                        failureInjector.maybeFail(FailureStep.AFTER_SOURCE_DELETE, objectName, chunkId);
                        chunkTransfer.deleteChunk(source, chunkId);
                    } catch (IOException e) {
                        // Per spec: if delete fails, do NOT revert metadata.
                        System.err.println("[REBALANCE] WARNING — source delete failed for " + chunkId +
                                ": " + e.getMessage());
                    }

                    // Step 7: Local used-capacity adjustment for hysteresis.
                    adjustLocalUsedCapacity(source, dest, chunk.getChunkSize());

                    System.out.println("[REBALANCE] SUCCESS — moved chunk=" + chunkId +
                            " from " + source.getNodeId() + " to " + dest.getNodeId());

                    return true;
                }
            } catch (IOException e) {
                // Failure after dest PUT/verify: clean up dest bytes.
                if (destPut) {
                    try {
                        chunkTransfer.deleteChunk(dest, chunkId);
                    } catch (IOException ignored) {
                    }
                }
                System.err.println("[REBALANCE] FAILED — chunk " + chunkId + " error: " + e.getMessage());
                return false;
            }
        } finally {
            chunkOperationLock.release(chunkId);
        }
    }

    /**
     * Submits an UPDATE_OBJECT entry through Raft that moves the replica from source to dest.
     *
     * <p>Behavior on failure:
     * <ul>
     *   <li>{@code raftNode.submit()} returns {@code false} — the entry was not committed.
     *       The verified dest replica is cleaned up (best-effort) and the source is preserved.
     *       The caller may retry on a later scan after a leadership change.</li>
     *   <li>{@code raftNode.submit()} throws — same as false; dest is cleaned up, source preserved.</li>
     * </ul>
     *
     * <p>The metadata mutation is applied to a fresh copy of the object, performs the same
     * add-dest/remove-source swap that {@code MetadataStore.moveChunkReplica()} does, and
     * serializes the updated object for the Raft log.
     *
     * @param destPut whether a verified copy already exists on the destination
     * @return true if the Raft entry was submitted and committed (source may still need deletion)
     */
    private boolean trySubmitReplicaMove(String objectName, String chunkId,
                                          NodeInfo source, NodeInfo dest,
                                          String expectedChecksum,
                                          ChunkInfo currentChunk,
                                          ObjectMetadata metadata,
                                          boolean destPut) {
        // Build the updated ObjectMetadata with the replica swap.
        ObjectMetadata updated;
        try {
            updated = buildUpdatedMetadata(objectName, chunkId, currentChunk, metadata, source.getNodeId(), dest.getNodeId());
        } catch (Exception e) {
            // Build failure: clean up dest if we wrote it.
            if (destPut) {
                try { chunkTransfer.deleteChunk(dest, chunkId); } catch (IOException ignored) {}
            }
            System.err.println("[REBALANCE] FAILED — build updated metadata for " + chunkId + ": " + e.getMessage());
            return false;
        }

        // Serialize for the Raft log.
        byte[] data;
        try {
            data = OBJECT_MAPPER.writeValueAsBytes(updated);
        } catch (IOException e) {
            if (destPut) {
                try { chunkTransfer.deleteChunk(dest, chunkId); } catch (IOException ignored) {}
            }
            System.err.println("[REBALANCE] FAILED — serialize metadata bytes for " + chunkId + ": " + e.getMessage());
            return false;
        }

        LogEntry entry = LogEntry.create(
                raftNode.getCurrentTerm(),
                LogEntry.OpType.UPDATE_OBJECT,
                data,
                "rebalance:" + chunkId,
                objectName + ":" + chunkId + ":" + System.nanoTime());

        try {
            boolean committed = raftNode.submit(entry);
            if (!committed) {
                // Raft did not commit (no majority / leader step-down during submit).
                // Clean up the verified dest replica so we don't leave a stray copy.
                if (destPut) {
                    try { chunkTransfer.deleteChunk(dest, chunkId); } catch (IOException ignored) {}
                }
                System.err.println("[REBALANCE] FAILED — Raft submit not committed for chunk " + chunkId);
                return false;
            }
            return true;
        } catch (Exception e) {
            // Raft submit threw (should not happen with the current RaftNode API,
            // but handle defensively): clean up dest and return false.
            if (destPut) {
                try { chunkTransfer.deleteChunk(dest, chunkId); } catch (IOException ignored) {}
            }
            System.err.println("[REBALANCE] FAILED — Raft submit threw for chunk " + chunkId + ": " + e.getMessage());
            return false;
        }
    }

    /**
     * Builds a fresh ObjectMetadata copy with the replica swap applied.
     * Returns a new ObjectMetadata instance; the original is not mutated.
     */
    private ObjectMetadata buildUpdatedMetadata(String objectName, String chunkId,
                                                 ChunkInfo currentChunk,
                                                 ObjectMetadata metadata,
                                                 String sourceNodeId, String destNodeId) {
        ObjectMetadata copy = new ObjectMetadata();
        copy.setObjectName(objectName);
        copy.setFileSize(currentChunk.getChunkSize());
        copy.setChunkSize(currentChunk.getChunkSize());

        for (ChunkInfo c : metadata.getChunks()) {
            if (c.getChunkId().equals(chunkId)) {
                // Build a new ChunkInfo with the swapped replicas.
                List<String> newReplicas = new ArrayList<>(c.getReplicaNodeIds());
                newReplicas.add(destNodeId);
                newReplicas.remove(sourceNodeId);
                ChunkInfo newChunk = new ChunkInfo(
                        c.getChunkId(),
                        c.getChunkIndex(),
                        c.getChunkSize(),
                        newReplicas,
                        c.getChecksum());
                copy.addChunk(newChunk);
            } else {
                copy.addChunk(c);
            }
        }
        return copy;
    }

    private List<NodeInfo> eligibleDestinations(List<String> existingReplicaIds, ChunkInfo chunk) {
        long chunkSizeBytes = chunk.getChunkSize();
        List<NodeInfo> eligible = new ArrayList<>();

        for (NodeInfo node : nodeRegistry.getHealthyNodes()) {
            if (existingReplicaIds.contains(node.getNodeId())) {
                continue;
            }

            if (node.getAvailableCapacityBytes() < chunkSizeBytes) {
                continue;
            }

            double usedRatio = usedRatio(node);
            if (usedRatio >= lowThreshold) {
                continue;
            }

            // Refuse destinations that would exceed high threshold after accepting this chunk.
            long total = node.getTotalCapacityBytes();
            if (total > 0) {
                double afterUsedRatio = (double) (node.getUsedCapacityBytes() + chunkSizeBytes) / (double) total;
                if (afterUsedRatio > highThreshold) {
                    continue;
                }
            }

            eligible.add(node);
        }

        // Keep deterministic order; caller still chooses min.
        eligible.sort(usedRatioComparator());
        return eligible;
    }

    private Comparator<NodeInfo> overloadedSourceComparator() {
        // Most overloaded first, tie by nodeId.
        return Comparator.<NodeInfo>comparingDouble(this::usedRatio).reversed()
                .thenComparing(NodeInfo::getNodeId);
    }

    private Comparator<NodeInfo> usedRatioComparator() {
        return Comparator
                .comparingDouble(this::usedRatio)
                .thenComparing(NodeInfo::getNodeId);
    }

    private boolean isOverloaded(NodeInfo node) {
        long total = node.getTotalCapacityBytes();
        if (total <= 0) {
            // Unknown capacity is never considered overloaded.
            return false;
        }
        double ratio = usedRatio(node);
        return ratio > highThreshold;
    }

    private double usedRatio(NodeInfo node) {
        long total = node.getTotalCapacityBytes();
        if (total <= 0) {
            // Unknown capacity is treated as stable (ratio=0).
            return 0.0;
        }
        return (double) node.getUsedCapacityBytes() / (double) total;
    }

    private void adjustLocalUsedCapacity(NodeInfo source, NodeInfo dest, long chunkSizeBytes) {
        // Debit source, credit destination for hysteresis decisions.
        if (source.getTotalCapacityBytes() > 0) {
            long newUsed = Math.max(0, source.getUsedCapacityBytes() - chunkSizeBytes);
            source.setUsedCapacityBytes(newUsed);
        }
        if (dest.getTotalCapacityBytes() > 0) {
            long newUsed = dest.getUsedCapacityBytes() + chunkSizeBytes;
            dest.setUsedCapacityBytes(newUsed);
        }
    }

    private ChunkInfo findChunk(ObjectMetadata metadata, String chunkId) {
        for (ChunkInfo c : metadata.getChunks()) {
            if (c.getChunkId().equals(chunkId)) {
                return c;
            }
        }
        return null;
    }

    private static String computeSha256Hex(byte[] data) {
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

    /**
     * TCP implementation of the chunk transfer protocol used by storage-node.
     *
     * Uses the same wire protocol as {@link RepairManager#getChunkFromNode} and
     * {@link RepairManager#putChunkToNode}, extended with DELETE.
     */
    static class TcpChunkTransfer implements ChunkTransfer {

        @Override
        public byte[] getChunk(NodeInfo node, String chunkId) throws IOException {
            try (SocketChannel channel = SocketChannel.open(
                    new InetSocketAddress(node.getHost(), node.getPort()))) {

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

        @Override
        public void putChunk(NodeInfo node, String chunkId, byte[] chunkData) throws IOException {
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

        @Override
        public void deleteChunk(NodeInfo node, String chunkId) throws IOException {
            try (SocketChannel channel = SocketChannel.open(
                    new InetSocketAddress(node.getHost(), node.getPort()))) {

                byte[] chunkIdBytes = chunkId.getBytes();
                int requestSize = 1 + 4 + chunkIdBytes.length;

                ByteBuffer request = ByteBuffer.allocate(4 + requestSize);
                request.putInt(requestSize);
                request.put((byte) 3); // DELETE_CHUNK
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
                    throw new IOException("DELETE_CHUNK failed on " + node.getNodeId() + ": " + new String(data));
                }
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
    }

    /**
     * Transfers chunk bytes to/from storage-node.
     */
    public interface ChunkTransfer {
        byte[] getChunk(NodeInfo node, String chunkId) throws IOException;
        void putChunk(NodeInfo node, String chunkId, byte[] chunkData) throws IOException;
        void deleteChunk(NodeInfo node, String chunkId) throws IOException;
    }

    public enum FailureStep {
        AFTER_GET,
        AFTER_PUT,
        AFTER_VERIFY_DEST,
        BEFORE_METADATA_PERSIST,
        AFTER_SOURCE_DELETE
    }

    /**
     * Test-only deterministic failure injection.
     */
    public interface FailureInjector {

        void maybeFail(FailureStep step, String objectName, String chunkId) throws IOException;

        static FailureInjector noop() {
            return (step, objectName, chunkId) -> { };
        }
    }

    public record RebalanceResult(int chunksScanned, int chunksMoved, int chunksSkipped, int chunksFailed) {}
}
