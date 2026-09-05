package com.storix.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.storix.metadata.raft.*;
import com.storix.metadata.wal.DeduplicationCache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Handles metadata client requests.
 * Processes multiple requests on the same connection in a loop.
 * Supports request deduplication for idempotent operations.
 */
public class MetadataHandler {

    private final MetadataStore store;
    private final NodeRegistry nodeRegistry;
    private final PlacementManager placementManager;
    private final RepairManager repairManager;
    private final RaftNode raftNode;
    private final MetadataStateMachine stateMachine;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Request deduplication cache (TTL 1 hour)
    private final DeduplicationCache deduplicationCache = new DeduplicationCache();

    // Deduplication key prefix per operation type
    private static final String CREATE_PREFIX = "CREATE:";
    private static final String UPDATE_PREFIX = "UPDATE:";
    private static final String DELETE_PREFIX = "DELETE:";

    public MetadataHandler(MetadataStore store, NodeRegistry nodeRegistry,
                          PlacementManager placementManager, RepairManager repairManager) {
        this(store, nodeRegistry, placementManager, repairManager, null, null);
    }

    public MetadataHandler(MetadataStore store, NodeRegistry nodeRegistry,
                          PlacementManager placementManager, RepairManager repairManager,
                          RaftNode raftNode, MetadataStateMachine stateMachine) {
        this.store = store;
        this.nodeRegistry = nodeRegistry;
        this.placementManager = placementManager;
        this.repairManager = repairManager;
        this.raftNode = raftNode;
        this.stateMachine = stateMachine;
    }

    /**
     * Handles a client connection. Processes requests in a loop until the connection is closed.
     */
    public void handle(SocketChannel channel) throws IOException {
        // Process multiple requests on the same connection
        while (channel.isOpen()) {
            try {
                // Read request length (4 bytes)
                ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
                if (!readFully(channel, lengthBuffer)) {
                    // Client closed connection gracefully
                    break;
                }
                lengthBuffer.flip();
                int requestLength = lengthBuffer.getInt();

                if (requestLength <= 0 || requestLength > 5 * 1024 * 1024) { // Max 5MB
                    sendError(channel, MetadataProtocol.ERROR, "Invalid request length");
                    break;
                }

                // Read request body
                ByteBuffer requestBuffer = ByteBuffer.allocate(requestLength);
                if (!readFully(channel, requestBuffer)) {
                    // Incomplete request - client disconnected mid-request
                    break;
                }
                requestBuffer.flip();

                // Parse and process request
                byte opcode = requestBuffer.get();
                int payloadLength = requestBuffer.getInt();
                byte[] payload = new byte[payloadLength];
                if (payloadLength > 0) {
                    requestBuffer.get(payload);
                }

                ByteBuffer response = processRequest(opcode, payload);
                response.flip();
                writeFully(channel, response);
            } catch (IOException e) {
                // Connection error - break out of loop
                break;
            }
        }
    }

    /**
     * Processes a request and returns the response buffer.
     */
    private ByteBuffer processRequest(byte opcode, byte[] payload) throws IOException {
        try {
            return switch (opcode) {
                case MetadataProtocol.CREATE_OBJECT -> handleCreateObject(payload);
                case MetadataProtocol.GET_OBJECT -> handleGetObject(payload);
                case MetadataProtocol.UPDATE_OBJECT -> handleUpdateObject(payload);
                case MetadataProtocol.DELETE_OBJECT -> handleDeleteObject(payload);
                case MetadataProtocol.LIST_OBJECTS -> handleListObjects();

                // Node Registry commands
                case MetadataProtocol.REGISTER_NODE -> handleRegisterNode(payload);
                case MetadataProtocol.HEARTBEAT -> handleHeartbeat(payload);
                case MetadataProtocol.GET_NODES -> handleGetNodes();
                case MetadataProtocol.GET_PLACEMENT -> handleGetPlacement(payload);
                case MetadataProtocol.GET_CLUSTER_STATUS -> handleGetClusterStatus();
                case MetadataProtocol.REPAIR -> handleRepair();

                default -> createErrorResponse(MetadataProtocol.ERROR, "Unknown opcode: " + opcode);
            };
        } catch (Exception e) {
            return createErrorResponse(MetadataProtocol.ERROR, e.getMessage());
        }
    }

    private ByteBuffer handleCreateObject(byte[] payload) throws IOException {
        // Check if we're the leader (if Raft is enabled)
        if (raftNode != null && !raftNode.isLeader()) {
            return createNotLeaderResponse();
        }

        ObjectMetadata metadata = objectMapper.readValue(payload, ObjectMetadata.class);

        // Check deduplication cache
        String dedupKey = CREATE_PREFIX + metadata.getObjectName();
        Optional<DeduplicationCache.CachedResult> cached = deduplicationCache.get(dedupKey);
        if (cached.isPresent()) {
            // Return cached result for idempotent operation
            if (cached.get().success()) {
                return createSuccessResponse(cached.get().responseData());
            } else {
                return createErrorResponse(MetadataProtocol.ERROR, cached.get().errorMessage());
            }
        }

        if (raftNode != null && stateMachine != null) {
            // Submit to Raft for replication
            byte[] data = objectMapper.writeValueAsBytes(metadata);
            LogEntry entry = LogEntry.create(raftNode.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data);

            if (!raftNode.submit(entry)) {
                return createErrorResponse(MetadataProtocol.ERROR, "Failed to submit to Raft");
            }
            // Cache successful result
            deduplicationCache.put(dedupKey, DeduplicationCache.CachedResult.success(new byte[0]));
            return createSuccessResponse(new byte[0]);
        } else {
            // Direct write (single-node mode)
            store.createObject(metadata);
            // Cache successful result
            deduplicationCache.put(dedupKey, DeduplicationCache.CachedResult.success(new byte[0]));
            return createSuccessResponse(new byte[0]);
        }
    }

    private ByteBuffer handleGetObject(byte[] payload) throws IOException {
        Map<String, String> request = objectMapper.readValue(payload, Map.class);
        String objectName = request.get("objectName");

        return store.getObject(objectName)
                .map(meta -> {
                    try {
                        byte[] data = objectMapper.writeValueAsBytes(meta);
                        return createSuccessResponse(data);
                    } catch (IOException e) {
                        return createErrorResponse(MetadataProtocol.ERROR, "Serialization error");
                    }
                })
                .orElseGet(() -> createErrorResponse(MetadataProtocol.NOT_FOUND, "Object not found"));
    }

    private ByteBuffer handleUpdateObject(byte[] payload) throws IOException {
        // Check if we're the leader (if Raft is enabled)
        if (raftNode != null && !raftNode.isLeader()) {
            return createNotLeaderResponse();
        }

        ObjectMetadata metadata = objectMapper.readValue(payload, ObjectMetadata.class);

        // Check deduplication cache
        String dedupKey = UPDATE_PREFIX + metadata.getObjectName();
        Optional<DeduplicationCache.CachedResult> cached = deduplicationCache.get(dedupKey);
        if (cached.isPresent()) {
            if (cached.get().success()) {
                return createSuccessResponse(cached.get().responseData());
            } else {
                return createErrorResponse(MetadataProtocol.ERROR, cached.get().errorMessage());
            }
        }

        if (raftNode != null && stateMachine != null) {
            // Submit to Raft for replication
            byte[] data = objectMapper.writeValueAsBytes(metadata);
            LogEntry entry = LogEntry.create(raftNode.getCurrentTerm(), LogEntry.OpType.UPDATE_OBJECT, data);

            if (!raftNode.submit(entry)) {
                return createErrorResponse(MetadataProtocol.ERROR, "Failed to submit to Raft");
            }
            // Cache successful result
            deduplicationCache.put(dedupKey, DeduplicationCache.CachedResult.success(new byte[0]));
            return createSuccessResponse(new byte[0]);
        } else {
            // Direct write (single-node mode)
            store.updateObject(metadata);
            // Cache successful result
            deduplicationCache.put(dedupKey, DeduplicationCache.CachedResult.success(new byte[0]));
            return createSuccessResponse(new byte[0]);
        }
    }

    private ByteBuffer handleDeleteObject(byte[] payload) throws IOException {
        // Check if we're the leader (if Raft is enabled)
        if (raftNode != null && !raftNode.isLeader()) {
            return createNotLeaderResponse();
        }

        Map<String, String> request = objectMapper.readValue(payload, Map.class);
        String objectName = request.get("objectName");

        // Check deduplication cache
        String dedupKey = DELETE_PREFIX + objectName;
        Optional<DeduplicationCache.CachedResult> cached = deduplicationCache.get(dedupKey);
        if (cached.isPresent()) {
            if (cached.get().success()) {
                return createSuccessResponse(cached.get().responseData());
            } else {
                return createErrorResponse(MetadataProtocol.ERROR, cached.get().errorMessage());
            }
        }

        if (raftNode != null && stateMachine != null) {
            // Submit to Raft for replication
            LogEntry entry = LogEntry.create(raftNode.getCurrentTerm(), LogEntry.OpType.DELETE_OBJECT, objectName.getBytes());

            if (!raftNode.submit(entry)) {
                return createErrorResponse(MetadataProtocol.ERROR, "Failed to submit to Raft");
            }
            // Cache successful result
            deduplicationCache.put(dedupKey, DeduplicationCache.CachedResult.success(new byte[0]));
            return createSuccessResponse(new byte[0]);
        } else {
            // Direct write (single-node mode)
            boolean deleted = store.deleteObject(objectName);
            // Cache result (even if not found, for idempotency)
            deduplicationCache.put(dedupKey,
                deleted ? DeduplicationCache.CachedResult.success(new byte[0])
                        : DeduplicationCache.CachedResult.error("Object not found"));
            if (deleted) {
                return createSuccessResponse(new byte[0]);
            } else {
                return createErrorResponse(MetadataProtocol.NOT_FOUND, "Object not found");
            }
        }
    }

    private ByteBuffer handleListObjects() throws IOException {
        String[] objects = store.listObjects().toArray(new String[0]);
        byte[] data = objectMapper.writeValueAsBytes(objects);
        return createSuccessResponse(data);
    }

    // Node registry handlers

    private ByteBuffer handleRegisterNode(byte[] payload) throws IOException {
        NodeInfo nodeInfo = objectMapper.readValue(payload, NodeInfo.class);
        nodeRegistry.registerNode(nodeInfo.getNodeId(), nodeInfo.getHost(), nodeInfo.getPort());
        return createSuccessResponse(new byte[0]);
    }

    private ByteBuffer handleHeartbeat(byte[] payload) throws IOException {
        Map<String, String> request = objectMapper.readValue(payload, Map.class);
        String nodeId = request.get("nodeId");

        boolean found = nodeRegistry.heartbeat(nodeId);
        if (found) {
            return createSuccessResponse(new byte[0]);
        } else {
            return createErrorResponse(MetadataProtocol.NOT_FOUND, "Node not registered");
        }
    }

    private ByteBuffer handleGetNodes() throws IOException {
        List<NodeInfo> healthyNodes = nodeRegistry.getHealthyNodes();
        byte[] data = objectMapper.writeValueAsBytes(healthyNodes);
        return createSuccessResponse(data);
    }

    private ByteBuffer handleGetPlacement(byte[] payload) throws IOException {
        Map<String, Integer> request = objectMapper.readValue(payload, Map.class);
        Integer chunkIndex = request.get("chunkIndex");
        if (chunkIndex == null) {
            return createErrorResponse(MetadataProtocol.ERROR, "Missing chunkIndex");
        }

        List<NodeInfo> selectedNodes = placementManager.selectNodes(chunkIndex);
        byte[] data = objectMapper.writeValueAsBytes(selectedNodes);
        return createSuccessResponse(data);
    }

    private ByteBuffer handleGetClusterStatus() throws IOException {
        Collection<NodeInfo> allNodes = nodeRegistry.getAllNodes();

        // Calculate replication stats
        int objects = store.listObjects().size();
        int chunks = 0;
        int healthyChunks = 0;
        int degradedChunks = 0;

        for (String objectName : store.listObjects()) {
            ObjectMetadata metadata = store.getObject(objectName).orElse(null);
            if (metadata == null) continue;

            chunks += metadata.getChunkCount();

            for (ChunkInfo chunk : metadata.getChunks()) {
                long healthyReplicas = chunk.getReplicaNodeIds().stream()
                        .map(id -> nodeRegistry.getNode(id))
                        .filter(opt -> opt.isPresent() && opt.get().getStatus() == NodeStatus.ACTIVE)
                        .count();

                if (healthyReplicas >= placementManager.getReplicationFactor()) {
                    healthyChunks++;
                } else {
                    degradedChunks++;
                }
            }
        }

        Map<String, Object> status = new HashMap<>();
        status.put("nodes", allNodes);
        status.put("objects", objects);
        status.put("chunks", chunks);
        status.put("healthyNodes", nodeRegistry.healthyCount());
        status.put("totalNodes", nodeRegistry.size());
        status.put("healthyChunks", healthyChunks);
        status.put("degradedChunks", degradedChunks);
        status.put("replicationFactor", placementManager.getReplicationFactor());
        status.put("isLeader", raftNode != null ? raftNode.isLeader() : true);
        status.put("raftState", raftNode != null ? raftNode.getState().name() : "SINGLE");

        byte[] data = objectMapper.writeValueAsBytes(status);
        return createSuccessResponse(data);
    }

    private ByteBuffer handleRepair() throws IOException {
        RepairManager.RepairResult result = repairManager.repairAll();
        byte[] data = objectMapper.writeValueAsBytes(result);
        return createSuccessResponse(data);
    }

    /**
     * Creates a NOT_LEADER response with leader info.
     */
    private ByteBuffer createNotLeaderResponse() {
        Map<String, String> leaderInfo = new HashMap<>();

        if (raftNode != null) {
            raftNode.getLeader().ifPresent(leader -> {
                leaderInfo.put("leaderHost", leader.host());
                leaderInfo.put("leaderPort", String.valueOf(leader.port()));
            });
        }

        try {
            byte[] data = objectMapper.writeValueAsBytes(leaderInfo);
            return createErrorResponse(MetadataProtocol.NOT_LEADER, objectMapper.writeValueAsString(leaderInfo));
        } catch (IOException e) {
            return createErrorResponse(MetadataProtocol.NOT_LEADER, "{}");
        }
    }

    private ByteBuffer createSuccessResponse(byte[] data) {
        int size = 1 + MetadataProtocol.encodedSize(data);
        ByteBuffer buffer = ByteBuffer.allocate(4 + size);
        buffer.putInt(size);
        buffer.put(MetadataProtocol.OK);
        MetadataProtocol.writeBytes(buffer, data);
        return buffer;
    }

    private ByteBuffer createErrorResponse(byte status, String message) {
        byte[] msgBytes = message.getBytes();
        int size = 1 + MetadataProtocol.encodedSize(msgBytes);
        ByteBuffer buffer = ByteBuffer.allocate(4 + size);
        buffer.putInt(size);
        buffer.put(status);
        MetadataProtocol.writeBytes(buffer, msgBytes);
        return buffer;
    }

    private void sendError(SocketChannel channel, byte status, String message) throws IOException {
        ByteBuffer response = createErrorResponse(status, message);
        response.flip();
        writeFully(channel, response);
    }

    /**
     * Reads exactly the buffer's capacity bytes from the channel.
     * @return true if successful, false if connection closed
     */
    private boolean readFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read == -1) {
                // Connection closed
                return false;
            }
        }
        return true;
    }

    private void writeFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }
}
