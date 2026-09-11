package com.storix.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.storix.metadata.raft.*;
import com.storix.metadata.wal.DeduplicationCache;
import com.storix.metadata.wal.RequestEnvelope;

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
 * Supports request deduplication for idempotent operations using clientId + requestId.
 */
public class MetadataHandler {

    private final MetadataStore store;
    private final NodeRegistry nodeRegistry;
    private final PlacementManager placementManager;
    private final RepairManager repairManager;
    private final RaftNode raftNode;
    private final MetadataStateMachine stateMachine;
    private final int port;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Request deduplication cache (TTL 1 hour)
    private final DeduplicationCache deduplicationCache = new DeduplicationCache();

    public MetadataHandler(MetadataStore store, NodeRegistry nodeRegistry,
                          PlacementManager placementManager, RepairManager repairManager) {
        this(store, nodeRegistry, placementManager, repairManager, null, null, 0);
    }

    public MetadataHandler(MetadataStore store, NodeRegistry nodeRegistry,
                          PlacementManager placementManager, RepairManager repairManager,
                          int port) {
        this(store, nodeRegistry, placementManager, repairManager, null, null, port);
    }

    public MetadataHandler(MetadataStore store, NodeRegistry nodeRegistry,
                          PlacementManager placementManager, RepairManager repairManager,
                          RaftNode raftNode, MetadataStateMachine stateMachine, int port) {
        this.store = store;
        this.nodeRegistry = nodeRegistry;
        this.placementManager = placementManager;
        this.repairManager = repairManager;
        this.raftNode = raftNode;
        this.stateMachine = stateMachine;
        this.port = port;
    }

    /**
     * Backward-compatible constructor without port.
     */
    public MetadataHandler(MetadataStore store, NodeRegistry nodeRegistry,
                          PlacementManager placementManager, RepairManager repairManager,
                          RaftNode raftNode, MetadataStateMachine stateMachine) {
        this(store, nodeRegistry, placementManager, repairManager, raftNode, stateMachine, 0);
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
                // Flush to ensure client receives complete response before any close
                if (channel.isOpen()) {
                    // Socket write flush is implicit; no extra flush needed for blocking channel
                }
            } catch (IOException e) {
                // Connection error - break out of loop
                break;
            }
        }
    }

    // Normalized request context produced by central unwrapping
    private record RequestContext(byte opcode, byte[] innerPayload, String clientId, String requestId, String operation) {}

    /**
     * Parses the request envelope ONCE at the beginning of request processing.
     */
    private static RequestContext unwrapRequest(byte opcode, byte[] payload) {
        RequestEnvelope envelope = parseRequestEnvelopeStatic(payload);
        String clientId = envelope != null ? envelope.clientId() : null;
        String requestId = envelope != null ? envelope.requestId() : null;
        String operation = envelope != null ? envelope.operation() : null;
        byte[] innerPayload = envelope != null ? envelope.payload() : payload;
        return new RequestContext(opcode, innerPayload, clientId, requestId, operation);
    }

    private static RequestEnvelope parseRequestEnvelopeStatic(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return null;
        }
        try {
            // Try to parse as JSON envelope
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            Map<String, Object> json = mapper.readValue(payload, Map.class);
            if (json.containsKey("clientId") && json.containsKey("requestId")) {
                String clientId = String.valueOf(json.get("clientId"));
                String requestId = String.valueOf(json.get("requestId"));
                String operation = json.containsKey("operation") ? String.valueOf(json.get("operation")) : null;

                byte[] innerPayload = payload;
                if (json.containsKey("payload")) {
                    Object payloadObj = json.get("payload");
                    if (payloadObj instanceof String) {
                        innerPayload = ((String) payloadObj).getBytes();
                    } else if (payloadObj instanceof Map) {
                        innerPayload = mapper.writeValueAsBytes(payloadObj);
                    } else if (payloadObj instanceof java.util.LinkedHashMap || payloadObj instanceof java.util.HashMap) {
                        innerPayload = mapper.writeValueAsBytes(payloadObj);
                    } else {
                        // If payload is a JSON object embedded as map, serialize it back to JSON bytes
                        innerPayload = mapper.writeValueAsBytes(payloadObj);
                    }
                }
                return RequestEnvelope.of(clientId, requestId, operation, innerPayload);
            }
        } catch (Exception e) {
            // Not a JSON envelope, treat as raw payload
        }
        return null;
    }

    /**
     * Processes a request and returns the response buffer.
     */
    private ByteBuffer processRequest(byte opcode, byte[] payload) throws IOException {
        RequestContext ctx = unwrapRequest(opcode, payload);
        try {
            return switch (opcode) {
                case MetadataProtocol.CREATE_OBJECT -> handleCreateObject(ctx);
                case MetadataProtocol.GET_OBJECT -> handleGetObject(ctx);
                case MetadataProtocol.UPDATE_OBJECT -> handleUpdateObject(ctx);
                case MetadataProtocol.DELETE_OBJECT -> handleDeleteObject(ctx);
                case MetadataProtocol.LIST_OBJECTS -> handleListObjects(ctx);

                // Node Registry commands
                case MetadataProtocol.REGISTER_NODE -> handleRegisterNode(ctx);
                case MetadataProtocol.HEARTBEAT -> handleHeartbeat(ctx);
                case MetadataProtocol.GET_NODES -> handleGetNodes(ctx);
                case MetadataProtocol.GET_PLACEMENT -> handleGetPlacement(ctx);
                case MetadataProtocol.GET_CLUSTER_STATUS -> handleGetClusterStatus(ctx);
                case MetadataProtocol.REPAIR -> handleRepair(ctx);

                default -> createErrorResponse(MetadataProtocol.ERROR, "Unknown opcode: " + opcode);
            };
        } catch (Exception e) {
            return createErrorResponse(MetadataProtocol.ERROR, e.getMessage());
        }
    }

    private ByteBuffer handleCreateObject(RequestContext ctx) throws IOException {
        // Check if we're the leader (if Raft is enabled)
        if (raftNode != null && !raftNode.isLeader()) {
            return createNotLeaderResponse();
        }
        byte[] payload = ctx.innerPayload;
        String clientId = ctx.clientId;
        String requestId = ctx.requestId;
        ObjectMetadata metadata = objectMapper.readValue(payload, ObjectMetadata.class);

        // Use clientId + requestId for true deduplication
        String dedupKey = getRequestDedupKey(clientId, requestId);

        // Check deduplication cache
        Optional<DeduplicationCache.CachedResult> cached = deduplicationCache.get(dedupKey);
        if (cached.isPresent()) {
            // Return cached result for idempotent operation
            System.out.println("[HANDLER] Duplicate request detected: " + dedupKey);
            return createSuccessResponse(cached.get().responseData());
        }

        if (raftNode != null && stateMachine != null) {
            // Submit to Raft for replication with client identity
            byte[] data = objectMapper.writeValueAsBytes(metadata);
            LogEntry entry = LogEntry.create(raftNode.getCurrentTerm(), LogEntry.OpType.CREATE_OBJECT, data, clientId, requestId);

            if (!raftNode.submit(entry)) {
                // Do NOT cache failure - allow retry to attempt again
                return createErrorResponse(MetadataProtocol.ERROR, "Failed to submit to Raft");
            }
            // Cache successful result ONLY after successful submit
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

    private ByteBuffer handleGetObject(RequestContext ctx) throws IOException {
        byte[] payload = ctx.innerPayload;
        System.out.println("[DEBUG-GET_OBJECT] payload=" + new String(payload));
        Map<String, String> request = objectMapper.readValue(payload, Map.class);
        String objectName = request.get("objectName");
        if (objectName == null) {
            return createErrorResponse(MetadataProtocol.ERROR, "Missing objectName. Received requests keys: " + request.keySet());
        }

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

    private ByteBuffer handleUpdateObject(RequestContext ctx) throws IOException {
        byte[] payload = ctx.innerPayload;
        String clientId = ctx.clientId;
        String requestId = ctx.requestId;
        // Check if we're the leader (if Raft is enabled)
        if (raftNode != null && !raftNode.isLeader()) {
            return createNotLeaderResponse();
        }
        ObjectMetadata metadata = objectMapper.readValue(payload, ObjectMetadata.class);

        // Use clientId + requestId for true deduplication
        String dedupKey = getRequestDedupKey(clientId, requestId);

        // Check deduplication cache
        Optional<DeduplicationCache.CachedResult> cached = deduplicationCache.get(dedupKey);
        if (cached.isPresent()) {
            System.out.println("[HANDLER] Duplicate request detected: " + dedupKey);
            return createSuccessResponse(cached.get().responseData());
        }

        if (raftNode != null && stateMachine != null) {
            // Submit to Raft for replication with client identity
            byte[] data = objectMapper.writeValueAsBytes(metadata);
            LogEntry entry = LogEntry.create(raftNode.getCurrentTerm(), LogEntry.OpType.UPDATE_OBJECT, data, clientId, requestId);

            if (!raftNode.submit(entry)) {
                // Do NOT cache failure - allow retry to attempt again
                return createErrorResponse(MetadataProtocol.ERROR, "Failed to submit to Raft");
            }
            // Cache successful result ONLY after successful submit
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

    private ByteBuffer handleDeleteObject(RequestContext ctx) throws IOException {
        byte[] payload = ctx.innerPayload;
        String clientId = ctx.clientId;
        String requestId = ctx.requestId;
        // Check if we're the leader (if Raft is enabled)
        if (raftNode != null && !raftNode.isLeader()) {
            return createNotLeaderResponse();
        }
        Map<String, String> request = objectMapper.readValue(payload, Map.class);
        String objectName = request.get("objectName");

        // Use clientId + requestId for true deduplication
        String dedupKey = getRequestDedupKey(clientId, requestId);

        // Check deduplication cache
        Optional<DeduplicationCache.CachedResult> cached = deduplicationCache.get(dedupKey);
        if (cached.isPresent()) {
            System.out.println("[HANDLER] Duplicate request detected: " + dedupKey);
            return createSuccessResponse(cached.get().responseData());
        }

        if (raftNode != null && stateMachine != null) {
            // Submit to Raft for replication with client identity
            LogEntry entry = LogEntry.create(raftNode.getCurrentTerm(), LogEntry.OpType.DELETE_OBJECT, objectName.getBytes(), clientId, requestId);

            if (!raftNode.submit(entry)) {
                // Do NOT cache failure - allow retry to attempt again
                return createErrorResponse(MetadataProtocol.ERROR, "Failed to submit to Raft");
            }
            // Cache successful result ONLY after successful submit
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

    private ByteBuffer handleListObjects(RequestContext ctx) throws IOException {
        String[] objects = store.listObjects().toArray(new String[0]);
        byte[] data = objectMapper.writeValueAsBytes(objects);
        return createSuccessResponse(data);
    }

    // Node registry handlers

    private ByteBuffer handleRegisterNode(RequestContext ctx) throws IOException {
        byte[] payload = ctx.innerPayload;
        NodeInfo nodeInfo = objectMapper.readValue(payload, NodeInfo.class);
        nodeRegistry.registerNode(
            nodeInfo.getNodeId(),
            nodeInfo.getHost(),
            nodeInfo.getPort(),
            nodeInfo.getTotalCapacityBytes(),
            nodeInfo.getUsedCapacityBytes());
        return createSuccessResponse(new byte[0]);
    }

    private ByteBuffer handleHeartbeat(RequestContext ctx) throws IOException {
        byte[] payload = ctx.innerPayload;
        Map<String, Object> request = objectMapper.readValue(payload, Map.class);
        String nodeId = request.get("nodeId") != null ? String.valueOf(request.get("nodeId")) : null;

        if (nodeId == null || nodeId.isEmpty()) {
            return createErrorResponse(MetadataProtocol.ERROR, "Missing nodeId in heartbeat request");
        }

        Object totalCapObj = request.get("totalCapacityBytes");
        Object usedCapObj = request.get("usedCapacityBytes");
        long totalCap = (totalCapObj instanceof Number) ? ((Number) totalCapObj).longValue() : -1;
        long usedCap = (usedCapObj instanceof Number) ? ((Number) usedCapObj).longValue() : 0;

        boolean found = nodeRegistry.heartbeat(nodeId, totalCap, usedCap);
        if (found) {
            return createSuccessResponse(new byte[0]);
        } else {
            return createErrorResponse(MetadataProtocol.NOT_FOUND, "Node not registered");
        }
    }

    private ByteBuffer handleGetNodes(RequestContext ctx) throws IOException {
        List<NodeInfo> healthyNodes = nodeRegistry.getHealthyNodes();
        byte[] data = objectMapper.writeValueAsBytes(healthyNodes);
        return createSuccessResponse(data);
    }

    private ByteBuffer handleGetPlacement(RequestContext ctx) throws IOException {
        byte[] payload = ctx.innerPayload;
        String payloadStr = new String(payload);
        System.out.println("[DEBUG-GET_PLACEMENT] payload=" + payloadStr);
        Map<String, Object> request = objectMapper.readValue(payload, Map.class); // Use Object to be safe
        Object chunkIndexObj = request.get("chunkIndex");
        System.out.println("[DEBUG-GET_PLACEMENT] chunkIndexObj=" + chunkIndexObj + " type=" + (chunkIndexObj != null ? chunkIndexObj.getClass().getName() : "null"));
        Integer chunkIndex = null;
        if (chunkIndexObj instanceof Number) {
            chunkIndex = ((Number) chunkIndexObj).intValue();
        } else if (chunkIndexObj instanceof String) {
            chunkIndex = Integer.parseInt((String) chunkIndexObj);
        }
        if (chunkIndex == null) {
            return createErrorResponse(MetadataProtocol.ERROR, "Missing chunkIndex. Received: " + request);
        }

        Object chunkSizeObj = request.get("chunkSizeBytes");
        Long chunkSizeBytes = null;
        if (chunkSizeObj instanceof Number) {
            chunkSizeBytes = ((Number) chunkSizeObj).longValue();
        } else if (chunkSizeObj instanceof String s) {
            try {
                chunkSizeBytes = Long.parseLong(s);
            } catch (NumberFormatException ignored) {
                // null means unknown, placement uses unlimited-capacity compatibility
            }
        }

        List<NodeInfo> selectedNodes = placementManager.selectNodes(chunkIndex, chunkSizeBytes);
        byte[] data = objectMapper.writeValueAsBytes(selectedNodes);
        return createSuccessResponse(data);
    }

    private ByteBuffer handleGetClusterStatus(RequestContext ctx) throws IOException {
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

    private ByteBuffer handleRepair(RequestContext ctx) throws IOException {
        RepairManager.RepairResult result = repairManager.repairAll();
        byte[] data = objectMapper.writeValueAsBytes(result);
        return createSuccessResponse(data);
    }

    /**
     * Creates a NOT_LEADER response with leader info.
     * Returns the CLIENT-FACING port, not the Raft port.
     */
    private ByteBuffer createNotLeaderResponse() {
        Map<String, String> leaderInfo = new HashMap<>();
        leaderInfo.put("leaderHost", "127.0.0.1");

        if (raftNode != null) {
            raftNode.getLeader().ifPresent(leader -> {
                leaderInfo.put("leaderHost", leader.host());
                leaderInfo.put("leaderId", leader.nodeId());
                // Use client-facing port (deduct 10000 from Raft port)
                leaderInfo.put("leaderPort", String.valueOf(leader.port() - 10000));
            });
            leaderInfo.put("raftState", raftNode.getState().name());
            leaderInfo.put("term", String.valueOf(raftNode.getCurrentTerm()));
        } else {
            leaderInfo.put("leaderPort", String.valueOf(port));
        }

        try {
            byte[] data = objectMapper.writeValueAsBytes(leaderInfo);
            return createErrorResponse(MetadataProtocol.NOT_LEADER, new String(data));
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
        if (message == null) message = "Unknown error (null message)";
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

    /**
     * Parses the request envelope from payload.
     * The payload may be a request envelope JSON or raw operation data (backward compatible).
     */
    private RequestEnvelope parseRequestEnvelope(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return null;
        }
        try {
            // Try to parse as JSON envelope
            Map<String, Object> json = objectMapper.readValue(payload, Map.class);
            if (json.containsKey("clientId") && json.containsKey("requestId")) {
                String clientId = String.valueOf(json.get("clientId"));
                String requestId = String.valueOf(json.get("requestId"));
                String operation = json.containsKey("operation") ? String.valueOf(json.get("operation")) : null;

                // Extract inner payload if present
                byte[] innerPayload = payload;
                if (json.containsKey("payload")) {
                    Object payloadObj = json.get("payload");
                    if (payloadObj instanceof String) {
                        innerPayload = ((String) payloadObj).getBytes();
                    } else if (payloadObj instanceof Map) {
                        innerPayload = objectMapper.writeValueAsBytes(payloadObj);
                    }
                }

                return RequestEnvelope.of(clientId, requestId, operation, innerPayload);
            }
        } catch (Exception e) {
            // Not a JSON envelope, treat as raw payload
        }
        return null;
    }

    /**
     * Generates a deduplication key from clientId and requestId.
     */
    private String getRequestDedupKey(String clientId, String requestId) {
        if (clientId != null && requestId != null) {
            return clientId + ":" + requestId;
        }
        return "unknown:" + System.identityHashCode(this);
    }

    /**
     * Gets the deduplication cache for testing.
     */
    public DeduplicationCache getDeduplicationCache() {
        return deduplicationCache;
    }
}
