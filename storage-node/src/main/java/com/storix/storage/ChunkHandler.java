package com.storix.storage;

import com.storix.storage.observability.StorageMetrics;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Handles a single client connection.
 * Processes multiple requests on the same connection in a loop.
 */
public class ChunkHandler {

    private final String nodeId;
    private final ChunkStorage storage;
    private final StorageMetrics metrics;

    /**
     * Legacy constructor (nodeId unknown). Used in existing code paths.
     */
    public ChunkHandler(ChunkStorage storage) {
        this(null, storage, new StorageMetrics());
    }

    /**
     * Creates handler with node identity.
     */
    public ChunkHandler(String nodeId, ChunkStorage storage) {
        this(nodeId, storage, new StorageMetrics());
    }

    /**
     * Creates handler with node identity and optional telemetry.
     */
    public ChunkHandler(String nodeId, ChunkStorage storage, StorageMetrics metrics) {
        this.nodeId = nodeId;
        this.storage = storage;
        this.metrics = metrics != null ? metrics : new StorageMetrics();
    }

    /**
     * Handles a client connection. Processes requests in a loop until the connection is closed.
     */
    public void handle(SocketChannel channel) throws IOException {
        metrics.connectionOpened();
        try {
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

                    if (requestLength <= 0 || requestLength > 10 * 1024 * 1024) { // Max 10MB
                        sendError(channel, "Invalid request length");
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
                    try {
                        ChunkRequest request = ChunkRequest.fromBuffer(requestBuffer);
                        ChunkResponse response = processRequest(request);
                        sendResponse(channel, response);
                    } catch (IllegalArgumentException e) {
                        // Malformed request; do not leak exception details.
                        sendError(channel, "Malformed request");
                        break;
                    }
                } catch (IOException e) {
                    // Connection error - break out of loop
                    break;
                }
            }
        } finally {
            metrics.connectionClosed();
        }
    }

    /**
     * Processes a parsed request and returns the appropriate response.
     */
    private ChunkResponse processRequest(ChunkRequest request) {
        return switch (request.opcode()) {
            case Protocol.PUT_CHUNK -> handlePutChunk(request);
            case Protocol.GET_CHUNK -> handleGetChunk(request);
            case Protocol.DELETE_CHUNK -> handleDeleteChunk(request);
            case Protocol.VERIFY_CHUNK -> handleVerifyChunk(request);
            case Protocol.LIST_NODE_STATE -> handleListNodeState();
            default -> ChunkResponse.error("Unknown opcode: " + request.opcode());
        };
    }

    private ChunkResponse handlePutChunk(ChunkRequest request) {
        try {
            storage.putChunk(request.chunkId(), request.data());
            metrics.recordChunkWriteSuccess();
            return ChunkResponse.ok(new byte[0]);
        } catch (IOException e) {
            metrics.recordChunkWriteFailure();
            return ChunkResponse.error("Internal error");
        }
    }

    private ChunkResponse handleGetChunk(ChunkRequest request) {
        try {
            byte[] data = storage.getChunk(request.chunkId());

            // Verify checksum if provided.
            String expectedChecksum = request.expectedChecksum();
            if (expectedChecksum != null && !expectedChecksum.isEmpty()) {
                boolean matches = storage.verifyIntegrity(request.chunkId(), expectedChecksum);
                if (!matches) {
                    // Chunk is corrupted - quarantine it.
                    try {
                        storage.quarantineCorruptChunk(request.chunkId());
                    } catch (IOException quarantineFailure) {
                        metrics.recordChunkReadFailure();
                        return ChunkResponse.error("Internal error");
                    }

                    metrics.recordChecksumFailure();
                    metrics.recordChunkReadFailure();
                    return ChunkResponse.error("CHUNK_CORRUPTED: " + request.chunkId());
                }
            }

            metrics.recordChunkReadSuccess();
            return ChunkResponse.ok(data);
        } catch (IOException e) {
            metrics.recordChunkReadFailure();
            return ChunkResponse.error("Internal error");
        }
    }

    private ChunkResponse handleDeleteChunk(ChunkRequest request) {
        try {
            storage.deleteChunk(request.chunkId());
            return ChunkResponse.ok(new byte[0]);
        } catch (IOException e) {
            return ChunkResponse.error("Internal error");
        }
    }

    private ChunkResponse handleVerifyChunk(ChunkRequest request) {
        try {
            // Explicit integrity check.
            String expectedChecksum = request.expectedChecksum();
            if (expectedChecksum == null || expectedChecksum.isEmpty()) {
                return ChunkResponse.error("VERIFY_CHUNK requires checksum");
            }

            if (storage.verifyIntegrity(request.chunkId(), expectedChecksum)) {
                metrics.recordChunkReadSuccess();
                return ChunkResponse.ok(new byte[0]);
            }

            // Verification failed; quarantine and record checksum failure.
            try {
                storage.quarantineCorruptChunk(request.chunkId());
            } catch (IOException quarantineFailure) {
                metrics.recordChunkReadFailure();
                return ChunkResponse.error("Internal error");
            }

            metrics.recordChecksumFailure();
            metrics.recordChunkReadFailure();
            return ChunkResponse.error("CHUNK_CORRUPTED: " + request.chunkId());
        } catch (IOException e) {
            metrics.recordChunkReadFailure();
            return ChunkResponse.error("Internal error");
        }
    }

    private ChunkResponse handleListNodeState() {
        try {
            byte[] payload = buildNodeStatePayload();
            return ChunkResponse.ok(payload);
        } catch (IOException e) {
            return ChunkResponse.error("Internal error");
        }
    }

    private byte[] buildNodeStatePayload() throws IOException {
        // Binary payload layout:
        // [nodeIdLen:4][nodeId:N]
        // [totalCapacity:8][usedCapacity:8]
        // [chunkCount:4]
        // For each chunk: [chunkIdLen:4][chunkId:N]
        String effectiveNodeId = (nodeId != null) ? nodeId : "unknown";

        byte[] nodeIdBytes = effectiveNodeId.getBytes(StandardCharsets.UTF_8);
        List<String> chunkIds = storage.listStoredChunkIds();

        long total = storage.getTotalCapacityBytes();
        long used = storage.getUsedCapacityBytes();

        int totalLen = 4 + nodeIdBytes.length + 8 + 8 + 4;
        for (String id : chunkIds) {
            byte[] b = id.getBytes(StandardCharsets.UTF_8);
            totalLen += 4 + b.length;
        }

        ByteBuffer buf = ByteBuffer.allocate(totalLen);
        buf.putInt(nodeIdBytes.length);
        buf.put(nodeIdBytes);
        buf.putLong(total);
        buf.putLong(used);
        buf.putInt(chunkIds.size());
        for (String id : chunkIds) {
            byte[] b = id.getBytes(StandardCharsets.UTF_8);
            buf.putInt(b.length);
            buf.put(b);
        }
        return buf.array();
    }

    /**
     * Sends an error response.
     */
    private void sendError(SocketChannel channel, String message) throws IOException {
        sendResponse(channel, ChunkResponse.error(message));
    }

    /**
     * Sends a response with length prefix.
     */
    private void sendResponse(SocketChannel channel, ChunkResponse response) throws IOException {
        int totalSize = 4 + response.encodedSize();
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(response.encodedSize());
        response.writeTo(buffer);
        buffer.flip();
        writeFully(channel, buffer);
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

    /**
     * Writes all remaining bytes in the buffer to the channel.
     */
    private void writeFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }
}
