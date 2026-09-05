package com.storix.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Handles a single client connection.
 * Processes multiple requests on the same connection in a loop.
 */
public class ChunkHandler {

    private final ChunkStorage storage;

    public ChunkHandler(ChunkStorage storage) {
        this.storage = storage;
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
                    // Malformed request
                    sendError(channel, "Malformed request: " + e.getMessage());
                    break;
                }
            } catch (IOException e) {
                // Connection error - break out of loop
                break;
            }
        }
    }

    /**
     * Processes a parsed request and returns the appropriate response.
     */
    private ChunkResponse processRequest(ChunkRequest request) {
        try {
            return switch (request.opcode()) {
                case Protocol.PUT_CHUNK -> {
                    storage.putChunk(request.chunkId(), request.data());
                    yield ChunkResponse.ok(new byte[0]);
                }
                case Protocol.GET_CHUNK -> {
                    byte[] data = storage.getChunk(request.chunkId());

                    // Verify checksum if provided
                    String expectedChecksum = request.expectedChecksum();
                    if (expectedChecksum != null && !expectedChecksum.isEmpty()) {
                        if (!storage.verifyIntegrity(request.chunkId(), expectedChecksum)) {
                            // Chunk is corrupted - quarantine it
                            storage.quarantineCorruptChunk(request.chunkId());
                            yield ChunkResponse.error("CHUNK_CORRUPTED: " + request.chunkId());
                        }
                    }

                    yield ChunkResponse.ok(data);
                }
                case Protocol.DELETE_CHUNK -> {
                    storage.deleteChunk(request.chunkId());
                    yield ChunkResponse.ok(new byte[0]);
                }
                case Protocol.VERIFY_CHUNK -> {
                    // Explicit integrity check
                    String expectedChecksum = request.expectedChecksum();
                    if (expectedChecksum == null || expectedChecksum.isEmpty()) {
                        yield ChunkResponse.error("VERIFY_CHUNK requires checksum");
                    }
                    if (storage.verifyIntegrity(request.chunkId(), expectedChecksum)) {
                        yield ChunkResponse.ok(new byte[0]);
                    } else {
                        storage.quarantineCorruptChunk(request.chunkId());
                        yield ChunkResponse.error("CHUNK_CORRUPTED: " + request.chunkId());
                    }
                }
                default -> ChunkResponse.error("Unknown opcode: " + request.opcode());
            };
        } catch (IOException e) {
            return ChunkResponse.error(e.getMessage());
        }
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
