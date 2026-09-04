package com.storix.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles metadata client requests.
 * Processes multiple requests on the same connection in a loop.
 */
public class MetadataHandler {

    private final MetadataStore store;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public MetadataHandler(MetadataStore store) {
        this.store = store;
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

                if (requestLength <= 0 || requestLength > 1024 * 1024) { // Max 1MB
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
                default -> createErrorResponse(MetadataProtocol.ERROR, "Unknown opcode: " + opcode);
            };
        } catch (Exception e) {
            return createErrorResponse(MetadataProtocol.ERROR, e.getMessage());
        }
    }

    private ByteBuffer handleCreateObject(byte[] payload) throws IOException {
        ObjectMetadata metadata = objectMapper.readValue(payload, ObjectMetadata.class);
        store.createObject(metadata);
        return createSuccessResponse(new byte[0]);
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
        ObjectMetadata metadata = objectMapper.readValue(payload, ObjectMetadata.class);
        store.updateObject(metadata);
        return createSuccessResponse(new byte[0]);
    }

    private ByteBuffer handleDeleteObject(byte[] payload) throws IOException {
        Map<String, String> request = objectMapper.readValue(payload, Map.class);
        String objectName = request.get("objectName");

        boolean deleted = store.deleteObject(objectName);
        if (deleted) {
            return createSuccessResponse(new byte[0]);
        } else {
            return createErrorResponse(MetadataProtocol.NOT_FOUND, "Object not found");
        }
    }

    private ByteBuffer handleListObjects() throws IOException {
        String[] objects = store.listObjects().toArray(new String[0]);
        byte[] data = objectMapper.writeValueAsBytes(objects);
        return createSuccessResponse(data);
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
