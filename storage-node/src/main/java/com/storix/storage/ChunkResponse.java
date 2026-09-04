package com.storix.storage;

import java.nio.ByteBuffer;

/**
 * Immutable response to send back to the client.
 */
public record ChunkResponse(byte status, byte[] data) {

    /**
     * Creates a successful response with data.
     */
    public static ChunkResponse ok(byte[] data) {
        return new ChunkResponse(Protocol.OK, data);
    }

    /**
     * Creates an error response with a message.
     */
    public static ChunkResponse error(String message) {
        return new ChunkResponse(Protocol.ERROR, message.getBytes());
    }

    /**
     * Writes this response to a ByteBuffer.
     */
    public void writeTo(ByteBuffer buffer) {
        buffer.put(status);
        Protocol.writeBytes(buffer, data);
    }

    /**
     * Calculates the total size of this response when encoded.
     */
    public int encodedSize() {
        return 1 + Protocol.encodedSize(data);
    }
}
