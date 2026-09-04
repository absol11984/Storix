package com.storix.storage;

import java.nio.ByteBuffer;

/**
 * Immutable request parsed from the wire format.
 */
public record ChunkRequest(byte opcode, String chunkId, byte[] data) {

    /**
     * Parses a request from a ByteBuffer.
     * The buffer must already contain the complete request.
     */
    public static ChunkRequest fromBuffer(ByteBuffer buffer) {
        byte opcode = buffer.get();
        String chunkId = new String(Protocol.readBytes(buffer));

        byte[] data = new byte[0];
        if (opcode == Protocol.PUT_CHUNK) {
            data = Protocol.readBytes(buffer);
        }

        return new ChunkRequest(opcode, chunkId, data);
    }

    /**
     * Calculates the total size of this request when encoded.
     */
    public int encodedSize() {
        int size = 1; // opcode
        size += Protocol.encodedSize(chunkId.getBytes());
        if (opcode == Protocol.PUT_CHUNK) {
            size += Protocol.encodedSize(data);
        }
        return size;
    }
}
