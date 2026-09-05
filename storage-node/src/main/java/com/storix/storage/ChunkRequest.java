package com.storix.storage;

import java.nio.ByteBuffer;

/**
 * Immutable request parsed from the wire format.
 */
public record ChunkRequest(byte opcode, String chunkId, byte[] data, String expectedChecksum) {

    /**
     * Creates a request without checksum.
     */
    public static ChunkRequest simple(byte opcode, String chunkId, byte[] data) {
        return new ChunkRequest(opcode, chunkId, data, null);
    }

    /**
     * Parses a request from a ByteBuffer.
     * The buffer must already contain the complete request.
     */
    public static ChunkRequest fromBuffer(ByteBuffer buffer) {
        byte opcode = buffer.get();
        String chunkId = new String(Protocol.readBytes(buffer));

        byte[] data = new byte[0];
        String checksum = null;

        if (opcode == Protocol.PUT_CHUNK) {
            data = Protocol.readBytes(buffer);
            // Optional checksum after data
            if (buffer.hasRemaining()) {
                try {
                    checksum = new String(Protocol.readBytes(buffer));
                } catch (Exception e) {
                    // No checksum present
                }
            }
        } else if (opcode == Protocol.GET_CHUNK || opcode == Protocol.VERIFY_CHUNK) {
            // Optional checksum for verification
            if (buffer.hasRemaining()) {
                try {
                    checksum = new String(Protocol.readBytes(buffer));
                } catch (Exception e) {
                    // No checksum present
                }
            }
        }

        return new ChunkRequest(opcode, chunkId, data, checksum);
    }

    /**
     * Calculates the total size of this request when encoded.
     */
    public int encodedSize() {
        int size = 1; // opcode
        size += Protocol.encodedSize(chunkId.getBytes());
        if (opcode == Protocol.PUT_CHUNK) {
            size += Protocol.encodedSize(data);
            if (expectedChecksum != null && !expectedChecksum.isEmpty()) {
                size += Protocol.encodedSize(expectedChecksum.getBytes());
            }
        } else if (expectedChecksum != null && !expectedChecksum.isEmpty()) {
            size += Protocol.encodedSize(expectedChecksum.getBytes());
        }
        return size;
    }
}
