package com.storix.storage;

import java.nio.ByteBuffer;

/**
 * Binary protocol constants and utilities.
 *
 * Wire format:
 * Request:  [opcode:1][chunkIdLen:4][chunkId:N][dataLen:4][data:N][checksumLen:4][checksum:N]
 * Response: [status:1][dataLen:4][data:N]
 */
public final class Protocol {

    // Opcodes
    public static final byte PUT_CHUNK = 1;
    public static final byte GET_CHUNK = 2;
    public static final byte DELETE_CHUNK = 3;
    public static final byte VERIFY_CHUNK = 4;

    // Status codes
    public static final byte OK = 0;
    public static final byte ERROR = 1;
    public static final byte CHUNK_CORRUPTED = 2;

    private Protocol() {}

    /**
     * Reads a length-prefixed byte array from the buffer.
     * Format: [length:4][bytes:N]
     */
    public static byte[] readBytes(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length < 0) {
            throw new IllegalArgumentException("Negative length: " + length);
        }
        byte[] data = new byte[length];
        buffer.get(data);
        return data;
    }

    /**
     * Writes a length-prefixed byte array to the buffer.
     * Format: [length:4][bytes:N]
     */
    public static void writeBytes(ByteBuffer buffer, byte[] data) {
        buffer.putInt(data.length);
        buffer.put(data);
    }

    /**
     * Calculates the encoded size of a byte array (4 bytes for length + data).
     */
    public static int encodedSize(byte[] data) {
        return 4 + data.length;
    }
}
