package com.storix.metadata;

import java.nio.ByteBuffer;

/**
 * Protocol constants and utilities for metadata server communication.
 *
 * Wire format:
 * Request:  [opcode:1][payloadLen:4][payload:N]
 * Response: [status:1][payloadLen:4][payload:N]
 */
public final class MetadataProtocol {

    // Prompt 1 opcodes
    public static final byte CREATE_OBJECT = 1;
    public static final byte GET_OBJECT = 2;
    public static final byte UPDATE_OBJECT = 3;
    public static final byte DELETE_OBJECT = 4;
    public static final byte LIST_OBJECTS = 5;

    // Prompt 2 opcodes — node registry and cluster management
    public static final byte REGISTER_NODE = 10;
    public static final byte HEARTBEAT = 11;
    public static final byte GET_NODES = 12;
    public static final byte GET_CLUSTER_STATUS = 13;
    public static final byte GET_PLACEMENT = 14;
    public static final byte REPAIR = 15;

    // Status codes
    public static final byte OK = 0;
    public static final byte ERROR = 1;
    public static final byte NOT_FOUND = 2;
    public static final byte NOT_LEADER = 3;

    private MetadataProtocol() {}

    /**
     * Reads a length-prefixed byte array from the buffer.
     */
    public static byte[] readBytes(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length < 0) {
            throw new IllegalArgumentException("Negative length: " + length);
        }
        if (length == 0) {
            return new byte[0];
        }
        byte[] data = new byte[length];
        buffer.get(data);
        return data;
    }

    /**
     * Writes a length-prefixed byte array to the buffer.
     */
    public static void writeBytes(ByteBuffer buffer, byte[] data) {
        if (data == null) {
            buffer.putInt(0);
        } else {
            buffer.putInt(data.length);
            if (data.length > 0) {
                buffer.put(data);
            }
        }
    }

    /**
     * Calculates the encoded size of a byte array (4 bytes for length + data).
     */
    public static int encodedSize(byte[] data) {
        return 4 + (data != null ? data.length : 0);
    }
}
