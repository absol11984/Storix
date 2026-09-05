package com.storix.metadata.raft;

/**
 * A log entry in the Raft replicated log.
 */
public record LogEntry(
    long term,           // Term number when entry was created
    long index,          // Unique monotonically increasing index
    long timestamp,      // Entry creation time (millis since epoch)
    OpType opType,       // Operation type
    byte[] data          // Serialized operation payload
) {
    public enum OpType {
        NO_OP(0),
        CREATE_OBJECT(1),
        UPDATE_OBJECT(2),
        DELETE_OBJECT(3),
        REGISTER_NODE(4),
        UNREGISTER_NODE(5),
        SNAPSHOT_RESTORE(6);  // Special op for snapshot installation

        private final byte code;

        OpType(int code) {
            this.code = (byte) code;
        }

        public byte code() {
            return code;
        }

        public static OpType fromCode(byte code) {
            for (OpType op : values()) {
                if (op.code == code) {
                    return op;
                }
            }
            throw new IllegalArgumentException("Unknown OpType code: " + code);
        }
    }

    /**
     * Creates a new log entry with auto-generated index.
     */
    public static LogEntry create(long term, OpType opType, byte[] data) {
        return new LogEntry(term, 0, System.currentTimeMillis(), opType, data);
    }

    /**
     * Returns a copy with the given index.
     */
    public LogEntry withIndex(long index) {
        return new LogEntry(term, index, timestamp, opType, data);
    }
}
