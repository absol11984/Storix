package com.storix.metadata.raft;

import java.util.List;

/**
 * Detached, read-only operational snapshot of a Raft node.
 *
 * This is safe to serialize in STATUS without exposing live mutable internals.
 */
public record RaftOperationalSnapshot(
        String nodeId,
        long term,
        String role,
        String leaderId,
        long commitIndex,
        long lastApplied,
        long lastLogIndex,
        long lastLogTerm,
        int logSize,
        long lastHeartbeat,
        long electionDeadline,
        boolean rpcServerReady,
        List<PeerCursor> peerCursors
) {
    public record PeerCursor(String peerId, long nextIndex, long matchIndex) {}

    public static RaftOperationalSnapshot singleNodeSnapshot() {
        return new RaftOperationalSnapshot(
                "single",
                0,
                "SINGLE",
                "single",
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                true,
                List.of()
        );
    }
}
