package com.storix.metadata.raft;

import java.util.List;
import java.util.Objects;

/**
 * Raft protocol messages.
 * All messages include a version field for forward/backward compatibility.
 */
public sealed interface RaftMessage {

    /**
     * Current protocol version.
     * Increment when making incompatible changes.
     */
    int CURRENT_VERSION = 1;

    /**
     * Returns the protocol version of this message.
     */
    default int version() {
        return CURRENT_VERSION;
    }

    // ===== RequestVote =====

    record RequestVote(
        long term,
        String candidateId,
        long lastLogIndex,
        long lastLogTerm
    ) implements RaftMessage {
        @Override
        public int version() {
            return CURRENT_VERSION;
        }
    }

    record RequestVoteResponse(
        long term,
        boolean voteGranted
    ) implements RaftMessage {
        @Override
        public int version() {
            return CURRENT_VERSION;
        }
    }

    // ===== AppendEntries =====

    record AppendEntries(
        long term,
        String leaderId,
        long prevLogIndex,
        long prevLogTerm,
        List<LogEntry> entries,
        long leaderCommit
    ) implements RaftMessage {
        @Override
        public int version() {
            return CURRENT_VERSION;
        }
    }

    record AppendEntriesResponse(
        long term,
        boolean success,
        long matchIndex
    ) implements RaftMessage {
        @Override
        public int version() {
            return CURRENT_VERSION;
        }
    }

    // ===== InstallSnapshot (for follower catch-up) =====

    record InstallSnapshot(
        long term,
        String leaderId,
        long lastIncludedIndex,
        long lastIncludedTerm,
        long offset,
        byte[] data,
        boolean done
    ) implements RaftMessage {
        @Override
        public int version() {
            return CURRENT_VERSION;
        }
    }

    record InstallSnapshotResponse(
        long term,
        boolean success,
        long bytesAccepted
    ) implements RaftMessage {
        @Override
        public int version() {
            return CURRENT_VERSION;
        }
    }

    // ===== Cluster Configuration =====

    record ClusterConfiguration(
        String clusterId,
        List<RaftPeer> peers
    ) implements RaftMessage {
        @Override
        public int version() {
            return CURRENT_VERSION;
        }
    }
}
