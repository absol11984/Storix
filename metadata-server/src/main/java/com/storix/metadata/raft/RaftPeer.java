package com.storix.metadata.raft;

/**
 * Represents a Raft peer in the cluster.
 */
public record RaftPeer(String nodeId, String host, int port) {

    @Override
    public String toString() {
        return nodeId + "@" + host + ":" + port;
    }
}
