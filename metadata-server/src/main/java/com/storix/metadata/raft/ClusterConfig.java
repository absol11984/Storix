package com.storix.metadata.raft;

import java.util.List;

/**
 * Cluster bootstrap configuration for a Raft node.
 */
public record ClusterConfig(
    String clusterId,
    String nodeId,
    String host,
    int port,
    int raftPort,
    List<RaftPeer> initialPeers
) {
    /**
     * Convenience constructor that uses the same port for both client and Raft RPC.
     */
    public ClusterConfig(String clusterId, String nodeId, String host, int port, List<RaftPeer> initialPeers) {
        this(clusterId, nodeId, host, port, port, initialPeers);
    }

    /**
     * Returns all peers including self.
     */
    public List<RaftPeer> getAllPeers() {
        RaftPeer self = new RaftPeer(nodeId, host, raftPort);
        if (initialPeers == null || initialPeers.isEmpty()) {
            return List.of(self);
        }
        // Filter out self from initialPeers to avoid duplicates
        List<RaftPeer> otherPeers = initialPeers.stream()
            .filter(p -> !p.nodeId().equals(nodeId))
            .toList();
        List<RaftPeer> result = new java.util.ArrayList<>();
        result.add(self);
        result.addAll(otherPeers);
        return result;
    }

    /**
     * Returns peers excluding self.
     */
    public List<RaftPeer> getOtherPeers() {
        if (initialPeers == null) {
            return List.of();
        }
        return initialPeers.stream()
            .filter(p -> !p.nodeId().equals(nodeId))
            .toList();
    }

    /**
     * Returns true if this is a single-node cluster.
     */
    public boolean isSingleNode() {
        return initialPeers == null || initialPeers.isEmpty() ||
               initialPeers.stream().noneMatch(p -> !p.nodeId().equals(nodeId));
    }
}
