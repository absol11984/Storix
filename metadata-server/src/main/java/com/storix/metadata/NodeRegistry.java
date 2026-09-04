package com.storix.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry of storage nodes in the cluster.
 * Thread-safe — uses ConcurrentHashMap internally.
 */
public class NodeRegistry {

    private final Map<String, NodeInfo> nodes = new ConcurrentHashMap<>();

    /**
     * Registers a node. If the node already exists, updates its host/port/status.
     */
    public void registerNode(String nodeId, String host, int port) {
        nodes.compute(nodeId, (id, existing) -> {
            if (existing != null) {
                existing.setHost(host);
                existing.setPort(port);
                existing.recordHeartbeat();
                System.out.println("[REGISTRY] Node re-registered: " + nodeId + " " + host + ":" + port);
                return existing;
            } else {
                System.out.println("[REGISTRY] Node registered: " + nodeId + " " + host + ":" + port);
                return new NodeInfo(nodeId, host, port);
            }
        });
    }

    /**
     * Records a heartbeat for a node.
     */
    public boolean heartbeat(String nodeId) {
        NodeInfo node = nodes.get(nodeId);
        if (node != null) {
            node.recordHeartbeat();
            return true;
        }
        return false;
    }

    /**
     * Checks all nodes for timeout and marks unhealthy ones.
     */
    public List<String> checkHealth(long timeoutMillis) {
        List<String> newlyUnhealthy = new ArrayList<>();
        for (NodeInfo node : nodes.values()) {
            if (node.getStatus() == NodeStatus.ACTIVE && node.isTimedOut(timeoutMillis)) {
                node.setStatus(NodeStatus.UNHEALTHY);
                newlyUnhealthy.add(node.getNodeId());
                System.out.println("[HEALTH] Node " + node.getNodeId() + " marked UNHEALTHY");
            }
        }
        return newlyUnhealthy;
    }

    /**
     * Returns a list of all healthy (ACTIVE) nodes.
     */
    public List<NodeInfo> getHealthyNodes() {
        return nodes.values().stream()
                .filter(n -> n.getStatus() == NodeStatus.ACTIVE)
                .toList();
    }

    /**
     * Returns all registered nodes.
     */
    public Collection<NodeInfo> getAllNodes() {
        return nodes.values();
    }

    /**
     * Gets a specific node by ID.
     */
    public Optional<NodeInfo> getNode(String nodeId) {
        return Optional.ofNullable(nodes.get(nodeId));
    }

    /**
     * Returns the number of registered nodes.
     */
    public int size() {
        return nodes.size();
    }

    /**
     * Returns the number of healthy nodes.
     */
    public int healthyCount() {
        return (int) nodes.values().stream()
                .filter(n -> n.getStatus() == NodeStatus.ACTIVE)
                .count();
    }
}
