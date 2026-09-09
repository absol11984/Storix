package com.storix.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Determines where chunks should be placed across storage nodes.
 * Uses deterministic round-robin placement.
 * Only selects from healthy nodes. Avoids placing duplicate replicas on the same node.
 */
public class PlacementManager {

    private final NodeRegistry nodeRegistry;
    private final int replicationFactor;
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);

    public PlacementManager(NodeRegistry nodeRegistry, int replicationFactor) {
        this.nodeRegistry = nodeRegistry;
        this.replicationFactor = replicationFactor;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    /**
     * Selects nodes for a chunk based on its index.
     * Returns exactly replicationFactor distinct healthy nodes.
     * Throws IOException when insufficient healthy nodes are available.
     *
     * @param chunkIndex the index of the chunk being placed
     * @return list of NodeInfo targets for this chunk's replicas
     * @throws IOException if healthy nodes < replication factor
     */
    public List<NodeInfo> selectNodes(int chunkIndex) throws IOException {
        List<NodeInfo> healthy = nodeRegistry.getHealthyNodes();
        if (healthy.isEmpty()) {
            throw new IOException("No healthy storage nodes available");
        }

        if (healthy.size() < replicationFactor) {
            throw new IOException("Insufficient healthy storage nodes: " +
                healthy.size() + " available, " + replicationFactor + " required");
        }

        // Sort by nodeId for deterministic ordering
        healthy = new ArrayList<>(healthy);
        healthy.sort((a, b) -> a.getNodeId().compareTo(b.getNodeId()));

        List<NodeInfo> selected = new ArrayList<>(replicationFactor);

        // Round-robin starting position based on chunk index
        int startIdx = chunkIndex % healthy.size();

        for (int i = 0; i < replicationFactor; i++) {
            int idx = (startIdx + i) % healthy.size();
            selected.add(healthy.get(idx));
        }

        StringBuilder sb = new StringBuilder("[PLACEMENT] chunk-" + chunkIndex + " → ");
        for (int i = 0; i < selected.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(selected.get(i).getNodeId());
        }
        System.out.println(sb);

        return selected;
    }

    /**
     * Selects a destination node for repair that is healthy and not already
     * hosting a replica of the given chunk.
     *
     * @param existingNodeIds node IDs that already have a copy
     * @return a healthy node not in existingNodeIds, or null if none available
     */
    public NodeInfo selectRepairTarget(List<String> existingNodeIds) {
        List<NodeInfo> healthy = nodeRegistry.getHealthyNodes();
        for (NodeInfo node : healthy) {
            if (!existingNodeIds.contains(node.getNodeId())) {
                return node;
            }
        }
        return null;
    }
}
