package com.storix.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Determines where chunks should be placed across storage nodes.
 * Uses deterministic round-robin placement with optional capacity eligibility.
 * Only selects from healthy nodes. Avoids placing duplicate replicas on the same node.
 */
public class PlacementManager {

    private final NodeRegistry nodeRegistry;
    private final int replicationFactor;

    public PlacementManager(NodeRegistry nodeRegistry, int replicationFactor) {
        this.nodeRegistry = nodeRegistry;
        this.replicationFactor = replicationFactor;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    /**
     * Backward-compatible placement: does not apply capacity eligibility.
     */
    public List<NodeInfo> selectNodes(int chunkIndex) throws IOException {
        return selectNodes(chunkIndex, null);
    }

    /**
     * Selects nodes for a chunk based on its index.
     * Returns exactly replicationFactor distinct healthy nodes.
     * Throws IOException when insufficient eligible nodes are available.
     *
     * Eligibility rules:
     * - Node must be HEALTHY (ACTIVE)
     * - If chunkSizeBytes is provided: node.availableCapacityBytes >= chunkSizeBytes
     *
     * Deterministic balancing:
     * - Sort eligible nodes by (usedCapacity/totalCapacity) ascending, then nodeId
     * - Use round-robin based on chunkIndex within the eligible sorted list
     *
     * Capacity unknown (totalCapacityBytes <= 0) is treated as unlimited for eligibility
     * and as usedRatio=0 for stable ordering.
     */
    public List<NodeInfo> selectNodes(int chunkIndex, Long chunkSizeBytes) throws IOException {
        List<NodeInfo> eligible = new ArrayList<>(nodeRegistry.getHealthyNodes());

        if (eligible.isEmpty()) {
            throw new IOException("No healthy storage nodes available");
        }

        if (chunkSizeBytes != null && chunkSizeBytes > 0) {
            eligible.removeIf(n -> n.getAvailableCapacityBytes() < chunkSizeBytes);
        }

        if (eligible.size() < replicationFactor) {
            throw new IOException("Insufficient eligible storage nodes: " +
                    eligible.size() + " available, " + replicationFactor + " required");
        }

        // Sort eligible nodes by least used ratio, then nodeId for determinism.
        eligible.sort(usedRatioComparator());

        int startIdx = Math.floorMod(chunkIndex, eligible.size());

        List<NodeInfo> selected = new ArrayList<>(replicationFactor);
        for (int i = 0; i < replicationFactor; i++) {
            int idx = (startIdx + i) % eligible.size();
            selected.add(eligible.get(idx));
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
     * Backward-compatible repair selection (no capacity eligibility).
     */
    public NodeInfo selectRepairTarget(List<String> existingNodeIds) {
        return selectRepairTarget(existingNodeIds, null);
    }

    /**
     * Selects a destination node for repair that is healthy, not already
     * hosting a replica of the given chunk, and (optionally) has enough capacity.
     *
     * @param existingNodeIds node IDs that already have a copy
     * @param chunkSizeBytes if provided and >0: destination.availableCapacityBytes >= chunkSizeBytes
     * @return a healthy destination node not in existingNodeIds, or null if none available
     */
    public NodeInfo selectRepairTarget(List<String> existingNodeIds, Long chunkSizeBytes) {
        List<NodeInfo> eligible = new ArrayList<>();
        boolean enforceCapacity = chunkSizeBytes != null && chunkSizeBytes > 0;

        for (NodeInfo node : nodeRegistry.getHealthyNodes()) {
            if (existingNodeIds != null && existingNodeIds.contains(node.getNodeId())) {
                continue;
            }
            if (enforceCapacity && node.getAvailableCapacityBytes() < chunkSizeBytes) {
                continue;
            }
            eligible.add(node);
        }

        if (eligible.isEmpty()) {
            return null;
        }

        eligible.sort(usedRatioComparator());
        return eligible.get(0);
    }

    private Comparator<NodeInfo> usedRatioComparator() {
        return Comparator
                .comparingDouble(this::usedRatio)
                .thenComparing(NodeInfo::getNodeId);
    }

    /**
     * used/total ratio used for deterministic load balancing.
     * For unknown total capacity (<=0), treat ratio as 0 so ordering falls back to nodeId.
     */
    private double usedRatio(NodeInfo node) {
        long total = node.getTotalCapacityBytes();
        if (total <= 0) {
            return 0.0;
        }
        return (double) node.getUsedCapacityBytes() / (double) total;
    }
}
