package com.storix.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 4 capacity-aware placement/repair tests.
 *
 * Validates that:
 * - Placement respects node capacity eligibility.
 * - Nodes already holding a chunk are excluded from repair placement.
 * - Capacity-aware repair selects least-loaded eligible nodes.
 * - Insufficient capacity causes explicit failure.
 * - Under-loaded nodes are preferred for balancing.
 * - Unknown capacity remains backward-compatible.
 */
class Phase4CapacityAwarePlacementTest {

    private NodeRegistry registry;
    private PlacementManager placementManager;

    @BeforeEach
    void setUp() {
        registry = new NodeRegistry();
        placementManager = new PlacementManager(registry, 3);
    }

    /**
     * Test: Total capacity unknown => unlimited eligibility, no placement failure.
     */
    @Test
    void testUnknownCapacityAllowsAllNodes() throws IOException {
        registry.registerNode("node-a", "127.0.0.1", 9001);
        registry.registerNode("node-b", "127.0.0.1", 9002);
        registry.registerNode("node-c", "127.0.0.1", 9003);

        List<NodeInfo> selected = placementManager.selectNodes(0, 10_000_000L);
        assertEquals(3, selected.size());
    }

    /**
     * Test: Node with insufficient capacity is excluded from placement.
     */
    @Test
    void testInsufficientCapacityExcludedFromPlacement() throws IOException {
        registry.registerNode("node-a", "127.0.0.1", 9001, 1000, 0);
        registry.registerNode("node-b", "127.0.0.1", 9002, 10_000, 9_000);
        registry.registerNode("node-c", "127.0.0.1", 9003, 10_000, 0);

        // chunk size = 5000: only node-a (avail=1000) and node-b (avail=1000) are ineligible
        // so only node-c (avail=10000) is eligible. RF=3 should fail.
        assertThrows(IOException.class, () -> placementManager.selectNodes(0, 5000L));

        // chunk size = 500: node-a, node-b, and node-c are eligible.
        List<NodeInfo> selected = placementManager.selectNodes(0, 500L);
        assertEquals(3, selected.size(), "All nodes should be eligible at smaller chunk size");

        // Re-register node-b with sufficient capacity (already eligible, but validates re-registration path)
        registry.registerNode("node-b", "127.0.0.1", 9002, 10_000, 0);
        List<NodeInfo> selectedAfterReRegister = placementManager.selectNodes(0, 500L);
        assertEquals(3, selectedAfterReRegister.size());
        assertTrue(selectedAfterReRegister.stream().noneMatch(n -> n.getNodeId().isEmpty()));

    }

    /**
     * Test: Node already holding chunk is excluded from repair target.
     */
    @Test
    void testRepairTargetExcludesExistingReplicas() throws IOException {
        registry.registerNode("node-a", "127.0.0.1", 9001, 10_000, 0);
        registry.registerNode("node-b", "127.0.0.1", 9002, 10_000, 0);
        registry.registerNode("node-c", "127.0.0.1", 9003, 10_000, 0);

        List<NodeInfo> existing = placementManager.selectNodes(0);
        assertEquals(3, existing.size());

        // No eligible replacement exists for full RF=3 placement.
        NodeInfo replacement = placementManager.selectRepairTarget(
                existing.stream().map(NodeInfo::getNodeId).toList(),
                100L);
        assertNull(replacement, "No eligible replacement should exist when all nodes hold the chunk");
    }

    /**
     * Test: Capacity-aware repair selects the least loaded eligible node.
     */
    @Test
    void testCapacityAwareRepairSelectsLeastLoadedNode() {
        registry.registerNode("node-a", "127.0.0.1", 9001, 10_000, 9_000);
        registry.registerNode("node-b", "127.0.0.1", 9002, 10_000, 1_000);
        registry.registerNode("node-c", "127.0.0.1", 9003, 10_000, 5_000);

        List<String> existing = List.of("node-c");
        NodeInfo target = placementManager.selectRepairTarget(existing, 100L);
        assertNotNull(target);
        assertEquals("node-b", target.getNodeId(),
                "node-b has the least used ratio among eligible nodes");
    }

    /**
     * Test: Repair skips nodes that do not have enough capacity.
     */
    @Test
    void testRepairSkipsInsufficientCapacityNodes() {
        registry.registerNode("node-a", "127.0.0.1", 9001, 10_000, 9_000);
        registry.registerNode("node-b", "127.0.0.1", 9002, 10_000, 8_500);
        registry.registerNode("node-c", "127.0.0.1", 9003, 10_000, 0);

        List<String> existing = List.of("node-a", "node-b");
        // Node-c has 10_000 available; chunk size = 5_000 should fit.
        NodeInfo target = placementManager.selectRepairTarget(existing, 5_000L);
        assertNotNull(target);
        assertEquals("node-c", target.getNodeId());

        // chunk size = 15_000: no eligible node
        target = placementManager.selectRepairTarget(existing, 15_000L);
        assertNull(target);
    }

    /**
     * Test: Node with full capacity is ineligible for placement.
     */
    @Test
    void testFullCapacityNodeExcludedFromPlacement() throws IOException {
        registry.registerNode("node-a", "127.0.0.1", 9001, 10_000, 10_000);
        registry.registerNode("node-b", "127.0.0.1", 9002, 10_000, 0);
        registry.registerNode("node-c", "127.0.0.1", 9003, 10_000, 0);
        registry.registerNode("node-d", "127.0.0.1", 9004, 10_000, 0);

        List<NodeInfo> selected = placementManager.selectNodes(0, 5_000L);
        assertEquals(3, selected.size());
        assertTrue(selected.stream().noneMatch(n -> "node-a".equals(n.getNodeId())),
                "Full node must be excluded from placement");
    }

    /**
     * Test: Balancing distributes across least-loaded eligible nodes.
     */
    @Test
    void testPlacementBalancingAcrossChunks() throws IOException {
        // Node-a small capacity (6000), node-b large capacity (10000), node-c unlimited
        registry.registerNode("node-a", "127.0.0.1", 9001, 6_000, 0);
        registry.registerNode("node-b", "127.0.0.1", 9002, 10_000, 0);
        registry.registerNode("node-c", "127.0.0.1", 9003, -1, 0);

        List<NodeInfo> chunk0 = placementManager.selectNodes(0, 1_000L);
        List<NodeInfo> chunk1 = placementManager.selectNodes(1, 1_000L);

        // All three eligible; ordering is deterministic by usedRatio ascending then nodeId.
        assertEquals(3, chunk0.size());
        assertEquals(3, chunk1.size());

        // Verify nodes are distinct within each placement.
        assertEquals(3, chunk0.stream().map(NodeInfo::getNodeId).distinct().count());
        assertEquals(3, chunk1.stream().map(NodeInfo::getNodeId).distinct().count());
    }

    /**
     * Test: Used capacity is accounted for in ordering.
     */
    @Test
    void testPlacementPreferLessLoadedNodes() throws IOException {
        registry.registerNode("node-a", "127.0.0.1", 9001, 10_000, 8_000);
        registry.registerNode("node-b", "127.0.0.1", 9002, 10_000, 1_000);
        registry.registerNode("node-c", "127.0.0.1", 9003, 10_000, 0);

        List<NodeInfo> selected = placementManager.selectNodes(0, 100L);
        assertEquals(3, selected.size());

        // node-c (ratio 0) or node-b (ratio 0.1) should appear before node-a (ratio 0.8)
        // since sorting is ascending usedRatio.
        int nodeAIndex = selected.indexOf(registry.getNode("node-a").get());
        int nodeCIndex = selected.indexOf(registry.getNode("node-c").get());
        assertTrue(nodeCIndex < nodeAIndex, "Less-loaded node-c must precede more-loaded node-a");
    }

    /**
     * Test: Capacity eligibility preserves exact RF by throwing when insufficient.
     */
    @Test
    void testCapacityAwareFailsExplicitlyWhenInsufficientNodes() {
        registry.registerNode("node-a", "127.0.0.1", 9001, 10_000, 9_000);
        registry.registerNode("node-b", "127.0.0.1", 9002, 10_000, 9_000);
        registry.registerNode("node-c", "127.0.0.1", 9003, 10_000, 9_000);

        assertThrows(IOException.class, () -> placementManager.selectNodes(0, 2_000L),
                "Insufficient capacity must cause explicit failure");
    }

    /**
     * Test: Repair target selection with unknown capacity is backward-compatible.
     */
    @Test
    void testRepairTargetUnknownCapacityBackwardCompatible() {
        registry.registerNode("node-a", "127.0.0.1", 9001);
        registry.registerNode("node-b", "127.0.0.1", 9002);
        registry.registerNode("node-c", "127.0.0.1", 9003);

        NodeInfo target = placementManager.selectRepairTarget(List.of("node-a"), (Long) null);
        assertNotNull(target, "Unknown capacity should default to unlimited eligibility");
    }

    /**
     * Test: Capacity-aware placement still excludes unhealthy nodes.
     */
    @Test
    void testCapacityAwarePlacementExcludesUnhealthyNodes() throws IOException {
        registry.registerNode("node-a", "127.0.0.1", 9001, 10_000, 0);
        registry.registerNode("node-b", "127.0.0.1", 9002, 10_000, 0);
        registry.registerNode("node-c", "127.0.0.1", 9003, 10_000, 0);
        registry.registerNode("node-d", "127.0.0.1", 9004, 10_000, 0);
        registry.getNode("node-b").ifPresent(n -> n.setStatus(NodeStatus.UNHEALTHY));

        List<NodeInfo> selected = placementManager.selectNodes(0, 1_000L);
        assertEquals(3, selected.size());
        assertTrue(selected.stream().noneMatch(n -> "node-b".equals(n.getNodeId())),
                "Unhealthy node must be excluded even when capacity is known");
    }

    /**
     * Test: Node with zero used capacity is eligible and preferred.
     */
    @Test
    void testZeroUsedCapacityNodeEligible() throws IOException {
        registry.registerNode("node-a", "127.0.0.1", 9001, 10_000, 0);
        registry.registerNode("node-b", "127.0.0.1", 9002, 10_000, 8_000);
        registry.registerNode("node-c", "127.0.0.1", 9003, 10_000, 5_000);

        List<NodeInfo> selected = placementManager.selectNodes(0, 1_000L);
        assertEquals(3, selected.size());

        // node-a should be first due to the lowest used ratio.
        assertEquals("node-a", selected.get(0).getNodeId());
    }

    /**
     * Test: Repair with chunk size null preserves backward-compatible eligibility.
     */
    @Test
    void testRepairNullChunkSizeIgnoresCapacity() {
        registry.registerNode("node-a", "127.0.0.1", 9001, 10, 10);
        registry.registerNode("node-b", "127.0.0.1", 9002, 10, 0);

        List<String> existing = List.of("node-a");
        NodeInfo target = placementManager.selectRepairTarget(existing, (Long) null);
        assertNotNull(target);
        assertEquals("node-b", target.getNodeId());
    }
}
