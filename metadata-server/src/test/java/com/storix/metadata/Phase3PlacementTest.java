package com.storix.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 3 tests for chunk placement foundation.
 * Validates placement returns correct replication factor, no duplicates,
 * excludes unavailable nodes, and fails explicitly when insufficient nodes.
 */
class Phase3PlacementTest {

    private NodeRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new NodeRegistry();
    }

    /**
     * Test 1: Placement returns the requested replication factor.
     */
    @Test
    void testPlacementReturnsRequestedReplicationFactor() throws IOException {
        // Register 5 healthy nodes
        for (int i = 1; i <= 5; i++) {
            registry.registerNode("node-" + i, "127.0.0.1", 9000 + i);
        }

        PlacementManager pm = new PlacementManager(registry, 3);
        List<NodeInfo> selected = pm.selectNodes(0);

        assertEquals(3, selected.size(),
                "Placement should return exactly the requested replication factor");
    }

    /**
     * Test 2: Placement never selects the same node twice.
     */
    @Test
    void testPlacementNeverSelectsSameNodeTwice() throws IOException {
        // Register 4 nodes
        registry.registerNode("node-a", "127.0.0.1", 9001);
        registry.registerNode("node-b", "127.0.0.1", 9002);
        registry.registerNode("node-c", "127.0.0.1", 9003);
        registry.registerNode("node-d", "127.0.0.1", 9004);

        PlacementManager pm = new PlacementManager(registry, 4);
        List<NodeInfo> selected = pm.selectNodes(0);

        assertEquals(4, selected.size());
        long distinctCount = selected.stream()
                .map(NodeInfo::getNodeId)
                .distinct()
                .count();

        assertEquals(4, distinctCount,
                "Placement must never select the same node twice for replicas");
    }

    /**
     * Test 3: Placement excludes unavailable (UNHEALTHY) nodes.
     */
    @Test
    void testPlacementExcludesUnavailableNodes() throws IOException {
        registry.registerNode("node-a", "127.0.0.1", 9001);
        registry.registerNode("node-b", "127.0.0.1", 9002);
        registry.registerNode("node-c", "127.0.0.1", 9003);

        // Mark node-b as UNHEALTHY
        registry.getNode("node-b").ifPresent(n -> n.setStatus(NodeStatus.UNHEALTHY));

        PlacementManager pm = new PlacementManager(registry, 2);
        List<NodeInfo> selected = pm.selectNodes(0);

        assertEquals(2, selected.size());
        assertTrue(selected.stream().noneMatch(n -> "node-b".equals(n.getNodeId())),
                "Placement must exclude UNHEALTHY nodes");
        assertTrue(selected.stream().anyMatch(n -> "node-a".equals(n.getNodeId())));
        assertTrue(selected.stream().anyMatch(n -> "node-c".equals(n.getNodeId())));
    }

    /**
     * Test 4: Insufficient nodes causes explicit failure.
     */
    @Test
    void testInsufficientNodesFailsExplicitly() {
        // Only 2 nodes available but replication factor is 3
        registry.registerNode("node-a", "127.0.0.1", 9001);
        registry.registerNode("node-b", "127.0.0.1", 9002);

        PlacementManager pm = new PlacementManager(registry, 3);

        assertThrows(IOException.class, () -> pm.selectNodes(0),
                "When insufficient nodes, placement must fail explicitly");
    }

    /**
     * Test 5: No healthy nodes throws IOException.
     */
    @Test
    void testNoHealthyNodesFails() {
        registry.registerNode("node-a", "127.0.0.1", 9001);
        registry.registerNode("node-b", "127.0.0.1", 9002);

        // Mark all nodes unhealthy
        registry.getNode("node-a").ifPresent(n -> n.setStatus(NodeStatus.UNHEALTHY));
        registry.getNode("node-b").ifPresent(n -> n.setStatus(NodeStatus.UNHEALTHY));

        PlacementManager pm = new PlacementManager(registry, 2);

        assertThrows(IOException.class, () -> pm.selectNodes(0),
                "When no healthy nodes available, placement must fail");
    }

    /**
     * Test 6: Round-robin distribution works across multiple chunks.
     */
    @Test
    void testRoundRobinDistribution() throws IOException {
        registry.registerNode("node-a", "127.0.0.1", 9001);
        registry.registerNode("node-b", "127.0.0.1", 9002);
        registry.registerNode("node-c", "127.0.0.1", 9003);

        PlacementManager pm = new PlacementManager(registry, 2);

        // chunk 0: should start at node-a (index 0), select [a, b]
        List<NodeInfo> chunk0 = pm.selectNodes(0);
        assertEquals(List.of("node-a", "node-b"),
                chunk0.stream().map(NodeInfo::getNodeId).toList());

        // chunk 1: should start at node-b (index 1), select [b, c]
        List<NodeInfo> chunk1 = pm.selectNodes(1);
        assertEquals(List.of("node-b", "node-c"),
                chunk1.stream().map(NodeInfo::getNodeId).toList());

        // chunk 2: should start at node-c (index 2), select [c, a]
        List<NodeInfo> chunk2 = pm.selectNodes(2);
        assertEquals(List.of("node-c", "node-a"),
                chunk2.stream().map(NodeInfo::getNodeId).toList());

        // chunk 3: should start at node-a (index 3 % 3 = 0), select [a, b] again
        List<NodeInfo> chunk3 = pm.selectNodes(3);
        assertEquals(List.of("node-a", "node-b"),
                chunk3.stream().map(NodeInfo::getNodeId).toList());
    }
}
