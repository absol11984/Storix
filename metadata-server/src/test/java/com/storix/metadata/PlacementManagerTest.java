package com.storix.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PlacementManagerTest {

    private NodeRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new NodeRegistry();
        registry.registerNode("node-a", "127.0.0.1", 9001);
        registry.registerNode("node-b", "127.0.0.1", 9002);
        registry.registerNode("node-c", "127.0.0.1", 9003);
    }

    @Test
    void testHealthyNodesOnly() throws IOException {
        // Mark node-b unhealthy
        registry.getNode("node-b").ifPresent(n -> n.setStatus(NodeStatus.UNHEALTHY));

        PlacementManager pm = new PlacementManager(registry, 2);
        List<NodeInfo> selected = pm.selectNodes(0);

        assertEquals(2, selected.size());
        assertFalse(selected.stream().anyMatch(n -> n.getNodeId().equals("node-b")), "Should not select unhealthy nodes");
        assertTrue(selected.stream().anyMatch(n -> n.getNodeId().equals("node-a")));
        assertTrue(selected.stream().anyMatch(n -> n.getNodeId().equals("node-c")));
    }

    @Test
    void testNoDuplicateReplicas() throws IOException {
        PlacementManager pm = new PlacementManager(registry, 3);
        List<NodeInfo> selected = pm.selectNodes(0);
        assertEquals(3, selected.size());
        assertEquals(3, selected.stream().map(NodeInfo::getNodeId).distinct().count());
    }

    @Test
    void testBalancedBasicPlacement() throws IOException {
        PlacementManager pm = new PlacementManager(registry, 2);

        // Given nodes are A, B, C (alphabetically sorted)
        List<NodeInfo> chunk0 = pm.selectNodes(0); // expect [A, B]
        List<NodeInfo> chunk1 = pm.selectNodes(1); // expect [B, C]
        List<NodeInfo> chunk2 = pm.selectNodes(2); // expect [C, A]
        List<NodeInfo> chunk3 = pm.selectNodes(3); // expect [A, B] again

        assertEquals("node-a", chunk0.get(0).getNodeId());
        assertEquals("node-b", chunk0.get(1).getNodeId());

        assertEquals("node-b", chunk1.get(0).getNodeId());
        assertEquals("node-c", chunk1.get(1).getNodeId());

        assertEquals("node-c", chunk2.get(0).getNodeId());
        assertEquals("node-a", chunk2.get(1).getNodeId());

        assertEquals("node-a", chunk3.get(0).getNodeId());
        assertEquals("node-b", chunk3.get(1).getNodeId());
    }

    @Test
    void testInsufficientNodes() {
        // Require 3 replicas but only 1 node is active (others UNHEALTHY)
        registry.getNode("node-b").ifPresent(n -> n.setStatus(NodeStatus.UNHEALTHY));
        registry.getNode("node-c").ifPresent(n -> n.setStatus(NodeStatus.UNHEALTHY));

        PlacementManager pm = new PlacementManager(registry, 3);
        assertThrows(IOException.class, () -> pm.selectNodes(0));
    }
}
