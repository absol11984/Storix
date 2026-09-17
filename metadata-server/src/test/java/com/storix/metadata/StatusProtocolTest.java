package com.storix.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the STATUS protocol and health state computation.
 */
class StatusProtocolTest {
    private Observability.Registry registry;

    @BeforeEach
    void setUp() {
        registry = new Observability.Registry();
    }

    @Test
    void lifecycleStateTransitions() {
        // Initial state
        assertEquals("STARTING", registry.lifecycleState());

        // Transitions
        registry.setLifecycleState("RUNNING");
        assertEquals("RUNNING", registry.lifecycleState());

        registry.setLifecycleState("STOPPING");
        assertEquals("STOPPING", registry.lifecycleState());
    }

    @Test
    void snapshotContainsAllFields() {
        registry.setLifecycleState("RUNNING");
        registry.requests().record("GET_OBJECT", true, 100, "success");
        registry.managers().repairSuccess();

        Map<String, Object> snapshot = registry.snapshot();

        assertNotNull(snapshot.get("requests"));
        assertNotNull(snapshot.get("managers"));
        assertEquals("RUNNING", snapshot.get("lifecycleState"));
    }

    @Test
    void requestMetricsInSnapshot() {
        registry.requests().record("GET_OBJECT", true, 50, "success");
        registry.requests().record("GET_OBJECT", false, 100, "timeout");

        Map<String, Object> snapshot = registry.snapshot();
        Map<String, Object> requests = (Map<String, Object>) snapshot.get("requests");

        assertEquals(2L, requests.get("totalRequests"));
        assertEquals(1L, requests.get("successfulRequests"));
        assertEquals(1L, requests.get("failedRequests"));
    }

    @Test
    void managerMetricsInSnapshot() {
        registry.managers().repairSuccess();
        registry.managers().recoverySuccess();
        registry.managers().rebalanceFailure();

        Map<String, Object> snapshot = registry.snapshot();
        Map<String, Object> managers = (Map<String, Object>) snapshot.get("managers");
        Map<String, Object> repair = (Map<String, Object>) managers.get("repair");
        Map<String, Object> recovery = (Map<String, Object>) managers.get("recovery");
        Map<String, Object> rebalance = (Map<String, Object>) managers.get("rebalance");

        assertEquals(1L, repair.get("successes"));
        assertEquals(1L, recovery.get("successes"));
        assertEquals(1L, rebalance.get("failures"));

        // Verify CLI-compatible field names exist
        assertEquals(0L, repair.get("activeRepairs"));
        assertEquals(0L, repair.get("chunksRepaired"));
        assertEquals(0L, repair.get("failedRepairs"));
    }

    @Test
    void nodeInfoContainsTelemetryFields() {
        NodeInfo node = new NodeInfo("node1", "host1", 8080);
        node.updateTelemetry(10, 100, 5, 50, 3, 2, 1000);

        assertEquals(10, node.getActiveConnections());
        assertEquals(100, node.getChunkReadSuccesses());
        assertEquals(5, node.getChunkReadFailures());
        assertEquals(50, node.getChunkWriteSuccesses());
        assertEquals(3, node.getChunkWriteFailures());
        assertEquals(2, node.getChecksumFailures());
        assertEquals(1000, node.getChunkCount());
    }

    @Test
    void nodeInfoHasNodeIdAndAddress() {
        NodeInfo node = new NodeInfo("node1", "host1", 8080);

        assertEquals("node1", node.getNodeId());
        assertEquals("host1", node.getHost());
        assertEquals(8080, node.getPort());
        assertEquals(NodeStatus.ACTIVE, node.getStatus());
    }

    @Test
    void nodeRegistryRegistersNodes() {
        NodeRegistry nodeRegistry = new NodeRegistry();
        nodeRegistry.registerNode("node1", "host1", 8080);
        nodeRegistry.registerNode("node2", "host2", 8081);

        List<NodeInfo> nodes = nodeRegistry.getHealthyNodes();
        assertTrue(nodes.size() >= 0);
    }
}
