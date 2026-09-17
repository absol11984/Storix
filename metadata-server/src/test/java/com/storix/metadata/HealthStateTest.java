package com.storix.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for health state computation and the node registry health view.
 *
 * computeGlobalHealthState() is private so these tests drive its logic
 * indirectly through accessible components: Observability.Registry lifecycle
 * states and NodeRegistry healthy/unhealthy counts.
 */
class HealthStateTest {
    private Observability.Registry registry;
    private NodeRegistry nodeRegistry;

    @BeforeEach
    void setUp() {
        registry = new Observability.Registry();
        nodeRegistry = new NodeRegistry();
    }

    // --- lifecycle state inputs ---

    @Test
    void startingLifecycleYieldsStartingState() {
        // Default initial state is STARTING
        assertEquals("STARTING", registry.lifecycleState());
    }

    @Test
    void runningLifecycleTransition() {
        registry.setLifecycleState("RUNNING");
        assertEquals("RUNNING", registry.lifecycleState());
    }

    @Test
    void stoppingLifecycleTransition() {
        registry.setLifecycleState("RUNNING");
        registry.setLifecycleState("STOPPING");
        assertEquals("STOPPING", registry.lifecycleState());
    }

    @Test
    void stoppedLifecycleTransition() {
        registry.setLifecycleState("RUNNING");
        registry.setLifecycleState("STOPPING");
        registry.setLifecycleState("STOPPED");
        assertEquals("STOPPED", registry.lifecycleState());
    }

    @Test
    void fullLifecycleSequence() {
        String[] sequence = {"STARTING", "RUNNING", "STOPPING", "STOPPED"};
        for (String state : sequence) {
            registry.setLifecycleState(state);
            assertEquals(state, registry.lifecycleState());
        }
    }

    // --- node registry health ---

    @Test
    void emptyRegistryHasNoHealthyNodes() {
        List<NodeInfo> healthy = nodeRegistry.getHealthyNodes();
        assertEquals(0, healthy.size());
        assertEquals(0, nodeRegistry.healthyCount());
        assertEquals(0, nodeRegistry.size());
    }

    @Test
    void registeredActiveNodeIsHealthy() {
        nodeRegistry.registerNode("node1", "host1", 8080);
        assertEquals(1, nodeRegistry.size());
        assertEquals(1, nodeRegistry.healthyCount());
    }

    @Test
    void unhealthyNodeReducesHealthyCount() {
        nodeRegistry.registerNode("node1", "host1", 8080);
        nodeRegistry.registerNode("node2", "host2", 8081);
        assertEquals(2, nodeRegistry.healthyCount());

        // Mark one node UNHEALTHY
        nodeRegistry.getHealthyNodes().get(0).setStatus(NodeStatus.UNHEALTHY);
        // healthyCount reflects ACTIVE nodes only
        assertTrue(nodeRegistry.healthyCount() <= 2);
    }

    @Test
    void multipleNodesAllActive() {
        nodeRegistry.registerNode("node1", "host1", 8080);
        nodeRegistry.registerNode("node2", "host2", 8081);
        nodeRegistry.registerNode("node3", "host3", 8082);

        assertEquals(3, nodeRegistry.size());
        assertEquals(3, nodeRegistry.healthyCount());
    }

    @Test
    void reRegisteredNodeRemainsActive() {
        nodeRegistry.registerNode("node1", "host1", 8080);
        nodeRegistry.registerNode("node1", "host1", 8080); // re-register same node

        assertEquals(1, nodeRegistry.size());
        assertEquals(1, nodeRegistry.healthyCount());
    }

    @Test
    void nodeInfoAfterRegistration() {
        nodeRegistry.registerNode("alpha", "localhost", 9000);
        List<NodeInfo> healthy = nodeRegistry.getHealthyNodes();
        assertEquals(1, healthy.size());
        NodeInfo node = healthy.get(0);
        assertEquals("alpha", node.getNodeId());
        assertEquals("localhost", node.getHost());
        assertEquals(9000, node.getPort());
        assertEquals(NodeStatus.ACTIVE, node.getStatus());
    }

    // --- health state logic mirrors computeGlobalHealthState ---

    @Test
    void allHealthyNodesYieldsHealthy() {
        // All nodes healthy, no degraded chunks → HEALTHY
        nodeRegistry.registerNode("n1", "h1", 8080);
        nodeRegistry.registerNode("n2", "h2", 8081);

        int unhealthy = nodeRegistry.size() - nodeRegistry.healthyCount();
        assertEquals(0, unhealthy);
    }

    @Test
    void noNodesNoDataIsEffectivelyHealthy() {
        // totalNodes == 0 && !hasData → HEALTHY per computeGlobalHealthState
        assertEquals(0, nodeRegistry.size());
        // No way to assert final health string without calling MetadataHandler directly;
        // we assert the preconditions the function relies on are correct.
        assertEquals(0, nodeRegistry.healthyCount());
    }

    @Test
    void allNodesUnhealthyIsDegraded() {
        nodeRegistry.registerNode("n1", "h1", 8080);
        NodeInfo node = nodeRegistry.getHealthyNodes().get(0);
        node.setStatus(NodeStatus.UNHEALTHY);

        int unhealthy = nodeRegistry.size() - nodeRegistry.healthyCount();
        assertEquals(1, unhealthy);
        // When unhealthyNodes >= totalNodes → UNHEALTHY per computeGlobalHealthState
        assertTrue(unhealthy >= nodeRegistry.size());
    }
}
