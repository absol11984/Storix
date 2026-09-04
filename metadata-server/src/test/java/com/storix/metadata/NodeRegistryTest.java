package com.storix.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class NodeRegistryTest {

    private NodeRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new NodeRegistry();
    }

    @Test
    void testRegisterNode() {
        registry.registerNode("node-a", "127.0.0.1", 9001);
        assertEquals(1, registry.size());
        assertEquals(1, registry.healthyCount());

        Optional<NodeInfo> opt = registry.getNode("node-a");
        assertTrue(opt.isPresent());
        assertEquals("127.0.0.1", opt.get().getHost());
        assertEquals(9001, opt.get().getPort());
        assertEquals(NodeStatus.ACTIVE, opt.get().getStatus());
    }

    @Test
    void testDuplicateRegistrationUpdatesExisting() throws InterruptedException {
        registry.registerNode("node-a", "127.0.0.1", 9001);
        long firstHeartbeat = registry.getNode("node-a").get().getLastHeartbeat();

        Thread.sleep(10); // Ensure time passes

        registry.registerNode("node-a", "127.0.0.2", 9002);
        assertEquals(1, registry.size());

        NodeInfo node = registry.getNode("node-a").get();
        assertEquals("127.0.0.2", node.getHost());
        assertEquals(9002, node.getPort());
        assertTrue(node.getLastHeartbeat() > firstHeartbeat);
    }

    @Test
    void testHeartbeat() throws InterruptedException {
        registry.registerNode("node-a", "127.0.0.1", 9001);
        long t1 = registry.getNode("node-a").get().getLastHeartbeat();

        Thread.sleep(10);
        assertTrue(registry.heartbeat("node-a"));

        long t2 = registry.getNode("node-a").get().getLastHeartbeat();
        assertTrue(t2 > t1);
    }

    @Test
    void testStatusUpdateAndUnhealthyDetection() throws InterruptedException {
        registry.registerNode("node-a", "127.0.0.1", 9001);
        registry.registerNode("node-b", "127.0.0.1", 9002);

        // node-a heartbeats, node-b doesn't
        Thread.sleep(20);
        registry.heartbeat("node-a");

        // Health check with very short timeout
        List<String> unhealthy = registry.checkHealth(10);

        assertEquals(1, unhealthy.size());
        assertTrue(unhealthy.contains("node-b"));

        assertEquals(NodeStatus.ACTIVE, registry.getNode("node-a").get().getStatus());
        assertEquals(NodeStatus.UNHEALTHY, registry.getNode("node-b").get().getStatus());

        assertEquals(1, registry.healthyCount());
        assertEquals(2, registry.size());

        // Heartbeat should restore node-b to active
        registry.heartbeat("node-b");
        assertEquals(NodeStatus.ACTIVE, registry.getNode("node-b").get().getStatus());
        assertEquals(2, registry.healthyCount());
    }
}
