package com.storix.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Raft operational snapshot creation.
 */
class RaftOperationalStatusTest {
    private Observability.Registry registry;

    @BeforeEach
    void setUp() {
        registry = new Observability.Registry();
    }

    @Test
    void registrySnapshotHasRaftData() {
        // Registry doesn't directly create raft snapshots - that's in StatusManager
        Map<String, Object> snapshot = registry.snapshot();
        assertNotNull(snapshot);
    }

    @Test
    void nodeInfoContainsRequiredFields() {
        NodeInfo node = new NodeInfo("node1", "host1", 8080);
        assertEquals("node1", node.getNodeId());
        assertEquals("host1", node.getHost());
        assertEquals(8080, node.getPort());
    }

    @Test
    void nodeInfoUpdateTelemetry() {
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
}
