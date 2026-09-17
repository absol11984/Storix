package com.storix.metadata;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for NodeInfo telemetry fields populated via heartbeat / updateTelemetry.
 */
class StorageTelemetryTest {

    @Test
    void initialTelemetryIsZero() {
        NodeInfo node = new NodeInfo("node1", "host1", 8080);

        assertEquals(0, node.getActiveConnections());
        assertEquals(0, node.getChunkReadSuccesses());
        assertEquals(0, node.getChunkReadFailures());
        assertEquals(0, node.getChunkWriteSuccesses());
        assertEquals(0, node.getChunkWriteFailures());
        assertEquals(0, node.getChecksumFailures());
        assertEquals(0, node.getChunkCount());
    }

    @Test
    void updateTelemetryStoresAllFields() {
        NodeInfo node = new NodeInfo("node1", "host1", 8080);
        node.updateTelemetry(10, 200, 5, 150, 3, 2, 1000);

        assertEquals(10, node.getActiveConnections());
        assertEquals(200, node.getChunkReadSuccesses());
        assertEquals(5, node.getChunkReadFailures());
        assertEquals(150, node.getChunkWriteSuccesses());
        assertEquals(3, node.getChunkWriteFailures());
        assertEquals(2, node.getChecksumFailures());
        assertEquals(1000, node.getChunkCount());
    }

    @Test
    void updateTelemetryOverwritesPreviousValues() {
        NodeInfo node = new NodeInfo("node1", "host1", 8080);
        node.updateTelemetry(5, 100, 2, 50, 1, 0, 500);
        node.updateTelemetry(12, 300, 8, 250, 4, 3, 750);

        assertEquals(12, node.getActiveConnections());
        assertEquals(300, node.getChunkReadSuccesses());
        assertEquals(8, node.getChunkReadFailures());
        assertEquals(250, node.getChunkWriteSuccesses());
        assertEquals(4, node.getChunkWriteFailures());
        assertEquals(3, node.getChecksumFailures());
        assertEquals(750, node.getChunkCount());
    }

    @Test
    void negativeValuesClampedToZero() {
        NodeInfo node = new NodeInfo("node1", "host1", 8080);
        node.updateTelemetry(-1, -10, -5, -50, -3, -2, -100);

        assertEquals(0, node.getActiveConnections());
        assertEquals(0, node.getChunkReadSuccesses());
        assertEquals(0, node.getChunkReadFailures());
        assertEquals(0, node.getChunkWriteSuccesses());
        assertEquals(0, node.getChunkWriteFailures());
        assertEquals(0, node.getChecksumFailures());
        assertEquals(0, node.getChunkCount());
    }

    @Test
    void recordHeartbeatRestoresActiveStatus() {
        NodeInfo node = new NodeInfo("node1", "host1", 8080);
        node.setStatus(NodeStatus.UNHEALTHY);
        assertEquals(NodeStatus.UNHEALTHY, node.getStatus());

        node.recordHeartbeat();

        assertEquals(NodeStatus.ACTIVE, node.getStatus());
    }

    @Test
    void recordHeartbeatUpdatesTimestamp() throws InterruptedException {
        NodeInfo node = new NodeInfo("node1", "host1", 8080);
        long before = System.currentTimeMillis();
        Thread.sleep(1);
        node.recordHeartbeat();
        long after = System.currentTimeMillis();

        assertTrue(node.getLastHeartbeat() >= before);
        assertTrue(node.getLastHeartbeat() <= after);
    }

    @Test
    void recordHeartbeatDoesNotRestoreWhenRecoveryHold() {
        NodeInfo node = new NodeInfo("node1", "host1", 8080);
        node.setRecoveryHold(true);
        node.setStatus(NodeStatus.UNHEALTHY);

        node.recordHeartbeat();

        // Status must remain UNHEALTHY when recovery hold is active.
        assertEquals(NodeStatus.UNHEALTHY, node.getStatus());
    }

    @Test
    void capacityFieldsDefaultToUnknown() {
        NodeInfo node = new NodeInfo("node1", "host1", 8080);

        // Unknown capacity is represented by totalCapacityBytes <= 0.
        assertTrue(node.getTotalCapacityBytes() <= 0);
        // Available capacity should return Long.MAX_VALUE when capacity is unknown.
        assertEquals(Long.MAX_VALUE, node.getAvailableCapacityBytes());
    }

    @Test
    void availableCapacityComputedCorrectly() {
        NodeInfo node = new NodeInfo("node1", "host1", 8080);
        node.setTotalCapacityBytes(1_000_000);
        node.setUsedCapacityBytes(400_000);

        assertEquals(600_000, node.getAvailableCapacityBytes());
    }

    @Test
    void usedCapacityNegativeClampedToZero() {
        NodeInfo node = new NodeInfo("node1", "host1", 8080);
        node.setTotalCapacityBytes(500_000);
        node.setUsedCapacityBytes(-999);

        assertEquals(0, node.getUsedCapacityBytes());
        assertEquals(500_000, node.getAvailableCapacityBytes());
    }

    @Test
    void telemetryAndCapacityIndependent() {
        NodeInfo node = new NodeInfo("node1", "host1", 8080);
        node.setTotalCapacityBytes(2_000_000);
        node.setUsedCapacityBytes(500_000);
        node.updateTelemetry(8, 100, 0, 80, 0, 0, 200);

        assertEquals(1_500_000, node.getAvailableCapacityBytes());
        assertEquals(200, node.getChunkCount());
        assertEquals(8, node.getActiveConnections());
    }
}
