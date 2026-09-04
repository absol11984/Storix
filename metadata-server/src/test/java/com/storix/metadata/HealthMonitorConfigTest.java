package com.storix.metadata;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for HealthMonitor configuration.
 */
class HealthMonitorConfigTest {

    @Test
    void testHealthMonitorUsesConfiguredInterval() throws IOException {
        // Create a registry and placement manager
        NodeRegistry registry = new NodeRegistry();
        PlacementManager placementManager = new PlacementManager(registry, 2);

        // Create repair manager (needs metadata store)
        MetadataStore store = new MetadataStore(Path.of("/tmp/test-health-monitor-" + System.nanoTime() + ".json"));
        RepairManager repairManager = new RepairManager(store, registry, placementManager);

        // Create health monitor with specific interval
        long expectedInterval = 1000L;
        HealthMonitor monitor = new HealthMonitor(registry, repairManager, 5000L, expectedInterval);

        // Verify the interval is stored correctly
        assertEquals(expectedInterval, monitor.getCheckIntervalMillis(),
                "HealthMonitor should store the configured interval");
    }

    @Test
    void testHealthMonitorDefaultInterval() throws IOException {
        NodeRegistry registry = new NodeRegistry();
        PlacementManager placementManager = new PlacementManager(registry, 2);
        MetadataStore store = new MetadataStore(Path.of("/tmp/test-health-monitor-default-" + System.nanoTime() + ".json"));
        RepairManager repairManager = new RepairManager(store, registry, placementManager);

        // Create with default 2000ms interval
        long defaultInterval = 2000L;
        HealthMonitor monitor = new HealthMonitor(registry, repairManager, 6000L, defaultInterval);

        assertEquals(defaultInterval, monitor.getCheckIntervalMillis());
    }

    @Test
    void testHealthMonitorSmallInterval() throws IOException {
        NodeRegistry registry = new NodeRegistry();
        PlacementManager placementManager = new PlacementManager(registry, 2);
        MetadataStore store = new MetadataStore(Path.of("/tmp/test-health-monitor-small-" + System.nanoTime() + ".json"));
        RepairManager repairManager = new RepairManager(store, registry, placementManager);

        // Test with a small interval (useful for testing)
        long smallInterval = 100L;
        HealthMonitor monitor = new HealthMonitor(registry, repairManager, 5000L, smallInterval);

        assertEquals(smallInterval, monitor.getCheckIntervalMillis());
    }
}
