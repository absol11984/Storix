package com.storix.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for RebalanceManager metrics integration.
 */
class RebalanceMetricsTest {

    private Observability.Registry registry;

    @BeforeEach
    void setUp() {
        registry = new Observability.Registry();
    }

    @Test
    void rebalanceMetricsBasicCounters() {
        Observability.ManagerMetrics managers = registry.managers();

        managers.rebalanceAttempt();
        managers.rebalanceSuccess();
        managers.rebalanceFailure();
        managers.rebalanceChunks(2);

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> rebalance = (Map<String, Object>) snapshot.get("rebalance");
        assertEquals(1L, rebalance.get("attempts"));
        assertEquals(1L, rebalance.get("successes"));
        assertEquals(1L, rebalance.get("failures"));
        assertEquals(2L, rebalance.get("chunksMoved"));
    }

    @Test
    void rebalanceActiveCounting() {
        Observability.ManagerMetrics managers = registry.managers();

        managers.rebalanceActiveStart();
        managers.rebalanceActiveStart();

        managers.rebalanceActiveEnd();

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> rebalance = (Map<String, Object>) snapshot.get("rebalance");
        assertNotNull(rebalance);
    }

    @Test
    void rebalanceMetricsSnapshotDetached() {
        Observability.ManagerMetrics managers = registry.managers();

        managers.rebalanceSuccess();

        Map<String, Object> snapshot1 = managers.snapshot();
        managers.rebalanceSuccess();

        Map<String, Object> snapshot2 = managers.snapshot();

        // snapshot1 should have 1 success, snapshot2 should have 2
        assertEquals(1L, ((Map<String, Object>) snapshot1.get("rebalance")).get("successes"));
        assertEquals(2L, ((Map<String, Object>) snapshot2.get("rebalance")).get("successes"));
    }
}
