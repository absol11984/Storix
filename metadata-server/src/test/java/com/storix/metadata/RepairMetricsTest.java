package com.storix.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for RepairManager metrics integration.
 */
class RepairMetricsTest {

    private Observability.Registry registry;

    @BeforeEach
    void setUp() {
        registry = new Observability.Registry();
    }

    @Test
    void repairMetricsBasicCounters() {
        Observability.ManagerMetrics managers = registry.managers();

        managers.repairAttempt();
        managers.repairSuccess();
        managers.repairFailure();
        managers.repairChunks(5);

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> repair = (Map<String, Object>) snapshot.get("repair");
        assertEquals(1L, repair.get("attempts"));
        assertEquals(1L, repair.get("successes"));
        assertEquals(1L, repair.get("failures"));
        assertEquals(5L, repair.get("chunksProcessed"));
    }

    @Test
    void repairActiveCounting() {
        Observability.ManagerMetrics managers = registry.managers();

        managers.repairActiveStart();
        managers.repairActiveStart();
        managers.repairActiveStart();

        managers.repairActiveEnd();
        managers.repairActiveEnd();

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> repair = (Map<String, Object>) snapshot.get("repair");
        assertNotNull(repair);
    }

    @Test
    void repairMetricsSnapshotDetached() {
        Observability.ManagerMetrics managers = registry.managers();

        managers.repairSuccess();

        Map<String, Object> snapshot1 = managers.snapshot();
        managers.repairSuccess();

        Map<String, Object> snapshot2 = managers.snapshot();

        // snapshot1 should have 1 success, snapshot2 should have 2
        assertEquals(1L, ((Map<String, Object>) snapshot1.get("repair")).get("successes"));
        assertEquals(2L, ((Map<String, Object>) snapshot2.get("repair")).get("successes"));
    }
}
