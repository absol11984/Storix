package com.storix.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for NodeRecoveryManager metrics integration.
 */
class RecoveryMetricsTest {

    private Observability.Registry registry;

    @BeforeEach
    void setUp() {
        registry = new Observability.Registry();
    }

    @Test
    void recoveryMetricsBasicCounters() {
        Observability.ManagerMetrics managers = registry.managers();

        managers.recoveryAttempt();
        managers.recoverySuccess();
        managers.recoveryFailure();
        managers.recoveryChunks(3);

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> recovery = (Map<String, Object>) snapshot.get("recovery");
        assertEquals(1L, recovery.get("attempts"));
        assertEquals(1L, recovery.get("successes"));
        assertEquals(1L, recovery.get("failures"));
        assertEquals(3L, recovery.get("chunksRestored"));
    }

    @Test
    void recoveryActiveCounting() {
        Observability.ManagerMetrics managers = registry.managers();

        managers.recoveryActiveStart();
        managers.recoveryActiveStart();

        managers.recoveryActiveEnd();

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> recovery = (Map<String, Object>) snapshot.get("recovery");
        assertNotNull(recovery);
    }

    @Test
    void recoveryMetricsSnapshotDetached() {
        Observability.ManagerMetrics managers = registry.managers();

        managers.recoverySuccess();

        Map<String, Object> snapshot1 = managers.snapshot();
        managers.recoverySuccess();

        Map<String, Object> snapshot2 = managers.snapshot();

        // snapshot1 should have 1 success, snapshot2 should have 2
        assertEquals(1L, ((Map<String, Object>) snapshot1.get("recovery")).get("successes"));
        assertEquals(2L, ((Map<String, Object>) snapshot2.get("recovery")).get("successes"));
    }
}
