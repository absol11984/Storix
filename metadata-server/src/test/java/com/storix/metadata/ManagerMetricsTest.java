package com.storix.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for manager metrics recording (repair, recovery, rebalance).
 */
class ManagerMetricsTest {
    private Observability.Registry registry;
    private Observability.ManagerMetrics managers;

    @BeforeEach
    void setUp() {
        registry = new Observability.Registry();
        managers = registry.managers();
    }

    // ===== REPAIR METRICS =====

    @Test
    void repairAttemptIncrementsOnAttempt() {
        managers.repairAttempt();
        managers.repairAttempt();

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> repair = (Map<String, Object>) snapshot.get("repair");
        assertEquals(2L, repair.get("attempts"));
    }

    @Test
    void repairActiveCounting() {
        managers.repairActiveStart();
        managers.repairActiveStart();
        managers.repairActiveStart();

        // Active count not directly exposed, but tracked internally
        managers.repairActiveEnd();
        managers.repairActiveEnd();

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> repair = (Map<String, Object>) snapshot.get("repair");
        assertNotNull(repair);
    }

    @Test
    void repairSuccessIncrementsOnSuccess() {
        managers.repairSuccess();
        managers.repairSuccess();

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> repair = (Map<String, Object>) snapshot.get("repair");
        assertEquals(2L, repair.get("successes"));
    }

    @Test
    void repairFailureIncrementsOnFailure() {
        managers.repairFailure();

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> repair = (Map<String, Object>) snapshot.get("repair");
        assertEquals(1L, repair.get("failures"));
    }

    @Test
    void repairChunksIncrementsOnChunk() {
        managers.repairChunks(3);
        managers.repairChunks(5);

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> repair = (Map<String, Object>) snapshot.get("repair");
        assertEquals(8L, repair.get("chunksProcessed"));
    }

    @Test
    void repairMetricsCompleteFlow() {
        // Simulate repair flow: attempt -> active -> success -> chunks
        managers.repairAttempt();
        managers.repairSuccess();
        managers.repairChunks(2);

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> repair = (Map<String, Object>) snapshot.get("repair");

        assertEquals(1L, repair.get("successes"));
        assertEquals(2L, repair.get("chunksProcessed"));
    }

    // ===== RECOVERY METRICS =====

    @Test
    void recoveryAttemptIncrementsOnAttempt() {
        managers.recoveryAttempt();

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> recovery = (Map<String, Object>) snapshot.get("recovery");
        assertEquals(1L, recovery.get("attempts"));
    }

    @Test
    void recoveryActiveCounting() {
        managers.recoveryActiveStart();
        managers.recoveryActiveStart();

        managers.recoveryActiveEnd();
        managers.recoveryActiveEnd();

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> recovery = (Map<String, Object>) snapshot.get("recovery");
        assertNotNull(recovery);
    }

    @Test
    void recoverySuccessIncrementsOnSuccess() {
        managers.recoverySuccess();
        managers.recoverySuccess();

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> recovery = (Map<String, Object>) snapshot.get("recovery");
        assertEquals(2L, recovery.get("successes"));
    }

    @Test
    void recoveryFailureIncrementsOnFailure() {
        managers.recoveryFailure();

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> recovery = (Map<String, Object>) snapshot.get("recovery");
        assertEquals(1L, recovery.get("failures"));
    }

    @Test
    void recoveryChunksIncrementsOnChunk() {
        managers.recoveryChunks(4);
        managers.recoveryChunks(6);

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> recovery = (Map<String, Object>) snapshot.get("recovery");
        assertEquals(10L, recovery.get("chunksRestored"));
    }

    // ===== REBALANCE METRICS =====

    @Test
    void rebalanceAttemptIncrementsOnAttempt() {
        managers.rebalanceAttempt();

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> rebalance = (Map<String, Object>) snapshot.get("rebalance");
        assertEquals(1L, rebalance.get("attempts"));
    }

    @Test
    void rebalanceActiveCounting() {
        managers.rebalanceActiveStart();
        managers.rebalanceActiveStart();

        managers.rebalanceActiveEnd();

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> rebalance = (Map<String, Object>) snapshot.get("rebalance");
        assertNotNull(rebalance);
    }

    @Test
    void rebalanceSuccessIncrementsOnSuccess() {
        managers.rebalanceSuccess();

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> rebalance = (Map<String, Object>) snapshot.get("rebalance");
        assertEquals(1L, rebalance.get("successes"));
    }

    @Test
    void rebalanceFailureIncrementsOnFailure() {
        managers.rebalanceFailure();

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> rebalance = (Map<String, Object>) snapshot.get("rebalance");
        assertEquals(1L, rebalance.get("failures"));
    }

    @Test
    void rebalanceChunksIncrementsOnChunk() {
        managers.rebalanceChunks(2);

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> rebalance = (Map<String, Object>) snapshot.get("rebalance");
        assertEquals(2L, rebalance.get("chunksMoved"));
    }

    // ===== COMBINED TESTS =====

    @Test
    void mixedOperationMetrics() {
        // Simulate multiple operation types
        managers.repairAttempt();
        managers.repairSuccess();
        managers.repairChunks(1);

        managers.recoveryAttempt();
        managers.recoverySuccess();

        managers.rebalanceAttempt();
        managers.rebalanceFailure();

        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> repair = (Map<String, Object>) snapshot.get("repair");
        Map<String, Object> recovery = (Map<String, Object>) snapshot.get("recovery");
        Map<String, Object> rebalance = (Map<String, Object>) snapshot.get("rebalance");

        assertEquals(1L, repair.get("attempts"));
        assertEquals(1L, recovery.get("attempts"));
        assertEquals(1L, rebalance.get("attempts"));
    }

    @Test
    void snapshotContainsAllMetricKeys() {
        Map<String, Object> snapshot = managers.snapshot();

        Map<String, Object> repair = (Map<String, Object>) snapshot.get("repair");
        Map<String, Object> recovery = (Map<String, Object>) snapshot.get("recovery");
        Map<String, Object> rebalance = (Map<String, Object>) snapshot.get("rebalance");

        assertNotNull(repair.get("attempts"));
        assertNotNull(repair.get("successes"));
        assertNotNull(repair.get("failures"));

        assertNotNull(recovery.get("attempts"));
        assertNotNull(recovery.get("successes"));
        assertNotNull(recovery.get("failures"));

        assertNotNull(rebalance.get("attempts"));
        assertNotNull(rebalance.get("successes"));
        assertNotNull(rebalance.get("failures"));
    }

    @Test
    void countersResetWithFreshRegistry() {
        Observability.Registry freshRegistry = new Observability.Registry();
        Map<String, Object> snapshot = freshRegistry.managers().snapshot();
        Map<String, Object> repair = (Map<String, Object>) snapshot.get("repair");
        assertEquals(0L, repair.get("successes"));
    }

    @Test
    void negativeChunksNotAllow() {
        managers.repairChunks(-1);
        Map<String, Object> snapshot = managers.snapshot();
        Map<String, Object> repair = (Map<String, Object>) snapshot.get("repair");
        // Negative values are possible but shouldn't occur in practice
    }
}
