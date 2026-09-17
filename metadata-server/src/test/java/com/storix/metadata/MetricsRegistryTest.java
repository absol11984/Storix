package com.storix.metadata;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests verifying that Observability.Registry is a coherent, single-owner
 * metrics store — each server instance should have exactly one registry and
 * all metric subsystems are accessible through it.
 */
class MetricsRegistryTest {

    @Test
    void registryProvidesRequestMetrics() {
        Observability.Registry registry = new Observability.Registry();
        assertNotNull(registry.requests());
    }

    @Test
    void registryProvidesManagerMetrics() {
        Observability.Registry registry = new Observability.Registry();
        assertNotNull(registry.managers());
    }

    @Test
    void requestsAndManagersAreDistinctSubsystems() {
        Observability.Registry registry = new Observability.Registry();
        assertNotSame(registry.requests(), registry.managers());
    }

    @Test
    void sameRegistryInstanceReturnsSameRequests() {
        Observability.Registry registry = new Observability.Registry();
        assertSame(registry.requests(), registry.requests());
    }

    @Test
    void sameRegistryInstanceReturnsSameManagers() {
        Observability.Registry registry = new Observability.Registry();
        assertSame(registry.managers(), registry.managers());
    }

    @Test
    void twoRegistriesAreIndependent() {
        Observability.Registry r1 = new Observability.Registry();
        Observability.Registry r2 = new Observability.Registry();

        r1.requests().record("GET", true, 50, "success");

        Map<String, Object> snap1 = r1.requests().snapshot();
        Map<String, Object> snap2 = r2.requests().snapshot();

        assertEquals(1L, snap1.get("totalRequests"));
        assertEquals(0L, snap2.get("totalRequests"));
    }

    @Test
    void twoRegistriesHaveIndependentLifecycleState() {
        Observability.Registry r1 = new Observability.Registry();
        Observability.Registry r2 = new Observability.Registry();

        r1.setLifecycleState("RUNNING");

        assertEquals("RUNNING", r1.lifecycleState());
        assertEquals("STARTING", r2.lifecycleState()); // default
    }

    @Test
    void twoRegistriesHaveIndependentManagerMetrics() {
        Observability.Registry r1 = new Observability.Registry();
        Observability.Registry r2 = new Observability.Registry();

        r1.managers().repairSuccess();
        r1.managers().repairSuccess();

        Map<String, Object> snap1 = r1.managers().snapshot();
        Map<String, Object> snap2 = r2.managers().snapshot();

        Map<String, Object> repair1 = (Map<String, Object>) snap1.get("repair");
        Map<String, Object> repair2 = (Map<String, Object>) snap2.get("repair");

        assertEquals(2L, repair1.get("successes"));
        assertEquals(0L, repair2.get("successes"));
    }

    @Test
    void snapshotIncludesAllTopLevelKeys() {
        Observability.Registry registry = new Observability.Registry();
        Map<String, Object> snapshot = registry.snapshot();

        assertNotNull(snapshot.get("requests"));
        assertNotNull(snapshot.get("managers"));
        assertNotNull(snapshot.get("lifecycleState"));
    }

    @Test
    void freshRegistryHasZeroRequestCounts() {
        Observability.Registry registry = new Observability.Registry();
        Map<String, Object> snap = registry.requests().snapshot();

        assertEquals(0L, snap.get("totalRequests"));
        assertEquals(0L, snap.get("successfulRequests"));
        assertEquals(0L, snap.get("failedRequests"));
    }

    @Test
    void freshRegistryHasZeroManagerCounts() {
        Observability.Registry registry = new Observability.Registry();
        Map<String, Object> managers = registry.managers().snapshot();

        Map<String, Object> repair = (Map<String, Object>) managers.get("repair");
        Map<String, Object> recovery = (Map<String, Object>) managers.get("recovery");
        Map<String, Object> rebalance = (Map<String, Object>) managers.get("rebalance");

        assertEquals(0L, repair.get("successes"));
        assertEquals(0L, recovery.get("successes"));
        assertEquals(0L, rebalance.get("successes"));
    }

    @Test
    void defaultLifecycleStateIsStarting() {
        Observability.Registry registry = new Observability.Registry();
        assertEquals("STARTING", registry.lifecycleState());
    }

    @Test
    void snapshotReflectsCurrentLifecycleState() {
        Observability.Registry registry = new Observability.Registry();
        registry.setLifecycleState("RUNNING");

        Map<String, Object> snapshot = registry.snapshot();
        assertEquals("RUNNING", snapshot.get("lifecycleState"));
    }

    @Test
    void nullLifecycleStateSanitizedToUnknown() {
        Observability.Registry registry = new Observability.Registry();
        registry.setLifecycleState(null);
        assertEquals("UNKNOWN", registry.lifecycleState());
    }
}
