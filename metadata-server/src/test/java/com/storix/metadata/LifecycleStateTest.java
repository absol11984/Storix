package com.storix.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for lifecycle state transitions and health state computation.
 */
class LifecycleStateTest {
    private Observability.Registry registry;

    @BeforeEach
    void setUp() {
        registry = new Observability.Registry();
    }

    @Test
    void initialLifecycleStateIsStarting() {
        String state = registry.lifecycleState();
        assertEquals("STARTING", state);
    }

    @Test
    void lifecycleTransitions() {
        registry.setLifecycleState("STARTING");
        assertEquals("STARTING", registry.lifecycleState());

        registry.setLifecycleState("RUNNING");
        assertEquals("RUNNING", registry.lifecycleState());

        registry.setLifecycleState("STOPPING");
        assertEquals("STOPPING", registry.lifecycleState());
    }

    @Test
    void snapshotContainsLifecycleState() {
        registry.setLifecycleState("RUNNING");
        Map<String, Object> snapshot = registry.snapshot();
        assertEquals("RUNNING", snapshot.get("lifecycleState"));
    }

    @Test
    void snapshotContainsAllFields() {
        registry.setLifecycleState("STARTING");
        registry.requests().record("GET", true, 50, "success");
        registry.managers().repairSuccess();

        Map<String, Object> snapshot = registry.snapshot();

        assertNotNull(snapshot.get("requests"));
        assertNotNull(snapshot.get("managers"));
        assertEquals("STARTING", snapshot.get("lifecycleState"));
    }

    @Test
    void managerMetricsSnapshotContainsAllKeys() {
        Map<String, Object> managers = registry.managers().snapshot();
        assertNotNull(managers.get("repair"));
        assertNotNull(managers.get("recovery"));
        assertNotNull(managers.get("rebalance"));
    }
}
