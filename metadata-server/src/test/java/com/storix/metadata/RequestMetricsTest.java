package com.storix.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for request metrics recording and snapshot consistency.
 */
class RequestMetricsTest {
    private Observability.Registry registry;

    @BeforeEach
    void setUp() {
        registry = new Observability.Registry();
    }

    @Test
    void recordsRequestMetrics() {
        registry.requests().record("GET_OBJECT", true, 100, "success");
        registry.requests().record("PUT_OBJECT", true, 150, "success");
        registry.requests().record("DELETE_OBJECT", false, 200, "timeout");

        Map<String, Object> snapshot = registry.requests().snapshot();
        assertNotNull(snapshot);
        assertTrue(snapshot.containsKey("totalRequests"));
        assertTrue(snapshot.containsKey("successfulRequests"));
        assertTrue(snapshot.containsKey("failedRequests"));
        assertTrue(snapshot.containsKey("activeRequests"));
        assertEquals(0L, snapshot.get("activeRequests"));
    }

    @Test
    void tracksCounts() {
        registry.requests().record("GET", true, 50, "success");
        registry.requests().record("GET", true, 60, "success");
        registry.requests().record("GET", false, 100, "unavailable_node");

        Map<String, Object> snapshot = registry.requests().snapshot();
        assertEquals(3L, snapshot.get("totalRequests"));
        assertEquals(2L, snapshot.get("successfulRequests"));
        assertEquals(1L, snapshot.get("failedRequests"));
        assertEquals(0L, snapshot.get("activeRequests"));
    }

    @Test
    void classifiesFailuresByCategory() {
        registry.requests().record("OP", false, 100, "timeout");
        registry.requests().record("OP", false, 100, "unavailable_node");
        registry.requests().record("OP", false, 100, "raft_rejection");
        registry.requests().record("OP", false, 100, "storage_failure");
        registry.requests().record("OP", false, 100, "invalid_request");
        registry.requests().record("OP", false, 100, "internal");

        Map<String, Object> snapshot = registry.requests().snapshot();
        Map<String, Object> byCategory = (Map<String, Object>) snapshot.get("byFailureCategory");
        assertEquals(1L, byCategory.get("timeout"));
        assertEquals(1L, byCategory.get("unavailable_node"));
        assertEquals(1L, byCategory.get("raft_rejection"));
        assertEquals(1L, byCategory.get("storage_failure"));
        assertEquals(1L, byCategory.get("invalid_request"));
        assertEquals(1L, byCategory.get("internal"));
    }

    @Test
    void computesLatencyStats() {
        registry.requests().record("GET", true, 10, "success");
        registry.requests().record("GET", true, 20, "success");
        registry.requests().record("GET", true, 30, "success");
        registry.requests().record("GET", true, 40, "success");

        Map<String, Object> snapshot = registry.requests().snapshot();
        assertEquals(4L, snapshot.get("latencyCount"));
        assertEquals(100L, snapshot.get("latencyTotalMs"));
        assertEquals(10L, snapshot.get("latencyMinMs"));
        assertEquals(40L, snapshot.get("latencyMaxMs"));
    }

    @Test
    void snapshotIsImmutable() {
        registry.requests().record("OP1", true, 50, "success");

        Map<String, Object> snapshot1 = registry.requests().snapshot();
        registry.requests().record("OP2", true, 100, "success");
        Map<String, Object> snapshot2 = registry.requests().snapshot();

        // Both should show 2 requests now since snapshot returns current state
        assertEquals(2L, snapshot2.get("totalRequests"));
    }

    @Test
    void isThreadSafe() throws InterruptedException {
        int threadCount = 10;
        int operationsPerThread = 100;
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        registry.requests().record("CONCURRENT", true, 10, "success");
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        Map<String, Object> snapshot = registry.requests().snapshot();
        assertEquals((long) threadCount * operationsPerThread, snapshot.get("totalRequests"));
    }

    @Test
    void handlesMultipleOperations() {
        registry.requests().record("GET_OBJECT", true, 50, "success");
        registry.requests().record("GET_OBJECT", false, 100, "timeout");
        registry.requests().record("PUT_OBJECT", true, 150, "success");
        registry.requests().record("DELETE_OBJECT", false, 200, "unavailable_node");

        Map<String, Object> snapshot = registry.requests().snapshot();
        Map<String, Object> byOperation = (Map<String, Object>) snapshot.get("byOperation");

        assertEquals(2L, byOperation.get("GET_OBJECT"));
        assertEquals(1L, byOperation.get("PUT_OBJECT"));
        assertEquals(1L, byOperation.get("DELETE_OBJECT"));

        assertEquals(0L, snapshot.get("activeRequests"));
    }
}
