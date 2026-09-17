package com.storix.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests proving activeRequests starts at 0, tracks concurrent in-flight requests,
 * and never leaks on exception paths.
 */
class ActiveRequestMetricsTest {

    private Observability.Registry registry;

    @BeforeEach
    void setUp() {
        registry = new Observability.Registry();
    }

    @Test
    void activeRequestsBasicLifecycle() {
        Map<String, Object> snap0 = registry.requests().snapshot();
        assertEquals(0L, snap0.get("activeRequests"));

        registry.requests().activeRequestStart();
        Map<String, Object> snap1 = registry.requests().snapshot();
        assertEquals(1L, snap1.get("activeRequests"));

        registry.requests().activeRequestEnd();
        Map<String, Object> snap2 = registry.requests().snapshot();
        assertEquals(0L, snap2.get("activeRequests"));
    }

    @Test
    void activeRequestsConcurrencyCountsInFlight() throws InterruptedException {
        int concurrent = 10;
        CountDownLatch started = new CountDownLatch(concurrent);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(concurrent);

        for (int i = 0; i < concurrent; i++) {
            executor.submit(() -> {
                try {
                    registry.requests().activeRequestStart();
                    started.countDown();
                    // keep request "in-flight" until release
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    registry.requests().activeRequestEnd();
                }
            });
        }

        assertTrue(started.await(5, TimeUnit.SECONDS));
        Map<String, Object> snap = registry.requests().snapshot();
        assertEquals((long) concurrent, snap.get("activeRequests"));

        release.countDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        Map<String, Object> snapDone = registry.requests().snapshot();
        assertEquals(0L, snapDone.get("activeRequests"));
    }

    @Test
    void activeRequestsExceptionPathDoesNotLeak() {
        Map<String, Object> snap0 = registry.requests().snapshot();
        assertEquals(0L, snap0.get("activeRequests"));

        registry.requests().activeRequestStart();
        try {
            throw new RuntimeException("boom");
        } catch (RuntimeException ignored) {
            // simulate handler finally
        } finally {
            registry.requests().activeRequestEnd();
        }

        Map<String, Object> snap = registry.requests().snapshot();
        assertEquals(0L, snap.get("activeRequests"));
    }

    @Test
    void activeRequestsRepeatedSequentialRequestsReturnToZero() {
        for (int i = 0; i < 50; i++) {
            registry.requests().activeRequestStart();
            registry.requests().activeRequestEnd();
        }

        Map<String, Object> snap = registry.requests().snapshot();
        assertEquals(0L, snap.get("activeRequests"));
    }

    @Test
    void metricsSnapshotContainsActiveRequestsValue() {
        registry.requests().activeRequestStart();
        registry.requests().activeRequestStart();

        Map<String, Object> snap = registry.requests().snapshot();
        assertEquals(2L, snap.get("activeRequests"));
    }
}
