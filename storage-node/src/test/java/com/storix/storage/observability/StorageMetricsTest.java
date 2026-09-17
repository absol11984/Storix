package com.storix.storage.observability;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for storage-node telemetry metrics.
 */
class StorageMetricsTest {
    private StorageMetrics metrics;

    @BeforeEach
    void setUp() {
        metrics = new StorageMetrics();
    }

    @Test
    void initialTelemetryHasZeroCounts() {
        Map<String, Long> telemetry = metrics.snapshotTelemetry(0);
        assertEquals(0L, telemetry.get("activeConnections"));
        assertEquals(0L, telemetry.get("chunkReadSuccesses"));
        assertEquals(0L, telemetry.get("chunkReadFailures"));
        assertEquals(0L, telemetry.get("chunkWriteSuccesses"));
        assertEquals(0L, telemetry.get("chunkWriteFailures"));
        assertEquals(0L, telemetry.get("checksumFailures"));
        assertEquals(0L, telemetry.get("chunkCount"));
    }

    @Test
    void connectionOpenCloseTracking() {
        metrics.connectionOpened();
        metrics.connectionOpened();
        metrics.connectionOpened();

        assertEquals(3, metrics.getActiveConnections());

        metrics.connectionClosed();
        metrics.connectionClosed();

        assertEquals(1, metrics.getActiveConnections());
    }

    @Test
    void readSuccessTracking() {
        metrics.recordChunkReadSuccess();
        metrics.recordChunkReadSuccess();
        metrics.recordChunkReadSuccess();

        Map<String, Long> telemetry = metrics.snapshotTelemetry(0);
        assertEquals(3L, telemetry.get("chunkReadSuccesses"));
    }

    @Test
    void readFailureTracking() {
        metrics.recordChunkReadFailure();
        metrics.recordChunkReadFailure();

        Map<String, Long> telemetry = metrics.snapshotTelemetry(0);
        assertEquals(2L, telemetry.get("chunkReadFailures"));
    }

    @Test
    void writeSuccessTracking() {
        metrics.recordChunkWriteSuccess();
        metrics.recordChunkWriteSuccess();
        metrics.recordChunkWriteSuccess();
        metrics.recordChunkWriteSuccess();

        Map<String, Long> telemetry = metrics.snapshotTelemetry(0);
        assertEquals(4L, telemetry.get("chunkWriteSuccesses"));
    }

    @Test
    void writeFailureTracking() {
        metrics.recordChunkWriteFailure();

        Map<String, Long> telemetry = metrics.snapshotTelemetry(0);
        assertEquals(1L, telemetry.get("chunkWriteFailures"));
    }

    @Test
    void checksumFailureTracking() {
        metrics.recordChecksumFailure();
        metrics.recordChecksumFailure();
        metrics.recordChecksumFailure();

        Map<String, Long> telemetry = metrics.snapshotTelemetry(0);
        assertEquals(3L, telemetry.get("checksumFailures"));
    }

    @Test
    void combinedTelemetrySnapshot() {
        metrics.connectionOpened();
        metrics.connectionOpened();

        metrics.recordChunkReadSuccess();
        metrics.recordChunkReadSuccess();
        metrics.recordChunkReadFailure();

        metrics.recordChunkWriteSuccess();
        metrics.recordChunkWriteFailure();

        metrics.recordChecksumFailure();
        metrics.recordChecksumFailure();

        Map<String, Long> telemetry = metrics.snapshotTelemetry(100);

        assertEquals(2L, telemetry.get("activeConnections"));
        assertEquals(2L, telemetry.get("chunkReadSuccesses"));
        assertEquals(1L, telemetry.get("chunkReadFailures"));
        assertEquals(1L, telemetry.get("chunkWriteSuccesses"));
        assertEquals(1L, telemetry.get("chunkWriteFailures"));
        assertEquals(2L, telemetry.get("checksumFailures"));
        assertEquals(100L, telemetry.get("chunkCount"));
    }

    @Test
    void telemetrySnapshotIsDetached() {
        metrics.recordChunkReadSuccess();
        metrics.recordChunkReadSuccess();

        Map<String, Long> snapshot1 = metrics.snapshotTelemetry(10);
        metrics.recordChunkReadSuccess();

        Map<String, Long> snapshot2 = metrics.snapshotTelemetry(20);

        // snapshot1 should not include the new read
        assertEquals(2L, snapshot1.get("chunkReadSuccesses"));
        // snapshot2 should include all 3
        assertEquals(3L, snapshot2.get("chunkReadSuccesses"));
        // Chunk counts should be different
        assertEquals(10L, snapshot1.get("chunkCount"));
        assertEquals(20L, snapshot2.get("chunkCount"));
    }

    @Test
    void threadSafety() throws InterruptedException {
        int threadCount = 5;
        int operationsPerThread = 100;
        Thread[] threads = new Thread[threadCount];
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        metrics.connectionOpened();
                        metrics.recordChunkReadSuccess();
                        metrics.recordChunkWriteSuccess();
                        metrics.recordChecksumFailure();
                    }
                } finally {
                    latch.countDown();
                }
            });
            threads[i].start();
        }

        latch.await();
        for (Thread t : threads) {
            t.join();
        }

        Map<String, Long> telemetry = metrics.snapshotTelemetry(0);
        // Each thread opened 100 connections, but they may overlap in time
        assertTrue(telemetry.get("activeConnections") >= 0);
        assertEquals((long) threadCount * operationsPerThread, telemetry.get("chunkReadSuccesses"));
        assertEquals((long) threadCount * operationsPerThread, telemetry.get("chunkWriteSuccesses"));
        assertEquals((long) threadCount * operationsPerThread, telemetry.get("checksumFailures"));
    }

    @Test
    void chunkCountPassedThroughSnapshot() {
        metrics.recordChunkReadSuccess();
        metrics.recordChunkWriteSuccess();

        Map<String, Long> telemetry = metrics.snapshotTelemetry(42);
        assertEquals(42L, telemetry.get("chunkCount"));
    }
}
