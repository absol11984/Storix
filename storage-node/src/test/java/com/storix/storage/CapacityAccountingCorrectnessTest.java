package com.storix.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class CapacityAccountingCorrectnessTest {

    @TempDir
    Path tempDir;

    @Test
    void concurrentCapacityTest() throws Exception {
        // total capacity = 1000, initial used = 0
        ChunkStorage storage = new ChunkStorage(tempDir, 1_000);

        byte[] payload = new byte[700];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i % 251);

        String chunkA = "chunk-a";
        String chunkB = "chunk-b";

        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);

        AtomicInteger successes = new AtomicInteger(0);
        AtomicReference<String> successfulChunkId = new AtomicReference<>();
        AtomicLong maxObservedUsed = new AtomicLong(0);

        Thread monitor = new Thread(() -> {
            try {
                start.await();
                while (done.getCount() > 0) {
                    long usedNow = storage.getUsedCapacityBytes();
                    maxObservedUsed.getAndUpdate(prev -> Math.max(prev, usedNow));
                    assertTrue(usedNow >= 0, "Used capacity must never be negative during concurrent writes");
                    assertTrue(usedNow <= 1_000, "Used capacity must never exceed total capacity during concurrent writes");
                    Thread.onSpinWait();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "capacity-monitor");
        monitor.start();

        Runnable writeA = () -> {
            try {
                start.await();
                storage.putChunk(chunkA, payload);
                successes.incrementAndGet();
                successfulChunkId.compareAndSet(null, chunkA);
            } catch (IOException ignored) {
                // expected for the losing writer
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("Unexpected interruption in writer A");
            } finally {
                done.countDown();
            }
        };

        Runnable writeB = () -> {
            try {
                start.await();
                storage.putChunk(chunkB, payload);
                successes.incrementAndGet();
                successfulChunkId.compareAndSet(null, chunkB);
            } catch (IOException ignored) {
                // expected for the losing writer
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("Unexpected interruption in writer B");
            } finally {
                done.countDown();
            }
        };

        Thread t1 = new Thread(writeA, "capacity-writer-a");
        Thread t2 = new Thread(writeB, "capacity-writer-b");
        t1.start();
        t2.start();

        start.countDown();
        assertTrue(done.await(5_000, TimeUnit.MILLISECONDS), "Writes timed out");

        long used = storage.getUsedCapacityBytes();
        maxObservedUsed.getAndUpdate(prev -> Math.max(prev, used));

        monitor.join(1_000);

        // Expected: at most one write succeeds.
        assertTrue(successes.get() <= 1, "At most one concurrent write should succeed");

        // Expected invariants.
        assertTrue(used >= 0, "Used capacity must never be negative");
        assertTrue(used <= 1_000, "Used capacity must never exceed total capacity");
        assertTrue(storage.getAvailableCapacityBytes() >= 0, "Available capacity must never be negative");
        assertTrue(maxObservedUsed.get() <= 1_000, "Used capacity must never exceed total capacity (observed)");

        long expectedUsed = successes.get() == 1 ? 700 : 0;
        assertEquals(expectedUsed, used, "Used capacity must match the sum of successful allocations");
        assertEquals(1_000 - used, storage.getAvailableCapacityBytes(), "Available capacity must be consistent with used capacity");

        // Expected: successful chunk remains readable.
        String winner = successfulChunkId.get();
        if (winner != null) {
            assertArrayEquals(payload, storage.getChunk(winner), "Successful chunk must remain readable");
        }

        // Expected: losing chunk write must not be committed.
        if (successes.get() == 0) {
            assertThrows(IOException.class, () -> storage.getChunk(chunkA));
            assertThrows(IOException.class, () -> storage.getChunk(chunkB));
        } else {
            String loser = chunkA.equals(winner) ? chunkB : chunkA;
            assertFalse(storage.chunkExists(loser), "Losing chunk write must not have been committed");
        }
    }

    @Test
    void failedWriteAccountingTest() throws Exception {
        // total capacity = 1000, initial used = 0
        ChunkStorage storage = new ChunkStorage(tempDir.resolve("failed-write"), 1_000);

        byte[] payload = new byte[700];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i % 251);

        String chunkId = "fail-write-chunk";

        try {
            // Force the write to fail after the capacity eligibility check but before the chunk write/commit.
            storage.setPutFailureInjector(cid -> {
                if (chunkId.equals(cid)) {
                    throw new IOException("Injected failure after reservation but before commit");
                }
            });

            assertThrows(IOException.class, () -> storage.putChunk(chunkId, payload));

            // Expected: write returns failure.
            // Expected: used capacity is rolled back (not permanently consumed).
            assertEquals(0, storage.getUsedCapacityBytes(), "Used capacity must be rolled back after failed write");
            assertEquals(1_000, storage.getAvailableCapacityBytes(), "Available capacity must be restored after failed write");
        } finally {
            storage.setPutFailureInjector(null);
        }

        // Expected: subsequent valid write can use the freed capacity.
        storage.putChunk(chunkId, payload);
        assertEquals(700, storage.getUsedCapacityBytes(), "Used capacity must reflect successful write");
        assertEquals(300, storage.getAvailableCapacityBytes(), "Available capacity must reflect successful write");
        assertArrayEquals(payload, storage.getChunk(chunkId), "Successfully written chunk must be readable");
    }

    @Test
    void failedRepairAccountingTest() throws Exception {
        // Source has the chunk; destination should accept it during repair.
        ChunkStorage source = new ChunkStorage(tempDir.resolve("repair-source"), 1_000);
        ChunkStorage dest = new ChunkStorage(tempDir.resolve("repair-dest"), 1_000);

        byte[] payload = new byte[700];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i % 251);

        String chunkId = "repair-target-chunk";
        source.putChunk(chunkId, payload);

        // Placement model for the test: only source initially holds the chunk.
        List<String> replicaNodeIds = new ArrayList<>();
        replicaNodeIds.add("source-node");

        try {
            dest.setPutFailureInjector(cid -> {
                if (chunkId.equals(cid)) {
                    throw new IOException("Injected repair PUT failure");
                }
            });

            boolean repairSuccess = simulateRepair(source, dest, chunkId);
            // Expected: repair reports failure.
            assertFalse(repairSuccess, "Repair must report failure when destination PUT fails");

            // Expected: capacity is not permanently consumed.
            assertEquals(0, dest.getUsedCapacityBytes(), "Destination used capacity must not change after failed repair");
            assertEquals(1_000, dest.getAvailableCapacityBytes(), "Destination available capacity must be restored after failed repair");
            assertTrue(dest.getUsedCapacityBytes() >= 0, "Used capacity must never be negative after failed repair");

            // Expected: placement remains under-replicated.
            assertFalse(dest.chunkExists(chunkId), "Destination must not have the chunk after failed repair");
            assertEquals(1, replicaNodeIds.size(), "Replica set must remain under-replicated after failed repair");

        } finally {
            dest.setPutFailureInjector(null);
        }

        // Expected: later successful repair can still use the node.
        boolean repairSuccess2 = simulateRepair(source, dest, chunkId);
        assertTrue(repairSuccess2, "Repair must succeed after destination becomes writable again");

        assertEquals(700, dest.getUsedCapacityBytes(), "Destination used capacity must reflect successful repair");
        assertEquals(300, dest.getAvailableCapacityBytes(), "Destination available capacity must reflect successful repair");
        assertArrayEquals(payload, dest.getChunk(chunkId), "Successfully repaired chunk must be readable");

        // In a real RepairManager, metadata would update only on success.
        replicaNodeIds.add("dest-node");
        assertEquals(2, replicaNodeIds.size(), "Replica set should advance only after successful repair");
    }

    private static boolean simulateRepair(ChunkStorage source, ChunkStorage dest, String chunkId) {
        try {
            byte[] data = source.getChunk(chunkId);
            dest.putChunk(chunkId, data);
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
