package com.storix.storage.observability;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe, in-process storage-node telemetry.
 *
 * Counters are updated only on real request paths; snapshots are detached
 * (copied into immutable maps) for heartbeat/status serialization.
 */
public final class StorageMetrics {

    private final AtomicLong activeConnections = new AtomicLong(0);

    private final LongAdder chunkReadSuccesses = new LongAdder();
    private final LongAdder chunkReadFailures = new LongAdder();

    private final LongAdder chunkWriteSuccesses = new LongAdder();
    private final LongAdder chunkWriteFailures = new LongAdder();

    private final LongAdder checksumFailures = new LongAdder();

    public void connectionOpened() {
        activeConnections.incrementAndGet();
    }

    public void connectionClosed() {
        long cur = activeConnections.decrementAndGet();
        if (cur < 0) {
            activeConnections.set(0);
        }
    }

    public void recordChunkReadSuccess() {
        chunkReadSuccesses.increment();
    }

    public void recordChunkReadFailure() {
        chunkReadFailures.increment();
    }

    public void recordChunkWriteSuccess() {
        chunkWriteSuccesses.increment();
    }

    public void recordChunkWriteFailure() {
        chunkWriteFailures.increment();
    }

    public void recordChecksumFailure() {
        checksumFailures.increment();
    }

    public long getActiveConnections() {
        return activeConnections.get();
    }

    public long getChunkReadSuccesses() {
        return chunkReadSuccesses.sum();
    }

    public long getChunkReadFailures() {
        return chunkReadFailures.sum();
    }

    public long getChunkWriteSuccesses() {
        return chunkWriteSuccesses.sum();
    }

    public long getChunkWriteFailures() {
        return chunkWriteFailures.sum();
    }

    public long getChecksumFailures() {
        return checksumFailures.sum();
    }

    public Map<String, Long> snapshotTelemetry(long chunkCount) {
        Map<String, Long> map = new HashMap<>();
        map.put("activeConnections", Math.max(0, activeConnections.get()));
        map.put("chunkReadSuccesses", chunkReadSuccesses.sum());
        map.put("chunkReadFailures", chunkReadFailures.sum());
        map.put("chunkWriteSuccesses", chunkWriteSuccesses.sum());
        map.put("chunkWriteFailures", chunkWriteFailures.sum());
        map.put("checksumFailures", checksumFailures.sum());
        map.put("chunkCount", Math.max(0, chunkCount));
        return Collections.unmodifiableMap(map);
    }
}
