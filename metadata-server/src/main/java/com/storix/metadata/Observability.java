package com.storix.metadata;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Lightweight, in-process operational metrics owned by one metadata server.
 *
 * This class provides thread-safe, bounded metrics collection with:
 * - In-process aggregation using LongAdder/AtomicLong (no background threads)
 * - Detached snapshots to avoid exposing live mutable collections
 * - Request operation and failure-category classification with bounded keys
 * - Immutable operational status records for health, node telemetry, and Raft state
 *
 * All metrics are collected during actual request processing and heartbeat events.
 * No network calls, disk I/O, or background threads are used for metric collection.
 */
public final class Observability {
    private Observability() {}

    public static final class Metrics {
        private final LongAdder requests = new LongAdder();
        private final LongAdder successes = new LongAdder();
        private final LongAdder failures = new LongAdder();

        // Tracks number of metadata requests currently being processed.
        // Used by CLI/status output; must be maintained independently from
        // the immutable counters in record(...).
        private final java.util.concurrent.atomic.AtomicInteger activeRequests = new java.util.concurrent.atomic.AtomicInteger(0);

        private final LongAdder latencyCount = new LongAdder();
        private final LongAdder latencyTotalMs = new LongAdder();
        private final AtomicLong latencyMinMs = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong latencyMaxMs = new AtomicLong();
        private final Map<String, LongAdder> byOperation = new ConcurrentHashMap<>();
        private final Map<String, LongAdder> byFailureCategory = new ConcurrentHashMap<>();

        public void record(String operation, boolean success, long latencyMs) {
            record(operation, success, latencyMs, success ? null : "internal");
        }

        public void record(String operation, boolean success, long latencyMs, String failureCategory) {
            requests.increment();
            (success ? successes : failures).increment();
            byOperation.computeIfAbsent(operation == null ? "UNKNOWN" : operation, k -> new LongAdder()).increment();
            if (!success && failureCategory != null) {
                byFailureCategory.computeIfAbsent(failureCategory, k -> new LongAdder()).increment();
            }
            latencyCount.increment();
            latencyTotalMs.add(Math.max(0, latencyMs));
            latencyMinMs.getAndUpdate(v -> Math.min(v, Math.max(0, latencyMs)));
            latencyMaxMs.getAndUpdate(v -> Math.max(v, Math.max(0, latencyMs)));
        }

        public void activeRequestStart() {
            activeRequests.incrementAndGet();
        }

        public void activeRequestEnd() {
            while (true) {
                int current = activeRequests.get();
                if (current <= 0) {
                    // Never allow negative counts; also handles duplicate end calls.
                    return;
                }
                if (activeRequests.compareAndSet(current, current - 1)) {
                    return;
                }
            }
        }

        public int activeRequests() {
            return activeRequests.get();
        }

        public Map<String, Object> snapshot() {
            long count = latencyCount.sum();
            return Map.of(
                    "totalRequests", requests.sum(),
                    "successfulRequests", successes.sum(),
                    "failedRequests", failures.sum(),
                    "activeRequests", (long) activeRequests.get(),
                    "latencyCount", count,
                    "latencyTotalMs", latencyTotalMs.sum(),
                    "latencyMinMs", count == 0 ? 0 : latencyMinMs.get(),
                    "latencyMaxMs", latencyMaxMs.get(),
                    "byOperation", byOperation.entrySet().stream()
                            .collect(java.util.stream.Collectors.toUnmodifiableMap(
                                    Entry::getKey,
                                    e -> e.getValue().sum())),
                    "byFailureCategory", byFailureCategory.entrySet().stream()
                            .collect(java.util.stream.Collectors.toUnmodifiableMap(
                                    Entry::getKey,
                                    e -> e.getValue().sum()))
            );
        }
    }

    public static final class ManagerMetrics {
        private final LongAdder repairAttempts = new LongAdder();
        private final LongAdder repairSuccesses = new LongAdder();
        private final LongAdder repairFailures = new LongAdder();
        private final LongAdder repairChunks = new LongAdder();
        private final LongAdder recoveryAttempts = new LongAdder();
        private final LongAdder recoverySuccesses = new LongAdder();
        private final LongAdder recoveryFailures = new LongAdder();
        private final LongAdder recoveryChunks = new LongAdder();
        private final LongAdder rebalanceAttempts = new LongAdder();
        private final LongAdder rebalanceSuccesses = new LongAdder();
        private final LongAdder rebalanceFailures = new LongAdder();
        private final LongAdder rebalanceChunks = new LongAdder();
        private final LongAdder repairActive = new LongAdder();
        private final LongAdder recoveryActive = new LongAdder();
        private final LongAdder rebalanceActive = new LongAdder();

        public void repairAttempt() { repairAttempts.increment(); }
        public void repairSuccess() { repairSuccesses.increment(); }
        public void repairFailure() { repairFailures.increment(); }
        public void repairChunks(long count) { repairChunks.add(count); }
        public void repairActiveStart() { repairActive.increment(); }
        public void repairActiveEnd() { repairActive.decrement(); }

        public void recoveryAttempt() { recoveryAttempts.increment(); }
        public void recoverySuccess() { recoverySuccesses.increment(); }
        public void recoveryFailure() { recoveryFailures.increment(); }
        public void recoveryChunks(long count) { recoveryChunks.add(count); }
        public void recoveryActiveStart() { recoveryActive.increment(); }
        public void recoveryActiveEnd() { recoveryActive.decrement(); }

        public void rebalanceAttempt() { rebalanceAttempts.increment(); }
        public void rebalanceSuccess() { rebalanceSuccesses.increment(); }
        public void rebalanceFailure() { rebalanceFailures.increment(); }
        public void rebalanceChunks(long count) { rebalanceChunks.add(count); }
        public void rebalanceActiveStart() { rebalanceActive.increment(); }
        public void rebalanceActiveEnd() { rebalanceActive.decrement(); }

        public Map<String, Object> snapshot() {
            Map<String, Object> result = new java.util.HashMap<>();
            Map<String, Object> repair = new java.util.HashMap<>();
            repair.put("attempts", repairAttempts.sum());
            repair.put("successes", repairSuccesses.sum());
            repair.put("failures", repairFailures.sum());
            repair.put("chunksProcessed", repairChunks.sum());
            repair.put("activeRepairs", repairActive.sum());
            repair.put("chunksRepaired", repairChunks.sum());
            repair.put("failedRepairs", repairFailures.sum());
            result.put("repair", repair);

            Map<String, Object> recovery = new java.util.HashMap<>();
            recovery.put("attempts", recoveryAttempts.sum());
            recovery.put("successes", recoverySuccesses.sum());
            recovery.put("failures", recoveryFailures.sum());
            recovery.put("chunksRestored", recoveryChunks.sum());
            recovery.put("activeRecoveries", recoveryActive.sum());
            recovery.put("failedRecoveries", recoveryFailures.sum());
            result.put("recovery", recovery);

            Map<String, Object> rebalance = new java.util.HashMap<>();
            rebalance.put("attempts", rebalanceAttempts.sum());
            rebalance.put("successes", rebalanceSuccesses.sum());
            rebalance.put("failures", rebalanceFailures.sum());
            rebalance.put("chunksMoved", rebalanceChunks.sum());
            rebalance.put("activeMoves", rebalanceActive.sum());
            rebalance.put("failedMoves", rebalanceFailures.sum());
            result.put("rebalance", rebalance);

            return java.util.Collections.unmodifiableMap(result);
        }
    }

    public static final class Registry {
        private final Metrics requests = new Metrics();
        private final ManagerMetrics managers = new ManagerMetrics();
        private volatile String lifecycleState = "STARTING";

        public Metrics requests() { return requests; }
        public ManagerMetrics managers() { return managers; }

        public void setLifecycleState(String state) {
            this.lifecycleState = (state != null) ? state : "UNKNOWN";
        }

        public String lifecycleState() {
            return lifecycleState;
        }

        public Map<String, Object> snapshot() {
            return Map.of(
                    "requests", requests.snapshot(),
                    "managers", managers.snapshot(),
                    "lifecycleState", lifecycleState
            );
        }
    }
}