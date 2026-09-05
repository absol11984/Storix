package com.storix.metadata.wal;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cache for request deduplication to support idempotent operations.
 */
public class DeduplicationCache {

    private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();
    private final Duration ttl;

    public DeduplicationCache(Duration ttl) {
        this.ttl = ttl;
    }

    public DeduplicationCache() {
        this(Duration.ofHours(1));
    }

    /**
     * Checks if a request has already been processed.
     * Returns the cached result if found and not expired.
     */
    public Optional<CachedResult> get(String requestId) {
        CacheEntry entry = cache.get(requestId);
        if (entry == null) {
            return Optional.empty();
        }

        if (entry.isExpired(ttl)) {
            cache.remove(requestId);
            return Optional.empty();
        }

        return Optional.of(entry.result());
    }

    /**
     * Stores a request result for future deduplication.
     */
    public void put(String requestId, CachedResult result) {
        cache.put(requestId, new CacheEntry(result));
    }

    /**
     * Removes a request from the cache.
     */
    public void remove(String requestId) {
        cache.remove(requestId);
    }

    /**
     * Clears expired entries.
     */
    public void cleanup() {
        cache.entrySet().removeIf(e -> e.getValue().isExpired(ttl));
    }

    /**
     * Returns the number of cached entries.
     */
    public int size() {
        cleanup();
        return cache.size();
    }

    /**
     * Clears the entire cache.
     */
    public void clear() {
        cache.clear();
    }

    private record CacheEntry(CachedResult result, Instant timestamp) {
        CacheEntry(CachedResult result) {
            this(result, Instant.now());
        }

        boolean isExpired(Duration ttl) {
            return Instant.now().isAfter(timestamp.plus(ttl));
        }
    }

    /**
     * Represents a cached request result.
     */
    public record CachedResult(
        boolean success,
        byte[] responseData,
        String errorMessage
    ) {
        public static CachedResult success(byte[] data) {
            return new CachedResult(true, data, null);
        }

        public static CachedResult error(String message) {
            return new CachedResult(false, null, message);
        }
    }
}
