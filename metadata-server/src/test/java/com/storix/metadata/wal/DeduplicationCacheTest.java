package com.storix.metadata.wal;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for DeduplicationCache.
 */
class DeduplicationCacheTest {

    @Test
    void testCachePutAndGet() {
        DeduplicationCache cache = new DeduplicationCache();

        String requestId = "request-123";
        DeduplicationCache.CachedResult result = DeduplicationCache.CachedResult.success("response".getBytes());

        cache.put(requestId, result);

        Optional<DeduplicationCache.CachedResult> cached = cache.get(requestId);
        assertTrue(cached.isPresent());
        assertTrue(cached.get().success());
        assertArrayEquals("response".getBytes(), cached.get().responseData());
    }

    @Test
    void testCacheMiss() {
        DeduplicationCache cache = new DeduplicationCache();

        Optional<DeduplicationCache.CachedResult> cached = cache.get("non-existent");
        assertTrue(cached.isEmpty());
    }

    @Test
    void testCacheRemove() {
        DeduplicationCache cache = new DeduplicationCache();

        String requestId = "request-123";
        cache.put(requestId, DeduplicationCache.CachedResult.success("response".getBytes()));

        cache.remove(requestId);

        assertTrue(cache.get(requestId).isEmpty());
    }

    @Test
    void testCacheClear() {
        DeduplicationCache cache = new DeduplicationCache();

        cache.put("req1", DeduplicationCache.CachedResult.success("resp1".getBytes()));
        cache.put("req2", DeduplicationCache.CachedResult.success("resp2".getBytes()));

        assertEquals(2, cache.size());

        cache.clear();

        assertEquals(0, cache.size());
    }

    @Test
    void testCachedResultSuccess() {
        byte[] data = "success data".getBytes();
        DeduplicationCache.CachedResult result = DeduplicationCache.CachedResult.success(data);

        assertTrue(result.success());
        assertNull(result.errorMessage());
        assertArrayEquals(data, result.responseData());
    }

    @Test
    void testCachedResultError() {
        DeduplicationCache.CachedResult result = DeduplicationCache.CachedResult.error("something went wrong");

        assertFalse(result.success());
        assertEquals("something went wrong", result.errorMessage());
        assertNull(result.responseData());
    }

    @Test
    void testCacheTTLExpiry() throws InterruptedException {
        // Create cache with very short TTL
        DeduplicationCache cache = new DeduplicationCache(Duration.ofMillis(50));

        String requestId = "request-123";
        cache.put(requestId, DeduplicationCache.CachedResult.success("response".getBytes()));

        // Should be present initially
        assertTrue(cache.get(requestId).isPresent());

        // Wait for TTL to expire
        Thread.sleep(100);

        // Should be expired now
        assertTrue(cache.get(requestId).isEmpty());
    }
}
