package com.storix.metadata;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Per-chunk lock to prevent repair and rebalancing from operating on the same chunk concurrently.
 */
public class ChunkOperationLock {

    private final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();

    private ReentrantLock getOrCreate(String chunkId) {
        return locks.computeIfAbsent(chunkId, id -> new ReentrantLock());
    }

    /**
     * Attempts to acquire the lock for the given chunk.
     *
     * @return true if the lock was acquired, false if it is currently held.
     */
    public boolean tryAcquire(String chunkId) {
        return getOrCreate(chunkId).tryLock();
    }

    /**
     * Releases the lock for the given chunk if held by the current thread.
     */
    public void release(String chunkId) {
        ReentrantLock lock = locks.get(chunkId);
        if (lock == null) {
            return;
        }
        if (lock.isHeldByCurrentThread()) {
            lock.unlock();
        }
    }

    /**
     * @return true if the lock for the given chunk is currently held.
     */
    public boolean isHeld(String chunkId) {
        ReentrantLock lock = locks.get(chunkId);
        return lock != null && lock.isLocked();
    }
}
