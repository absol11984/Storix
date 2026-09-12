package com.storix.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Handles chunk persistence using the filesystem.
 *
 * Capacity model (optional):
 * - If totalCapacityBytes <= 0, capacity is treated as unlimited.
 * - If totalCapacityBytes > 0, put/delete/quarantine enforce and update usedCapacityBytes.
 */
public class ChunkStorage {

    private final Path storageDir;
    private final Path quarantineDir;

    private final long totalCapacityBytes;
    private long usedCapacityBytes;

    /**
     * Test hook: allows forcing failures at deterministic points inside putChunk.
     */
    @FunctionalInterface
    public interface PutFailureInjector {
        void beforeCommit(String chunkId) throws IOException;
    }

    private volatile PutFailureInjector putFailureInjector;

    // Global lock ensures capacity accounting is consistent under concurrent operations.
    private final ReentrantLock capacityLock = new ReentrantLock();

    /**
     * Sets a test-only failure injector. When non-null, it is invoked inside putChunk
     * after capacity has been reserved but before the filesystem write is committed.
     */
    public void setPutFailureInjector(PutFailureInjector injector) {
        this.putFailureInjector = injector;
    }

    public ChunkStorage(Path storageDir) throws IOException {
        this(storageDir, -1);
    }

    public ChunkStorage(Path storageDir, long totalCapacityBytes) throws IOException {
        this.storageDir = storageDir;
        this.quarantineDir = storageDir.resolve("quarantine");
        this.totalCapacityBytes = totalCapacityBytes;
        Files.createDirectories(storageDir);
        Files.createDirectories(quarantineDir);

        // Initialize used capacity from any existing chunk files.
        this.usedCapacityBytes = computeInitialUsedCapacityBytes();
    }

    private long computeInitialUsedCapacityBytes() throws IOException {
        if (totalCapacityBytes <= 0) {
            return 0;
        }
        long[] totalRef = new long[]{0};
        try (var stream = Files.list(storageDir)) {
            stream.filter(p -> p.getFileName().toString().endsWith(".chunk"))
                    .forEach(p -> {
                        try {
                            totalRef[0] += Files.size(p);
                        } catch (IOException ignored) {
                            // Best-effort; capacity will be corrected by subsequent operations.
                        }
                    });
        }
        return totalRef[0];
    }

    public long getTotalCapacityBytes() {
        return totalCapacityBytes;
    }

    public long getUsedCapacityBytes() {
        capacityLock.lock();
        try {
            return usedCapacityBytes;
        } finally {
            capacityLock.unlock();
        }
    }

    /**
     * Available capacity in bytes.
     *
     * If total capacity is unknown (<=0), returns Long.MAX_VALUE.
     */
    public long getAvailableCapacityBytes() {
        if (totalCapacityBytes <= 0) {
            return Long.MAX_VALUE;
        }
        capacityLock.lock();
        try {
            return Math.max(0, totalCapacityBytes - usedCapacityBytes);
        } finally {
            capacityLock.unlock();
        }
    }

    /**
     * Stores a chunk to disk. Enforces capacity and updates accounting.
     * Overwrites if exists.
     */
    public void putChunk(String chunkId, byte[] data) throws IOException {
        Path chunkFile = resolveChunkPath(chunkId);
        long newSize = data != null ? data.length : 0;

        capacityLock.lock();
        try {
            long oldSize = Files.exists(chunkFile) ? Files.size(chunkFile) : 0;
            long newUsed = usedCapacityBytes - oldSize + newSize;

            if (totalCapacityBytes > 0 && newUsed > totalCapacityBytes) {
                throw new IOException("Insufficient capacity for chunk " + chunkId + ". " +
                        "available=" + Math.max(0, totalCapacityBytes - usedCapacityBytes) +
                        ", required=" + newSize);
            }

            // Reserve succeeded (capacity check passed). Test hook may now force a failure
            // before the actual filesystem write/commit.
            if (putFailureInjector != null) {
                putFailureInjector.beforeCommit(chunkId);
            }

            // Write file (may throw); only update used capacity after success.
            Files.write(chunkFile, data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            usedCapacityBytes = newUsed;

        } finally {
            capacityLock.unlock();
        }
    }

    /**
     * Retrieves a chunk from disk.
     * @throws IOException if chunk doesn't exist or read fails
     */
    public byte[] getChunk(String chunkId) throws IOException {
        Path chunkFile = resolveChunkPath(chunkId);
        return Files.readAllBytes(chunkFile);
    }

    /**
     * Checks if a chunk exists.
     */
    public boolean chunkExists(String chunkId) {
        return Files.exists(resolveChunkPath(chunkId));
    }

    /**
     * Deletes a chunk from disk and updates accounting.
     * @return true if chunk was deleted, false if it didn't exist
     */
    public boolean deleteChunk(String chunkId) throws IOException {
        Path chunkFile = resolveChunkPath(chunkId);

        capacityLock.lock();
        try {
            if (!Files.exists(chunkFile)) {
                return false;
            }

            long oldSize = Files.size(chunkFile);
            boolean deleted = Files.deleteIfExists(chunkFile);
            if (deleted) {
                usedCapacityBytes = usedCapacityBytes - oldSize;
            }
            return deleted;
        } finally {
            capacityLock.unlock();
        }
    }

    /**
     * Verifies chunk integrity against expected checksum.
     * @param chunkId the chunk ID
     * @param expectedChecksum SHA-256 checksum in hex
     * @return true if checksums match
     * @throws IOException if chunk doesn't exist or read fails
     */
    public boolean verifyIntegrity(String chunkId, String expectedChecksum) throws IOException {
        if (expectedChecksum == null || expectedChecksum.isEmpty()) {
            return true; // No checksum to verify
        }

        byte[] data = getChunk(chunkId);
        String actualChecksum = computeSha256(data);
        return expectedChecksum.equalsIgnoreCase(actualChecksum);
    }

    /**
     * Quarantines a corrupted chunk by moving it to quarantine directory.
     * Capacity accounting is updated when capacity is enabled.
     */
    public void quarantineCorruptChunk(String chunkId) throws IOException {
        Path chunkFile = resolveChunkPath(chunkId);
        if (!Files.exists(chunkFile)) {
            return; // Already gone
        }

        capacityLock.lock();
        try {
            long oldSize = Files.size(chunkFile);
            String timestampedFileName = chunkId + "_" + System.currentTimeMillis() + ".corrupt";
            Path quarantineFile = quarantineDir.resolve(timestampedFileName);

            Files.move(chunkFile, quarantineFile);
            if (totalCapacityBytes > 0) {
                usedCapacityBytes = Math.max(0, usedCapacityBytes - oldSize);
            }
            System.err.println("[CORRUPT] Quarantined chunk " + chunkId + " to " + quarantineFile);
        } finally {
            capacityLock.unlock();
        }
    }

    /**
     * Returns the quarantine directory for inspection.
     */
    public Path getQuarantineDir() {
        return quarantineDir;
    }

    /**
     * Resolves the chunk ID to a file path.
     * Sanitizes the chunk ID to prevent directory traversal.
     */
    private Path resolveChunkPath(String chunkId) {
        // Sanitize: only allow alphanumeric, dash, underscore
        String sanitized = chunkId.replaceAll("[^a-zA-Z0-9_-]", "_");
        return storageDir.resolve(sanitized + ".chunk");
    }

    private String computeSha256(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data);
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }
}
