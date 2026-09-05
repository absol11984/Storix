package com.storix.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Handles chunk persistence using the filesystem.
 * Each chunk is stored as a separate file under the storage directory.
 */
public class ChunkStorage {

    private final Path storageDir;
    private final Path quarantineDir;

    public ChunkStorage(Path storageDir) throws IOException {
        this.storageDir = storageDir;
        this.quarantineDir = storageDir.resolve("quarantine");
        Files.createDirectories(storageDir);
        Files.createDirectories(quarantineDir);
    }

    /**
     * Stores a chunk to disk. Overwrites if exists.
     */
    public void putChunk(String chunkId, byte[] data) throws IOException {
        Path chunkFile = resolveChunkPath(chunkId);
        Files.write(chunkFile, data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
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
     * Deletes a chunk from disk.
     * @return true if chunk was deleted, false if it didn't exist
     */
    public boolean deleteChunk(String chunkId) throws IOException {
        Path chunkFile = resolveChunkPath(chunkId);
        return Files.deleteIfExists(chunkFile);
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
     * @param chunkId the chunk ID to quarantine
     * @throws IOException if quarantine fails
     */
    public void quarantineCorruptChunk(String chunkId) throws IOException {
        Path chunkFile = resolveChunkPath(chunkId);
        if (!Files.exists(chunkFile)) {
            return; // Already gone
        }

        String timestampedFileName = chunkId + "_" + System.currentTimeMillis() + ".corrupt";
        Path quarantineFile = quarantineDir.resolve(timestampedFileName);

        Files.move(chunkFile, quarantineFile);
        System.err.println("[CORRUPT] Quarantined chunk " + chunkId + " to " + quarantineFile);
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
