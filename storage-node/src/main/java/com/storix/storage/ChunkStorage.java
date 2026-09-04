package com.storix.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Handles chunk persistence using the filesystem.
 * Each chunk is stored as a separate file under the storage directory.
 */
public class ChunkStorage {

    private final Path storageDir;

    public ChunkStorage(Path storageDir) throws IOException {
        this.storageDir = storageDir;
        Files.createDirectories(storageDir);
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
     * Resolves the chunk ID to a file path.
     * Sanitizes the chunk ID to prevent directory traversal.
     */
    private Path resolveChunkPath(String chunkId) {
        // Sanitize: only allow alphanumeric, dash, underscore
        String sanitized = chunkId.replaceAll("[^a-zA-Z0-9_-]", "_");
        return storageDir.resolve(sanitized + ".chunk");
    }
}
