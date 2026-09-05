package com.storix.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe metadata store with JSON persistence.
 */
public class MetadataStore {

    // Staging suffix for atomic file swap
    private static final String STAGING_SUFFIX = ".staging";

    private final Map<String, ObjectMetadata> objects = new ConcurrentHashMap<>();
    private final Path storageFile;
    private final ObjectMapper objectMapper;

    public MetadataStore(Path storageFile) throws IOException {
        this.storageFile = storageFile;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        load();
    }

    /**
     * Creates a new object metadata entry.
     * @throws IllegalStateException if object already exists
     */
    public void createObject(ObjectMetadata metadata) {
        ObjectMetadata existing = objects.putIfAbsent(metadata.getObjectName(), metadata);
        if (existing != null) {
            throw new IllegalStateException("Object already exists: " + metadata.getObjectName());
        }
        save();
    }

    /**
     * Retrieves object metadata by name.
     */
    public Optional<ObjectMetadata> getObject(String objectName) {
        return Optional.ofNullable(objects.get(objectName));
    }

    /**
     * Updates existing object metadata.
     * @throws IllegalArgumentException if object doesn't exist
     */
    public void updateObject(ObjectMetadata metadata) {
        if (!objects.containsKey(metadata.getObjectName())) {
            throw new IllegalArgumentException("Object not found: " + metadata.getObjectName());
        }
        objects.put(metadata.getObjectName(), metadata);
        save();
    }

    /**
     * Deletes object metadata.
     * @return true if object was deleted, false if it didn't exist
     */
    public boolean deleteObject(String objectName) {
        boolean removed = objects.remove(objectName) != null;
        if (removed) {
            save();
        }
        return removed;
    }

    /**
     * Creates object metadata without persisting.
     * Used for snapshot restoration where batch save happens after all objects are restored.
     * @throws IllegalStateException if object already exists
     */
    public void createObjectDirect(ObjectMetadata metadata) {
        ObjectMetadata existing = objects.putIfAbsent(metadata.getObjectName(), metadata);
        if (existing != null) {
            throw new IllegalStateException("Object already exists: " + metadata.getObjectName());
        }
        // No save() - batch operation
    }

    /**
     * Updates object metadata without persisting.
     * Used for snapshot restoration where batch save happens after all objects are restored.
     * @throws IllegalArgumentException if object doesn't exist
     */
    public void updateObjectDirect(ObjectMetadata metadata) {
        if (!objects.containsKey(metadata.getObjectName())) {
            throw new IllegalArgumentException("Object not found: " + metadata.getObjectName());
        }
        objects.put(metadata.getObjectName(), metadata);
        // No save() - batch operation
    }

    /**
     * Deletes object metadata without persisting.
     * Used for snapshot restoration where batch save happens after all operations are complete.
     * @return true if object was deleted, false if it didn't exist
     */
    public boolean deleteObjectDirect(String objectName) {
        return objects.remove(objectName) != null;
    }

    /**
     * Explicitly saves the current state to disk.
     * Used after batch operations (like snapshot restoration) to persist the final state.
     */
    public void save() {
        try {
            objectMapper.writeValue(storageFile.toFile(), objects);
        } catch (IOException e) {
            System.err.println("Failed to save metadata: " + e.getMessage());
        }
    }

    /**
     * Batch restore from a snapshot - clears existing state and rebuilds from snapshot.
     * More efficient than individual creates/updates as it saves only once at the end.
     * @param snapshotData Map of object name to ObjectMetadata
     */
    public void restoreFromSnapshot(Map<String, ObjectMetadata> snapshotData) {
        // Clear existing state using direct method
        objects.clear();

        // Restore all objects from snapshot
        for (Map.Entry<String, ObjectMetadata> entry : snapshotData.entrySet()) {
            objects.put(entry.getKey(), entry.getValue());
        }

        // Save once at the end
        save();
    }

    /**
     * Restores snapshot data to an isolated candidate map (does NOT modify live store).
     * This implements Option A: isolated candidate restoration.
     *
     * @param snapshotData The raw snapshot bytes to restore
     * @return A new Map containing the restored objects (ISOLATED from live store)
     * @throws IOException if restoration fails
     */
    @SuppressWarnings("unchecked")
    public Map<String, ObjectMetadata> restoreToCandidate(byte[] snapshotData) throws IOException {
        if (snapshotData == null || snapshotData.length == 0) {
            throw new IOException("Cannot restore from null or empty snapshot data");
        }

        // Create an isolated map for the candidate state
        Map<String, ObjectMetadata> candidateMap = new ConcurrentHashMap<>();

        // Parse the snapshot JSON
        Map<String, Object> snapshot = objectMapper.readValue(snapshotData,
            objectMapper.getTypeFactory().constructMapType(HashMap.class, String.class, Object.class));

        // Restore each object to the candidate map
        for (Map.Entry<String, Object> entry : snapshot.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            // Convert to ObjectMetadata
            String json = objectMapper.writeValueAsString(value);
            ObjectMetadata metadata = objectMapper.readValue(json, ObjectMetadata.class);

            // Validate object metadata
            validateObjectMetadata(metadata);

            candidateMap.put(key, metadata);
        }

        return candidateMap;
    }

    /**
     * Validates object metadata for integrity.
     * @throws IOException if validation fails
     */
    private void validateObjectMetadata(ObjectMetadata metadata) throws IOException {
        if (metadata == null) {
            throw new IOException("Object metadata is null");
        }
        if (metadata.getObjectName() == null || metadata.getObjectName().isEmpty()) {
            throw new IOException("Object name is null or empty");
        }
        if (metadata.getFileSize() < 0) {
            throw new IOException("Invalid file size for object: " + metadata.getObjectName());
        }
        if (metadata.getChunks() == null) {
            throw new IOException("Chunks list is null for object: " + metadata.getObjectName());
        }
        // Validate each chunk
        for (ChunkInfo chunk : metadata.getChunks()) {
            if (chunk.getChunkId() == null || chunk.getChunkId().isEmpty()) {
                throw new IOException("Chunk ID is null or empty for object: " + metadata.getObjectName());
            }
            if (chunk.getReplicaNodeIds() == null || chunk.getReplicaNodeIds().isEmpty()) {
                throw new IOException("Replica nodes list is null or empty for chunk: " + chunk.getChunkId());
            }
        }
    }

    /**
     * Atomically publishes a candidate state to the live store using a staging file swap.
     *
     * This implements crash-safe atomic publication:
     * 1. Serialize candidate state to bytes
     * 2. Write bytes to staging file with full fsync (data + metadata)
     * 3. Atomic rename staging → live (overwrite)
     *
     * If any step fails, the old storage file remains intact and recoverable.
     * The in-memory map is only updated after the atomic rename succeeds.
     *
     * @param candidateState The candidate state to publish (from restoreToCandidate)
     * @throws IOException if publication fails
     */
    public void publishCandidate(Map<String, ObjectMetadata> candidateState) throws IOException {
        if (candidateState == null) {
            throw new IOException("Cannot publish null candidate state");
        }

        // Validate all objects before publishing
        for (Map.Entry<String, ObjectMetadata> entry : candidateState.entrySet()) {
            validateObjectMetadata(entry.getValue());
        }

        // Step 1: Serialize candidate state to a byte array
        byte[] data;
        try {
            data = objectMapper.writeValueAsBytes(candidateState);
        } catch (IOException e) {
            throw new IOException("Failed to serialize candidate state", e);
        }

        if (data.length == 0) {
            throw new IOException("Candidate state serialized to empty byte array");
        }

        // Step 2: Write to staging file with full fsync
        // Use a unique staging filename to avoid collisions
        Path stagingFile = storageFile.resolveSibling(storageFile.getFileName() + STAGING_SUFFIX + "-" + System.nanoTime());
        RandomAccessFile raf = null;
        try {
            // Delete any existing staging file first
            Files.deleteIfExists(stagingFile);

            raf = new RandomAccessFile(stagingFile.toFile(), "rw");
            raf.write(data);
            raf.getFD().sync(); // Sync data to disk
            raf.close();
            raf = null;

            // Verify the staging file has the expected content
            long actualSize = Files.size(stagingFile);
            if (actualSize != data.length) {
                Files.deleteIfExists(stagingFile);
                throw new IOException("Staging file size mismatch: expected " + data.length + ", got " + actualSize);
            }
        } catch (IOException e) {
            if (raf != null) {
                try { raf.close(); } catch (IOException ignored) {}
            }
            try { Files.deleteIfExists(stagingFile); } catch (IOException ignored) {}
            throw new IOException("Failed to write and sync candidate state to staging file", e);
        }

        // Step 3: Atomic rename staging → live (overwrite existing storage file)
        // This is atomic on POSIX: either the old file remains (if rename fails)
        // or the new file replaces it. No intermediate empty file possible.
        try {
            Files.move(stagingFile, storageFile,
                StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            // Clean up staging file on rename failure
            try { Files.deleteIfExists(stagingFile); } catch (IOException ignored) {}
            throw new IOException("Failed to atomically rename candidate state to live storage", e);
        }

        // Step 4: fsync the parent directory to ensure the rename is durable
        // This is needed on some filesystems (ext4, etc.) where rename durability
        // requires a directory sync after the atomic move.
        try {
            fsyncDirectory(storageFile.getParent());
        } catch (IOException e) {
            // Non-fatal: the atomic move succeeded. Directory sync is best-effort.
            System.err.println("[METADATA] Warning: failed to fsync directory after publish: " + e.getMessage());
        }

        // Step 5: Update in-memory map only after atomic rename succeeds
        // This order ensures:
        // - Crash AFTER rename: load() reads new storage file → in-memory matches
        // - Crash BEFORE rename: old storage file untouched → load() recovers old state
        // - Crash AFTER in-memory update: load() recovers new state → consistent
        objects.clear();
        objects.putAll(candidateState);
    }

    /**
     * Force fsync a directory to ensure directory entry changes are durable.
     * This is needed on some filesystems to make renames truly durable.
     */
    private void fsyncDirectory(Path dir) throws IOException {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        // Opening the directory and syncing its file descriptor makes the
        // directory entry durable on ext4 and similar filesystems.
        try (RandomAccessFile raf = new RandomAccessFile(dir.toFile(), "r")) {
            raf.getFD().sync();
        }
    }

    /**
     * Lists all object names.
     */
    public Collection<String> listObjects() {
        return objects.keySet();
    }

    /**
     * Checks if an object exists.
     */
    public boolean objectExists(String objectName) {
        return objects.containsKey(objectName);
    }

    /**
     * Adds a new replica node to a chunk.
     * @param objectName the object name
     * @param chunkId the chunk ID
     * @param nodeId the new replica node ID to add
     */
    public void updateChunkReplica(String objectName, String chunkId, String nodeId) {
        ObjectMetadata metadata = objects.get(objectName);
        if (metadata == null) {
            return;
        }

        boolean updated = false;
        for (ChunkInfo chunk : metadata.getChunks()) {
            if (chunk.getChunkId().equals(chunkId)) {
                if (!chunk.getReplicaNodeIds().contains(nodeId)) {
                    chunk.addReplicaNode(nodeId);
                    updated = true;
                }
                break;
            }
        }

        if (updated) {
            save();
        }
    }

    /**
     * Loads metadata from disk.
     */
    @SuppressWarnings("unchecked")
    private void load() throws IOException {
        if (!Files.exists(storageFile)) {
            return;
        }

        try {
            Map<String, Object> data = objectMapper.readValue(storageFile.toFile(), Map.class);
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                String json = objectMapper.writeValueAsString(entry.getValue());
                ObjectMetadata metadata = objectMapper.readValue(json, ObjectMetadata.class);
                objects.put(entry.getKey(), metadata);
            }
        } catch (IOException e) {
            throw new IOException("Failed to load metadata from " + storageFile, e);
        }
    }
}
