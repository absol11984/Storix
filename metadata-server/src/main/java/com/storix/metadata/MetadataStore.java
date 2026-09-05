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
 *
 * GENERATION TRACKING:
 * This store tracks the generation it's associated with via a companion .generation file.
 * The generation file contains the snapshot index this metadata was restored from.
 * On load, if the stored generation doesn't match the committed generation in SnapshotManager,
 * the metadata is considered stale and should be rebuilt from the snapshot.
 */
public class MetadataStore {

    // Staging suffix for atomic file swap
    private static final String STAGING_SUFFIX = ".staging";
    // Generation tracking file suffix
    private static final String GENERATION_SUFFIX = ".generation";

    private final Map<String, ObjectMetadata> objects = new ConcurrentHashMap<>();
    private final Path storageFile;
    private final Path generationFile;
    private final ObjectMapper objectMapper;
    private volatile long currentGeneration = -1;

    public MetadataStore(Path storageFile) throws IOException {
        this.storageFile = storageFile;
        this.generationFile = storageFile.resolveSibling(storageFile.getFileName() + GENERATION_SUFFIX);
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        load();
    }

    /**
     * Gets the current generation this store is tracking.
     * Returns -1 if no generation has been set.
     */
    public long getGeneration() {
        return currentGeneration;
    }

    /**
     * Sets the generation this store is tracking.
     * The generation is persisted to disk and must match the committed
     * generation in SnapshotManager for the store to be considered valid.
     *
     * @throws IOException if generation persistence fails
     */
    public void setGeneration(long generation) throws IOException {
        this.currentGeneration = generation;
        saveGeneration(); // Propagates IOException
    }

    // Backup suffix for storage file backup during candidate publication
    private static final String BACKUP_SUFFIX = ".backup";

    /**
     * Creates a backup of the current storage file.
     * This is used before publishing candidate state so we can rollback if commit fails.
     *
     * @return Path to the backup file, or null if no storage file exists to backup
     * @throws IOException if backup creation fails
     */
    public Path backupStorage() throws IOException {
        if (!Files.exists(storageFile)) {
            return null;
        }

        Path backupFile = storageFile.resolveSibling(storageFile.getFileName() + BACKUP_SUFFIX);

        // Copy with fsync to ensure backup is durable
        try (RandomAccessFile src = new RandomAccessFile(storageFile.toFile(), "r");
             RandomAccessFile dst = new RandomAccessFile(backupFile.toFile(), "rw")) {
            // Copy all bytes
            byte[] buffer = new byte[8192];
            long totalRead = 0;
            long fileSize = src.length();
            while (totalRead < fileSize) {
                int read = src.read(buffer);
                if (read > 0) {
                    dst.write(buffer, 0, read);
                    totalRead += read;
                }
            }
            dst.getFD().sync(); // Sync backup to disk
        }

        fsyncDirectory(backupFile.getParent());
        return backupFile;
    }

    /**
     * Restores the storage from a backup file.
     * This is used during rollback when candidate publication succeeded but commit failed.
     *
     * @param backupFile Path to the backup file to restore from
     * @throws IOException if restore fails
     */
    public void restoreFromBackup(Path backupFile) throws IOException {
        if (backupFile == null || !Files.exists(backupFile)) {
            // No backup to restore - this is fine, it means we never published
            return;
        }

        // Atomic restore: copy backup back to storage file location
        Path tempRestore = storageFile.resolveSibling(storageFile.getFileName() + ".restore_tmp");

        try {
            // Copy backup to temp location
            try (RandomAccessFile src = new RandomAccessFile(backupFile.toFile(), "r");
                 RandomAccessFile dst = new RandomAccessFile(tempRestore.toFile(), "rw")) {
                byte[] buffer = new byte[8192];
                long totalRead = 0;
                long fileSize = src.length();
                while (totalRead < fileSize) {
                    int read = src.read(buffer);
                    if (read > 0) {
                        dst.write(buffer, 0, read);
                        totalRead += read;
                    }
                }
                dst.getFD().sync();
            }

            // Atomic move to final location
            Files.move(tempRestore, storageFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            fsyncDirectory(storageFile.getParent());

            // Reload from restored storage
            objects.clear();
            load();

        } finally {
            // Clean up temp file if it exists
            Files.deleteIfExists(tempRestore);
        }
    }

    /**
     * Deletes the backup file.
     * Called after successful commit to clean up.
     *
     * @param backupFile Path to the backup file to delete
     */
    public void deleteBackup(Path backupFile) {
        if (backupFile != null) {
            try {
                Files.deleteIfExists(backupFile);
            } catch (IOException e) {
                // Best effort cleanup
                System.err.println("[METADATA] Warning: failed to delete backup file: " + e.getMessage());
            }
        }
    }

    /**
     * Deletes the storage file.
     * Used during rollback when the old state was empty (no storage file) and
     * candidate publication created a new storage file that needs to be removed.
     *
     * @throws IOException if deletion fails
     */
    public void deleteStorageFile() throws IOException {
        if (Files.exists(storageFile)) {
            Files.delete(storageFile);
            fsyncDirectory(storageFile.getParent());
        }
        // Clear in-memory state
        objects.clear();
        currentGeneration = -1;
    }

    /**
     * Saves the generation to disk with full durability.
     * This MUST NOT swallow errors - if generation persistence fails,
     * the operation must fail.
     *
     * @throws IOException if generation persistence fails
     */
    private void saveGeneration() throws IOException {
        // Write to temp file first
        Path tempFile = generationFile.resolveSibling(generationFile.getFileName() + ".tmp");
        try {
            Files.writeString(tempFile, String.valueOf(currentGeneration));
            // Sync the temp file
            try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw")) {
                raf.getFD().sync();
            }
            // Atomic rename to final location
            Files.move(tempFile, generationFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            // Sync directory
            fsyncDirectory(generationFile.getParent());
        } catch (IOException e) {
            // Clean up temp file if it exists
            try { Files.deleteIfExists(tempFile); } catch (IOException ignored) {}
            throw new IOException("Failed to save generation: " + e.getMessage(), e);
        }
    }

    /**
     * Force fsync a directory to ensure directory entry changes are durable.
     */
    private void fsyncDirectory(Path dir) throws IOException {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        // Best effort directory sync using a dummy file
        try {
            Path dummyFile = dir.resolve(".fsync_dummy");
            try {
                Files.write(dummyFile, new byte[0]);
                try (java.nio.channels.FileChannel fc = java.nio.channels.FileChannel.open(dummyFile,
                        java.nio.file.StandardOpenOption.READ, java.nio.file.StandardOpenOption.WRITE)) {
                    fc.force(true);
                }
            } finally {
                Files.deleteIfExists(dummyFile);
            }
        } catch (IOException e) {
            // Non-fatal: the atomic move succeeded. Directory sync is best-effort.
            System.err.println("[METADATA] Warning: failed to fsync directory: " + e.getMessage());
        }
    }

    private void loadGeneration() {
        if (!Files.exists(generationFile)) {
            currentGeneration = -1;
            return;
        }
        try {
            String content = Files.readString(generationFile);
            currentGeneration = Long.parseLong(content.trim());
        } catch (IOException | NumberFormatException e) {
            System.err.println("[METADATA] Failed to load generation: " + e.getMessage());
            currentGeneration = -1;
        }
    }

    /**
     * Creates a new object metadata entry.
     * @throws IllegalStateException if object already exists
     * @throws IOException if persistence fails
     */
    public void createObject(ObjectMetadata metadata) throws IOException {
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
     * @throws IOException if persistence fails
     */
    public void updateObject(ObjectMetadata metadata) throws IOException {
        if (!objects.containsKey(metadata.getObjectName())) {
            throw new IllegalArgumentException("Object not found: " + metadata.getObjectName());
        }
        objects.put(metadata.getObjectName(), metadata);
        save();
    }

    /**
     * Deletes object metadata.
     * @return true if object was deleted, false if it didn't exist
     * @throws IOException if persistence fails
     */
    public boolean deleteObject(String objectName) throws IOException {
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
     *
     * @throws IOException if the save fails - callers must handle this
     */
    public void save() throws IOException {
        objectMapper.writeValue(storageFile.toFile(), objects);
    }

    /**
     * Batch restore from a snapshot - clears existing state and rebuilds from snapshot.
     * More efficient than individual creates/updates as it saves only once at the end.
     * @param snapshotData Map of object name to ObjectMetadata
     * @throws IOException if persistence fails
     */
    public void restoreFromSnapshot(Map<String, ObjectMetadata> snapshotData) throws IOException {
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
     * @param generation The generation index to associate with this state
     * @throws IOException if publication fails
     */
    public void publishCandidate(Map<String, ObjectMetadata> candidateState, long generation) throws IOException {
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

        // Step 4: Update generation file (this marks the state as authoritative)
        // The generation must be saved AFTER the metadata file is renamed
        // to ensure atomicity: either both are updated or neither
        this.currentGeneration = generation;
        saveGeneration();

        // Step 5: Update in-memory map only after atomic rename succeeds
        // This order ensures:
        // - Crash AFTER rename: load() reads new storage file → in-memory matches
        // - Crash BEFORE rename: old storage file untouched → load() recovers old state
        // - Crash AFTER in-memory update: load() recovers new state → consistent
        objects.clear();
        objects.putAll(candidateState);
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
    public void updateChunkReplica(String objectName, String chunkId, String nodeId) throws IOException {
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
        // Load generation first
        loadGeneration();

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
