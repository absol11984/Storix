package com.storix.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe metadata store with JSON persistence.
 */
public class MetadataStore {

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
