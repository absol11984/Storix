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

    /**
     * Saves metadata to disk.
     */
    private void save() {
        try {
            objectMapper.writeValue(storageFile.toFile(), objects);
        } catch (IOException e) {
            System.err.println("Failed to save metadata: " + e.getMessage());
        }
    }
}
