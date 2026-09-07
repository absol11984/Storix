package com.storix.metadata.raft;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.storix.metadata.MetadataStore;
import com.storix.metadata.ObjectMetadata;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Applies committed Raft log entries to the metadata state machine.
 */
public class MetadataStateMachine {

    // True deduplication cache: clientId + requestId → committed result
    private final Map<String, byte[]> committedResults = new ConcurrentHashMap<>();

    private final MetadataStore store;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public MetadataStateMachine(MetadataStore store) {
        this.store = store;
    }

    /**
     * Returns a cached result for a duplicate request identity.
     */
    public byte[] getCachedResult(String dedupKey) {
        return committedResults.get(dedupKey);
    }

    /**
     * Clears the deduplication cache (used during snapshot/recovery).
     */
    public void clearDedupCache() {
        committedResults.clear();
    }

    /**
     * Returns the underlying store (for testing).
     */
    public MetadataStore getStore() {
        return store;
    }

    /**
     * Applies a single log entry to the state machine.
     */
    public void apply(LogEntry entry) throws IOException {
        // True deduplication using clientId + requestId
        if (entry.clientId() != null && entry.requestId() != null) {
            String dedupKey = entry.clientId() + ":" + entry.requestId();
            byte[] cachedResult = committedResults.get(dedupKey);
            if (cachedResult != null) {
                // Duplicate retry: already committed, skip re-application
                return;
            }
            // Apply operation and cache result for future retries
            applyOperation(entry);
            byte[] result = snapshot(); // Store result snapshot after commit
            committedResults.put(dedupKey, result);
            return;
        }
        // No request identity: apply directly (backward compatibility)
        applyOperation(entry);
    }

    private void applyOperation(LogEntry entry) throws IOException {
        switch (entry.opType()) {
            case NO_OP -> {
                // No action needed
            }
            case CREATE_OBJECT -> {
                ObjectMetadata metadata = deserialize(entry.data(), ObjectMetadata.class);
                store.createObject(metadata);
            }
            case UPDATE_OBJECT -> {
                ObjectMetadata metadata = deserialize(entry.data(), ObjectMetadata.class);
                store.updateObject(metadata);
            }
            case DELETE_OBJECT -> {
                String objectName = new String(entry.data());
                store.deleteObject(objectName);
            }
            case REGISTER_NODE, UNREGISTER_NODE -> {
                // Node registration is handled directly by NodeRegistry, not through Raft
                // These operations don't need to be replicated
            }
            case SNAPSHOT_RESTORE -> {
                // Restore entire state from snapshot
                restore(entry.data());
            }
        }
    }

    /**
     * Generates a snapshot of the current state.
     */
    public byte[] snapshot() throws IOException {
        // Serialize the entire store state by iterating all objects
        Map<String, ObjectMetadata> snapshot = new HashMap<>();

        for (String objectName : store.listObjects()) {
            // Get the object metadata by name
            store.getObject(objectName).ifPresent(obj -> snapshot.put(objectName, obj));
        }

        return objectMapper.writeValueAsBytes(snapshot);
    }

    /**
     * Restores state from a snapshot by clearing and rebuilding.
     * Uses batch restore for efficiency.
     */
    @SuppressWarnings("unchecked")
    public void restore(byte[] snapshotData) throws IOException {
        if (snapshotData == null || snapshotData.length == 0) {
            return;
        }

        Map<String, ObjectMetadata> snapshot = objectMapper.readValue(snapshotData,
            objectMapper.getTypeFactory().constructMapType(HashMap.class, String.class, ObjectMetadata.class));

        // Use batch restore for efficiency
        store.restoreFromSnapshot(snapshot);
    }

    /**
     * Clears all objects from the store (used before restoring from snapshot).
     */
    public void clear() throws IOException {
        for (String objectName : store.listObjects()) {
            store.deleteObject(objectName);
        }
    }

    private <T> T deserialize(byte[] data, Class<T> clazz) throws IOException {
        if (data == null || data.length == 0) {
            throw new IOException("Cannot deserialize null or empty data");
        }
        return objectMapper.readValue(data, clazz);
    }

    /**
     * Serializes an object for the log.
     */
    public byte[] serialize(Object obj) throws IOException {
        return objectMapper.writeValueAsBytes(obj);
    }
}
