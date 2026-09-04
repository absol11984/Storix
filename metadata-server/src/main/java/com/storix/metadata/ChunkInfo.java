package com.storix.metadata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Information about a single chunk.
 * Supports multiple replica locations and SHA-256 checksum.
 *
 * Backward compatible: if storageNodeId is present (Prompt 1 format),
 * it is treated as a single-element replicaNodeIds list.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ChunkInfo {

    private String chunkId;
    private int chunkIndex;
    private int chunkSize;
    private String checksum; // SHA-256 hex string

    // Prompt 2: multiple replica locations
    private List<String> replicaNodeIds;

    // Prompt 1 backward compatibility fields (read-only for deserialization)
    private String storageNodeId;
    private String storageNodeHost;
    private int storageNodePort;

    public ChunkInfo() {
        this.replicaNodeIds = new ArrayList<>();
    }

    /**
     * Prompt 1 constructor — single storage node.
     */
    public ChunkInfo(String chunkId, int chunkIndex, int chunkSize,
                     String storageNodeId, String storageNodeHost, int storageNodePort) {
        this.chunkId = chunkId;
        this.chunkIndex = chunkIndex;
        this.chunkSize = chunkSize;
        this.storageNodeId = storageNodeId;
        this.storageNodeHost = storageNodeHost;
        this.storageNodePort = storageNodePort;
        this.replicaNodeIds = new ArrayList<>();
        this.replicaNodeIds.add(storageNodeId);
    }

    /**
     * Prompt 2 constructor — multiple replicas with checksum.
     */
    public ChunkInfo(String chunkId, int chunkIndex, int chunkSize,
                     List<String> replicaNodeIds, String checksum) {
        this.chunkId = chunkId;
        this.chunkIndex = chunkIndex;
        this.chunkSize = chunkSize;
        this.replicaNodeIds = new ArrayList<>(replicaNodeIds);
        this.checksum = checksum;
    }

    public String getChunkId() { return chunkId; }
    public void setChunkId(String chunkId) { this.chunkId = chunkId; }

    public int getChunkIndex() { return chunkIndex; }
    public void setChunkIndex(int chunkIndex) { this.chunkIndex = chunkIndex; }

    public int getChunkSize() { return chunkSize; }
    public void setChunkSize(int chunkSize) { this.chunkSize = chunkSize; }

    public String getChecksum() { return checksum; }
    public void setChecksum(String checksum) { this.checksum = checksum; }

    public List<String> getReplicaNodeIds() {
        // Backward compatibility: if replicaNodeIds is empty but storageNodeId is set
        if ((replicaNodeIds == null || replicaNodeIds.isEmpty()) && storageNodeId != null) {
            replicaNodeIds = new ArrayList<>();
            replicaNodeIds.add(storageNodeId);
        }
        return replicaNodeIds;
    }

    public void setReplicaNodeIds(List<String> replicaNodeIds) {
        this.replicaNodeIds = replicaNodeIds != null ? new ArrayList<>(replicaNodeIds) : new ArrayList<>();
    }

    // Backward compatibility getters/setters for Prompt 1 metadata
    public String getStorageNodeId() { return storageNodeId; }
    public void setStorageNodeId(String storageNodeId) { this.storageNodeId = storageNodeId; }

    public String getStorageNodeHost() { return storageNodeHost; }
    public void setStorageNodeHost(String storageNodeHost) { this.storageNodeHost = storageNodeHost; }

    public int getStorageNodePort() { return storageNodePort; }
    public void setStorageNodePort(int storageNodePort) { this.storageNodePort = storageNodePort; }
}
