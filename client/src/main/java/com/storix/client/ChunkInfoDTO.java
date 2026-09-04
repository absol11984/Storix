package com.storix.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Data transfer object for chunk information.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ChunkInfoDTO {

    private String chunkId;
    private int chunkIndex;
    private int chunkSize;
    private String checksum;

    private List<String> replicaNodeIds;

    // Backward compatibility fields
    private String storageNodeId;
    private String storageNodeHost;
    private int storageNodePort;

    public ChunkInfoDTO() {
        this.replicaNodeIds = new ArrayList<>();
    }

    public ChunkInfoDTO(String chunkId, int chunkIndex, int chunkSize,
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

    public ChunkInfoDTO(String chunkId, int chunkIndex, int chunkSize,
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
        if ((replicaNodeIds == null || replicaNodeIds.isEmpty()) && storageNodeId != null) {
            replicaNodeIds = new ArrayList<>();
            replicaNodeIds.add(storageNodeId);
        }
        return replicaNodeIds;
    }
    public void setReplicaNodeIds(List<String> replicaNodeIds) {
        this.replicaNodeIds = replicaNodeIds != null ? new ArrayList<>(replicaNodeIds) : new ArrayList<>();
    }

    public String getStorageNodeId() { return storageNodeId; }
    public void setStorageNodeId(String storageNodeId) { this.storageNodeId = storageNodeId; }

    public String getStorageNodeHost() { return storageNodeHost; }
    public void setStorageNodeHost(String storageNodeHost) { this.storageNodeHost = storageNodeHost; }

    public int getStorageNodePort() { return storageNodePort; }
    public void setStorageNodePort(int storageNodePort) { this.storageNodePort = storageNodePort; }
}
