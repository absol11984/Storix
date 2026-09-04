package com.storix.client;

/**
 * Data transfer object for chunk information.
 */
public class ChunkInfoDTO {

    private String chunkId;
    private int chunkIndex;
    private int chunkSize;
    private String storageNodeId;
    private String storageNodeHost;
    private int storageNodePort;

    public ChunkInfoDTO() {}

    public ChunkInfoDTO(String chunkId, int chunkIndex, int chunkSize,
                        String storageNodeId, String storageNodeHost, int storageNodePort) {
        this.chunkId = chunkId;
        this.chunkIndex = chunkIndex;
        this.chunkSize = chunkSize;
        this.storageNodeId = storageNodeId;
        this.storageNodeHost = storageNodeHost;
        this.storageNodePort = storageNodePort;
    }

    public String getChunkId() {
        return chunkId;
    }

    public void setChunkId(String chunkId) {
        this.chunkId = chunkId;
    }

    public int getChunkIndex() {
        return chunkIndex;
    }

    public void setChunkIndex(int chunkIndex) {
        this.chunkIndex = chunkIndex;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public String getStorageNodeId() {
        return storageNodeId;
    }

    public void setStorageNodeId(String storageNodeId) {
        this.storageNodeId = storageNodeId;
    }

    public String getStorageNodeHost() {
        return storageNodeHost;
    }

    public void setStorageNodeHost(String storageNodeHost) {
        this.storageNodeHost = storageNodeHost;
    }

    public int getStorageNodePort() {
        return storageNodePort;
    }

    public void setStorageNodePort(int storageNodePort) {
        this.storageNodePort = storageNodePort;
    }
}
