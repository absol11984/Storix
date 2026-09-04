package com.storix.client;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Data transfer object for object metadata.
 * Used for JSON serialization between client and metadata server.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ObjectMetadataDTO {

    private String objectName;
    private long fileSize;
    private int chunkSize;
    private List<ChunkInfoDTO> chunks;

    public ObjectMetadataDTO() {
        this.chunks = new ArrayList<>();
    }

    public ObjectMetadataDTO(String objectName, long fileSize, int chunkSize) {
        this.objectName = objectName;
        this.fileSize = fileSize;
        this.chunkSize = chunkSize;
        this.chunks = new ArrayList<>();
    }

    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public List<ChunkInfoDTO> getChunks() {
        return chunks;
    }

    public void setChunks(List<ChunkInfoDTO> chunks) {
        this.chunks = chunks != null ? chunks : new ArrayList<>();
    }

    public void addChunk(ChunkInfoDTO chunk) {
        this.chunks.add(chunk);
    }

    /**
     * Derived field - computed from chunks.size().
     * Ignored in JSON to avoid serialization issues with server.
     */
    @JsonIgnore
    public int getChunkCount() {
        return chunks.size();
    }
}
