package com.storix.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Metadata for a stored object.
 * Contains object info and chunk locations.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ObjectMetadata {

    private String objectName;
    private long fileSize;
    private int chunkSize;
    private List<ChunkInfo> chunks;

    public ObjectMetadata() {
        this.chunks = new ArrayList<>();
    }

    public ObjectMetadata(String objectName, long fileSize, int chunkSize) {
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

    public List<ChunkInfo> getChunks() {
        return Collections.unmodifiableList(chunks);
    }

    public void setChunks(List<ChunkInfo> chunks) {
        this.chunks = new ArrayList<>(chunks);
    }

    public void addChunk(ChunkInfo chunk) {
        this.chunks.add(chunk);
    }

    /**
     * Derived field - computed from chunks.size().
     * Ignored in JSON to avoid serialization issues with client.
     */
    @JsonIgnore
    public int getChunkCount() {
        return chunks.size();
    }
}
