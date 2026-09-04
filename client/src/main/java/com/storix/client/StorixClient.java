package com.storix.client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * High-level client for Storix operations.
 * Orchestrates chunking, metadata management, and chunk storage.
 */
public class StorixClient implements AutoCloseable {

    private final MetadataClient metadataClient;
    private final StorageNodeClient storageClient;
    private final Chunker chunker;
    private final String storageNodeId;
    private final String storageNodeHost;
    private final int storageNodePort;

    public StorixClient(String metadataHost, int metadataPort,
                        String storageNodeHost, int storageNodePort,
                        int chunkSize) {
        this.metadataClient = new MetadataClient(metadataHost, metadataPort);
        this.storageClient = new StorageNodeClient(storageNodeHost, storageNodePort);
        this.chunker = new Chunker(chunkSize);
        this.storageNodeId = "node-1";
        this.storageNodeHost = storageNodeHost;
        this.storageNodePort = storageNodePort;
    }

    /**
     * Uploads a file to Storix.
     * Splits into chunks, stores chunks, and records metadata.
     * Uses streaming to avoid loading all chunks into memory.
     */
    public void putFile(Path filePath) throws IOException {
        String objectName = filePath.getFileName().toString();
        long fileSize = Files.size(filePath);

        System.out.println("Uploading " + objectName);
        System.out.println("File size: " + fileSize + " bytes");

        // Create metadata object
        ObjectMetadataDTO metadata = new ObjectMetadataDTO(objectName, fileSize, chunker.getChunkSize());

        // Track uploaded chunks for cleanup on failure
        List<String> uploadedChunkIds = new ArrayList<>();

        // Connect to storage node
        storageClient.connect();

        // Process chunks one at a time
        final int[] chunkCount = {0};
        try {
            int totalChunks = chunker.processChunks(filePath, chunk -> {
                chunkCount[0]++;
                System.out.println("Uploading chunk " + chunkCount[0]);

                try {
                    // Store chunk
                    storageClient.putChunk(chunk.chunkId(), chunk.data());

                    // Add chunk info to metadata
                    ChunkInfoDTO chunkInfo = new ChunkInfoDTO(
                        chunk.chunkId(),
                        chunk.chunkIndex(),
                        chunk.data().length,
                        storageNodeId,
                        storageNodeHost,
                        storageNodePort
                    );
                    metadata.addChunk(chunkInfo);

                    // Track for potential cleanup
                    uploadedChunkIds.add(chunk.chunkId());
                } catch (IOException e) {
                    throw new RuntimeException("Failed to upload chunk " + chunk.chunkId(), e);
                }
            });

            System.out.println("Total chunks: " + totalChunks);

            // Save metadata
            metadataClient.connect();
            metadataClient.createObject(metadata);

            System.out.println("Upload successful");
        } catch (RuntimeException e) {
            // Clean up uploaded chunks on failure
            for (String chunkId : uploadedChunkIds) {
                try {
                    storageClient.deleteChunk(chunkId);
                } catch (IOException ignored) {}
            }
            throw new IOException("Upload failed: " + e.getMessage(), e);
        }
    }

    /**
     * Downloads a file from Storix.
     * Retrieves metadata, downloads chunks, and reconstructs the file.
     * Uses streaming to avoid loading all chunks into memory.
     */
    public void getFile(String objectName, Path outputPath) throws IOException {
        System.out.println("Downloading " + objectName);

        // Get metadata
        metadataClient.connect();
        ObjectMetadataDTO metadata = metadataClient.getObject(objectName);
        if (metadata == null) {
            throw new IOException("Object not found: " + objectName);
        }

        System.out.println("Chunks to download: " + metadata.getChunkCount());

        // Connect to storage node once
        storageClient.connect();

        // Sort chunks by index to ensure correct order
        List<ChunkInfoDTO> sortedChunks = new ArrayList<>(metadata.getChunks());
        sortedChunks.sort((a, b) -> Integer.compare(a.getChunkIndex(), b.getChunkIndex()));

        // Stream chunks directly to output file
        try (var out = Files.newOutputStream(outputPath)) {
            int chunkNum = 1;
            for (ChunkInfoDTO chunkInfo : sortedChunks) {
                System.out.println("Downloading chunk " + chunkNum + "/" + metadata.getChunkCount());

                byte[] data = storageClient.getChunk(chunkInfo.getChunkId());
                out.write(data);
                chunkNum++;
            }
        }

        System.out.println("Download successful: " + outputPath);
    }

    /**
     * Deletes an object from Storix.
     * Removes chunks and metadata.
     *
     * Deletion strategy:
     * 1. Get metadata to find all chunks
     * 2. Attempt to delete all chunks from storage node
     * 3. Track any chunk deletion failures
     * 4. Only delete metadata if ALL chunks were successfully deleted
     * 5. If any chunk deletion fails, report error without deleting metadata
     *
     * This prevents orphaned chunks and ensures consistency.
     */
    public void deleteObject(String objectName) throws IOException {
        System.out.println("Deleting " + objectName);

        // Get metadata
        metadataClient.connect();
        ObjectMetadataDTO metadata = metadataClient.getObject(objectName);
        if (metadata == null) {
            throw new IOException("Object not found: " + objectName);
        }

        // Delete chunks from storage node
        storageClient.connect();
        List<String> failedChunks = new ArrayList<>();

        for (ChunkInfoDTO chunkInfo : metadata.getChunks()) {
            try {
                storageClient.deleteChunk(chunkInfo.getChunkId());
            } catch (IOException e) {
                System.err.println("Failed to delete chunk " + chunkInfo.getChunkId() + ": " + e.getMessage());
                failedChunks.add(chunkInfo.getChunkId());
            }
        }

        // If any chunk deletion failed, do NOT delete metadata
        if (!failedChunks.isEmpty()) {
            throw new IOException("Failed to delete " + failedChunks.size() + " chunk(s): " +
                String.join(", ", failedChunks) + ". Metadata preserved for recovery.");
        }

        // All chunks deleted successfully, now delete metadata
        metadataClient.connect();
        boolean metadataDeleted = metadataClient.deleteObject(objectName);
        if (!metadataDeleted) {
            throw new IOException("Failed to delete metadata for " + objectName);
        }

        System.out.println("Delete successful");
    }

    /**
     * Gets information about an object.
     */
    public ObjectMetadataDTO getInfo(String objectName) throws IOException {
        metadataClient.connect();
        return metadataClient.getObject(objectName);
    }

    /**
     * Lists all objects.
     */
    public String[] listObjects() throws IOException {
        metadataClient.connect();
        return metadataClient.listObjects();
    }

    public void close() throws IOException {
        metadataClient.close();
        storageClient.close();
    }
}
