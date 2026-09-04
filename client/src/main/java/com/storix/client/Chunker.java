package com.storix.client;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Splits files into chunks and reconstructs them.
 * Uses streaming I/O to avoid loading entire files into memory.
 */
public class Chunker {

    private final int chunkSize;

    public Chunker(int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("Chunk size must be positive");
        }
        this.chunkSize = chunkSize;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    /**
     * Splits a file into chunks.
     * @param filePath the file to split
     * @return list of chunk data with their IDs
     * @deprecated Use processChunks for streaming to avoid loading all chunks into memory
     */
    @Deprecated
    public List<ChunkData> splitFile(Path filePath) throws IOException {
        List<ChunkData> chunks = new ArrayList<>();
        processChunks(filePath, chunks::add);
        return chunks;
    }

    /**
     * Processes file chunks one at a time in streaming fashion.
     * Each chunk is passed to the consumer and can be immediately processed/uploaded
     * without keeping all chunks in memory.
     *
     * @param filePath the file to split
     * @param consumer receives each chunk as it's read
     * @return total number of chunks processed
     */
    public int processChunks(Path filePath, Consumer<ChunkData> consumer) throws IOException {
        String baseId = UUID.randomUUID().toString();

        try (InputStream in = new BufferedInputStream(Files.newInputStream(filePath))) {
            byte[] buffer = new byte[chunkSize];
            int chunkIndex = 0;
            int bytesRead;

            while ((bytesRead = in.read(buffer)) != -1) {
                String chunkId = baseId + "-chunk-" + chunkIndex;

                // Copy only the actual bytes read
                byte[] chunkData = new byte[bytesRead];
                System.arraycopy(buffer, 0, chunkData, 0, bytesRead);

                consumer.accept(new ChunkData(chunkId, chunkIndex, chunkData));
                chunkIndex++;
            }

            // Handle empty files - create one empty chunk
            if (chunkIndex == 0) {
                consumer.accept(new ChunkData(baseId + "-chunk-0", 0, new byte[0]));
                chunkIndex = 1;
            }

            return chunkIndex;
        }
    }

    /**
     * Reconstructs a file from chunks.
     * @param chunks the chunks in order
     * @param outputPath where to write the reconstructed file
     */
    public void reconstructFile(List<ChunkData> chunks, Path outputPath) throws IOException {
        // Sort by index to ensure correct order
        chunks.sort((a, b) -> Integer.compare(a.chunkIndex(), b.chunkIndex()));

        try (var out = Files.newOutputStream(outputPath)) {
            for (ChunkData chunk : chunks) {
                out.write(chunk.data());
            }
        }
    }

    /**
     * Represents a single chunk with its ID, index, and data.
     */
    public record ChunkData(String chunkId, int chunkIndex, byte[] data) {}
}
