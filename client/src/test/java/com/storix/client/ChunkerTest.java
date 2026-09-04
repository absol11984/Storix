package com.storix.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Chunker.
 */
class ChunkerTest {

    @TempDir
    Path tempDir;

    private Chunker chunker;

    @BeforeEach
    void setUp() {
        chunker = new Chunker(1024); // 1 KB chunks
    }

    @Test
    void splitEmptyFile() throws IOException {
        Path emptyFile = tempDir.resolve("empty.txt");
        Files.writeString(emptyFile, "");

        List<Chunker.ChunkData> chunks = chunker.splitFile(emptyFile);

        assertEquals(1, chunks.size());
        assertEquals(0, chunks.get(0).data().length);
    }

    @Test
    void splitSmallFile() throws IOException {
        String content = "Hello, World!";
        Path file = tempDir.resolve("small.txt");
        Files.writeString(file, content);

        List<Chunker.ChunkData> chunks = chunker.splitFile(file);

        assertEquals(1, chunks.size());
        assertEquals(content, new String(chunks.get(0).data()));
    }

    @Test
    void splitExactChunkSize() throws IOException {
        String content = "x".repeat(1024);
        Path file = tempDir.resolve("exact.txt");
        Files.writeString(file, content);

        List<Chunker.ChunkData> chunks = chunker.splitFile(file);

        assertEquals(1, chunks.size());
        assertEquals(1024, chunks.get(0).data().length);
    }

    @Test
    void splitMultipleChunks() throws IOException {
        String content = "x".repeat(2500);
        Path file = tempDir.resolve("multi.txt");
        Files.writeString(file, content);

        List<Chunker.ChunkData> chunks = chunker.splitFile(file);

        assertEquals(3, chunks.size());
        assertEquals(1024, chunks.get(0).data().length);
        assertEquals(1024, chunks.get(1).data().length);
        assertEquals(452, chunks.get(2).data().length); // 2500 - 2048
    }

    @Test
    void chunkIndexing() throws IOException {
        String content = "x".repeat(2500);
        Path file = tempDir.resolve("indexed.txt");
        Files.writeString(file, content);

        List<Chunker.ChunkData> chunks = chunker.splitFile(file);

        for (int i = 0; i < chunks.size(); i++) {
            assertEquals(i, chunks.get(i).chunkIndex());
        }
    }

    @Test
    void reconstructFile() throws IOException {
        String originalContent = "x".repeat(2500);
        Path originalFile = tempDir.resolve("original.txt");
        Files.writeString(originalFile, originalContent);

        // Split
        List<Chunker.ChunkData> chunks = chunker.splitFile(originalFile);

        // Reconstruct
        Path reconstructedFile = tempDir.resolve("reconstructed.txt");
        chunker.reconstructFile(chunks, reconstructedFile);

        String reconstructedContent = Files.readString(reconstructedFile);
        assertEquals(originalContent, reconstructedContent);
    }

    @Test
    void reconstructInWrongOrder() throws IOException {
        String originalContent = "0123456789".repeat(100);
        Path originalFile = tempDir.resolve("original.txt");
        Files.writeString(originalFile, originalContent);

        // Split
        List<Chunker.ChunkData> chunks = chunker.splitFile(originalFile);

        // Reverse chunks
        List<Chunker.ChunkData> reversed = new java.util.ArrayList<>(chunks);
        java.util.Collections.reverse(reversed);

        // Reconstruct (should sort by index)
        Path reconstructedFile = tempDir.resolve("reconstructed.txt");
        chunker.reconstructFile(reversed, reconstructedFile);

        String reconstructedContent = Files.readString(reconstructedFile);
        assertEquals(originalContent, reconstructedContent);
    }

    @Test
    void largeFile() throws IOException {
        // Create a 5 MB file
        byte[] buffer = new byte[1024 * 1024];
        for (int i = 0; i < buffer.length; i++) {
            buffer[i] = (byte) (i % 256);
        }

        Path largeFile = tempDir.resolve("large.bin");
        for (int i = 0; i < 5; i++) {
            Files.write(largeFile, buffer, i == 0
                ? java.nio.file.StandardOpenOption.CREATE
                : java.nio.file.StandardOpenOption.APPEND);
        }

        // Split and reconstruct
        List<Chunker.ChunkData> chunks = chunker.splitFile(largeFile);
        Path reconstructedFile = tempDir.resolve("reconstructed.bin");
        chunker.reconstructFile(chunks, reconstructedFile);

        // Verify
        byte[] original = Files.readAllBytes(largeFile);
        byte[] reconstructed = Files.readAllBytes(reconstructedFile);
        assertArrayEquals(original, reconstructed);
    }
}
