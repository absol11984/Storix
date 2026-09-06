package com.storix.metadata.wal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.storix.metadata.ObjectMetadata;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.zip.CRC32;

/**
 * Manages immutable generation directories with atomic CURRENT pointer.
 *
 * ARCHITECTURE:
 * storage/
 * ├── generations/
 * │   ├── gen-1/
 * │   │   ├── metadata.json
 * │   │   ├── snapshot.bin
 * │   │   └── manifest.json
 * │   └── gen-2/
 * │       ├── metadata.json
 * │       ├── snapshot.bin
 * │       └── manifest.json
 * └── CURRENT
 *
 * RULES:
 * 1. CURRENT is the ONLY authoritative generation pointer
 * 2. Each generation is immutable after becoming authoritative
 * 3. Never modify gen-N/ files after CURRENT=N
 * 4. Candidate generation can exist without becoming authoritative
 * 5. Crash before CURRENT switch: old generation remains authoritative
 * 6. Crash after CURRENT switch: new generation is authoritative
 */
public class GenerationManager {

    public static final String GENERATIONS_DIR = "generations";
    public static final String GEN_PREFIX = "gen-";
    public static final String CURRENT_FILE = "CURRENT";
    public static final String METADATA_FILE = "metadata.json";
    public static final String SNAPSHOT_FILE = "snapshot.bin";
    public static final String MANIFEST_FILE = "manifest.json";

    private static final long SNAPSHOT_MAGIC = 0x534E4150L; // "SNAP"
    private static final int SNAPSHOT_VERSION = 1;

    private final Path storageDir;
    private final Path generationsDir;
    private final Path currentFile;
    private final ObjectMapper objectMapper;

    public GenerationManager(Path storageDir) throws IOException {
        this.storageDir = storageDir;
        this.generationsDir = storageDir.resolve(GENERATIONS_DIR);
        this.currentFile = storageDir.resolve(CURRENT_FILE);
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

        // Ensure directories exist
        Files.createDirectories(generationsDir);
    }

    /**
     * Gets the current authoritative generation number.
     * Returns -1 if no generation is current (fresh start).
     */
    public long getCurrentGeneration() throws IOException {
        if (!Files.exists(currentFile)) {
            return -1;
        }

        String content = Files.readString(currentFile).trim();
        try {
            return Long.parseLong(content);
        } catch (NumberFormatException e) {
            throw new IOException("Invalid CURRENT file content: " + content, e);
        }
    }

    /**
     * Checks if a generation is authoritative (matches CURRENT).
     */
    public boolean isAuthoritative(long generation) throws IOException {
        return generation == getCurrentGeneration();
    }

    /**
     * Gets the path for a specific generation directory.
     */
    public Path getGenerationDir(long generation) {
        return generationsDir.resolve(GEN_PREFIX + generation);
    }

    /**
     * Checks if a generation directory exists.
     */
    public boolean generationExists(long generation) {
        return Files.exists(getGenerationDir(generation));
    }

    /**
     * Creates a new generation directory for the candidate.
     * The candidate is NOT authoritative until CURRENT is switched.
     *
     * @param generation The generation number
     * @return Path to the new generation directory
     */
    public Path createCandidateGeneration(long generation) throws IOException {
        Path genDir = getGenerationDir(generation);

        if (Files.exists(genDir)) {
            throw new IOException("Generation directory already exists: " + genDir);
        }

        Files.createDirectories(genDir);
        return genDir;
    }

    /**
     * Writes metadata to a generation directory.
     *
     * @throws IOException if the generation is already authoritative (immutable)
     */
    public void writeMetadata(long generation, Map<String, ObjectMetadata> objects) throws IOException {
        // Immutability check: cannot modify an authoritative generation
        if (isAuthoritative(generation)) {
            throw new IOException("FATAL: Attempted to modify authoritative generation " +
                generation + ". Generations are IMMUTABLE after becoming authoritative. " +
                "Use prepareNextGeneration() to create a new generation for mutations.");
        }

        Path genDir = getGenerationDir(generation);
        Path metadataFile = genDir.resolve(METADATA_FILE);

        objectMapper.writeValue(metadataFile.toFile(), objects);

        // fsync metadata file
        try (RandomAccessFile raf = new RandomAccessFile(metadataFile.toFile(), "rw")) {
            raf.getFD().sync();
        }
    }

    /**
     * Writes snapshot to a generation directory.
     *
     * @throws IOException if the generation is already authoritative (immutable)
     */
    public void writeSnapshot(long generation, long lastIncludedIndex, long lastIncludedTerm,
                            byte[] stateData, int checksum) throws IOException {
        // Immutability check: cannot modify an authoritative generation
        if (isAuthoritative(generation)) {
            throw new IOException("FATAL: Attempted to modify authoritative generation " +
                generation + ". Generations are IMMUTABLE after becoming authoritative.");
        }

        Path genDir = getGenerationDir(generation);
        Path snapshotFile = genDir.resolve(SNAPSHOT_FILE);

        try (FileOutputStream fos = new FileOutputStream(snapshotFile.toFile());
             FileChannel fc = fos.getChannel()) {

            // Format:
            // MAGIC(8) + VERSION(4) = 12 bytes header
            // LAST_INCLUDED_INDEX(8) + LAST_INCLUDED_TERM(8) = 16 bytes
            // STATE_LENGTH(4) + STATE(N) + CHECKSUM(4)
            ByteBuffer headerBuf = ByteBuffer.allocate(12 + 16 + 4);
            headerBuf.putLong(SNAPSHOT_MAGIC);
            headerBuf.putInt(SNAPSHOT_VERSION);
            headerBuf.putLong(lastIncludedIndex);
            headerBuf.putLong(lastIncludedTerm);
            headerBuf.putInt(stateData.length);
            headerBuf.flip();
            fc.write(headerBuf);

            fc.write(ByteBuffer.wrap(stateData));

            ByteBuffer checksumBuf = ByteBuffer.allocate(4);
            checksumBuf.putInt(checksum);
            checksumBuf.flip();
            fc.write(checksumBuf);

            fc.force(true);
        }
    }

    /**
     * Writes manifest to a generation directory.
     *
     * @throws IOException if the generation is already authoritative (immutable)
     */
    public void writeManifest(long generation, long lastIncludedIndex, long lastIncludedTerm,
                            int snapshotChecksum) throws IOException {
        // Immutability check: cannot modify an authoritative generation
        if (isAuthoritative(generation)) {
            throw new IOException("FATAL: Attempted to modify authoritative generation " +
                generation + ". Generations are IMMUTABLE after becoming authoritative.");
        }

        Path genDir = getGenerationDir(generation);
        Path manifestFile = genDir.resolve(MANIFEST_FILE);

        GenerationManifest manifest = new GenerationManifest(
            generation, lastIncludedIndex, lastIncludedTerm, snapshotChecksum
        );

        objectMapper.writeValue(manifestFile.toFile(), manifest);

        // fsync manifest file
        try (RandomAccessFile raf = new RandomAccessFile(manifestFile.toFile(), "rw")) {
            raf.getFD().sync();
        }
    }

    /**
     * Reads metadata from a generation directory.
     */
    @SuppressWarnings("unchecked")
    public Map<String, ObjectMetadata> readMetadata(long generation) throws IOException {
        Path genDir = getGenerationDir(generation);
        Path metadataFile = genDir.resolve(METADATA_FILE);

        if (!Files.exists(metadataFile)) {
            throw new IOException("Metadata file not found for generation " + generation);
        }

        // Read as generic Map first, then convert values to ObjectMetadata
        Map<String, Object> rawMap = objectMapper.readValue(metadataFile.toFile(), Map.class);
        Map<String, ObjectMetadata> result = new java.util.HashMap<>();
        for (Map.Entry<String, Object> entry : rawMap.entrySet()) {
            String json = objectMapper.writeValueAsString(entry.getValue());
            ObjectMetadata obj = objectMapper.readValue(json, ObjectMetadata.class);
            result.put(entry.getKey(), obj);
        }
        return result;
    }

    /**
     * Reads and validates a snapshot from a generation directory.
     */
    public SnapshotData readSnapshot(long generation) throws IOException {
        Path genDir = getGenerationDir(generation);
        Path snapshotFile = genDir.resolve(SNAPSHOT_FILE);

        if (!Files.exists(snapshotFile)) {
            throw new IOException("Snapshot file not found for generation " + generation);
        }

        long fileSize = Files.size(snapshotFile);
        if (fileSize < 36) {
            throw new IOException("Snapshot file too small: " + snapshotFile);
        }

        try (FileInputStream fis = new FileInputStream(snapshotFile.toFile());
             FileChannel fc = fis.getChannel()) {

            // Read header
            ByteBuffer headerBuf = ByteBuffer.allocate(12);
            if (fc.read(headerBuf) != 12) {
                throw new IOException("Failed to read snapshot header");
            }
            headerBuf.flip();

            long magic = headerBuf.getLong();
            if (magic != SNAPSHOT_MAGIC) {
                throw new IOException("Invalid snapshot magic: " + Long.toHexString(magic));
            }

            int version = headerBuf.getInt();
            if (version != SNAPSHOT_VERSION) {
                throw new IOException("Unsupported snapshot version: " + version);
            }

            // Read metadata
            ByteBuffer metaBuf = ByteBuffer.allocate(16);
            if (fc.read(metaBuf) != 16) {
                throw new IOException("Failed to read snapshot metadata");
            }
            metaBuf.flip();
            long lastIncludedIndex = metaBuf.getLong();
            long lastIncludedTerm = metaBuf.getLong();

            // Read state length
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            if (fc.read(lenBuf) != 4) {
                throw new IOException("Failed to read state length");
            }
            lenBuf.flip();
            int stateLength = lenBuf.getInt();

            if (stateLength < 0 || stateLength > 100 * 1024 * 1024) {
                throw new IOException("Invalid state length: " + stateLength);
            }

            // Read state data
            byte[] stateData = new byte[stateLength];
            ByteBuffer stateBuf = ByteBuffer.wrap(stateData);
            while (stateBuf.hasRemaining()) {
                int read = fc.read(stateBuf);
                if (read == -1) {
                    throw new IOException("Unexpected end of snapshot file");
                }
            }

            // Read checksum
            ByteBuffer checksumBuf = ByteBuffer.allocate(4);
            if (fc.read(checksumBuf) != 4) {
                throw new IOException("Failed to read checksum");
            }
            checksumBuf.flip();
            int storedChecksum = checksumBuf.getInt();

            // Verify checksum
            int computedChecksum = computeChecksum(stateData);
            if (storedChecksum != computedChecksum) {
                throw new IOException("Snapshot checksum mismatch: expected " +
                    computedChecksum + ", got " + storedChecksum);
            }

            return new SnapshotData(lastIncludedIndex, lastIncludedTerm, stateData, storedChecksum);
        }
    }

    /**
     * Reads manifest from a generation directory.
     */
    public GenerationManifest readManifest(long generation) throws IOException {
        Path genDir = getGenerationDir(generation);
        Path manifestFile = genDir.resolve(MANIFEST_FILE);

        if (!Files.exists(manifestFile)) {
            throw new IOException("Manifest file not found for generation " + generation);
        }

        return objectMapper.readValue(manifestFile.toFile(), GenerationManifest.class);
    }

    /**
     * Validates a complete generation for consistency.
     */
    public void validateGeneration(long generation) throws IOException {
        Path genDir = getGenerationDir(generation);

        // Check directory exists
        if (!Files.exists(genDir)) {
            throw new IOException("Generation directory does not exist: " + genDir);
        }

        // Read and validate manifest
        GenerationManifest manifest = readManifest(generation);

        // Verify generation number in manifest
        if (manifest.generation != generation) {
            throw new IOException("Generation mismatch in manifest: expected " +
                generation + ", got " + manifest.generation);
        }

        // Read and validate snapshot
        SnapshotData snapshot = readSnapshot(generation);

        // Verify snapshot index/term match manifest
        if (snapshot.lastIncludedIndex != manifest.lastIncludedIndex) {
            throw new IOException("Snapshot index mismatch: manifest says " +
                manifest.lastIncludedIndex + ", snapshot has " + snapshot.lastIncludedIndex);
        }

        if (snapshot.lastIncludedTerm != manifest.lastIncludedTerm) {
            throw new IOException("Snapshot term mismatch: manifest says " +
                manifest.lastIncludedTerm + ", snapshot has " + snapshot.lastIncludedTerm);
        }

        // Verify snapshot checksum matches manifest
        if (snapshot.checksum != manifest.snapshotChecksum) {
            throw new IOException("Snapshot checksum mismatch: manifest says " +
                manifest.snapshotChecksum + ", snapshot has " + snapshot.checksum);
        }

        // Validate metadata exists and can be parsed
        readMetadata(generation);
    }

    /**
     * Atomically switches CURRENT to a new generation.
     * This is the ONE logical commit point.
     *
     * @param generation The generation number to make authoritative
     */
    public void switchCurrent(long generation) throws IOException {
        // Validate generation exists
        if (!generationExists(generation)) {
            throw new IOException("Cannot switch to non-existent generation: " + generation);
        }

        // Validate complete generation before switching
        validateGeneration(generation);

        // Write CURRENT.tmp with generation number
        Path tempFile = currentFile.resolveSibling(CURRENT_FILE + ".tmp");
        Files.writeString(tempFile, String.valueOf(generation));

        // fsync temp file
        try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw")) {
            raf.getFD().sync();
        }

        // Atomically replace CURRENT with CURRENT.tmp
        Files.move(tempFile, currentFile,
            StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

        // fsync directory (best effort)
        fsyncDirectory(storageDir);

        System.out.println("[GEN] Switched CURRENT to generation " + generation);
    }

    /**
     * Loads the authoritative generation state.
     * Returns null if no generation is current.
     */
    public GenerationState loadAuthoritativeState() throws IOException {
        long currentGen = getCurrentGeneration();
        if (currentGen < 0) {
            return null;
        }

        // Validate the generation is complete and consistent
        validateGeneration(currentGen);

        // Load metadata
        Map<String, ObjectMetadata> objects = readMetadata(currentGen);

        // Load manifest
        GenerationManifest manifest = readManifest(currentGen);

        // Load snapshot
        SnapshotData snapshot = readSnapshot(currentGen);

        return new GenerationState(currentGen, manifest.lastIncludedIndex,
            manifest.lastIncludedTerm, objects, snapshot.stateData);
    }

    /**
     * Deletes a candidate generation that was never committed.
     * Only deletes if it's not the current generation.
     */
    public void deleteCandidateGeneration(long generation) throws IOException {
        long current = getCurrentGeneration();
        if (generation == current) {
            throw new IOException("Cannot delete authoritative generation: " + generation);
        }

        Path genDir = getGenerationDir(generation);
        if (Files.exists(genDir)) {
            // Delete all files in the directory
            try (var stream = Files.list(genDir)) {
                for (Path file : stream.toList()) {
                    Files.delete(file);
                }
            }
            Files.delete(genDir);
            System.out.println("[GEN] Deleted candidate generation: " + generation);
        }
    }

    /**
     * Lists all generation directories.
     */
    public List<Long> listGenerations() {
        List<Long> generations = new ArrayList<>();
        try (var stream = Files.list(generationsDir)) {
            for (Path dir : stream.toList()) {
                String name = dir.getFileName().toString();
                if (name.startsWith(GEN_PREFIX) && Files.isDirectory(dir)) {
                    try {
                        String numStr = name.substring(GEN_PREFIX.length());
                        generations.add(Long.parseLong(numStr));
                    } catch (NumberFormatException ignored) {}
                }
            }
        } catch (IOException e) {
            System.err.println("[GEN] Failed to list generations: " + e.getMessage());
        }
        return generations;
    }

    /**
     * Cleans up old generation directories, keeping only the current one.
     * Called on startup to remove orphaned candidates.
     */
    public void cleanupOldCandidates() throws IOException {
        long current = getCurrentGeneration();

        for (Long gen : listGenerations()) {
            if (gen != current) {
                // Delete candidate - it was never committed
                try {
                    deleteCandidateGeneration(gen);
                } catch (IOException e) {
                    System.err.println("[GEN] Failed to delete candidate generation " + gen + ": " + e.getMessage());
                }
            }
        }
    }

    /**
     * Returns the path to the current generation's metadata file.
     * @return Path to metadata.json in current generation, or null if no current generation
     */
    public Path getCurrentMetadataFile() throws IOException {
        long currentGen = getCurrentGeneration();
        if (currentGen < 0) {
            return null;
        }
        return getGenerationDir(currentGen).resolve(METADATA_FILE);
    }

    /**
     * Initializes the first generation for a fresh cluster start.
     * Creates gen-1 with empty state, manifest, and snapshot files.
     * Switches CURRENT to 1 to make it authoritative.
     * Safe to call multiple times - no-op if already initialized.
     */
    public void initializeFirstGeneration() throws IOException {
        if (getCurrentGeneration() >= 0) {
            return; // Already initialized
        }
        createCandidateGeneration(1);

        // Write metadata.json with empty state
        Map<String, ObjectMetadata> emptyState = new HashMap<>();
        writeMetadata(1, emptyState);

        // Write empty snapshot and manifest for generation 1 (index=0, term=0 for fresh start)
        byte[] emptySnapshot = objectMapper.writeValueAsBytes(emptyState);
        int checksum = computeChecksum(emptySnapshot);
        writeSnapshot(1, 0, 0, emptySnapshot, checksum);
        writeManifest(1, 0, 0, checksum);

        // Switch CURRENT to make gen-1 authoritative
        switchCurrent(1);
        System.out.println("[GEN] Initialized first generation: gen-1 (empty state)");
    }

    /**
     * Prepares the next generation for snapshot materialization.
     * Creates a candidate directory for the next generation number.
     * The caller is responsible for writing state and calling switchCurrent().
     * @param snapshotIndex The Raft snapshot index (used for logging)
     * @return The new generation number
     */
    public long prepareNextGeneration(long snapshotIndex) throws IOException {
        long currentGen = getCurrentGeneration();
        // If no current generation (fresh start), start from generation 1
        long nextGen = (currentGen >= 0) ? currentGen + 1 : 1;
        createCandidateGeneration(nextGen);
        System.out.println("[GEN] Prepared generation " + nextGen + " for snapshot at index " + snapshotIndex);
        return nextGen;
    }

    private void fsyncDirectory(Path dir) {
        try {
            Path dummyFile = dir.resolve(".fsync_dummy");
            try {
                Files.write(dummyFile, new byte[0]);
                try (FileChannel fc = FileChannel.open(dummyFile,
                        StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                    fc.force(true);
                }
            } finally {
                Files.deleteIfExists(dummyFile);
            }
        } catch (IOException e) {
            System.err.println("[GEN] Directory sync failed (best effort): " + e.getMessage());
        }
    }

    private int computeChecksum(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }

    /**
     * Manifest file structure.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class GenerationManifest {
        public long generation;
        public long lastIncludedIndex;
        public long lastIncludedTerm;
        public int snapshotChecksum;
        public long createdAt;

        public GenerationManifest() {}

        public GenerationManifest(long generation, long lastIncludedIndex,
                                 long lastIncludedTerm, int snapshotChecksum) {
            this.generation = generation;
            this.lastIncludedIndex = lastIncludedIndex;
            this.lastIncludedTerm = lastIncludedTerm;
            this.snapshotChecksum = snapshotChecksum;
            this.createdAt = System.currentTimeMillis();
        }
    }

    /**
     * Snapshot data structure.
     */
    public record SnapshotData(long lastIncludedIndex, long lastIncludedTerm,
                              byte[] stateData, int checksum) {}

    /**
     * Complete generation state.
     */
    public record GenerationState(long generation, long lastIncludedIndex, long lastIncludedTerm,
                                 Map<String, ObjectMetadata> objects, byte[] snapshotData) {}
}
