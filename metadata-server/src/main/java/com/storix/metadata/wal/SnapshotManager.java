package com.storix.metadata.wal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.storix.metadata.MetadataStore;
import com.storix.metadata.ObjectMetadata;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.zip.CRC32;

/**
 * Manages snapshots for log compaction.
 * Snapshots contain complete metadata state and are used to reconstruct
 * the system state after a restart.
 *
 * Snapshot format:
 * - MAGIC(8) + VERSION(4) = 12 bytes header
 * - LAST_INCLUDED_INDEX(8) + LAST_INCLUDED_TERM(8) = 16 bytes
 * - STATE_LENGTH(4) + STATE(variable) + CHECKSUM(4)
 */
public class SnapshotManager {

    private static final long SNAPSHOT_MAGIC = 0x534E4150L; // "SNAP"
    private static final int SNAPSHOT_VERSION = 1;
    private static final String SNAPSHOT_PREFIX = "snapshot-";
    private static final int MAX_SNAPSHOTS_TO_KEEP = 2;
    private static final long MAX_SNAPSHOT_SIZE = 100 * 1024 * 1024; // 100MB max

    private final Path snapshotDir;
    private final MetadataStore store;
    private final ObjectMapper objectMapper;

    public SnapshotManager(Path snapshotDir, MetadataStore store) {
        this.snapshotDir = snapshotDir;
        this.store = store;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

        // Clean up any stale candidate files from previous failed installations
        cleanupStaleCandidates();
    }

    /**
     * Cleans up any stale candidate snapshot files from previous failed installations.
     * Called on startup to ensure we don't have orphaned candidate files.
     */
    private void cleanupStaleCandidates() {
        if (!Files.exists(snapshotDir)) {
            return;
        }
        try {
            Files.list(snapshotDir)
                .filter(p -> p.getFileName().toString().contains("-candidate-"))
                .filter(Files::isRegularFile)
                .forEach(p -> {
                    try {
                        Files.delete(p);
                        System.out.println("[SNAPSHOT] Cleaned up stale candidate: " + p.getFileName());
                    } catch (IOException e) {
                        System.err.println("[SNAPSHOT] Failed to clean up candidate: " + p);
                    }
                });
        } catch (IOException e) {
            System.err.println("[SNAPSHOT] Failed to list directory for candidate cleanup: " + e.getMessage());
        }
    }

    /**
     * Takes a snapshot of the current state.
     * Uses atomic write: write to temp file, then atomic rename.
     *
     * @param lastIncludedIndex The last log index included in this snapshot
     * @param lastIncludedTerm The term of the log entry at lastIncludedIndex
     * @return The created snapshot
     * @throws IOException if snapshot creation fails
     */
    public Snapshot takeSnapshot(long lastIncludedIndex, long lastIncludedTerm) throws IOException {
        Files.createDirectories(snapshotDir);

        // Generate snapshot data (complete metadata state)
        byte[] stateData = generateStateSnapshot();

        // Compute checksum of state data
        int checksum = computeChecksum(stateData);

        // Write to temp file first, then atomic rename
        String filename = SNAPSHOT_PREFIX + lastIncludedIndex;
        Path snapshotFile = snapshotDir.resolve(filename);
        Path tempFile = snapshotDir.resolve(filename + ".tmp");

        try (FileOutputStream fos = new FileOutputStream(tempFile.toFile());
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

            // Write state data
            fc.write(ByteBuffer.wrap(stateData));

            // Write checksum
            ByteBuffer checksumBuf = ByteBuffer.allocate(4);
            checksumBuf.putInt(checksum);
            checksumBuf.flip();
            fc.write(checksumBuf);

            fc.force(true);
        }

        // Atomic rename
        Files.move(tempFile, snapshotFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

        System.out.println("[SNAPSHOT] Created snapshot at index " + lastIncludedIndex + ", term " + lastIncludedTerm);

        // Cleanup old snapshots
        cleanupOldSnapshots();

        return new Snapshot(lastIncludedIndex, lastIncludedTerm, snapshotFile, stateData);
    }

    /**
     * Loads the latest snapshot.
     * @return The latest snapshot, or empty if no snapshot exists
     * @throws IOException if snapshot loading fails due to system errors (not corrupted snapshots)
     */
    public Optional<Snapshot> loadLatestSnapshot() throws IOException {
        // Handle case where directory doesn't exist
        if (!Files.exists(snapshotDir)) {
            return Optional.empty();
        }

        List<Path> snapshots;
        try {
            snapshots = Files.list(snapshotDir)
                    .filter(p -> p.getFileName().toString().startsWith(SNAPSHOT_PREFIX))
                    .filter(p -> !p.getFileName().toString().contains("-candidate-")) // Exclude candidate files
                    .filter(Files::isRegularFile)
                    .collect(java.util.stream.Collectors.toList());
        } catch (IOException e) {
            // Directory exists but can't be read - this is an error
            throw e;
        }

        if (snapshots.isEmpty()) {
            return Optional.empty();
        }

        // Parse index from filename and find highest VALID snapshot
        Path latestValid = null;
        long highestIndex = -1;
        boolean hasAnySnapshots = false;

        for (Path p : snapshots) {
            String filename = p.getFileName().toString();
            try {
                // Format: snapshot-<index>
                String indexStr = filename.substring(SNAPSHOT_PREFIX.length());
                long index = Long.parseLong(indexStr);
                hasAnySnapshots = true;

                // Validate the snapshot by attempting to load it
                Optional<Snapshot> snap = loadSnapshot(p);
                if (snap.isPresent()) {
                    // Validate: index should match filename, term should be valid
                    // Accept term >= 0 (single-node clusters may have term=0 before leader election)
                    if (snap.get().lastIncludedIndex() == index && snap.get().lastIncludedTerm() >= 0) {
                        if (index > highestIndex) {
                            highestIndex = index;
                            latestValid = p;
                        }
                    } else {
                        System.err.println("[SNAPSHOT] Invalid snapshot " + filename +
                                ": index mismatch (file=" + index +
                                ", actual=" + snap.get().lastIncludedIndex() +
                                ", term=" + snap.get().lastIncludedTerm() + ")");
                    }
                }
            } catch (IOException e) {
                System.err.println("[SNAPSHOT] Corrupted/invalid snapshot " + filename + ": " + e.getMessage());
                // Skip corrupted snapshots and continue checking others
                // This ensures we find the highest VALID snapshot even if newer ones are corrupted
            } catch (NumberFormatException e) {
                // Skip files with invalid names
                System.err.println("[SNAPSHOT] Skipping invalid snapshot filename: " + filename);
            }
        }

        if (latestValid != null) {
            System.out.println("[SNAPSHOT] Found latest valid snapshot: " + latestValid.getFileName() +
                    " (index=" + highestIndex + ")");
            return loadSnapshot(latestValid);
        }

        // Snapshots exist but none are valid - this is an error condition
        // The system cannot safely recover without at least one valid snapshot
        if (hasAnySnapshots) {
            throw new IOException("No valid snapshots found among existing snapshot files");
        }

        return Optional.empty();
    }

    /**
     * Loads a specific snapshot file.
     * @throws IOException if snapshot is corrupted or cannot be read
     */
    public Optional<Snapshot> loadSnapshot(Path snapshotFile) throws IOException {
        if (!Files.exists(snapshotFile)) {
            return Optional.empty();
        }

        long fileSize = Files.size(snapshotFile);
        if (fileSize < 36) { // Minimum: 12 header + 16 metadata + 4 length + 4 checksum
            throw new IOException("Snapshot file too small: " + snapshotFile);
        }
        if (fileSize > MAX_SNAPSHOT_SIZE) {
            throw new IOException("Snapshot file too large: " + fileSize);
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

            if (stateLength < 0 || stateLength > MAX_SNAPSHOT_SIZE - 36) {
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
                throw new IOException("Snapshot checksum mismatch: expected " + computedChecksum + ", got " + storedChecksum);
            }

            return Optional.of(new Snapshot(lastIncludedIndex, lastIncludedTerm, snapshotFile, stateData));
        }
    }

    /**
     * Cleans up old snapshots, keeping only the most recent ones.
     * Uses logical index (lastIncludedIndex from filename) for ordering.
     */
    private void cleanupOldSnapshots() throws IOException {
        List<Path> allSnapshots = Files.list(snapshotDir)
                .filter(p -> p.getFileName().toString().startsWith(SNAPSHOT_PREFIX))
                .filter(p -> !p.getFileName().toString().contains("-candidate-")) // Exclude candidate files
                .filter(Files::isRegularFile)
                .collect(java.util.stream.Collectors.toList());

        if (allSnapshots.size() <= MAX_SNAPSHOTS_TO_KEEP) {
            return;
        }

        // Parse indices from filenames and sort by index descending
        List<SnapshotIndex> indexedSnapshots = new java.util.ArrayList<>();
        for (Path p : allSnapshots) {
            String filename = p.getFileName().toString();
            try {
                String indexStr = filename.substring(SNAPSHOT_PREFIX.length());
                long index = Long.parseLong(indexStr);
                indexedSnapshots.add(new SnapshotIndex(p, index));
            } catch (NumberFormatException e) {
                System.err.println("[SNAPSHOT] Skipping invalid snapshot filename during cleanup: " + filename);
            }
        }

        // Sort by index descending
        indexedSnapshots.sort((a, b) -> Long.compare(b.index, a.index));

        // Delete older snapshots beyond max (skip first MAX_SNAPSHOTS_TO_KEEP)
        for (int i = MAX_SNAPSHOTS_TO_KEEP; i < indexedSnapshots.size(); i++) {
            Path toDelete = indexedSnapshots.get(i).path;
            try {
                Files.delete(toDelete);
                System.out.println("[SNAPSHOT] Deleted old snapshot: " + toDelete.getFileName());
            } catch (IOException e) {
                System.err.println("[SNAPSHOT] Failed to delete old snapshot: " + toDelete);
            }
        }
    }

    /**
     * Helper class to pair Path with its parsed index.
     */
    private static class SnapshotIndex {
        final Path path;
        final long index;

        SnapshotIndex(Path path, long index) {
            this.path = path;
            this.index = index;
        }
    }

    /**
     * Generates a snapshot of the current metadata state.
     * Serializes all objects with complete metadata.
     */
    private byte[] generateStateSnapshot() throws IOException {
        Map<String, ObjectMetadata> snapshot = new HashMap<>();
        for (String name : store.listObjects()) {
            store.getObject(name).ifPresent(obj -> snapshot.put(name, obj));
        }
        return objectMapper.writeValueAsBytes(snapshot);
    }

    /**
     * Computes CRC32 checksum of data.
     */
    private int computeChecksum(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }

    /**
     * Restores state from a snapshot into the MetadataStore.
     * This replaces the current state completely.
     *
     * @param snapshot The snapshot to restore from
     * @param targetStore The store to restore into
     * @throws IOException if restoration fails
     */
    public void restoreFromSnapshot(Snapshot snapshot, MetadataStore targetStore) throws IOException {
        if (snapshot.stateData() == null || snapshot.stateData().length == 0) {
            throw new IOException("Snapshot has no state data");
        }

        // Verify checksum before restoring
        int expectedChecksum = computeChecksum(snapshot.stateData());

        // Parse snapshot
        @SuppressWarnings("unchecked")
        Map<String, ObjectMetadata> snapshotData = objectMapper.readValue(
            snapshot.stateData(),
            objectMapper.getTypeFactory().constructMapType(HashMap.class, String.class, ObjectMetadata.class)
        );

        // Clear existing state and rebuild from snapshot
        // This ensures the store matches exactly what the snapshot represents
        // Use direct methods to avoid individual saves during clear
        for (String name : new java.util.ArrayList<>(targetStore.listObjects())) {
            targetStore.deleteObjectDirect(name);
        }

        // Restore all objects from snapshot
        for (Map.Entry<String, ObjectMetadata> entry : snapshotData.entrySet()) {
            String name = entry.getKey();
            ObjectMetadata metadata = entry.getValue();
            try {
                if (!targetStore.objectExists(name)) {
                    // Use direct method to avoid individual saves
                    targetStore.createObjectDirect(metadata);
                } else {
                    targetStore.updateObjectDirect(metadata);
                }
            } catch (IllegalStateException e) {
                // Object already exists - update it
                targetStore.updateObjectDirect(metadata);
            }
        }

        // Save once after all objects are restored
        targetStore.save();

        System.out.println("[SNAPSHOT] Restored " + snapshotData.size() + " objects from snapshot at index " + snapshot.lastIncludedIndex());
    }

    /**
     * Returns the snapshot directory for inspection.
     */
    public Path getSnapshotDir() {
        return snapshotDir;
    }

    private static final String CANDIDATE_PREFIX = "snapshot-candidate-";

    /**
     * Persists a snapshot as a candidate (not yet committed).
     * The snapshot is written to a candidate file that will not replace
     * the current authoritative snapshot until commitCandidateSnapshot is called.
     *
     * @param stateData The complete state data (JSON) to store in the snapshot
     * @param lastIncludedIndex The last log index included in this snapshot
     * @param lastIncludedTerm The term of the entry at lastIncludedIndex
     * @param expectedChecksum The expected CRC32 checksum (0 to skip validation)
     * @return Path to the candidate snapshot file
     * @throws IOException if persistence fails
     */
    public Path persistCandidateSnapshot(byte[] stateData, long lastIncludedIndex,
                                        long lastIncludedTerm, int expectedChecksum) throws IOException {
        // Validate checksum if provided
        if (expectedChecksum != 0) {
            int computedChecksum = computeChecksum(stateData);
            if (computedChecksum != expectedChecksum) {
                throw new IOException("Snapshot checksum mismatch: expected " +
                        expectedChecksum + ", computed " + computedChecksum);
            }
        }

        // Validate state data
        if (stateData == null || stateData.length == 0) {
            throw new IOException("Snapshot state data is empty");
        }

        Files.createDirectories(snapshotDir);

        // Write to candidate file with unique ID
        long candidateId = System.nanoTime();
        String candidateFilename = CANDIDATE_PREFIX + lastIncludedIndex + "-" + candidateId + ".tmp";
        Path candidateFile = snapshotDir.resolve(candidateFilename);

        // Compute checksum for the snapshot file
        int checksum = computeChecksum(stateData);

        try (FileOutputStream fos = new FileOutputStream(candidateFile.toFile());
             FileChannel fc = fos.getChannel()) {

            // Format same as regular snapshots
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

        System.out.println("[SNAPSHOT] Persisted candidate snapshot at: " + candidateFile);

        return candidateFile;
    }

    /**
     * Commits a candidate snapshot as the new authoritative snapshot.
     * This atomically replaces any existing snapshot at the same index.
     *
     * @param candidateFile The candidate snapshot file to commit
     * @param lastIncludedIndex The last log index included in this snapshot
     * @param lastIncludedTerm The term of the entry at lastIncludedIndex
     * @throws IOException if commit fails
     */
    public void commitCandidateSnapshot(Path candidateFile, long lastIncludedIndex, long lastIncludedTerm) throws IOException {
        if (candidateFile == null || !Files.exists(candidateFile)) {
            throw new IOException("Candidate snapshot file does not exist: " + candidateFile);
        }

        // The authoritative snapshot filename
        String filename = SNAPSHOT_PREFIX + lastIncludedIndex;
        Path snapshotFile = snapshotDir.resolve(filename);

        // Validate the candidate snapshot can be loaded before committing
        Optional<Snapshot> candidateSnapshot = loadSnapshot(candidateFile);
        if (candidateSnapshot.isEmpty()) {
            throw new IOException("Candidate snapshot is corrupted and cannot be loaded");
        }

        // Verify metadata matches
        Snapshot snap = candidateSnapshot.get();
        if (snap.lastIncludedIndex() != lastIncludedIndex || snap.lastIncludedTerm() != lastIncludedTerm) {
            throw new IOException("Candidate snapshot metadata mismatch: expected index=" +
                    lastIncludedIndex + ", term=" + lastIncludedTerm +
                    ", got index=" + snap.lastIncludedIndex() + ", term=" + snap.lastIncludedTerm());
        }

        // Atomic rename - candidate becomes the new authoritative snapshot
        Files.move(candidateFile, snapshotFile,
                StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

        System.out.println("[SNAPSHOT] Committed candidate snapshot as authoritative: index=" +
                lastIncludedIndex + ", term=" + lastIncludedTerm);

        // Cleanup old snapshots
        cleanupOldSnapshots();
    }

    /**
     * Installs a received snapshot from remote leader.
     * Writes the snapshot atomically to ensure durability.
     * This is used during InstallSnapshot RPC to persist the received snapshot.
     *
     * @param stateData The complete state data (JSON) to store in the snapshot
     * @param lastIncludedIndex The last log index included in this snapshot
     * @param lastIncludedTerm The term of the entry at lastIncludedIndex
     * @param expectedChecksum The expected CRC32 checksum (0 to skip validation)
     * @return The installed snapshot
     * @throws IOException if installation fails
     */
    public Snapshot installSnapshot(byte[] stateData, long lastIncludedIndex,
                                   long lastIncludedTerm, int expectedChecksum) throws IOException {
        // Validate checksum if provided
        if (expectedChecksum != 0) {
            int computedChecksum = computeChecksum(stateData);
            if (computedChecksum != expectedChecksum) {
                throw new IOException("Snapshot checksum mismatch: expected " +
                        expectedChecksum + ", computed " + computedChecksum);
            }
        }

        // Validate state data
        if (stateData == null || stateData.length == 0) {
            throw new IOException("Snapshot state data is empty");
        }

        Files.createDirectories(snapshotDir);

        // Write to temp file, then atomic rename
        String filename = SNAPSHOT_PREFIX + lastIncludedIndex;
        Path snapshotFile = snapshotDir.resolve(filename);
        Path tempFile = snapshotDir.resolve(filename + ".tmp");

        // Compute checksum for the snapshot file
        int checksum = computeChecksum(stateData);

        try (FileOutputStream fos = new FileOutputStream(tempFile.toFile());
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

            // Write state data
            fc.write(ByteBuffer.wrap(stateData));

            // Write checksum
            ByteBuffer checksumBuf = ByteBuffer.allocate(4);
            checksumBuf.putInt(checksum);
            checksumBuf.flip();
            fc.write(checksumBuf);

            fc.force(true);
        }

        // Atomic rename - this is the durable commit point
        Files.move(tempFile, snapshotFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

        System.out.println("[SNAPSHOT] Installed snapshot at index " + lastIncludedIndex +
                ", term " + lastIncludedTerm + ", checksum " + checksum);

        // Cleanup old snapshots
        cleanupOldSnapshots();

        return new Snapshot(lastIncludedIndex, lastIncludedTerm, snapshotFile, stateData);
    }

    /**
     * Represents a snapshot with its metadata.
     */
    public static class Snapshot {
        private final long lastIncludedIndex;
        private final long lastIncludedTerm;
        private final Path filePath;
        private final byte[] stateData;

        public Snapshot(long lastIncludedIndex, long lastIncludedTerm, Path filePath, byte[] stateData) {
            this.lastIncludedIndex = lastIncludedIndex;
            this.lastIncludedTerm = lastIncludedTerm;
            this.filePath = filePath;
            this.stateData = stateData;
        }

        public Snapshot(long lastIncludedIndex, long lastIncludedTerm, Path filePath) {
            this(lastIncludedIndex, lastIncludedTerm, filePath, null);
        }

        public long lastIncludedIndex() {
            return lastIncludedIndex;
        }

        public long lastIncludedTerm() {
            return lastIncludedTerm;
        }

        public Path filePath() {
            return filePath;
        }

        public byte[] stateData() {
            return stateData;
        }

        @Override
        public String toString() {
            return "Snapshot{index=" + lastIncludedIndex + ", term=" + lastIncludedTerm + "}";
        }
    }
}
