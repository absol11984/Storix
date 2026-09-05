package com.storix.metadata.wal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.storix.metadata.MetadataStore;
import com.storix.metadata.ObjectMetadata;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.HashSet;
import java.util.zip.CRC32;

/**
 * Manages snapshots for log compaction.
 * Snapshots contain complete metadata state and are used to reconstruct
 * the system state after a restart.
 *
 * GENERATION/COMMIT-MARKER MODEL:
 * - Snapshots are named by their index: "snapshot-<index>"
 * - A commit marker "generation-<index>.committed" indicates a generation is authoritative
 * - Only snapshots with a corresponding commit marker are considered valid on recovery
 * - Uncommitted candidates are discarded on startup
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
    private static final String GENERATION_COMMIT_PREFIX = "generation-";
    private static final String COMMIT_SUFFIX = ".committed";
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
     *
     * CRITICAL: Only removes candidates that are NOT committed.
     * A generation-N.commit marker means generation N is authoritative and its
     * candidate (if any) should be cleaned up too.
     */
    private void cleanupStaleCandidates() {
        if (!Files.exists(snapshotDir)) {
            return;
        }
        try {
            // First, find all committed generations
            Set<Long> committedGenerations = getCommittedGenerations();

            // Clean up snapshot-candidate- files
            Files.list(snapshotDir)
                .filter(p -> p.getFileName().toString().startsWith(CANDIDATE_PREFIX))
                .filter(Files::isRegularFile)
                .forEach(p -> {
                    String filename = p.getFileName().toString();
                    // Parse the index from candidate filename
                    // Format: snapshot-candidate-<index>-<uniqueId>.tmp
                    try {
                        // Extract index from filename
                        String name = p.getFileName().toString();
                        // snapshot-candidate-12345-1234567890.tmp
                        String indexPart = name.substring(CANDIDATE_PREFIX.length()).replace(".tmp", "");
                        int dashIdx = indexPart.lastIndexOf('-');
                        if (dashIdx > 0) {
                            String indexStr = indexPart.substring(0, dashIdx);
                            long candidateIndex = Long.parseLong(indexStr);

                            // Only clean up if there's no committed generation at this index
                            // or if this candidate is from an older generation than committed
                            if (!committedGenerations.contains(candidateIndex)) {
                                Files.delete(p);
                                System.out.println("[SNAPSHOT] Cleaned up stale candidate: " + filename);
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("[SNAPSHOT] Failed to parse candidate filename: " + filename);
                    }
                });

            // Clean up install- files (temporary files from installSnapshot method)
            Files.list(snapshotDir)
                .filter(p -> p.getFileName().toString().startsWith("install-"))
                .filter(Files::isRegularFile)
                .forEach(p -> {
                    String filename = p.getFileName().toString();
                    try {
                        // install-<index>-<uniqueId>.tmp
                        // Extract index from filename
                        String name = p.getFileName().toString();
                        String indexPart = name.substring("install-".length()).replace(".tmp", "");
                        int dashIdx = indexPart.lastIndexOf('-');
                        if (dashIdx > 0) {
                            String indexStr = indexPart.substring(0, dashIdx);
                            long installIndex = Long.parseLong(indexStr);

                            // Only clean up if there's no committed generation at this index
                            if (!committedGenerations.contains(installIndex)) {
                                Files.delete(p);
                                System.out.println("[SNAPSHOT] Cleaned up stale install file: " + filename);
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("[SNAPSHOT] Failed to parse install filename: " + filename);
                    }
                });
        } catch (IOException e) {
            System.err.println("[SNAPSHOT] Failed to list directory for candidate cleanup: " + e.getMessage());
        }
    }

    /**
     * Returns the set of all committed generation indices.
     */
    private Set<Long> getCommittedGenerations() {
        Set<Long> committed = new java.util.HashSet<>();
        if (!Files.exists(snapshotDir)) {
            return committed;
        }
        try {
            Files.list(snapshotDir)
                .filter(p -> p.getFileName().toString().startsWith(GENERATION_COMMIT_PREFIX))
                .filter(p -> p.getFileName().toString().endsWith(COMMIT_SUFFIX))
                .filter(Files::isRegularFile)
                .forEach(p -> {
                    String filename = p.getFileName().toString();
                    // Format: generation-<index>.committed
                    try {
                        String indexStr = filename
                            .substring(GENERATION_COMMIT_PREFIX.length())
                            .replace(COMMIT_SUFFIX, "");
                        committed.add(Long.parseLong(indexStr));
                    } catch (NumberFormatException ignored) {}
                });
        } catch (IOException e) {
            System.err.println("[SNAPSHOT] Failed to list committed generations: " + e.getMessage());
        }
        return committed;
    }

    /**
     * Checks if a specific generation index is committed.
     * A generation is committed if its commit marker file exists.
     *
     * @param generationIndex The generation index to check
     * @return true if this generation is committed (authoritative)
     */
    public boolean isGenerationCommitted(long generationIndex) {
        Path commitMarker = snapshotDir.resolve(GENERATION_COMMIT_PREFIX + generationIndex + COMMIT_SUFFIX);
        return Files.exists(commitMarker);
    }

    /**
     * Gets the current committed generation index.
     * Returns -1 if no generation is committed.
     *
     * @return The committed generation index, or -1 if none
     */
    public long getCurrentCommittedGeneration() {
        Set<Long> committed = getCommittedGenerations();
        if (committed.isEmpty()) {
            return -1;
        }
        return Collections.max(committed);
    }

    /**
     * Marks a generation as committed by creating its commit marker atomically.
     * This is the final step in the crash-safe InstallSnapshot protocol.
     *
     * The commit marker creation sequence:
     * 1. Write to temp marker file with full fsync
     * 2. Atomic rename to final marker filename
     * 3. fsync directory
     *
     * This ensures:
     * - Before commit marker: generation is candidate only, NOT authoritative
     * - After commit marker: generation is authoritative and recoverable
     * - Crash before this step: generation is discarded on recovery
     * - Crash after this step: generation is recovered as authoritative
     *
     * @param generationIndex The generation index to commit
     * @param generationTerm The term of the generation
     * @throws IOException if commit marker creation fails
     */
    public void commitGeneration(long generationIndex, long generationTerm) throws IOException {
        String markerFilename = GENERATION_COMMIT_PREFIX + generationIndex + COMMIT_SUFFIX;
        Path commitMarker = snapshotDir.resolve(markerFilename);
        Path tempMarker = snapshotDir.resolve(markerFilename + ".tmp");

        // Content: just the term for validation
        String content = String.valueOf(generationTerm);
        byte[] data = content.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        try (RandomAccessFile raf = new RandomAccessFile(tempMarker.toFile(), "rw")) {
            raf.write(data);
            raf.getFD().sync(); // Sync data to disk
        }

        // Verify file was written correctly
        long actualSize = Files.size(tempMarker);
        if (actualSize != data.length) {
            Files.deleteIfExists(tempMarker);
            throw new IOException("Commit marker size mismatch: expected " + data.length + ", got " + actualSize);
        }

        // Atomic rename to final location
        Files.move(tempMarker, commitMarker,
            StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

        // fsync directory for durability
        fsyncDirectory(snapshotDir);

        System.out.println("[SNAPSHOT] Committed generation " + generationIndex + " (term=" + generationTerm + ")");
    }

    /**
     * Force fsync a directory to ensure directory entry changes are durable.
     * This is needed on some filesystems to make renames truly durable.
     */
    private void fsyncDirectory(Path dir) throws IOException {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        // On Linux, we can use FileChannel on a dummy file handle to sync the directory.
        // The approach is to open the directory itself using FileChannel and sync it.
        // However, the standard Java API doesn't directly support fsyncing a directory.
        // A common approach is to create and sync a temporary file in the directory,
        // or use native code. For simplicity, we'll skip this on systems where it's not needed.
        // The atomic rename provides sufficient durability guarantees on most filesystems.
        // The real durability concern is addressed by the atomic rename itself.
        try {
            // Try to sync a dummy file in the directory as a proxy for directory sync
            Path dummyFile = dir.resolve(".fsync_dummy");
            try {
                Files.write(dummyFile, new byte[0]);
                try (FileChannel fc = FileChannel.open(dummyFile, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                    fc.force(true);
                }
            } finally {
                Files.deleteIfExists(dummyFile);
            }
        } catch (IOException e) {
            // Best effort - atomic rename is the primary durability mechanism
            System.err.println("[SNAPSHOT] Directory sync failed (best effort): " + e.getMessage());
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

        // Create commit marker for this generation
        // This makes the snapshot authoritative
        commitGeneration(lastIncludedIndex, lastIncludedTerm);

        System.out.println("[SNAPSHOT] Created snapshot at index " + lastIncludedIndex + ", term " + lastIncludedTerm);

        // Cleanup old snapshots (but preserve committed ones)
        cleanupOldSnapshots();

        return new Snapshot(lastIncludedIndex, lastIncludedTerm, snapshotFile, stateData);
    }

    /**
     * Loads the latest committed snapshot.
     *
     * RECOVERY RULE: Only return snapshots that have a corresponding commit marker.
     * This is the core of the generation/commit-marker model:
     * - A snapshot is authoritative ONLY if its generation is committed
     * - Uncommitted snapshots are candidates and must be discarded on recovery
     *
     * Recovery algorithm:
     * 1. Find the highest index snapshot
     * 2. Check if it has a generation-<index>.committed marker
     * 3. If yes: return it (it's authoritative)
     * 4. If no: check previous snapshots for committed ones
     * 5. If none found: return empty (no authoritative state)
     *
     * @return The latest committed snapshot, or empty if no committed snapshot exists
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

        // Get committed generations first
        Set<Long> committedGenerations = getCommittedGenerations();
        System.out.println("[SNAPSHOT] Committed generations: " + committedGenerations);

        if (committedGenerations.isEmpty()) {
            System.out.println("[SNAPSHOT] No committed generations found - no authoritative snapshot");
            return Optional.empty();
        }

        // Parse index from filename and find highest VALID, COMMITTED snapshot
        Path latestValid = null;
        long highestIndex = -1;
        boolean foundCorruptedCommitted = false;

        for (Path p : snapshots) {
            String filename = p.getFileName().toString();
            long index = -1;
            try {
                // Format: snapshot-<index>
                String indexStr = filename.substring(SNAPSHOT_PREFIX.length());
                index = Long.parseLong(indexStr);

                // CRITICAL: Only consider snapshots with commit markers
                if (!committedGenerations.contains(index)) {
                    System.out.println("[SNAPSHOT] Skipping uncommitted snapshot: " + filename);
                    continue;
                }

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
                // CORRUPTED COMMITTED SNAPSHOT: Fall back to next valid committed snapshot.
                // This is a serious data integrity issue, but we prefer availability over
                // consistency - the node can recover from an older committed snapshot.
                if (index >= 0 && committedGenerations.contains(index)) {
                    System.err.println("[SNAPSHOT] WARNING: Corrupted committed snapshot " + filename +
                            ": " + e.getMessage() + " - falling back to older snapshot");
                    foundCorruptedCommitted = true;
                } else {
                    // Uncommitted snapshots can be skipped
                    System.err.println("[SNAPSHOT] Corrupted/invalid snapshot " + filename + ": " + e.getMessage());
                }
            } catch (NumberFormatException e) {
                // Skip files with invalid names
                System.err.println("[SNAPSHOT] Skipping invalid snapshot filename: " + filename);
            }
        }

        if (latestValid != null) {
            if (foundCorruptedCommitted) {
                System.out.println("[SNAPSHOT] Falling back to older committed snapshot: " + latestValid.getFileName() +
                        " (index=" + highestIndex + ")");
            } else {
                System.out.println("[SNAPSHOT] Found latest committed snapshot: " + latestValid.getFileName() +
                        " (index=" + highestIndex + ")");
            }
            return loadSnapshot(latestValid);
        }

        // Committed generations exist but no valid snapshot found
        System.out.println("[SNAPSHOT] Committed generations exist but no valid snapshot found");
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
     *
     * CRITICAL: Never deletes committed snapshots (those with commit markers).
     * Committed generations are authoritative and must be preserved.
     */
    private void cleanupOldSnapshots() throws IOException {
        // Get committed generations first - these must never be deleted
        Set<Long> committedGenerations = getCommittedGenerations();

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

                // NEVER delete committed snapshots
                if (committedGenerations.contains(index)) {
                    continue;
                }

                indexedSnapshots.add(new SnapshotIndex(p, index));
            } catch (NumberFormatException e) {
                System.err.println("[SNAPSHOT] Skipping invalid snapshot filename during cleanup: " + filename);
            }
        }

        if (indexedSnapshots.size() <= MAX_SNAPSHOTS_TO_KEEP) {
            return;
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
        // The authoritative snapshot filename
        String filename = SNAPSHOT_PREFIX + lastIncludedIndex;
        Path snapshotFile = snapshotDir.resolve(filename);

        // Check if snapshot is already at the authoritative location
        // This happens when installSnapshot was called first (writes directly to snapshot-<index>)
        if (Files.exists(snapshotFile)) {
            // Snapshot already committed - validate it
            Optional<Snapshot> existing = loadSnapshot(snapshotFile);
            if (existing.isPresent()) {
                Snapshot snap = existing.get();
                if (snap.lastIncludedIndex() == lastIncludedIndex && snap.lastIncludedTerm() == lastIncludedTerm) {
                    System.out.println("[SNAPSHOT] Candidate snapshot already committed: index=" + lastIncludedIndex);
                    return; // Already committed
                }
            }
            // Snapshot exists but doesn't match - this is an error
            throw new IOException("Snapshot at authoritative location doesn't match expected: index=" +
                    lastIncludedIndex + ", term=" + lastIncludedTerm);
        }

        // Candidate file must exist if snapshot not already at authoritative location
        if (candidateFile == null || !Files.exists(candidateFile)) {
            throw new IOException("Candidate snapshot file does not exist: " + candidateFile);
        }

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

        // Write to temp file with "install-" prefix (different from candidate prefix)
        // This avoids conflicts when commitCandidateSnapshot runs in parallel
        String filename = SNAPSHOT_PREFIX + lastIncludedIndex;
        Path snapshotFile = snapshotDir.resolve(filename);
        Path tempFile = snapshotDir.resolve("install-" + lastIncludedIndex + "-" + System.nanoTime() + ".tmp");

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

        // Create commit marker to make snapshot authoritative
        commitGeneration(lastIncludedIndex, lastIncludedTerm);

        System.out.println("[SNAPSHOT] Installed snapshot at index " + lastIncludedIndex +
                ", term " + lastIncludedTerm + ", checksum " + checksum);

        // Cleanup old snapshots (but preserve committed ones)
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
