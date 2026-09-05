package com.storix.metadata.wal;

import com.storix.metadata.raft.LogEntry;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

/**
 * Write-Ahead Log for durability.
 * Provides fsync'd writes for crash recovery.
 *
 * WAL file format:
 * - Header: MAGIC(8) + VERSION(4) + HEADER_END_MARKER(4) = 16 bytes
 * - State record: STATE_RECORD_TYPE(1) + term(8) + votedForLen(4) + votedFor(variable) + commitIndex(8) + lastApplied(8)
 * - Log entries: ENTRY_RECORD_TYPE(1) + term(8) + index(8) + timestamp(8) + opType(1) + dataLen(4) + data(variable) + checksum(4)
 *
 * All reads use exact-byte semantics (readFully) to ensure reliability.
 * Recovery distinguishes: complete records, clean EOF, truncated final record, internal corruption.
 */
public class WAL implements AutoCloseable {

    // Magic number to identify WAL file
    private static final long WAL_MAGIC = 0x57414C01L; // "WAL\001"
    private static final int WAL_VERSION = 3; // Version 3 adds readFully semantics and proper corruption handling
    private static final int HEADER_END_MARKER = 0xDEADBEEF;
    private static final byte STATE_RECORD_TYPE = 1;
    private static final byte ENTRY_RECORD_TYPE = 2;

    // Configurable maximum record sizes
    private static final int MAX_DATA_SIZE = 10 * 1024 * 1024; // 10MB max data size
    private static final int MAX_VOTED_FOR_SIZE = 256; // 256 bytes max votedFor string
    private static final int MAX_ENTRY_RECORD_SIZE = 4 + MAX_DATA_SIZE + 4; // dataLen + data + checksum
    private static final int MAX_STATE_RECORD_SIZE = 4 + MAX_VOTED_FOR_SIZE + 16; // votedForLen + votedFor + indices

    private final Path walFile;
    private FileChannel channel;
    private final ReentrantLock lock = new ReentrantLock();
    private volatile boolean closed = false;

    // For recovery tracking
    private volatile long lastSyncedIndex = 0;

    // Recovered state
    private volatile long recoveredTerm = 0;
    private volatile String recoveredVotedFor = null;
    private volatile long recoveredCommitIndex = 0;
    private volatile long recoveredLastApplied = 0;

    // Pending state for next write
    private long pendingTerm = 0;
    private String pendingVotedFor = null;

    /**
     * Recovery exception with detailed information.
     */
    public static class WALRecoveryException extends IOException {
        public enum RecoveryStatus {
            CORRUPTED_RECORD,
            TRUNCATED_TAIL,
            INVALID_FORMAT,
            CHECKSUM_MISMATCH
        }

        private final RecoveryStatus status;

        public WALRecoveryException(String message, RecoveryStatus status) {
            super(message);
            this.status = status;
        }

        public RecoveryStatus getStatus() {
            return status;
        }
    }

    public WAL(Path walFile) throws IOException {
        this.walFile = walFile;
        Files.createDirectories(walFile.getParent());

        // CREATE if not exists, READ for recovery, WRITE for appending, SYNC for durability
        this.channel = FileChannel.open(walFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.SYNC);

        // Seek to end of file for appending (or start if new file)
        channel.position(channel.size());

        // Write header if new file
        if (channel.size() == 0) {
            writeHeader();
            // After writing header, seek to end for appending
            channel.position(channel.size());
        }
    }

    /**
     * Reads exactly the requested number of bytes from the channel.
     * Throws WALRecoveryException if unable to read the exact number of bytes.
     *
     * @param buffer The buffer to fill
     * @param position Current file position (will be incremented by length)
     * @param length Number of bytes to read
     * @throws WALRecoveryException if unable to read exact bytes (EOF or error)
     */
    private void readFully(ByteBuffer buffer, long position, int length) throws IOException {
        // Ensure buffer has enough capacity
        if (buffer.capacity() < length) {
            throw new IllegalArgumentException("Buffer capacity " + buffer.capacity() + " < required length " + length);
        }
        buffer.clear();
        buffer.limit(length);

        long totalRead = 0;
        while (totalRead < length) {
            long bytesRead = channel.read(buffer, position + totalRead);
            if (bytesRead <= 0) {
                throw new WALRecoveryException(
                    "Unexpected EOF while reading WAL: expected " + length + " bytes, read " + totalRead,
                    WALRecoveryException.RecoveryStatus.TRUNCATED_TAIL
                );
            }
            totalRead += bytesRead;
        }
        // Caller should call flip() to set position=0, limit=length
    }

    /**
     * Reads a single byte from the channel.
     *
     * @param position File position to read from
     * @return The byte value (0-255)
     * @throws WALRecoveryException if unable to read
     */
    private int readByte(long position) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(1);
        long bytesRead = channel.read(buf, position);
        if (bytesRead != 1) {
            throw new WALRecoveryException(
                "Unexpected EOF while reading byte at position " + position,
                WALRecoveryException.RecoveryStatus.TRUNCATED_TAIL
            );
        }
        buf.flip();
        return buf.get() & 0xFF;
    }

    /**
     * Updates the term to persist (writes state record).
     * Format: type(1) + term(8) + votedForLen(4) + votedFor(variable) + commitIndex(8) + lastApplied(8)
     */
    public synchronized void persistTerm(long term, String votedFor) throws IOException {
        lock.lock();
        try {
            if (closed) {
                throw new IOException("WAL is closed");
            }

            // Validate votedFor size
            byte[] votedForBytes = votedFor != null ? votedFor.getBytes() : new byte[0];
            if (votedForBytes.length > MAX_VOTED_FOR_SIZE) {
                throw new IOException("votedFor too long: " + votedForBytes.length + " > " + MAX_VOTED_FOR_SIZE);
            }

            this.pendingTerm = term;
            this.pendingVotedFor = votedFor;

            // Format: type(1) + term(8) + votedForLen(4) + votedFor + commitIndex(8) + lastApplied(8)
            ByteBuffer stateBuf = ByteBuffer.allocate(1 + 8 + 4 + votedForBytes.length + 8 + 8);
            stateBuf.put(STATE_RECORD_TYPE);
            stateBuf.putLong(term);
            stateBuf.putInt(votedForBytes.length);
            if (votedForBytes.length > 0) {
                stateBuf.put(votedForBytes);
            }
            stateBuf.putLong(recoveredCommitIndex);
            stateBuf.putLong(recoveredLastApplied);
            stateBuf.flip();

            channel.write(stateBuf);
            channel.force(true);

        } finally {
            lock.unlock();
        }
    }

    /**
     * Persists the current commit and applied indices.
     * Format: type(1) + term(8) + votedForLen(4) + votedFor(variable) + commitIndex(8) + lastApplied(8)
     */
    public synchronized void persistCommitIndex(long commitIndex, long lastApplied) throws IOException {
        lock.lock();
        try {
            if (closed) {
                throw new IOException("WAL is closed");
            }

            // votedFor string
            byte[] votedForBytes = pendingVotedFor != null ? pendingVotedFor.getBytes() : new byte[0];

            // Write a state record with current state
            // Format: type(1) + term(8) + votedForLen(4) + votedFor + commitIndex(8) + lastApplied(8)
            ByteBuffer stateBuf = ByteBuffer.allocate(1 + 8 + 4 + votedForBytes.length + 8 + 8);
            stateBuf.put(STATE_RECORD_TYPE);
            stateBuf.putLong(pendingTerm);
            stateBuf.putInt(votedForBytes.length);
            if (votedForBytes.length > 0) {
                stateBuf.put(votedForBytes);
            }
            stateBuf.putLong(commitIndex);
            stateBuf.putLong(lastApplied);
            stateBuf.flip();

            channel.write(stateBuf);
            channel.force(true);

            recoveredCommitIndex = commitIndex;
            recoveredLastApplied = lastApplied;

        } finally {
            lock.unlock();
        }
    }

    /**
     * Appends an entry to the WAL with fsync.
     */
    public synchronized void append(LogEntry entry) throws IOException {
        lock.lock();
        try {
            if (closed) {
                throw new IOException("WAL is closed");
            }

            ByteBuffer buffer = serialize(entry);

            // Store channel reference locally to avoid any field access issues
            FileChannel ch = this.channel;
            ch.write(buffer);
            ch.force(true); // fsync
            lastSyncedIndex = entry.index();

        } finally {
            lock.unlock();
        }
    }

    /**
     * Appends multiple entries to the WAL with a single fsync.
     */
    public synchronized void appendAll(List<LogEntry> entries) throws IOException {
        if (entries.isEmpty()) {
            return;
        }

        lock.lock();
        try {
            if (closed) {
                throw new IOException("WAL is closed");
            }

            for (LogEntry entry : entries) {
                ByteBuffer buffer = serialize(entry);
                channel.write(buffer);
            }
            channel.force(true); // fsync
            lastSyncedIndex = entries.get(entries.size() - 1).index();

        } finally {
            lock.unlock();
        }
    }

    /**
     * Result of WAL recovery with detailed status.
     */
    public static class WALRecoveryResult {
        public enum Status {
            SUCCESS,           // All valid records recovered
            TRUNCATED_TAIL,    // Final record was truncated, valid prefix recovered
            CORRUPTED          // Internal corruption detected
        }

        public final Status status;
        public final List<LogEntry> entries;
        public final long term;
        public final String votedFor;
        public final long commitIndex;
        public final long lastApplied;
        public final String errorMessage;

        public WALRecoveryResult(Status status, List<LogEntry> entries, long term, String votedFor,
                                  long commitIndex, long lastApplied, String errorMessage) {
            this.status = status;
            this.entries = entries;
            this.term = term;
            this.votedFor = votedFor;
            this.commitIndex = commitIndex;
            this.lastApplied = lastApplied;
            this.errorMessage = errorMessage;
        }

        public static WALRecoveryResult success(List<LogEntry> entries, long term, String votedFor,
                                                long commitIndex, long lastApplied) {
            return new WALRecoveryResult(Status.SUCCESS, entries, term, votedFor, commitIndex, lastApplied, null);
        }

        public static WALRecoveryResult truncated(List<LogEntry> entries, long term, String votedFor,
                                                  long commitIndex, long lastApplied, String message) {
            return new WALRecoveryResult(Status.TRUNCATED_TAIL, entries, term, votedFor, commitIndex, lastApplied, message);
        }

        public static WALRecoveryResult corrupted(List<LogEntry> entries, long term, String votedFor,
                                                 long commitIndex, long lastApplied, String message) {
            return new WALRecoveryResult(Status.CORRUPTED, entries, term, votedFor, commitIndex, lastApplied, message);
        }
    }

    /**
     * Recovers all entries from the WAL with strict corruption detection.
     *
     * Recovery rules:
     * 1. Complete records are recovered normally
     * 2. Clean EOF is handled gracefully
     * 3. Truncated final record: recover valid prefix, truncate tail
     * 4. Internal corruption: FAIL - do not silently continue
     *
     * @return WALRecoveryResult with status and recovered data
     * @throws WALRecoveryException on unrecoverable corruption
     */
    public WALRecoveryResult recover() throws IOException {
        lock.lock();
        try {
            return doRecover();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Internal recovery implementation.
     */
    private WALRecoveryResult doRecover() throws IOException {
        List<LogEntry> entries = new ArrayList<>();

        long fileSize = channel.size();
        // Header: 8 (magic) + 4 (version) + 4 (end marker) = 16 bytes
        if (fileSize < 16) {
            return WALRecoveryResult.success(entries, 0, null, 0, 0);
        }

        channel.position(0);

        // Verify header using exact byte read
        ByteBuffer headerBuf = ByteBuffer.allocate(16);
        readFully(headerBuf, 0, 16);
        headerBuf.flip();

        long magic = headerBuf.getLong();
        if (magic != WAL_MAGIC) {
            throw new WALRecoveryException(
                "Invalid WAL magic number: expected " + Long.toHexString(WAL_MAGIC) + ", got " + Long.toHexString(magic),
                WALRecoveryException.RecoveryStatus.INVALID_FORMAT
            );
        }

        int version = headerBuf.getInt();
        int endMarker = headerBuf.getInt();
        if (endMarker != HEADER_END_MARKER) {
            throw new WALRecoveryException(
                "Invalid WAL header end marker: expected " + Integer.toHexString(HEADER_END_MARKER) +
                ", got " + Integer.toHexString(endMarker),
                WALRecoveryException.RecoveryStatus.INVALID_FORMAT
            );
        }

        // Read records
        long recordStartPos = 16; // Position after header
        long lastValidPosition = 16; // Track last valid record end position
        WALRecoveryResult.Status overallStatus = WALRecoveryResult.Status.SUCCESS;
        String errorMessage = null;

        while (recordStartPos < fileSize) {
            try {
                // Read record type
                int typeByte = readByte(recordStartPos);
                byte recordType = (byte) typeByte;

                if (recordType == STATE_RECORD_TYPE) {
                    // Read state record
                    // Format: term(8) + votedForLen(4) + votedFor(variable) + commitIndex(8) + lastApplied(8)
                    // Total fixed: 8 + 4 + 8 + 8 = 28 bytes

                    // Read fixed part
                    ByteBuffer fixedBuf = ByteBuffer.allocate(8 + 4);
                    readFully(fixedBuf, recordStartPos + 1, 8 + 4);
                    fixedBuf.flip();

                    long term = fixedBuf.getLong();
                    int votedForLen = fixedBuf.getInt();

                    // Validate votedFor length
                    if (votedForLen < 0 || votedForLen > MAX_VOTED_FOR_SIZE) {
                        throw new WALRecoveryException(
                            "Invalid votedFor length: " + votedForLen,
                            WALRecoveryException.RecoveryStatus.CORRUPTED_RECORD
                        );
                    }

                    // Read variable votedFor
                    byte[] votedForBytes = null;
                    String votedFor = null;
                    if (votedForLen > 0) {
                        votedForBytes = new byte[votedForLen];
                        ByteBuffer vfBuf = ByteBuffer.wrap(votedForBytes);
                        readFully(vfBuf, recordStartPos + 1 + 8 + 4, votedForLen);
                        votedFor = new String(votedForBytes);
                    }

                    // Read indices
                    ByteBuffer indexBuf = ByteBuffer.allocate(16);
                    readFully(indexBuf, recordStartPos + 1 + 8 + 4 + votedForLen, 16);
                    indexBuf.flip();

                    long commitIndex = indexBuf.getLong();
                    long lastApplied = indexBuf.getLong();

                    // Update recovered state
                    recoveredTerm = term;
                    recoveredVotedFor = votedFor;
                    recoveredCommitIndex = commitIndex;
                    recoveredLastApplied = lastApplied;
                    pendingTerm = term;
                    pendingVotedFor = votedFor;

                    // Record end position
                    long recordEndPos = recordStartPos + 1 + 8 + 4 + votedForLen + 16;
                    lastValidPosition = recordEndPos;
                    recordStartPos = recordEndPos;

                } else if (recordType == ENTRY_RECORD_TYPE) {
                    // Read entry record
                    // Format: term(8) + index(8) + timestamp(8) + opType(1) + dataLen(4) + data(variable) + checksum(4)
                    // Fixed header: 8 + 8 + 8 + 1 + 4 = 29 bytes

                    ByteBuffer fixedBuf = ByteBuffer.allocate(29);
                    readFully(fixedBuf, recordStartPos + 1, 29);
                    fixedBuf.flip();

                    long term = fixedBuf.getLong();
                    long index = fixedBuf.getLong();
                    long timestamp = fixedBuf.getLong();
                    byte opTypeCode = fixedBuf.get();
                    int dataLen = fixedBuf.getInt();

                    // Validate data length
                    if (dataLen < 0 || dataLen > MAX_DATA_SIZE) {
                        throw new WALRecoveryException(
                            "Invalid data length in entry record: " + dataLen,
                            WALRecoveryException.RecoveryStatus.CORRUPTED_RECORD
                        );
                    }

                    // Read data + checksum
                    int recordBodySize = dataLen + 4;
                    long dataStartPos = recordStartPos + 1 + 29;
                    long recordEndPos = dataStartPos + recordBodySize;

                    // Check if record is complete
                    if (recordEndPos > fileSize) {
                        // Truncated final record - this is recoverable
                        System.err.println("[WAL] Truncated final record at position " + recordStartPos +
                            ": expected " + recordBodySize + " bytes, have " + (fileSize - dataStartPos));
                        overallStatus = WALRecoveryResult.Status.TRUNCATED_TAIL;
                        errorMessage = "Truncated final record at index " + index;
                        break;
                    }

                    // Read data
                    byte[] data = new byte[dataLen];
                    if (dataLen > 0) {
                        ByteBuffer dataBuf = ByteBuffer.wrap(data);
                        readFully(dataBuf, dataStartPos, dataLen);
                    }

                    // Read checksum
                    ByteBuffer checksumBuf = ByteBuffer.allocate(4);
                    readFully(checksumBuf, dataStartPos + dataLen, 4);
                    checksumBuf.flip();
                    int storedChecksum = checksumBuf.getInt();

                    // Verify checksum
                    int computedChecksum = computeChecksum(term, index, timestamp, opTypeCode, data);
                    if (storedChecksum != computedChecksum) {
                        throw new WALRecoveryException(
                            "Checksum mismatch for entry at index " + index +
                            ": expected " + computedChecksum + ", got " + storedChecksum,
                            WALRecoveryException.RecoveryStatus.CHECKSUM_MISMATCH
                        );
                    }

                    // Validate opType
                    LogEntry.OpType opType;
                    try {
                        opType = LogEntry.OpType.fromCode(opTypeCode);
                    } catch (IllegalArgumentException e) {
                        throw new WALRecoveryException(
                            "Invalid opType code in entry at index " + index + ": " + opTypeCode,
                            WALRecoveryException.RecoveryStatus.CORRUPTED_RECORD
                        );
                    }

                    LogEntry entry = new LogEntry(term, index, timestamp, opType, data);
                    entries.add(entry);
                    lastSyncedIndex = Math.max(lastSyncedIndex, entry.index());

                    // Update last valid position and move to next record
                    lastValidPosition = recordEndPos;
                    recordStartPos = recordEndPos;

                } else {
                    // Unknown record type - this is internal corruption, not truncated tail
                    // Unless we're at EOF
                    if (recordStartPos >= fileSize - 1) {
                        // Just padding at EOF - ignore
                        break;
                    }
                    throw new WALRecoveryException(
                        "Unknown record type at position " + recordStartPos + ": " + recordType,
                        WALRecoveryException.RecoveryStatus.CORRUPTED_RECORD
                    );
                }
            } catch (WALRecoveryException e) {
                if (e.getStatus() == WALRecoveryException.RecoveryStatus.TRUNCATED_TAIL &&
                    overallStatus == WALRecoveryResult.Status.SUCCESS) {
                    // Truncated tail is recoverable - return what we have
                    overallStatus = WALRecoveryResult.Status.TRUNCATED_TAIL;
                    errorMessage = e.getMessage();
                    break;
                } else {
                    // Internal corruption or unexpected EOF
                    throw e;
                }
            }
        }

        // Determine truncation position if needed
        long truncateTo = overallStatus == WALRecoveryResult.Status.TRUNCATED_TAIL ? lastValidPosition : -1;

        WALRecoveryResult result;
        if (overallStatus == WALRecoveryResult.Status.CORRUPTED) {
            result = WALRecoveryResult.corrupted(entries, recoveredTerm, recoveredVotedFor,
                recoveredCommitIndex, recoveredLastApplied, errorMessage);
        } else if (overallStatus == WALRecoveryResult.Status.TRUNCATED_TAIL) {
            result = WALRecoveryResult.truncated(entries, recoveredTerm, recoveredVotedFor,
                recoveredCommitIndex, recoveredLastApplied, errorMessage);
        } else {
            result = WALRecoveryResult.success(entries, recoveredTerm, recoveredVotedFor,
                recoveredCommitIndex, recoveredLastApplied);
        }

        // Handle physical truncation if needed
        if (truncateTo > 16 && channel != null && channel.isOpen()) {
            long currentSize = channel.size();
            if (truncateTo < currentSize) {
                channel.truncate(truncateTo);
                System.out.println("[WAL] Physically truncated WAL from " + currentSize + " to " + truncateTo);
            }
        }

        // Reset position to end for appending
        if (channel != null && channel.isOpen()) {
            channel.position(channel.size());
        }

        return result;
    }

    /**
     * Legacy recover method for backwards compatibility.
     * Throws exception on corruption.
     */
    public WALRecoveryData recoverLegacy() throws IOException {
        WALRecoveryResult result = recover();
        if (result.status == WALRecoveryResult.Status.CORRUPTED) {
            throw new WALRecoveryException(
                "WAL is corrupted: " + result.errorMessage,
                WALRecoveryException.RecoveryStatus.CORRUPTED_RECORD
            );
        }
        return new WALRecoveryData(result.entries, result.term, result.votedFor,
            result.commitIndex, result.lastApplied);
    }

    /**
     * Data class for WAL recovery results.
     */
    public record WALRecoveryData(
        List<LogEntry> entries,
        long term,
        String votedFor,
        long commitIndex,
        long lastApplied
    ) {}

    /**
     * Returns the last synced index.
     */
    public long getLastSyncedIndex() {
        return lastSyncedIndex;
    }

    /**
     * Computes a CRC32 checksum for a log entry.
     */
    private int computeChecksum(long term, long index, long timestamp, byte opTypeCode, byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(toBytes(term));
        crc.update(toBytes(index));
        crc.update(toBytes(timestamp));
        crc.update(opTypeCode);
        if (data != null && data.length > 0) {
            crc.update(data, 0, data.length);
        }
        return (int) crc.getValue();
    }

    private byte[] toBytes(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    /**
     * Compacts the WAL by keeping only entries after the snapshot boundary.
     * Writes to a temp file atomically.
     *
     * @param snapshotIndex Entries at or before this index are removed (covered by snapshot)
     * @param commitIndex The commit index to persist (may be higher than snapshotIndex)
     * @param term The current term to persist
     * @param votedFor The votedFor to persist
     */
    public void compact(long snapshotIndex, long commitIndex, long term, String votedFor) throws IOException {
        lock.lock();
        try {
            if (closed) {
                throw new IOException("WAL is closed");
            }

            // Read all entries
            WALRecoveryResult recoveryResult;
            try {
                recoveryResult = recover();
            } catch (WALRecoveryException e) {
                // WAL may be truncated - start fresh with state
                System.out.println("[WAL] WAL recovery error during compaction, starting fresh: " + e.getMessage());
                recoveryResult = WALRecoveryResult.truncated(List.of(), term, votedFor, commitIndex, 0, e.getMessage());
            }
            List<LogEntry> allEntries = recoveryResult.entries;

            // Filter to POST-snapshot entries (entries AFTER the snapshot boundary)
            // Entries <= snapshotIndex are now covered by the snapshot, so we REMOVE them
            // Entries > snapshotIndex are NOT in the snapshot, so we KEEP them
            List<LogEntry> postSnapshotEntries = allEntries.stream()
                    .filter(e -> e.index() > snapshotIndex)
                    .toList();

            // Close current channel before overwriting
            FileChannel oldChannel = this.channel;
            this.channel = null;

            // Write to temp file, then atomic rename
            Path tempFile = walFile.resolveSibling(walFile.getFileName() + ".tmp");
            try (FileChannel newChannel = FileChannel.open(tempFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.SYNC)) {

                // Write header
                writeHeaderTo(newChannel);

                // Write state record with current term and votedFor
                {
                    byte[] votedForBytes = votedFor != null ? votedFor.getBytes() : new byte[0];
                    ByteBuffer stateBuf = ByteBuffer.allocate(1 + 8 + 4 + votedForBytes.length + 8 + 8);
                    stateBuf.put(STATE_RECORD_TYPE);
                    stateBuf.putLong(term);
                    stateBuf.putInt(votedForBytes.length);
                    if (votedForBytes.length > 0) {
                        stateBuf.put(votedForBytes);
                    }
                    stateBuf.putLong(commitIndex);
                    stateBuf.putLong(recoveryResult.lastApplied);
                    stateBuf.flip();
                    newChannel.write(stateBuf);
                }

                // Write post-snapshot entries
                for (LogEntry entry : postSnapshotEntries) {
                    ByteBuffer entryBuf = serialize(entry);
                    newChannel.write(entryBuf);
                }
                newChannel.force(true);
            }

            // Close old channel
            try {
                if (oldChannel != null && oldChannel.isOpen()) {
                    oldChannel.close();
                }
            } catch (IOException e) {
                // Ignore close errors
            }

            // Atomic move
            Files.move(tempFile, walFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

            // Reopen channel and seek to end
            this.channel = FileChannel.open(walFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.SYNC);
            this.channel.position(this.channel.size());

        } finally {
            lock.unlock();
        }
    }

    private void writeHeaderTo(FileChannel ch) throws IOException {
        ByteBuffer headerBuf = ByteBuffer.allocate(16);
        headerBuf.putLong(WAL_MAGIC);
        headerBuf.putInt(WAL_VERSION);
        headerBuf.putInt(HEADER_END_MARKER);
        headerBuf.flip();
        ch.write(headerBuf);
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            closed = true;
            if (channel.isOpen()) {
                channel.close();
            }
        } finally {
            lock.unlock();
        }
    }

    private void writeHeader() throws IOException {
        // Header: MAGIC(8) + VERSION(4) + END_MARKER(4) = 16 bytes
        ByteBuffer headerBuf = ByteBuffer.allocate(16);
        headerBuf.putLong(WAL_MAGIC);
        headerBuf.putInt(WAL_VERSION);
        headerBuf.putInt(HEADER_END_MARKER);
        headerBuf.flip();
        channel.write(headerBuf);
        channel.force(true);
    }

    private ByteBuffer serialize(LogEntry entry) {
        byte[] data = entry.data() != null ? entry.data() : new byte[0];

        // Format: type(1) + term(8) + index(8) + timestamp(8) + opType(1) + dataLen(4) + data + checksum(4)
        int checksum = computeChecksum(entry.term(), entry.index(), entry.timestamp(), entry.opType().code(), data);

        ByteBuffer buf = ByteBuffer.allocate(1 + 8 + 8 + 8 + 1 + 4 + data.length + 4);
        buf.put(ENTRY_RECORD_TYPE);
        buf.putLong(entry.term());
        buf.putLong(entry.index());
        buf.putLong(entry.timestamp());
        buf.put(entry.opType().code());
        buf.putInt(data.length);
        buf.put(data);
        buf.putInt(checksum);
        buf.flip();
        return buf;
    }

    /**
     * Returns the WAL file path for testing/debugging.
     */
    public Path getWalFile() {
        return walFile;
    }
}
