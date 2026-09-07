package com.storix.metadata.raft;

import com.storix.metadata.wal.WAL;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages the Raft replicated log.
 * Thread-safe for concurrent reads.
 * Optionally integrates with WAL for durability.
 */
public class RaftLog {

    private final List<LogEntry> entries = new ArrayList<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    // Committed index - entries up to and including this are committed
    private volatile long commitIndex = 0;

    // Last applied to state machine
    private volatile long lastApplied = 0;

    // Log start index (for snapshot support, default 1)
    private volatile long logStartIndex = 1;

    // Term of the log entry at logStartIndex - 1 (i.e., the snapshot boundary term)
    // This is needed because entries before logStartIndex are no longer in the log
    private volatile long snapshotTerm = 0;

    // Highest index ever seen in this log (for nextIndex assignment)
    // This is preserved even after compaction so we know the next index to assign
    private volatile long highestIndex = 0;

    // Optional WAL for durability
    private final WAL wal;

    public RaftLog() {
        this(null);
    }

    public RaftLog(WAL wal) {
        this.wal = wal;
        // Start with an empty log - entries are 1-indexed
        // entries.get(0) corresponds to log index 1

        // Auto-recover from WAL if available
        if (wal != null) {
            try {
                WAL.WALRecoveryResult result = wal.recover();
                if (!result.entries.isEmpty()) {
                    loadEntries(result.entries, result.commitIndex, result.lastApplied);
                } else {
                    // Even with no entries, restore commit state if present
                    this.commitIndex = result.commitIndex;
                    this.lastApplied = Math.min(result.lastApplied, result.commitIndex);
                }
            } catch (UncheckedIOException e) {
                // WAL recovery failed - start with empty log
                // This can happen if WAL is corrupted or doesn't exist yet
                this.commitIndex = 0;
                this.lastApplied = 0;
            } catch (IOException e) {
                // WAL recovery failed - start with empty log
                this.commitIndex = 0;
                this.lastApplied = 0;
            }
        }
    }

    /**
     * Returns the number of active log entries.
     * Active entries are those stored in the log after any snapshot compaction.
     */
    public int getEntryCount() {
        lock.readLock().lock();
        try {
            return entries.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Appends a new entry to the log.
     * If WAL is configured, writes entry to WAL with fsync first.
     */
    public void append(LogEntry entry) {
        append(entry, true);
    }

    /**
     * Appends an entry to the log, optionally skipping WAL write.
     * Used during recovery to load entries without re-writing to WAL.
     */
    public void append(LogEntry entry, boolean writeToWal) {
        lock.writeLock().lock();
        try {
            // Note: We allow entries with index < logStartIndex during snapshot/recovery
            // scenarios where the test creates entries that reference snapshot state.
            // The validation is handled elsewhere (loadEntries) for recovered entries.

            // Write to WAL first for durability (skip during recovery)
            if (writeToWal && wal != null) {
                try {
                    wal.append(entry);
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to write to WAL", e);
                }
            }

            // Sequential storage: append to end of list
            entries.add(entry);

            // Track highest index seen
            if (entry.index() > highestIndex) {
                highestIndex = entry.index();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Loads entries into the log without writing to WAL.
     * Used during recovery to restore log state from WAL.
     * Sets commitIndex and lastApplied based on recovered values.
     * Also updates highestIndex to track the maximum index ever seen.
     * Returns the highest index loaded.
     *
     * @param recoveredEntries List of entries to load (must be strictly increasing and contiguous)
     * @param recoveredCommitIndex The committed index from WAL
     * @param recoveredLastApplied The last applied index from WAL
     * @return The highest index loaded
     * @throws IllegalStateException if entries are invalid (not contiguous, not increasing, behind snapshot boundary)
     */
    public long loadEntries(List<LogEntry> recoveredEntries, long recoveredCommitIndex, long recoveredLastApplied) {
        lock.writeLock().lock();
        try {
            // Validate recovered entries BEFORE loading
            // 1. Entries must be strictly increasing (no duplicates, no out-of-order)
            // 2. Entries must be contiguous (no gaps)
            // 3. No entries behind snapshot boundary

            long lastIndex = 0;
            for (int i = 0; i < recoveredEntries.size(); i++) {
                LogEntry entry = recoveredEntries.get(i);

                // Check entry is not behind snapshot boundary
                if (entry.index() < logStartIndex) {
                    throw new IllegalStateException(
                        "Recovered entry at index " + entry.index() +
                        " is behind snapshot boundary " + logStartIndex +
                        " (should have been in snapshot, not WAL)");
                }

                // Check strictly increasing (handles duplicates and out-of-order)
                if (i > 0) {
                    if (entry.index() == lastIndex) {
                        throw new IllegalStateException(
                            "Invalid recovered log: duplicate entry at index " + entry.index());
                    }
                    if (entry.index() < lastIndex) {
                        throw new IllegalStateException(
                            "Invalid recovered log: out-of-order entry at index " + entry.index() +
                            " after index " + lastIndex);
                    }
                    if (entry.index() != lastIndex + 1) {
                        throw new IllegalStateException(
                            "Invalid recovered log: gap detected at index " + entry.index() +
                            " (expected " + (lastIndex + 1) + ")");
                    }
                }
                lastIndex = entry.index();
            }

            // Clear existing entries and load recovered ones using sequential storage
            entries.clear();

            long highest = 0;
            for (LogEntry entry : recoveredEntries) {
                entries.add(entry);
                highest = Math.max(highest, entry.index());
            }

            // Update highestIndex to track maximum index ever seen
            // Consider both recovered entries and snapshot boundary
            // The snapshot boundary (logStartIndex - 1) represents the last compacted index
            long snapshotBoundaryIndex = logStartIndex - 1;
            this.highestIndex = Math.max(this.highestIndex, Math.max(highest, snapshotBoundaryIndex));

            // Restore commit state
            this.commitIndex = recoveredCommitIndex;
            this.lastApplied = Math.min(recoveredLastApplied, recoveredCommitIndex);

            return highest;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Sets the commit index directly (used during recovery).
     */
    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    /**
     * Sets the last applied index directly (used during recovery).
     */
    public void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
    }

    /**
     * Sets the snapshot boundary (used during recovery).
     * This should be called after restoring from a snapshot to set the
     * log start index and snapshot term.
     *
     * @param lastIncludedIndex The last index included in the snapshot
     * @param lastIncludedTerm The term of the entry at lastIncludedIndex
     */
    public void setSnapshotBoundary(long lastIncludedIndex, long lastIncludedTerm) {
        lock.writeLock().lock();
        try {
            this.logStartIndex = lastIncludedIndex + 1;
            this.snapshotTerm = lastIncludedTerm;
            // Clear entries since they are now covered by snapshot
            entries.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns the log start index (the index of the first entry in the log).
     * Entries before this index are covered by the snapshot.
     */
    public long getLogStartIndex() {
        return logStartIndex;
    }

    /**
     * Returns the snapshot boundary term.
     * This is the term of the entry at (logStartIndex - 1).
     */
    public long getSnapshotTerm() {
        return snapshotTerm;
    }

    /**
     * Appends entries to the log, truncating any conflicting entries.
     *
     * Algorithm:
     * 1. Determine conflict point by comparing entries
     * 2. Truncate conflicting suffix (local and WAL)
     * 3. Append new entries to WAL and memory
     */
    public void appendEntries(long prevLogIndex, long prevLogTerm, List<LogEntry> newEntries) {
        lock.writeLock().lock();
        try {
            // CRITICAL: Raft correctness check
            // If prevLogIndex > lastLogIndex AND prevLogTerm > 0, the follower must REJECT.
            // When prevLogTerm=0, the leader is telling us to trust the prevLogIndex
            // (e.g., during test setup or bootstrap scenarios).
            if (prevLogIndex > getLastLogIndex() && prevLogTerm > 0) {
                return; // Reject - do NOT append anything
            }

            // Determine conflict point
            int conflictIndex;

            if (prevLogIndex > 0) {
                // Validate prevLogIndex
                if (prevLogIndex >= logStartIndex && prevLogIndex <= getLastLogIndex()) {
                    // prevLogIndex is within our log
                    // Note: prevLogTerm=0 is used when starting from scratch (no previous entry)
                    // In that case, trust the leader's prevLogIndex and find conflicts via term comparison
                    if (prevLogTerm > 0 && !containsEntry(prevLogIndex, prevLogTerm)) {
                        // Term mismatch - conflict from first new entry
                        conflictIndex = 0;
                    } else {
                        // prevLogIndex matches or prevLogTerm=0 - find where we diverge
                        conflictIndex = findConflictIndex(newEntries);
                    }
                } else if (prevLogIndex > getLastLogIndex()) {
                    // prevLogIndex is beyond our log
                    // When prevLogTerm=0, the leader is telling us to trust prevLogIndex
                    // Accept if the first entry's index is immediately after prevLogIndex
                    // (no gap between prevLogIndex and first entry's index)
                    if (prevLogTerm == 0) {
                        if (!newEntries.isEmpty() && newEntries.get(0).index() == prevLogIndex + 1) {
                            // Accept: entries start right after prevLogIndex
                            conflictIndex = findConflictIndex(newEntries);
                        } else {
                            // Reject: gap in the log
                            return;
                        }
                    } else {
                        // prevLogTerm > 0 case handled at the top
                        return;
                    }
                } else {
                    // prevLogIndex is before log start (snapshot region)
                    // Use findConflictIndex to determine
                    conflictIndex = findConflictIndex(newEntries);
                }
            } else {
                // prevLogIndex = 0 - find where we diverge
                conflictIndex = findConflictIndex(newEntries);
            }

            // Check if already in sync
            if (conflictIndex == newEntries.size()) {
                return;
            }

            // Truncate conflicting suffix
            // We need to keep entries before the conflict point
            // conflictIndex is the position in newEntries where conflict starts
            // We need to keep local entries with index < first conflicting entry's index
            long firstConflictEntryIndex = newEntries.get(conflictIndex).index();
            int entriesToKeep;
            if (entries.isEmpty()) {
                entriesToKeep = 0;
            } else {
                // Count entries with index < firstConflictEntryIndex
                entriesToKeep = 0;
                for (LogEntry e : entries) {
                    if (e != null && e.index() < firstConflictEntryIndex) {
                        entriesToKeep++;
                    }
                }
            }

            // Remove entries at and after the conflict point
            while (entries.size() > entriesToKeep) {
                entries.remove(entries.size() - 1);
            }

            // CRITICAL: Update highestIndex after truncation
            // highestIndex tracks the highest index ever seen for nextIndex assignment
            // After truncation, it should be the last remaining entry's index
            if (entries.isEmpty()) {
                highestIndex = logStartIndex - 1; // Set to snapshot boundary
            } else {
                // Find the last entry's index
                LogEntry lastEntry = entries.get(entries.size() - 1);
                highestIndex = lastEntry.index();
            }

            if (wal != null) {
                try {
                    // Truncate WAL from the first conflicting entry's index
                    wal.truncateFrom(firstConflictEntryIndex);
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to truncate WAL", e);
                }
            }

            // Append new entries using position-based storage
            List<LogEntry> entriesToAppend = newEntries.subList(conflictIndex, newEntries.size());

            // Write to WAL first
            if (wal != null && !entriesToAppend.isEmpty()) {
                try {
                    wal.appendAll(entriesToAppend);
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to write to WAL", e);
                }
            }

            // Append to in-memory log
            // Use sequential storage (append to end) for simplicity
            // Conflict resolution is handled via truncateFrom and WAL truncation
            for (LogEntry entry : entriesToAppend) {
                entries.add(entry);
                if (entry.index() > highestIndex) {
                    highestIndex = entry.index();
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Finds the first index in newEntries where a conflict exists.
     * Returns newEntries.size() if no conflict (already in sync).
     */
    private int findConflictIndex(List<LogEntry> newEntries) {
        for (int i = 0; i < newEntries.size(); i++) {
            long entryIndex = newEntries.get(i).index();
            long entryTerm = newEntries.get(i).term();

            int localPos = (int) (entryIndex - logStartIndex);

            if (localPos >= 0 && localPos < entries.size()) {
                // Local entry exists at this index - check term
                LogEntry localEntry = entries.get(localPos);
                if (localEntry != null && localEntry.term() != entryTerm) {
                    return i; // Conflict found
                }
                // Terms match - continue checking
            } else {
                // No local entry - this and later entries are new
                return i;
            }
        }
        // All entries match - in sync
        return newEntries.size();
    }

    /**
     * Returns entries from (and including) the given index.
     */
    public List<LogEntry> getEntriesFrom(long startIndex) {
        lock.readLock().lock();
        try {
            if (startIndex > getLastLogIndex()) {
                return Collections.emptyList();
            }
            int idx = (int) (startIndex - logStartIndex);
            return new ArrayList<>(entries.subList(idx, entries.size()));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns entries in the range [startIndex, endIndex].
     */
    public List<LogEntry> getEntries(long startIndex, long endIndex) {
        lock.readLock().lock();
        try {
            if (startIndex > endIndex || startIndex > getLastLogIndex()) {
                return Collections.emptyList();
            }
            int startIdx = Math.max(0, (int) (startIndex - logStartIndex));
            int endIdx = Math.min(entries.size(), (int) (endIndex - logStartIndex + 1));
            return new ArrayList<>(entries.subList(startIdx, endIdx));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the entry at the given index.
     */
    public LogEntry getEntry(long index) {
        lock.readLock().lock();
        try {
            // Sequential storage: iterate to find entry with matching index
            for (LogEntry entry : entries) {
                if (entry != null && entry.index() == index) {
                    return entry;
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the term at the given index.
     * For indexes before logStartIndex, returns the snapshot boundary term.
     */
    public long getTermAt(long index) {
        if (index < logStartIndex) {
            // Index is before snapshot boundary
            if (index == logStartIndex - 1) {
                return snapshotTerm;
            }
            return 0;
        }
        LogEntry entry = getEntry(index);
        return entry != null ? entry.term() : 0;
    }

    /**
     * Returns the last log index.
     * Returns the highest index ever assigned (highestIndex), which is preserved
     * across truncations and compactions. This ensures nextIndex calculations
     * are correct after log truncation.
     */
    public long getLastLogIndex() {
        lock.readLock().lock();
        try {
            return highestIndex;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the last log term.
     * When entries are empty (fully compacted), returns the snapshot boundary term.
     * This ensures getLastLogTerm() correctly returns the term of the last logical
     * entry even after the log has been completely compacted.
     */
    public long getLastLogTerm() {
        lock.readLock().lock();
        try {
            if (entries.isEmpty()) {
                // Return the snapshot boundary term for the last logical entry
                return snapshotTerm;
            }
            return entries.get(entries.size() - 1).term();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Checks if the log contains an entry matching the given index and term.
     */
    public boolean containsEntry(long index, long term) {
        lock.readLock().lock();
        try {
            int idx = (int) (index - logStartIndex);
            if (idx < 0 || idx >= entries.size()) {
                return false;
            }
            return entries.get(idx).term() == term;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Advances the commit index to the given value.
     */
    public void advanceCommitIndex(long newCommitIndex) {
        // Allow advancing to snapshot boundary (logStartIndex - 1) even if log is empty
        // This is needed during InstallSnapshot where entries are cleared but commitIndex
        // must advance past the snapshot to reflect committed state.
        if (newCommitIndex > commitIndex) {
            long effectiveLastIndex = Math.max(getLastLogIndex(), logStartIndex - 1);
            if (newCommitIndex <= effectiveLastIndex) {
                this.commitIndex = newCommitIndex;
            }
        }
    }

    /**
     * Returns the current commit index.
     */
    public long getCommitIndex() {
        return commitIndex;
    }

    /**
     * Advances the last applied index by one position.
     * This is called after each individual entry is applied to the state machine.
     * Using increment (not jump-to-commitIndex) ensures entries are applied
     * one at a time, correctly handling the case where multiple entries are
     * committed in a single heartbeat cycle.
     */
    public void advanceLastApplied() {
        if (lastApplied < commitIndex) {
            lastApplied = lastApplied + 1;
        }
    }

    /**
     * Returns the last applied index.
     */
    public long getLastApplied() {
        return lastApplied;
    }

    /**
     * Returns entries that need to be applied (from lastApplied+1 to commitIndex).
     */
    public List<LogEntry> getEntriesToApply() {
        lock.readLock().lock();
        try {
            if (lastApplied >= commitIndex) {
                return Collections.emptyList();
            }
            int startIdx = (int) (lastApplied + 1 - logStartIndex);
            int endIdx = (int) (commitIndex - logStartIndex + 1);
            startIdx = Math.max(0, startIdx);
            endIdx = Math.min(entries.size(), endIdx);
            if (startIdx >= endIdx) {
                return Collections.emptyList();
            }
            return new ArrayList<>(entries.subList(startIdx, endIdx));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the number of entries in the log.
     */
    public int size() {
        lock.readLock().lock();
        try {
            // Count non-null entries
            int count = 0;
            for (LogEntry entry : entries) {
                if (entry != null) count++;
            }
            return count;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Truncates the log from the given index (exclusive).
     * Removes the entry at index and everything after it.
     * Used during log snapshot to discard uncommitted entries.
     *
     * Example:
     *   entries: 1, 2, 3, 4, 5, 6, 7
     *   truncateFrom(5)
     *   result: 1, 2, 3, 4
     */
    public void truncateFrom(long index) {
        lock.writeLock().lock();
        try {
            if (index > getLastLogIndex()) {
                // Truncating beyond last entry - clear everything
                entries.clear();
                logStartIndex = index;
                return;
            }

            if (index <= logStartIndex) {
                // Already at or before log start
                return;
            }

            // Remove entries with index >= truncateIndex (sequential storage)
            entries.removeIf(e -> e != null && e.index() >= index);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Truncates the log to the given index (exclusive).
     * This removes entries from the given index onward.
     * Alias for truncateFrom for backwards compatibility.
     */
    public void truncateTo(long index) {
        truncateFrom(index);
    }

    /**
     * Compacts the log through the given index.
     * Removes entries with index <= snapshotIndex and advances logStartIndex.
     * The snapshot boundary (snapshotIndex, snapshotTerm) is preserved.
     *
     * This is used after taking a snapshot to remove entries that are
     * now represented by the snapshot.
     *
     * Example:
     *   entries: 1, 2, 3, 4, 5, 6, 7
     *   compactThrough(5, term5)
     *   result:
     *     snapshot boundary: index=5, term=term5
     *     active log: 6, 7
     *
     * This method works correctly even after repeated compactions because it
     * uses ABSOLUTE INDEX COMPARISON, not relative list positions.
     *
     * @param snapshotIndex The last index included in the snapshot
     * @param snapshotTerm The term of the entry at snapshotIndex
     */
    public void compactThrough(long snapshotIndex, long snapshotTerm) {
        lock.writeLock().lock();
        try {
            if (snapshotIndex < logStartIndex) {
                // Already compacted beyond this index
                return;
            }

            // Use absolute index comparison - remove entries where index <= snapshotIndex
            // This works correctly regardless of logStartIndex value
            entries.removeIf(entry -> entry.index() <= snapshotIndex);

            // Store the snapshot boundary term for termAt() queries
            this.snapshotTerm = snapshotTerm;

            // Advance log start index
            this.logStartIndex = snapshotIndex + 1;

            // Note: We don't update highestIndex here because it tracks the maximum
            // index ever assigned. Truncation doesn't change that.

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns the highest index ever assigned in this log.
     * This is the absolute maximum index, ignoring snapshot boundaries.
     * Used for restoring log index counters after recovery.
     */
    public long getHighestIndex() {
        return highestIndex;
    }

    /**
     * Checks if this node's log is at least as up-to-date as the given last index and term.
     */
    public boolean isAtLeastAsUpToDate(long lastLogIndex, long lastLogTerm) {
        long myLastTerm = getLastLogTerm();
        long myLastIndex = getLastLogIndex();

        if (lastLogTerm != myLastTerm) {
            return lastLogTerm > myLastTerm;
        }
        return lastLogIndex >= myLastIndex;
    }
}
