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
    }

    /**
     * Returns the number of actual log entries (excluding the dummy entry at index 0).
     */
    public int getEntryCount() {
        lock.readLock().lock();
        try {
            // Subtract 1 to exclude the dummy entry at index 0
            return Math.max(0, entries.size() - 1);
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
            // Write to WAL first for durability (skip during recovery)
            if (writeToWal && wal != null) {
                try {
                    wal.append(entry);
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to write to WAL", e);
                }
            }
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
     */
    public long loadEntries(List<LogEntry> recoveredEntries, long recoveredCommitIndex, long recoveredLastApplied) {
        lock.writeLock().lock();
        try {
            // Clear existing entries and load recovered ones
            entries.clear();

            long highest = 0;
            for (LogEntry entry : recoveredEntries) {
                entries.add(entry);
                highest = Math.max(highest, entry.index());
            }

            // Update highestIndex to track maximum index ever seen
            this.highestIndex = Math.max(this.highestIndex, highest);

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
     * If WAL is configured, writes entries to WAL with fsync first.
     */
    public void appendEntries(long prevLogIndex, long prevLogTerm, List<LogEntry> newEntries) {
        lock.writeLock().lock();
        try {
            // Remove conflicting entries
            if (prevLogIndex >= logStartIndex && prevLogIndex < entries.size() + logStartIndex) {
                int idx = (int) (prevLogIndex - logStartIndex);
                if (entries.get(idx).term() != prevLogTerm) {
                    // Truncate from this point
                    int keepCount = Math.max(0, idx);
                    while (entries.size() > keepCount) {
                        entries.remove(entries.size() - 1);
                    }
                }
            }

            // Append new entries
            if (wal != null && !newEntries.isEmpty()) {
                try {
                    wal.appendAll(newEntries);
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to write to WAL", e);
                }
            }
            for (LogEntry entry : newEntries) {
                entries.add(entry);
            }
        } finally {
            lock.writeLock().unlock();
        }
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
            int idx = (int) (index - logStartIndex);
            if (idx < 0 || idx >= entries.size()) {
                return null;
            }
            return entries.get(idx);
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
     * After compaction, this returns the highest index seen, even if that entry
     * is no longer in the log (covered by snapshot).
     */
    public long getLastLogIndex() {
        lock.readLock().lock();
        try {
            long lastInLog = logStartIndex + entries.size() - 1;
            // Return the higher of actual log size or highest index ever seen
            return Math.max(lastInLog, highestIndex);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the last log term.
     */
    public long getLastLogTerm() {
        lock.readLock().lock();
        try {
            if (entries.isEmpty()) {
                return 0;
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
            if (index < logStartIndex || index > getLastLogIndex()) {
                return false;
            }
            int idx = (int) (index - logStartIndex);
            return entries.get(idx).term() == term;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Advances the commit index to the given value.
     */
    public void advanceCommitIndex(long newCommitIndex) {
        if (newCommitIndex > commitIndex && newCommitIndex <= getLastLogIndex()) {
            this.commitIndex = newCommitIndex;
        }
    }

    /**
     * Returns the current commit index.
     */
    public long getCommitIndex() {
        return commitIndex;
    }

    /**
     * Advances the last applied index.
     */
    public void advanceLastApplied() {
        if (lastApplied < commitIndex) {
            lastApplied = commitIndex;
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
            return entries.size();
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

            // Calculate how many entries to keep (entries before index)
            int keepCount = (int) (index - logStartIndex);

            // Remove entries at and after index
            while (entries.size() > keepCount) {
                entries.remove(entries.size() - 1);
            }
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

        } finally {
            lock.writeLock().unlock();
        }
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
