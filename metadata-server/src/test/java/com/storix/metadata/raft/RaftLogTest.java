package com.storix.metadata.raft;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for RaftLog.
 */
class RaftLogTest {

    @Test
    void testAppendAndGetEntry() {
        RaftLog log = new RaftLog();

        LogEntry entry = new LogEntry(1, 1, System.currentTimeMillis(),
                LogEntry.OpType.CREATE_OBJECT, "test".getBytes(), null, null);
        log.append(entry);

        assertEquals(1, log.getLastLogIndex());
        assertEquals(1, log.getLastLogTerm());
        assertEquals(entry.term(), log.getTermAt(1));
    }

    @Test
    void testGetEntriesFrom() {
        RaftLog log = new RaftLog();

        // Add 5 entries
        for (int i = 1; i <= 5; i++) {
            LogEntry entry = new LogEntry(1, i, System.currentTimeMillis(),
                    LogEntry.OpType.NO_OP, new byte[0], null, null);
            log.append(entry);
        }

        List<LogEntry> entries = log.getEntriesFrom(3);
        assertEquals(3, entries.size());
        assertEquals(3, entries.get(0).index());
        assertEquals(5, entries.get(2).index());
    }

    @Test
    void testCommitIndexAdvance() {
        RaftLog log = new RaftLog();

        // Add entries
        for (int i = 1; i <= 5; i++) {
            log.append(new LogEntry(1, i, System.currentTimeMillis(),
                    LogEntry.OpType.NO_OP, new byte[0], null, null));
        }

        assertEquals(0, log.getCommitIndex());

        log.advanceCommitIndex(3);
        assertEquals(3, log.getCommitIndex());

        // Cannot advance beyond last index
        log.advanceCommitIndex(10);
        assertEquals(3, log.getCommitIndex());
    }

    @Test
    void testIsAtLeastAsUpToDate() {
        RaftLog log = new RaftLog();

        // Add entries at term 1
        for (int i = 1; i <= 3; i++) {
            log.append(new LogEntry(1, i, System.currentTimeMillis(),
                    LogEntry.OpType.NO_OP, new byte[0], null, null));
        }

        // Same term, same or higher index
        assertTrue(log.isAtLeastAsUpToDate(3, 1));
        assertTrue(log.isAtLeastAsUpToDate(3, 1));

        // Higher term
        assertTrue(log.isAtLeastAsUpToDate(3, 2));

        // Lower term
        assertFalse(log.isAtLeastAsUpToDate(3, 0));

        // Same term, lower index
        assertFalse(log.isAtLeastAsUpToDate(2, 1));
    }

    @Test
    void testAppendEntriesWithConflict() {
        RaftLog log = new RaftLog();

        // Add entries: (1,1), (1,2), (1,3)
        for (int i = 1; i <= 3; i++) {
            log.append(new LogEntry(1, i, System.currentTimeMillis(),
                    LogEntry.OpType.NO_OP, new byte[0], null, null));
        }

        // Append entries at prevLogIndex=1 with prevLogTerm=2 (mismatch - we have term 1)
        // The appendEntries method checks if entry at prevLogIndex has matching term.
        // Since entry 1 has term 1 ≠ prevLogTerm 2, no truncation occurs,
        // and new entries are simply appended.
        List<LogEntry> newEntries = List.of(
                new LogEntry(2, 2, System.currentTimeMillis(), LogEntry.OpType.NO_OP, new byte[0], null, null),
                new LogEntry(2, 3, System.currentTimeMillis(), LogEntry.OpType.NO_OP, new byte[0], null, null)
        );

        log.appendEntries(1, 2, newEntries);

        // After append (no truncation since entry 1 exists with term 1 ≠ prevLogTerm 2):
        // We have entries 1 (term 1), 2 (term 2), 3 (term 2)
        assertEquals(3, log.getLastLogIndex());
        // Entry at index 2 should be term 2 (from new entries)
        assertEquals(2, log.getEntry(2).term());
    }

    @Test
    void testGetEntriesToApply() {
        RaftLog log = new RaftLog();

        // Add 5 entries
        for (int i = 1; i <= 5; i++) {
            log.append(new LogEntry(1, i, System.currentTimeMillis(),
                    LogEntry.OpType.CREATE_OBJECT, ("data" + i).getBytes(), null, null));
        }

        assertEquals(0, log.getLastApplied());
        assertTrue(log.getEntriesToApply().isEmpty());

        // Advance commit index
        log.advanceCommitIndex(3);

        List<LogEntry> toApply = log.getEntriesToApply();
        assertEquals(3, toApply.size());
        assertEquals(1, toApply.get(0).index());
        assertEquals(3, toApply.get(2).index());

        // Advance last applied - each call advances by one position
        log.advanceLastApplied();
        assertEquals(1, log.getLastApplied());
        assertEquals(2, log.getEntriesToApply().size()); // entries 2,3

        log.advanceLastApplied();
        assertEquals(2, log.getLastApplied());
        assertEquals(1, log.getEntriesToApply().size()); // entry 3

        log.advanceLastApplied();
        assertEquals(3, log.getLastApplied());
        assertTrue(log.getEntriesToApply().isEmpty());
    }

    @Test
    void testContainsEntry() {
        RaftLog log = new RaftLog();

        log.append(new LogEntry(1, 1, System.currentTimeMillis(),
                LogEntry.OpType.NO_OP, new byte[0], null, null));
        log.append(new LogEntry(1, 2, System.currentTimeMillis(),
                LogEntry.OpType.NO_OP, new byte[0], null, null));

        assertTrue(log.containsEntry(1, 1));
        assertTrue(log.containsEntry(2, 1));
        assertFalse(log.containsEntry(2, 2)); // Wrong term
        assertFalse(log.containsEntry(3, 1)); // Doesn't exist
    }

    /**
     * Regression test: getLastLogTerm() must return snapshotTerm after full compaction.
     * When all entries are compacted away, getLastLogTerm() should return the term
     * of the last logical entry (the snapshot boundary term), NOT 0.
     */
    @Test
    void testGetLastLogTermAfterFullCompaction() {
        RaftLog log = new RaftLog();

        // Append entries 1..10 at term 2
        for (int i = 1; i <= 10; i++) {
            log.append(new LogEntry(2, i, System.currentTimeMillis(),
                    LogEntry.OpType.NO_OP, new byte[0], null, null));
        }

        // Verify initial state
        assertEquals(10, log.getLastLogIndex());
        assertEquals(2, log.getLastLogTerm());
        assertEquals(10, log.size());
        assertNotNull(log.getEntry(10));
        assertEquals(2, log.getTermAt(10));

        // Compact through index 10 - this removes all entries
        log.compactThrough(10, 2);

        // After full compaction, entries should be empty
        assertEquals(0, log.size(), "Entries should be empty after full compaction");
        assertEquals(10, log.getLastLogIndex(), "lastLogIndex should still be 10");
        assertEquals(2, log.getLastLogTerm(), "getLastLogTerm should return 2 (snapshotTerm), not 0");
        assertEquals(2, log.getTermAt(10), "getTermAt(10) should return 2");

        // Append a new entry
        log.append(new LogEntry(3, 11, System.currentTimeMillis(),
                LogEntry.OpType.NO_OP, new byte[0], null, null));

        // Verify new entry
        assertNotNull(log.getEntry(11));
        assertEquals(11, log.getEntry(11).index());
        assertEquals(11, log.getLastLogIndex());
        assertEquals(3, log.getLastLogTerm(), "getLastLogTerm should now return 3");
    }

    /**
     * Test: Repeated compaction preserves absolute indexes.
     * After compactThrough, entries after the snapshot index remain in the active log.
     */
    @Test
    void testRepeatedCompactionPreservesAbsoluteIndexes() {
        RaftLog log = new RaftLog();

        // Append entries 1..20 at term 1
        for (int i = 1; i <= 20; i++) {
            log.append(new LogEntry(1, i, System.currentTimeMillis(),
                    LogEntry.OpType.NO_OP, new byte[0], null, null));
        }

        // First compaction through index 10
        // This removes entries 1-10 (by index), leaves 11-20
        log.compactThrough(10, 1);
        assertEquals(11, log.getLogStartIndex());
        assertEquals(20, log.getLastLogIndex(), "lastLogIndex should be 20 (entries 11-20 remain)");
        assertEquals(1, log.getLastLogTerm());
        assertEquals(10, log.size(), "10 entries (11-20) should remain");

        // Second compaction through index 20
        // This removes entries 11-20 (by index), leaves nothing
        log.compactThrough(20, 1);
        assertEquals(21, log.getLogStartIndex());
        assertEquals(20, log.getLastLogIndex(), "lastLogIndex should be 20 (snapshot boundary)");
        assertEquals(1, log.getLastLogTerm());
        assertEquals(0, log.size(), "Log should be empty after full compaction");

        // Append new entry
        log.append(new LogEntry(2, 21, System.currentTimeMillis(),
                LogEntry.OpType.NO_OP, new byte[0], null, null));
        assertEquals(21, log.getLastLogIndex());
        assertEquals(2, log.getLastLogTerm());
    }
}
