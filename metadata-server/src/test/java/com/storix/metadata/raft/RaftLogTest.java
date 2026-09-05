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
                LogEntry.OpType.CREATE_OBJECT, "test".getBytes());
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
                    LogEntry.OpType.NO_OP, new byte[0]);
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
                    LogEntry.OpType.NO_OP, new byte[0]));
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
                    LogEntry.OpType.NO_OP, new byte[0]));
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
                    LogEntry.OpType.NO_OP, new byte[0]));
        }

        // Append entries at prevLogIndex=1 with prevLogTerm=2 (mismatch - we have term 1)
        // The appendEntries method checks if entry at prevLogIndex has matching term.
        // Since entry 1 has term 1 ≠ prevLogTerm 2, no truncation occurs,
        // and new entries are simply appended.
        List<LogEntry> newEntries = List.of(
                new LogEntry(2, 2, System.currentTimeMillis(), LogEntry.OpType.NO_OP, new byte[0]),
                new LogEntry(2, 3, System.currentTimeMillis(), LogEntry.OpType.NO_OP, new byte[0])
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
                    LogEntry.OpType.CREATE_OBJECT, ("data" + i).getBytes()));
        }

        assertEquals(0, log.getLastApplied());
        assertTrue(log.getEntriesToApply().isEmpty());

        // Advance commit index
        log.advanceCommitIndex(3);

        List<LogEntry> toApply = log.getEntriesToApply();
        assertEquals(3, toApply.size());
        assertEquals(1, toApply.get(0).index());
        assertEquals(3, toApply.get(2).index());

        // Advance last applied
        log.advanceLastApplied();

        assertEquals(3, log.getLastApplied());
        assertTrue(log.getEntriesToApply().isEmpty());
    }

    @Test
    void testContainsEntry() {
        RaftLog log = new RaftLog();

        log.append(new LogEntry(1, 1, System.currentTimeMillis(),
                LogEntry.OpType.NO_OP, new byte[0]));
        log.append(new LogEntry(1, 2, System.currentTimeMillis(),
                LogEntry.OpType.NO_OP, new byte[0]));

        assertTrue(log.containsEntry(1, 1));
        assertTrue(log.containsEntry(2, 1));
        assertFalse(log.containsEntry(2, 2)); // Wrong term
        assertFalse(log.containsEntry(3, 1)); // Doesn't exist
    }
}
