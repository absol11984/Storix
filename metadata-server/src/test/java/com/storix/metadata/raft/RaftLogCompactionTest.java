package com.storix.metadata.raft;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for RaftLog compaction behavior.
 * Verifies absolute indexing remains correct after snapshotting/compaction.
 */
class RaftLogCompactionTest {

    /**
     * Test: compactThrough(5, term5) on entries 1..10
     * After compaction:
     * - snapshot boundary = 5
     * - active entries = 6, 7, 8, 9, 10
     * - getEntry(6).index() == 6
     * - getEntry(10).index() == 10
     * - getLastLogIndex() == 10
     * - termAt(5) == term5
     * - getEntry(1) == null
     * - getEntry(5) == null
     */
    @Test
    void testCompactionPreservesAbsoluteIndexes() {
        RaftLog log = new RaftLog();

        // Append entries 1..10 with varying terms
        for (int i = 1; i <= 10; i++) {
            long term = (i <= 5) ? 1 : ((i <= 8) ? 2 : 3); // Terms change at boundaries
            LogEntry entry = new LogEntry(term, i, System.currentTimeMillis(),
                    LogEntry.OpType.CREATE_OBJECT, ("data" + i).getBytes(), null, null);
            log.append(entry);
        }

        // Verify initial state
        assertEquals(10, log.getLastLogIndex());
        assertEquals(1, log.getEntry(1).index());
        assertEquals(10, log.getEntry(10).index());

        // Get term of entry 5
        long term5 = log.getEntry(5).term();

        // Compact through index 5
        log.compactThrough(5, term5);

        // After compaction: entries 1-5 should be gone
        assertNull(log.getEntry(1), "Entry 1 should be null after compaction");
        assertNull(log.getEntry(5), "Entry 5 should be null after compaction");

        // After compaction: entries 6-10 should exist with correct indexes
        assertNotNull(log.getEntry(6), "Entry 6 should exist after compaction");
        assertEquals(6, log.getEntry(6).index(), "Entry 6 index should be 6");
        assertEquals(term5, log.getTermAt(5), "termAt(5) should equal snapshot term");

        assertNotNull(log.getEntry(7));
        assertEquals(7, log.getEntry(7).index());

        assertNotNull(log.getEntry(10));
        assertEquals(10, log.getEntry(10).index());

        // Last log index should still be 10
        assertEquals(10, log.getLastLogIndex(), "Last log index should be 10 after compaction");

        // termAt for non-existent entries before snapshot should return 0
        assertEquals(0, log.getTermAt(1), "termAt(1) should be 0 (no entry)");
    }

    /**
     * Test: append after compaction
     * After compactThrough(5, term5), append entries 11 and 12
     * New entries must have absolute indexes 11 and 12
     */
    @Test
    void testAppendAfterCompaction() {
        RaftLog log = new RaftLog();

        // Append entries 1..10
        for (int i = 1; i <= 10; i++) {
            LogEntry entry = new LogEntry(1, i, System.currentTimeMillis(),
                    LogEntry.OpType.CREATE_OBJECT, ("data" + i).getBytes(), null, null);
            log.append(entry);
        }

        // Compact through index 5
        log.compactThrough(5, 1);

        // Append new entries
        LogEntry entry11 = new LogEntry(2, 11, System.currentTimeMillis(),
                LogEntry.OpType.CREATE_OBJECT, "data11".getBytes(), null, null);
        LogEntry entry12 = new LogEntry(2, 12, System.currentTimeMillis(),
                LogEntry.OpType.CREATE_OBJECT, "data12".getBytes(), null, null);

        log.append(entry11);
        log.append(entry12);

        // Verify new entries have correct indexes
        assertNotNull(log.getEntry(11));
        assertEquals(11, log.getEntry(11).index());
        assertEquals("data11", new String(log.getEntry(11).data()));

        assertNotNull(log.getEntry(12));
        assertEquals(12, log.getEntry(12).index());

        // Last index should be 12
        assertEquals(12, log.getLastLogIndex());

        // Old entries should still exist
        assertEquals(6, log.getEntry(6).index());
        assertEquals(10, log.getEntry(10).index());
    }

    /**
     * Test: compaction with 100 entries, compact at 50, append 101, 102
     * Verifies no index shifting occurs
     */
    @Test
    void testCompactionLargeLog() {
        RaftLog log = new RaftLog();

        // Append entries 1..100
        for (int i = 1; i <= 100; i++) {
            LogEntry entry = new LogEntry(1, i, System.currentTimeMillis(),
                    LogEntry.OpType.NO_OP, new byte[0], null, null);
            log.append(entry);
        }

        assertEquals(100, log.getLastLogIndex());
        assertEquals(50, log.getEntry(50).index());
        assertEquals(100, log.getEntry(100).index());

        // Compact through index 50
        log.compactThrough(50, 1);

        // Entries 51-100 should still exist with correct indexes
        assertEquals(51, log.getEntry(51).index());
        assertEquals(99, log.getEntry(99).index());
        assertEquals(100, log.getEntry(100).index());

        // Append new entries
        LogEntry entry101 = new LogEntry(2, 101, System.currentTimeMillis(),
                LogEntry.OpType.NO_OP, new byte[0], null, null);
        LogEntry entry102 = new LogEntry(2, 102, System.currentTimeMillis(),
                LogEntry.OpType.NO_OP, new byte[0], null, null);

        log.append(entry101);
        log.append(entry102);

        // Verify new entries
        assertEquals(101, log.getEntry(101).index());
        assertEquals(102, log.getEntry(102).index());
        assertEquals(102, log.getLastLogIndex());

        // Old entries unchanged
        assertEquals(51, log.getEntry(51).index());
        assertEquals(100, log.getEntry(100).index());
    }

    /**
     * Test: recovery preserves snapshot boundary and active entries
     * After snapshotIndex = 50, WAL should have entries 51..100
     */
    @Test
    void testRecoveryAfterCompaction() {
        RaftLog log = new RaftLog();

        // Simulate: snapshot at 50, WAL has entries 51..100
        // First add entries 1..100
        for (int i = 1; i <= 100; i++) {
            LogEntry entry = new LogEntry(1, i, System.currentTimeMillis(),
                    LogEntry.OpType.CREATE_OBJECT, ("data" + i).getBytes(), null, null);
            log.append(entry);
        }

        // Compact to 50
        log.compactThrough(50, 1);

        // Simulate recovery: entries 51..100 from WAL
        java.util.List<LogEntry> recoveredEntries = new java.util.ArrayList<>();
        for (int i = 51; i <= 100; i++) {
            LogEntry entry = new LogEntry(1, i, System.currentTimeMillis(),
                    LogEntry.OpType.CREATE_OBJECT, ("data" + i).getBytes(), null, null);
            recoveredEntries.add(entry);
        }

        // Load recovered entries
        long highestIndex = log.loadEntries(recoveredEntries, 100, 100);

        // After recovery:
        // - termAt(50) should be snapshot term (1)
        assertEquals(1, log.getTermAt(50), "termAt(50) should be snapshot term");
        assertEquals(0, log.getTermAt(40), "termAt(40) should be 0 (before snapshot)");

        // - getEntry(51) should be entry with index 51
        assertNotNull(log.getEntry(51));
        assertEquals(51, log.getEntry(51).index());

        // - getEntry(100) should be entry with index 100
        assertNotNull(log.getEntry(100));
        assertEquals(100, log.getEntry(100).index());

        // - lastIndex should be 100
        assertEquals(100, log.getLastLogIndex());

        // - nextIndex should be 101
        assertEquals(101, log.getLastLogIndex() + 1, "Next index should be lastIndex + 1");
    }

    /**
     * Test: getEntriesFrom after compaction
     */
    @Test
    void testGetEntriesFromAfterCompaction() {
        RaftLog log = new RaftLog();

        for (int i = 1; i <= 20; i++) {
            LogEntry entry = new LogEntry(1, i, System.currentTimeMillis(),
                    LogEntry.OpType.NO_OP, new byte[0], null, null);
            log.append(entry);
        }

        log.compactThrough(10, 1);

        var entries = log.getEntriesFrom(11);
        assertEquals(10, entries.size());
        assertEquals(11, entries.get(0).index());
        assertEquals(20, entries.get(9).index());
    }

    /**
     * Test: truncateTo vs compactThrough semantics
     * truncateTo(5) removes entries 5 and after
     * compactThrough(5, term) removes entries <= 5, preserving snapshot boundary
     */
    @Test
    void testTruncateVsCompact() {
        RaftLog log = new RaftLog();

        // Append entries 1..10
        for (int i = 1; i <= 10; i++) {
            LogEntry entry = new LogEntry(1, i, System.currentTimeMillis(),
                    LogEntry.OpType.NO_OP, new byte[0], null, null);
            log.append(entry);
        }

        // truncateTo(5) removes entries 5, 6, 7, 8, 9, 10
        // Result: entries 1, 2, 3, 4
        // Note: highestIndex is preserved at 10, so getLastLogIndex() returns 10
        log.truncateTo(5);
        assertEquals(10, log.getLastLogIndex()); // highestIndex is preserved
        assertEquals(4, log.getLogStartIndex() + log.size() - 1); // actual entries
        assertNotNull(log.getEntry(4));
        assertNull(log.getEntry(5));
        assertNull(log.getEntry(10));

        // Re-append entries
        for (int i = 5; i <= 10; i++) {
            LogEntry entry = new LogEntry(1, i, System.currentTimeMillis(),
                    LogEntry.OpType.NO_OP, new byte[0], null, null);
            log.append(entry);
        }

        // Now compactThrough(5, term) should remove entries 1-5, keeping 6-10
        log.compactThrough(5, 1);
        assertEquals(10, log.getLastLogIndex());
        assertNull(log.getEntry(1));
        assertNull(log.getEntry(5));
        assertNotNull(log.getEntry(6));
        assertEquals(6, log.getEntry(6).index());
        assertEquals(10, log.getEntry(10).index());
    }

    /**
     * Test: termAt with entries spanning multiple terms
     */
    @Test
    void testTermAtWithMultipleTerms() {
        RaftLog log = new RaftLog();

        // Entries 1-5 in term 1
        for (int i = 1; i <= 5; i++) {
            log.append(new LogEntry(1, i, System.currentTimeMillis(), LogEntry.OpType.NO_OP, new byte[0], null, null));
        }
        // Entries 6-10 in term 2
        for (int i = 6; i <= 10; i++) {
            log.append(new LogEntry(2, i, System.currentTimeMillis(), LogEntry.OpType.NO_OP, new byte[0], null, null));
        }

        assertEquals(1, log.getTermAt(5));
        assertEquals(2, log.getTermAt(6));
        assertEquals(2, log.getTermAt(10));

        // Compact through term boundary (index 5, term 1)
        log.compactThrough(5, 1);

        // termAt(5) should return snapshot term
        assertEquals(1, log.getTermAt(5), "termAt(5) should be snapshot term (1)");
        assertEquals(2, log.getTermAt(6), "termAt(6) should be term 2");
        assertEquals(2, log.getTermAt(10), "termAt(10) should be term 2");
    }

    /**
     * Test: entries to apply after compaction
     */
    @Test
    void testGetEntriesToApplyAfterCompaction() {
        RaftLog log = new RaftLog();

        for (int i = 1; i <= 20; i++) {
            log.append(new LogEntry(1, i, System.currentTimeMillis(),
                    LogEntry.OpType.CREATE_OBJECT, ("data" + i).getBytes(), null, null));
        }

        // Compact through index 10
        log.compactThrough(10, 1);

        // Set commit index to 15
        log.advanceCommitIndex(15);

        // Entries to apply should be 11-15
        var toApply = log.getEntriesToApply();
        assertEquals(5, toApply.size());
        assertEquals(11, toApply.get(0).index());
        assertEquals(15, toApply.get(4).index());
    }

    /**
     * Test: containsEntry after compaction
     */
    @Test
    void testContainsEntryAfterCompaction() {
        RaftLog log = new RaftLog();

        for (int i = 1; i <= 10; i++) {
            log.append(new LogEntry(1, i, System.currentTimeMillis(), LogEntry.OpType.NO_OP, new byte[0], null, null));
        }

        log.compactThrough(5, 1);

        // Should not contain entries before snapshot
        assertFalse(log.containsEntry(1, 1));
        assertFalse(log.containsEntry(5, 1));

        // Should contain entries after snapshot
        assertTrue(log.containsEntry(6, 1));
        assertTrue(log.containsEntry(10, 1));

        // Wrong term should return false
        assertFalse(log.containsEntry(6, 2));
    }
}
