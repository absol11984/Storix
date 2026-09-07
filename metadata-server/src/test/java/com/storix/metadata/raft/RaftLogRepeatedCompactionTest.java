package com.storix.metadata.raft;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for RaftLog repeated compaction scenarios.
 *
 * Verifies that compaction is correct for:
 * - Multiple successive compactions
 * - Append after compaction
 * - Compaction from non-zero start index
 *
 * This is a critical test because the previous implementation used
 * RELATIVE INDEXING (keepCount = snapshotIndex - logStartIndex + 1)
 * which became incorrect after the first compaction.
 *
 * The fix uses ABSOLUTE INDEX COMPARISON:
 * entries.removeIf(entry -> entry.index() <= snapshotIndex)
 */
class RaftLogRepeatedCompactionTest {

    /**
     * Test: append entries 1..10, compactThrough(5), compactThrough(10), append 11, 12
     *
     * Verifies absolute indexes are preserved across multiple compactions.
     */
    @Test
    void testRepeatedCompactionWithAppend() {
        RaftLog log = new RaftLog();

        // Append entries 1..10
        for (int i = 1; i <= 10; i++) {
            long term = 1; // All entries in term 1
            LogEntry entry = new LogEntry(term, i, System.currentTimeMillis(),
                    LogEntry.OpType.CREATE_OBJECT, ("data" + i).getBytes(), null, null);
            log.append(entry);
        }

        // Verify initial state
        assertEquals(10, log.getLastLogIndex());
        assertEquals(1, log.getEntry(1).index());
        assertEquals(10, log.getEntry(10).index());

        // Get term of entry 5 for snapshot
        long term5 = log.getEntry(5).term();
        assertEquals(1, term5);

        // FIRST COMPACTION: compactThrough(5, term5)
        log.compactThrough(5, term5);

        // After first compaction:
        assertNull(log.getEntry(1), "Entry 1 should be null after compaction");
        assertNull(log.getEntry(5), "Entry 5 should be null after compaction");
        assertNotNull(log.getEntry(6), "Entry 6 should exist after compaction");
        assertEquals(6, log.getEntry(6).index(), "Entry 6 index should be 6");
        assertEquals(10, log.getEntry(10).index(), "Entry 10 index should be 10");
        assertEquals(10, log.getLastLogIndex(), "Last log index should be 10 after compaction");
        assertEquals(1, log.getTermAt(5), "termAt(5) should be 1 (snapshot boundary term)");
        assertEquals(6, log.getLogStartIndex(), "logStartIndex should be 6 (5+1)");

        // SECOND COMPACTION: compactThrough(10, termOf10)
        long term10 = log.getEntry(10).term();
        log.compactThrough(10, term10);

        // After second compaction:
        assertNull(log.getEntry(1), "Entry 1 should still be null");
        assertNull(log.getEntry(5), "Entry 5 should still be null");
        assertNull(log.getEntry(6), "Entry 6 should be null after second compaction");
        assertNull(log.getEntry(10), "Entry 10 should be null after second compaction");
        assertEquals(10, log.getLastLogIndex(), "Last log index should still be 10");
        assertEquals(11, log.getLogStartIndex(), "logStartIndex should be 11 (10+1)");

        // APPEND: entry 11
        LogEntry entry11 = new LogEntry(2, 11, System.currentTimeMillis(),
                LogEntry.OpType.CREATE_OBJECT, "data11".getBytes(), null, null);
        log.append(entry11);

        assertNotNull(log.getEntry(11), "Entry 11 should exist");
        assertEquals(11, log.getEntry(11).index(), "Entry 11 index should be 11");
        assertEquals(11, log.getLastLogIndex(), "Last log index should be 11");

        // APPEND: entry 12
        LogEntry entry12 = new LogEntry(2, 12, System.currentTimeMillis(),
                LogEntry.OpType.CREATE_OBJECT, "data12".getBytes(), null, null);
        log.append(entry12);

        assertNotNull(log.getEntry(12), "Entry 12 should exist");
        assertEquals(12, log.getEntry(12).index(), "Entry 12 index should be 12");
        assertEquals(12, log.getLastLogIndex(), "Last log index should be 12");

        // Verify old entries (6-10) are still gone
        assertNull(log.getEntry(6), "Entry 6 should still be null");
        assertNull(log.getEntry(7), "Entry 7 should still be null");
        assertNull(log.getEntry(8), "Entry 8 should still be null");
        assertNull(log.getEntry(9), "Entry 9 should still be null");
        assertNull(log.getEntry(10), "Entry 10 should still be null");

        System.out.println("PASSED: Repeated compaction preserves absolute indexes");
    }

    /**
     * Test: compaction from non-zero start
     *
     * Initial state:
     *   snapshot boundary = 20
     *   active log = 21..40
     *
     * After compactThrough(30):
     *   snapshot boundary = 30
     *   active = 31..40
     *
     * After append 41:
     *   getEntry(41).index() == 41
     */
    @Test
    void testCompactionFromNonZeroStart() {
        RaftLog log = new RaftLog();

        // Set initial snapshot boundary at 20 (simulating recovery from snapshot)
        log.setSnapshotBoundary(20, 1);

        // Append entries 21..40
        for (int i = 21; i <= 40; i++) {
            LogEntry entry = new LogEntry(1, i, System.currentTimeMillis(),
                    LogEntry.OpType.CREATE_OBJECT, ("data" + i).getBytes(), null, null);
            log.append(entry);
        }

        // Verify initial state
        assertEquals(40, log.getLastLogIndex());
        assertEquals(21, log.getLogStartIndex());
        assertEquals(21, log.getEntry(21).index());
        assertEquals(40, log.getEntry(40).index());
        assertEquals(1, log.getTermAt(20), "termAt(20) should be snapshot term (1)");

        // COMPACT: through index 30
        log.compactThrough(30, 1);

        // After compaction:
        assertNull(log.getEntry(21), "Entry 21 should be null");
        assertNull(log.getEntry(30), "Entry 30 should be null");
        assertNotNull(log.getEntry(31), "Entry 31 should exist");
        assertEquals(31, log.getEntry(31).index(), "Entry 31 index should be 31");
        assertEquals(40, log.getEntry(40).index(), "Entry 40 index should be 40");
        assertEquals(40, log.getLastLogIndex(), "Last log index should be 40");
        assertEquals(31, log.getLogStartIndex(), "logStartIndex should be 31 (30+1)");

        // APPEND: entry 41
        LogEntry entry41 = new LogEntry(2, 41, System.currentTimeMillis(),
                LogEntry.OpType.CREATE_OBJECT, "data41".getBytes(), null, null);
        log.append(entry41);

        assertNotNull(log.getEntry(41), "Entry 41 should exist");
        assertEquals(41, log.getEntry(41).index(), "Entry 41 index should be 41");
        assertEquals(41, log.getLastLogIndex(), "Last log index should be 41");

        // Verify entries 21-30 are gone
        assertNull(log.getEntry(21), "Entry 21 should be null");
        assertNull(log.getEntry(25), "Entry 25 should be null");
        assertNull(log.getEntry(30), "Entry 30 should be null");

        // But entries 31-40 should still exist
        assertEquals(31, log.getEntry(31).index(), "Entry 31 should still exist");
        assertEquals(40, log.getEntry(40).index(), "Entry 40 should still exist");

        System.out.println("PASSED: Compaction from non-zero start works correctly");
    }

    /**
     * Test: termAt() returns correct term after repeated compaction
     */
    @Test
    void testTermAtAfterRepeatedCompaction() {
        RaftLog log = new RaftLog();

        // Append entries with different terms
        // Entries 1-5 in term 1
        for (int i = 1; i <= 5; i++) {
            log.append(new LogEntry(1, i, System.currentTimeMillis(), LogEntry.OpType.NO_OP, new byte[0], null, null));
        }
        // Entries 6-10 in term 2
        for (int i = 6; i <= 10; i++) {
            log.append(new LogEntry(2, i, System.currentTimeMillis(), LogEntry.OpType.NO_OP, new byte[0], null, null));
        }
        // Entries 11-15 in term 3
        for (int i = 11; i <= 15; i++) {
            log.append(new LogEntry(3, i, System.currentTimeMillis(), LogEntry.OpType.NO_OP, new byte[0], null, null));
        }

        // First compaction at index 5 (term 1)
        log.compactThrough(5, 1);
        assertEquals(1, log.getTermAt(5), "termAt(5) should be 1 (snapshot term)");
        assertEquals(2, log.getTermAt(6), "termAt(6) should be 2");
        assertEquals(3, log.getTermAt(15), "termAt(15) should be 3");

        // Second compaction at index 10 (term 2)
        log.compactThrough(10, 2);
        assertEquals(0, log.getTermAt(5), "termAt(5) should be 0 (not immediate previous index)");
        assertEquals(2, log.getTermAt(10), "termAt(10) should be 2 (snapshot term)");
        assertEquals(3, log.getTermAt(11), "termAt(11) should be 3");
        assertEquals(3, log.getTermAt(15), "termAt(15) should be 3");

        // Third compaction at index 15 (term 3)
        log.compactThrough(15, 3);
        assertEquals(0, log.getTermAt(5), "termAt(5) should be 0 (not immediate previous index)");
        assertEquals(0, log.getTermAt(10), "termAt(10) should be 0 (not immediate previous index)");
        assertEquals(3, log.getTermAt(15), "termAt(15) should be 3 (final snapshot term)");

        // All entries should be gone
        assertNull(log.getEntry(5));
        assertNull(log.getEntry(10));
        assertNull(log.getEntry(15));

        System.out.println("PASSED: termAt() returns correct term after repeated compaction");
    }

    /**
     * Test: getEntriesFrom() works correctly after repeated compaction
     */
    @Test
    void testGetEntriesFromAfterRepeatedCompaction() {
        RaftLog log = new RaftLog();

        // Append entries 1..20
        for (int i = 1; i <= 20; i++) {
            log.append(new LogEntry(1, i, System.currentTimeMillis(),
                    LogEntry.OpType.NO_OP, new byte[0], null, null));
        }

        // First compaction at index 10
        log.compactThrough(10, 1);

        var entries1 = log.getEntriesFrom(11);
        assertEquals(10, entries1.size());
        assertEquals(11, entries1.get(0).index());
        assertEquals(20, entries1.get(9).index());

        // Second compaction at index 15
        log.compactThrough(15, 1);

        var entries2 = log.getEntriesFrom(16);
        assertEquals(5, entries2.size());
        assertEquals(16, entries2.get(0).index());
        assertEquals(20, entries2.get(4).index());

        // Third compaction at index 20
        log.compactThrough(20, 1);

        var entries3 = log.getEntriesFrom(21);
        assertEquals(0, entries3.size(), "Should have no entries after compacting through 20");

        System.out.println("PASSED: getEntriesFrom() works after repeated compaction");
    }

    /**
     * Test: recovery after repeated compaction preserves state
     */
    @Test
    void testRecoveryAfterRepeatedCompaction() {
        RaftLog log = new RaftLog();

        // Simulate: entries 1..30, compacted to 20, then to 25
        // Active entries: 26..30

        // Append 1..30
        for (int i = 1; i <= 30; i++) {
            log.append(new LogEntry(1, i, System.currentTimeMillis(),
                    LogEntry.OpType.CREATE_OBJECT, ("data" + i).getBytes(), null, null));
        }

        // First compaction
        log.compactThrough(20, 1);
        assertEquals(21, log.getLogStartIndex());

        // Second compaction
        log.compactThrough(25, 1);
        assertEquals(26, log.getLogStartIndex());

        // Simulate recovery: WAL has entries 26..30
        var recoveredEntries = java.util.Arrays.asList(
                new LogEntry(1, 26, System.currentTimeMillis(), LogEntry.OpType.CREATE_OBJECT, "data26".getBytes(), null, null),
                new LogEntry(1, 27, System.currentTimeMillis(), LogEntry.OpType.CREATE_OBJECT, "data27".getBytes(), null, null),
                new LogEntry(1, 28, System.currentTimeMillis(), LogEntry.OpType.CREATE_OBJECT, "data28".getBytes(), null, null),
                new LogEntry(1, 29, System.currentTimeMillis(), LogEntry.OpType.CREATE_OBJECT, "data29".getBytes(), null, null),
                new LogEntry(1, 30, System.currentTimeMillis(), LogEntry.OpType.CREATE_OBJECT, "data30".getBytes(), null, null)
        );

        // Clear and reload
        log.loadEntries(recoveredEntries, 30, 30);

        // Verify state after recovery
        assertEquals(30, log.getLastLogIndex());
        assertEquals(26, log.getLogStartIndex());
        assertNotNull(log.getEntry(26));
        assertEquals(26, log.getEntry(26).index());
        assertNotNull(log.getEntry(30));
        assertEquals(30, log.getEntry(30).index());
        assertEquals(1, log.getTermAt(25), "termAt(25) should be snapshot term");

        // Append new entry
        LogEntry entry31 = new LogEntry(2, 31, System.currentTimeMillis(),
                LogEntry.OpType.CREATE_OBJECT, "data31".getBytes(), null, null);
        log.append(entry31);

        assertEquals(31, log.getEntry(31).index());
        assertEquals(31, log.getLastLogIndex());

        System.out.println("PASSED: Recovery after repeated compaction works correctly");
    }
}
