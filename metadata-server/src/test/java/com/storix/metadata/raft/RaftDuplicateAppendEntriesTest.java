package com.storix.metadata.raft;

import com.storix.metadata.wal.WAL;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for duplicate AppendEntries handling.
 *
 * Verifies:
 * 1. Duplicate entries are not appended to WAL
 * 2. Duplicate entries are not duplicated in memory
 * 3. State is consistent after restart
 */
class RaftDuplicateAppendEntriesTest {

    @TempDir
    Path tempDir;

    /**
     * Test: Send the same entry twice via AppendEntries.
     * Expected: In-memory log contains entry once, WAL contains entry once.
     */
    @Test
    void testDuplicateAppendEntriesNoWALDuplicate() throws Exception {
        Path walFile = tempDir.resolve("wal_dup1.dat");
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);

        // First AppendEntries with entry X
        List<LogEntry> entries1 = List.of(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1})
        );
        log.appendEntries(0, 0, entries1);

        assertEquals(1, log.size());
        assertEquals(1, log.getEntry(1).index());

        // Second AppendEntries with SAME entry X (duplicate)
        List<LogEntry> entries2 = List.of(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1})
        );
        log.appendEntries(0, 0, entries2);

        // Should still have only 1 entry
        assertEquals(1, log.size(), "Should still have only 1 entry after duplicate");
        assertEquals(1, log.getEntry(1).index());

        // Recover from WAL and verify
        wal.close();

        WAL recoveredWal = new WAL(walFile);
        RaftLog recoveredLog = new RaftLog(recoveredWal);

        assertEquals(1, recoveredLog.size(), "Should have 1 entry after recovery");
        assertEquals(1, recoveredLog.getEntry(1).index());

        recoveredWal.close();
    }

    /**
     * Test: Send entries [1, 2, 3] twice.
     * Expected: Log contains [1, 2, 3] once.
     */
    @Test
    void testDuplicateBatchAppendEntries() throws Exception {
        Path walFile = tempDir.resolve("wal_dup2.dat");
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);

        // First AppendEntries with entries [1, 2, 3]
        List<LogEntry> entries1 = List.of(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1}),
            new LogEntry(1, 2, 1001, LogEntry.OpType.CREATE_OBJECT, new byte[]{2}),
            new LogEntry(1, 3, 1002, LogEntry.OpType.CREATE_OBJECT, new byte[]{3})
        );
        log.appendEntries(0, 0, entries1);

        assertEquals(3, log.size());

        // Second AppendEntries with SAME entries [1, 2, 3] (duplicate)
        List<LogEntry> entries2 = List.of(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1}),
            new LogEntry(1, 2, 1001, LogEntry.OpType.CREATE_OBJECT, new byte[]{2}),
            new LogEntry(1, 3, 1002, LogEntry.OpType.CREATE_OBJECT, new byte[]{3})
        );
        log.appendEntries(0, 0, entries2);

        // Should still have only 3 entries
        assertEquals(3, log.size(), "Should still have only 3 entries after duplicate");

        // Recover from WAL
        wal.close();

        WAL recoveredWal = new WAL(walFile);
        RaftLog recoveredLog = new RaftLog(recoveredWal);

        assertEquals(3, recoveredLog.size(), "Should have 3 entries after recovery");

        recoveredWal.close();
    }

    /**
     * Test: Same entry but different prevLogIndex scenario.
     * Log has [1, 2], send AppendEntries(prevLogIndex=2, entries=[3]).
     * Then send same AppendEntries(prevLogIndex=2, entries=[3]) again.
     * Expected: Entry 3 appears only once.
     */
    @Test
    void testDuplicateAppendAfterExistingEntry() throws Exception {
        Path walFile = tempDir.resolve("wal_dup3.dat");
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);

        // First: appendEntries with [1]
        List<LogEntry> entries1 = List.of(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1})
        );
        log.appendEntries(0, 0, entries1);

        // Second: appendEntries with [2]
        List<LogEntry> entries2 = List.of(
            new LogEntry(1, 2, 1001, LogEntry.OpType.CREATE_OBJECT, new byte[]{2})
        );
        log.appendEntries(1, 1, entries2);

        assertEquals(2, log.size());

        // Third: duplicate AppendEntries with [3]
        List<LogEntry> entries3 = List.of(
            new LogEntry(1, 3, 1002, LogEntry.OpType.CREATE_OBJECT, new byte[]{3})
        );
        log.appendEntries(2, 1, entries3);

        assertEquals(3, log.size());

        // Fourth: SAME AppendEntries with [3] (duplicate)
        List<LogEntry> entries4 = List.of(
            new LogEntry(1, 3, 1002, LogEntry.OpType.CREATE_OBJECT, new byte[]{3})
        );
        log.appendEntries(2, 1, entries4);

        // Should still have only 3 entries
        assertEquals(3, log.size(), "Should still have only 3 entries");
        assertEquals(3, log.getLastLogIndex());

        wal.close();

        // Recover and verify
        WAL recoveredWal = new WAL(walFile);
        RaftLog recoveredLog = new RaftLog(recoveredWal);

        assertEquals(3, recoveredLog.size());
        assertEquals(3, recoveredLog.getLastLogIndex());

        recoveredWal.close();
    }

    /**
     * Test: WAL entry count is consistent with in-memory log.
     * This verifies WAL doesn't have duplicate entries.
     */
    @Test
    void testWALConsistencyWithInMemoryLog() throws Exception {
        Path walFile = tempDir.resolve("wal_dup4.dat");
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);

        // Append entries
        for (int i = 1; i <= 5; i++) {
            List<LogEntry> entries = List.of(
                new LogEntry(1, i, 1000 + i, LogEntry.OpType.CREATE_OBJECT, new byte[]{(byte) i})
            );
            log.appendEntries(i - 1, i > 1 ? 1 : 0, entries);
        }

        assertEquals(5, log.size());

        // Recover from WAL
        wal.close();

        WAL recoveredWal = new WAL(walFile);
        WAL.WALRecoveryResult result = recoveredWal.recover();

        assertEquals(5, result.entries.size(), "WAL should have 5 entries");

        // In-memory and WAL should match
        for (int i = 1; i <= 5; i++) {
            assertEquals(i, result.entries.get(i - 1).index());
        }

        recoveredWal.close();
    }

    /**
     * Test: WAL contains each entry only once after duplicate AppendEntries.
     * Inspect WAL recovery result to count entries.
     */
    @Test
    void testWALHasNoDuplicateEntriesAfterDuplicateAppend() throws Exception {
        Path walFile = tempDir.resolve("wal_dup5.dat");
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);

        // First AppendEntries
        List<LogEntry> entries1 = List.of(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1})
        );
        log.appendEntries(0, 0, entries1);

        // Duplicate AppendEntries
        List<LogEntry> entries2 = List.of(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1})
        );
        log.appendEntries(0, 0, entries2);

        // WAL should have only 1 entry
        WAL.WALRecoveryResult result = wal.recover();
        assertEquals(1, result.entries.size(), "WAL should have only 1 entry");

        wal.close();
    }
}
