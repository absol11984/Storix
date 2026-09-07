package com.storix.metadata.raft;

import com.storix.metadata.wal.WAL;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for RaftLog.appendEntries() conflict resolution.
 *
 * Verifies:
 * 1. Per-entry term conflict detection
 * 2. Conflict truncation
 * 3. WAL truncation consistency
 * 4. Recovery consistency
 */
class RaftLogConflictResolutionTest {

    @TempDir
    Path tempDir;

    /**
     * Test: Leader has 1(T1)2(T1)3(T2), Follower has 1(T1)2(T1)3(T3)4(T3).
     * Apply AppendEntries(prevLogIndex=2, prevLogTerm=T1, entries=[3(T2)]).
     * Expected: Follower becomes 1(T1)2(T1)3(T2), entry 4 is removed.
     */
    @Test
    void testConflictTruncation() throws Exception {
        Path walFile = tempDir.resolve("wal.dat");
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);

        // Set up follower state using appendEntries
        // Use prevLogIndex=0 to indicate starting from empty log
        List<LogEntry> initialEntries = List.of(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1}),
            new LogEntry(1, 2, 1001, LogEntry.OpType.CREATE_OBJECT, new byte[]{2}),
            new LogEntry(3, 3, 1002, LogEntry.OpType.CREATE_OBJECT, new byte[]{3}), // Wrong term
            new LogEntry(3, 4, 1003, LogEntry.OpType.CREATE_OBJECT, new byte[]{4})  // Wrong term
        );
        log.appendEntries(0, 0, initialEntries);

        assertEquals(4, log.size());
        assertEquals(1, log.getEntry(1).term());
        assertEquals(1, log.getEntry(2).term());
        assertEquals(3, log.getEntry(3).term());
        assertEquals(3, log.getEntry(4).term());

        // Leader sends entry 3 with correct term
        List<LogEntry> leaderEntries = List.of(
            new LogEntry(2, 3, 2000, LogEntry.OpType.CREATE_OBJECT, new byte[]{3})
        );

        log.appendEntries(2, 1, leaderEntries);

        // Verify: entries 1,2 unchanged, entry 3 replaced, entry 4 removed
        assertEquals(3, log.size(), "Should have 3 entries after truncation");
        assertEquals(1, log.getEntry(1).term());
        assertEquals(1, log.getEntry(2).term());
        assertEquals(2, log.getEntry(3).term(), "Entry 3 should now have term 2");
        assertNull(log.getEntry(4), "Entry 4 should have been truncated");

        wal.close();
    }

    /**
     * Test: Leader has 1(T1)2(T1)3(T2), Follower has 1(T1)2(T1)3(T3)4(T3).
     * After conflict resolution, recover from WAL.
     * Expected: Recovered state matches pre-recovery state.
     */
    @Test
    void testConflictTruncationPersistenceAfterRestart() throws Exception {
        Path walFile = tempDir.resolve("wal2.dat");
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);

        // Set up follower state
        // Use prevLogIndex=0 to indicate starting from empty log
        List<LogEntry> initialEntries = List.of(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1}),
            new LogEntry(1, 2, 1001, LogEntry.OpType.CREATE_OBJECT, new byte[]{2}),
            new LogEntry(3, 3, 1002, LogEntry.OpType.CREATE_OBJECT, new byte[]{3}),
            new LogEntry(3, 4, 1003, LogEntry.OpType.CREATE_OBJECT, new byte[]{4})
        );
        log.appendEntries(0, 0, initialEntries);

        // Apply conflict resolution
        List<LogEntry> leaderEntries = List.of(
            new LogEntry(2, 3, 2000, LogEntry.OpType.CREATE_OBJECT, new byte[]{3})
        );
        log.appendEntries(2, 1, leaderEntries);

        assertEquals(3, log.size());
        assertEquals(2, log.getEntry(3).term());

        wal.close();

        // Restart and recover
        WAL recoveredWal = new WAL(walFile);
        RaftLog recoveredLog = new RaftLog(recoveredWal);

        assertEquals(3, recoveredLog.size(), "Should have 3 entries after recovery");
        assertEquals(1, recoveredLog.getEntry(1).term());
        assertEquals(1, recoveredLog.getEntry(2).term());
        assertEquals(2, recoveredLog.getEntry(3).term(), "Entry 3 should have term 2 after recovery");
        assertNull(recoveredLog.getEntry(4), "Entry 4 should not exist after recovery");

        recoveredWal.close();
    }

    /**
     * Test: Same-term existing entries are kept, no duplicates.
     * Follower: 1(T1)2(T2), Leader sends: prevLogIndex=1, entries=[2(T2)].
     * Expected: Entry 2 remains unchanged, no duplicate.
     */
    @Test
    void testSameTermExistingEntryKept() throws Exception {
        Path walFile = tempDir.resolve("wal3.dat");
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);

        // Set up log
        List<LogEntry> initialEntries = List.of(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1}),
            new LogEntry(2, 2, 1001, LogEntry.OpType.CREATE_OBJECT, new byte[]{2})
        );
        log.appendEntries(0, 0, initialEntries);

        // Leader sends same entry
        List<LogEntry> leaderEntries = List.of(
            new LogEntry(2, 2, 2000, LogEntry.OpType.CREATE_OBJECT, new byte[]{2})
        );

        log.appendEntries(1, 1, leaderEntries);

        // Should still have only 2 entries
        assertEquals(2, log.size(), "Should still have 2 entries");
        assertEquals(2, log.getEntry(2).term(), "Entry 2 term should remain 2");
        assertEquals(1001, log.getEntry(2).timestamp(), "Entry 2 timestamp should remain unchanged");

        wal.close();
    }

    /**
     * Test: Multiple conflicts in a single AppendEntries.
     * Leader: 4(T4)5(T4), Follower: 3(T3)4(T3)5(T3).
     * Leader's prevLogIndex=3, prevLogTerm=3.
     * Expected: Entries 4 and 5 are replaced with term 4.
     */
    @Test
    void testMultipleConflictsInSingleAppend() throws Exception {
        Path walFile = tempDir.resolve("wal4.dat");
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);

        // Set up follower with entries 3,4,5
        List<LogEntry> initialEntries = List.of(
            new LogEntry(3, 3, 999, LogEntry.OpType.CREATE_OBJECT, new byte[]{3}),
            new LogEntry(3, 4, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{4}), // Wrong term
            new LogEntry(3, 5, 1001, LogEntry.OpType.CREATE_OBJECT, new byte[]{5})  // Wrong term
        );
        log.appendEntries(2, 0, initialEntries);

        assertEquals(3, log.size());

        // Leader sends correct entries for 4 and 5
        List<LogEntry> leaderEntries = List.of(
            new LogEntry(4, 4, 2000, LogEntry.OpType.CREATE_OBJECT, new byte[]{4}),
            new LogEntry(4, 5, 2001, LogEntry.OpType.CREATE_OBJECT, new byte[]{5})
        );

        log.appendEntries(3, 3, leaderEntries);

        // Both should be replaced
        assertEquals(3, log.size());
        assertEquals(3, log.getEntry(3).term());
        assertEquals(4, log.getEntry(4).term(), "Entry 4 should have term 4");
        assertEquals(4, log.getEntry(5).term(), "Entry 5 should have term 4");

        wal.close();
    }

    /**
     * Test: No conflict - follower is up to date.
     * Follower: 1(T1)2(T1), Leader sends: prevLogIndex=2, entries=[3(T1)].
     * Expected: Entry 3 appended, no truncation.
     */
    @Test
    void testNoConflictAppend() throws Exception {
        Path walFile = tempDir.resolve("wal5.dat");
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);

        // Set up initial entries
        List<LogEntry> initialEntries = List.of(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1}),
            new LogEntry(1, 2, 1001, LogEntry.OpType.CREATE_OBJECT, new byte[]{2})
        );
        log.appendEntries(0, 0, initialEntries);

        // Leader sends new entry
        List<LogEntry> leaderEntries = List.of(
            new LogEntry(1, 3, 2000, LogEntry.OpType.CREATE_OBJECT, new byte[]{3})
        );

        log.appendEntries(2, 1, leaderEntries);

        assertEquals(3, log.size());
        assertEquals(1, log.getEntry(1).term());
        assertEquals(1, log.getEntry(2).term());
        assertEquals(1, log.getEntry(3).term());

        wal.close();
    }
}
