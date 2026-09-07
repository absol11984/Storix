package com.storix.metadata.raft;

import com.storix.metadata.wal.WAL;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for truncateFrom() boundary conditions through RaftLog.appendEntries().
 *
 * The correct production path for truncation is:
 *   RaftLog.appendEntries() -> handles conflict detection -> calls WAL.truncateFrom()
 *
 * These tests verify:
 * 1. truncateFrom beyond lastLogIndex is a no-op
 * 2. truncateFrom at lastLogIndex removes the last entry
 * 3. truncateFrom in middle removes suffix
 * 4. WAL and in-memory log stay consistent
 * 5. Recovery produces correct state
 */
class RaftLogTruncateBoundaryTest {

    @TempDir
    Path tempDir;

    /**
     * Test: AppendEntries with prevLogIndex beyond follower log.
     * The follower should reject and not modify the log.
     */
    @Test
    void testAppendEntriesBeyondLastLogIndexIsNoOp() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: AppendEntries beyond lastLogIndex is NO-OP");
        System.out.println("========================================\n");

        Path walFile = tempDir.resolve("wal_trunc1.dat");
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);

        // Set up log: 1,2,3,4,5 via appendEntries
        List<LogEntry> initial = Arrays.asList(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1}),
            new LogEntry(1, 2, 1001, LogEntry.OpType.CREATE_OBJECT, new byte[]{2}),
            new LogEntry(1, 3, 1002, LogEntry.OpType.CREATE_OBJECT, new byte[]{3}),
            new LogEntry(1, 4, 1003, LogEntry.OpType.CREATE_OBJECT, new byte[]{4}),
            new LogEntry(1, 5, 1004, LogEntry.OpType.CREATE_OBJECT, new byte[]{5})
        );
        log.appendEntries(0, 0, initial);

        assertEquals(5, log.size());
        assertEquals(5, log.getLastLogIndex());

        // Now try to append entries with prevLogIndex=10 (beyond our log)
        // This simulates the leader claiming we have entry 10 when we only have up to 5
        List<LogEntry> newEntries = List.of(
            new LogEntry(2, 11, 2000, LogEntry.OpType.CREATE_OBJECT, new byte[]{11})
        );

        // Through appendEntries with prevLogIndex beyond our log
        log.appendEntries(10, 1, newEntries);

        // Log should be UNCHANGED - entries should NOT be appended
        // The conflict detection in appendEntries should handle this
        assertEquals(5, log.size(), "Log should remain at 5 entries");
        assertEquals(5, log.getLastLogIndex());
        assertNotNull(log.getEntry(1));
        assertNotNull(log.getEntry(5));
        assertNull(log.getEntry(6), "Entry 6 should NOT exist");

        System.out.println("After appendEntries(10, ...): log is still [1,2,3,4,5]");

        wal.close();

        // Verify after restart - should have exactly 5 entries
        WAL recoveredWal = new WAL(walFile);
        RaftLog recoveredLog = new RaftLog(recoveredWal);

        assertEquals(5, recoveredLog.size());
        assertEquals(5, recoveredLog.getLastLogIndex());
        assertNotNull(recoveredLog.getEntry(1));
        assertNull(recoveredLog.getEntry(6));

        recoveredWal.close();

        System.out.println("After restart: log is still [1,2,3,4,5]");
        System.out.println("\n========================================");
        System.out.println("TEST: AppendEntries beyond lastLogIndex - PASSED");
        System.out.println("========================================\n");
    }

    /**
     * Test: Conflict resolution via appendEntries truncates suffix correctly.
     * Initial: 1,2,3,4,5
     * New: Replace 4,5 with 4',5'
     */
    @Test
    void testConflictResolutionTruncatesSuffix() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Conflict resolution truncates suffix");
        System.out.println("========================================\n");

        Path walFile = tempDir.resolve("wal_trunc2.dat");
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);

        // Set up log: 1,2,3,4,5 (all term 1)
        List<LogEntry> initial = Arrays.asList(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1}),
            new LogEntry(1, 2, 1001, LogEntry.OpType.CREATE_OBJECT, new byte[]{2}),
            new LogEntry(1, 3, 1002, LogEntry.OpType.CREATE_OBJECT, new byte[]{3}),
            new LogEntry(1, 4, 1003, LogEntry.OpType.CREATE_OBJECT, new byte[]{4}),
            new LogEntry(1, 5, 1004, LogEntry.OpType.CREATE_OBJECT, new byte[]{5})
        );
        log.appendEntries(0, 0, initial);

        assertEquals(5, log.size());

        System.out.println("Initial: 1(T1)2(T1)3(T1)4(T1)5(T1)");

        // Now send entries with different terms starting at index 4
        // This simulates leader having: 1(T1)2(T1)3(T2)4(T2)5(T2)
        List<LogEntry> newEntries = Arrays.asList(
            new LogEntry(2, 4, 2000, LogEntry.OpType.CREATE_OBJECT, new byte[]{4}),
            new LogEntry(2, 5, 2001, LogEntry.OpType.CREATE_OBJECT, new byte[]{5})
        );
        log.appendEntries(3, 1, newEntries);

        // Entries 4 and 5 should be replaced with term 2
        assertEquals(5, log.size());
        assertEquals(1, log.getEntry(1).term());
        assertEquals(1, log.getEntry(2).term());
        assertEquals(1, log.getEntry(3).term());
        assertEquals(2, log.getEntry(4).term(), "Entry 4 should now be term 2");
        assertEquals(2, log.getEntry(5).term(), "Entry 5 should now be term 2");

        System.out.println("After conflict resolution: 1(T1)2(T1)3(T1)4(T2)5(T2)");

        wal.close();

        // Verify after restart
        WAL recoveredWal = new WAL(walFile);
        RaftLog recoveredLog = new RaftLog(recoveredWal);

        assertEquals(5, recoveredLog.size());
        assertEquals(2, recoveredLog.getEntry(4).term());
        assertEquals(2, recoveredLog.getEntry(5).term());

        recoveredWal.close();

        System.out.println("After restart: same state");
        System.out.println("\n========================================");
        System.out.println("TEST: Conflict resolution truncates suffix - PASSED");
        System.out.println("========================================\n");
    }

    /**
     * Test: truncateFrom(3) removes entries 3 and onwards.
     * Simulated via conflict resolution.
     */
    @Test
    void testConflictResolutionTruncatesFromMiddle() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Conflict resolution truncates from middle");
        System.out.println("========================================\n");

        Path walFile = tempDir.resolve("wal_trunc3.dat");
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);

        // Set up log: 1,2,3,4,5 (all term 1)
        List<LogEntry> initial = Arrays.asList(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1}),
            new LogEntry(1, 2, 1001, LogEntry.OpType.CREATE_OBJECT, new byte[]{2}),
            new LogEntry(1, 3, 1002, LogEntry.OpType.CREATE_OBJECT, new byte[]{3}),
            new LogEntry(1, 4, 1003, LogEntry.OpType.CREATE_OBJECT, new byte[]{4}),
            new LogEntry(1, 5, 1004, LogEntry.OpType.CREATE_OBJECT, new byte[]{5})
        );
        log.appendEntries(0, 0, initial);

        System.out.println("Initial: 1(T1)2(T1)3(T1)4(T1)5(T1)");

        // Simulate truncate from index 3 by sending new entries starting at 3
        // Leader has: 1(T1)2(T1)3'(T2)4'(T2)
        List<LogEntry> newEntries = Arrays.asList(
            new LogEntry(2, 3, 2000, LogEntry.OpType.CREATE_OBJECT, new byte[]{3}),
            new LogEntry(2, 4, 2001, LogEntry.OpType.CREATE_OBJECT, new byte[]{4})
        );
        log.appendEntries(2, 1, newEntries);

        // Log should be: 1,2,3',4'
        assertEquals(4, log.size());
        assertEquals(1, log.getEntry(1).term());
        assertEquals(1, log.getEntry(2).term());
        assertEquals(2, log.getEntry(3).term());
        assertEquals(2, log.getEntry(4).term());
        assertNull(log.getEntry(5), "Entry 5 should be removed");

        System.out.println("After truncation from 3: 1(T1)2(T1)3(T2)4(T2)");

        wal.close();

        // Verify after restart
        WAL recoveredWal = new WAL(walFile);
        RaftLog recoveredLog = new RaftLog(recoveredWal);

        assertEquals(4, recoveredLog.size());
        assertEquals(2, recoveredLog.getEntry(3).term());
        assertNull(recoveredLog.getEntry(5));

        recoveredWal.close();

        System.out.println("\n========================================");
        System.out.println("TEST: Conflict resolution truncates from middle - PASSED");
        System.out.println("========================================\n");
    }

    /**
     * Test: Multiple sequential conflict resolutions work correctly.
     */
    @Test
    void testSequentialConflictResolutions() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Sequential conflict resolutions");
        System.out.println("========================================\n");

        Path walFile = tempDir.resolve("wal_trunc4.dat");
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);

        // Start with entries 1,2,3,4,5
        List<LogEntry> initial = Arrays.asList(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1}),
            new LogEntry(1, 2, 1001, LogEntry.OpType.CREATE_OBJECT, new byte[]{2}),
            new LogEntry(1, 3, 1002, LogEntry.OpType.CREATE_OBJECT, new byte[]{3}),
            new LogEntry(1, 4, 1003, LogEntry.OpType.CREATE_OBJECT, new byte[]{4}),
            new LogEntry(1, 5, 1004, LogEntry.OpType.CREATE_OBJECT, new byte[]{5})
        );
        log.appendEntries(0, 0, initial);

        // First conflict: replace 4,5 with 4',5' (term 2)
        log.appendEntries(3, 1, Arrays.asList(
            new LogEntry(2, 4, 2000, LogEntry.OpType.CREATE_OBJECT, new byte[]{4}),
            new LogEntry(2, 5, 2001, LogEntry.OpType.CREATE_OBJECT, new byte[]{5})
        ));
        assertEquals(5, log.size());
        assertEquals(2, log.getEntry(4).term());
        System.out.println("After first: 1(T1)2(T1)3(T1)4(T2)5(T2)");

        // Second conflict: truncate from 3, replace with 3',4',5' (term 3)
        log.appendEntries(2, 1, Arrays.asList(
            new LogEntry(3, 3, 3000, LogEntry.OpType.CREATE_OBJECT, new byte[]{3}),
            new LogEntry(3, 4, 3001, LogEntry.OpType.CREATE_OBJECT, new byte[]{4}),
            new LogEntry(3, 5, 3002, LogEntry.OpType.CREATE_OBJECT, new byte[]{5})
        ));
        assertEquals(5, log.size());
        assertEquals(3, log.getEntry(3).term());
        assertEquals(3, log.getEntry(4).term());
        assertEquals(3, log.getEntry(5).term());
        System.out.println("After second: 1(T1)2(T1)3(T3)4(T3)5(T3)");

        // Third: append beyond, should be no-op
        log.appendEntries(10, 1, List.of(
            new LogEntry(4, 11, 4000, LogEntry.OpType.CREATE_OBJECT, new byte[]{11})
        ));
        assertEquals(5, log.size(), "Should remain 5 entries");
        System.out.println("After beyond: still 5 entries (no change)");

        wal.close();

        // Verify after restart
        WAL recoveredWal = new WAL(walFile);
        RaftLog recoveredLog = new RaftLog(recoveredWal);

        assertEquals(5, recoveredLog.size());
        assertEquals(1, recoveredLog.getEntry(1).term());
        assertEquals(1, recoveredLog.getEntry(2).term());
        assertEquals(3, recoveredLog.getEntry(3).term());
        assertEquals(3, recoveredLog.getEntry(4).term());
        assertEquals(3, recoveredLog.getEntry(5).term());

        recoveredWal.close();

        System.out.println("After restart: 1(T1)2(T1)3(T3)4(T3)5(T3)");
        System.out.println("\n========================================");
        System.out.println("TEST: Sequential conflict resolutions - PASSED");
        System.out.println("========================================\n");
    }

    /**
     * Test: WAL file is consistent after truncation.
     * Verify no extra entries in WAL after restart.
     */
    @Test
    void testWALConsistencyAfterTruncation() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: WAL consistency after truncation");
        System.out.println("========================================\n");

        Path walFile = tempDir.resolve("wal_trunc5.dat");
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);

        // Create entries 1-5
        for (int i = 1; i <= 5; i++) {
            log.append(new LogEntry(1, i, 1000 + i, LogEntry.OpType.CREATE_OBJECT, new byte[]{(byte) i}));
        }

        // Truncate via conflict from index 3
        log.appendEntries(2, 1, Arrays.asList(
            new LogEntry(2, 3, 2000, LogEntry.OpType.CREATE_OBJECT, new byte[]{3}),
            new LogEntry(2, 4, 2001, LogEntry.OpType.CREATE_OBJECT, new byte[]{4})
        ));

        assertEquals(4, log.size());

        wal.close();

        // Recover and count entries in WAL
        WAL recoveredWal = new WAL(walFile);
        var result = recoveredWal.recover();

        assertEquals(4, result.entries.size(), "WAL should have exactly 4 entries after recovery");
        assertEquals(1, result.entries.get(0).index());
        assertEquals(2, result.entries.get(1).index());
        assertEquals(3, result.entries.get(2).index());
        assertEquals(4, result.entries.get(3).index());
        assertNull(result.entries.stream().filter(e -> e.index() == 5).findFirst().orElse(null),
            "Entry 5 should NOT be in WAL");

        recoveredWal.close();

        System.out.println("WAL contains exactly 4 entries: 1,2,3,4");
        System.out.println("Entry 5 correctly absent from WAL");
        System.out.println("\n========================================");
        System.out.println("TEST: WAL consistency - PASSED");
        System.out.println("========================================\n");
    }
}
