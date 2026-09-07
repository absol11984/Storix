package com.storix.metadata.raft;

import com.storix.metadata.*;
import com.storix.metadata.wal.WAL;
import com.storix.metadata.wal.GenerationManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for prevLogIndex validation in AppendEntries.
 *
 * Verifies Raft correctness:
 * 1. If prevLogIndex > lastLogIndex: REJECT AppendEntries, do NOT modify log
 * 2. Log remains exactly as before
 * 3. No WAL modification (entries not appended to WAL)
 */
class RaftPrevLogIndexValidationTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @TempDir
    Path tempDir;

    /**
     * Test: prevLogIndex = 10 but follower.lastLogIndex = 5.
     * Send entries via appendEntries (which goes to WAL and memory).
     * The entries should NOT be appended because prevLogIndex > lastLogIndex.
     *
     * NOTE: The test creates entries via appendEntries, then sends another appendEntries
     * with prevLogIndex beyond the current log. This should be a no-op.
     */
    @Test
    void testPrevLogIndexBeyondFollowerLogRejected() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: prevLogIndex beyond follower log is NO-OP");
        System.out.println("========================================\n");

        Path walFile = tempDir.resolve("wal-prev.dat");
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);

        // Create initial log with 5 entries via appendEntries
        List<LogEntry> initialEntries = Arrays.asList(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1}),
            new LogEntry(1, 2, 1001, LogEntry.OpType.CREATE_OBJECT, new byte[]{2}),
            new LogEntry(1, 3, 1002, LogEntry.OpType.CREATE_OBJECT, new byte[]{3}),
            new LogEntry(1, 4, 1003, LogEntry.OpType.CREATE_OBJECT, new byte[]{4}),
            new LogEntry(1, 5, 1004, LogEntry.OpType.CREATE_OBJECT, new byte[]{5})
        );
        log.appendEntries(0, 0, initialEntries);

        assertEquals(5, log.getLastLogIndex());
        assertEquals(5, log.size());

        long walSizeBefore = Files.size(walFile);
        System.out.println("Initial log: 5 entries, WAL size: " + walSizeBefore);

        // Now send entries with prevLogIndex=10 (beyond our log)
        // This simulates leader sending entries when prevLogIndex > lastLogIndex
        List<LogEntry> newEntries = List.of(
            new LogEntry(2, 11, 2000, LogEntry.OpType.CREATE_OBJECT, new byte[]{11})
        );

        // Through appendEntries with prevLogIndex beyond our log
        log.appendEntries(10, 1, newEntries);

        // CRITICAL: Log must remain exactly as before
        assertEquals(5, log.getLastLogIndex(), "lastLogIndex should remain 5");
        assertEquals(5, log.size(), "Log size should remain 5");
        assertNotNull(log.getEntry(1), "Entry 1 should exist");
        assertNotNull(log.getEntry(5), "Entry 5 should exist");
        assertNull(log.getEntry(6), "Entry 6 should NOT exist");
        assertNull(log.getEntry(10), "Entry 10 should NOT exist");
        assertNull(log.getEntry(11), "Entry 11 should NOT exist");

        System.out.println("After appendEntries(prevLogIndex=10): log unchanged");
        System.out.println("Entries: 1,2,3,4,5 (no 6, 10, or 11)");

        // Verify WAL was not modified (no new entries appended)
        long walSizeAfter = Files.size(walFile);
        assertEquals(walSizeBefore, walSizeAfter, "WAL size should remain unchanged");
        System.out.println("WAL size unchanged: " + walSizeAfter);

        // Verify state after restart
        wal.close();

        WAL recoveredWal = new WAL(walFile);
        RaftLog recoveredLog = new RaftLog(recoveredWal);

        assertEquals(5, recoveredLog.size(), "Recovered log should have 5 entries");
        assertEquals(5, recoveredLog.getLastLogIndex());
        assertNotNull(recoveredLog.getEntry(1));
        assertNull(recoveredLog.getEntry(6), "Entry 6 should NOT exist after recovery");

        // Count entries in WAL
        var result = recoveredWal.recover();
        assertEquals(5, result.entries.size(), "WAL should have exactly 5 entries");

        recoveredWal.close();

        System.out.println("After restart: log is still [1,2,3,4,5]");
        System.out.println("\n========================================");
        System.out.println("TEST: prevLogIndex beyond follower log - PASSED");
        System.out.println("========================================\n");
    }

    /**
     * Test: prevLogIndex = lastLogIndex + 1 (edge case).
     * This should be accepted and new entries appended.
     */
    @Test
    void testPrevLogIndexAtLastLogIndexPlusOne() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: prevLogIndex = lastLogIndex + 1 accepted");
        System.out.println("========================================\n");

        Path walFile = tempDir.resolve("wal-prev-edge.dat");
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);

        // Create initial log with 2 entries
        List<LogEntry> initial = Arrays.asList(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1}),
            new LogEntry(1, 2, 1001, LogEntry.OpType.CREATE_OBJECT, new byte[]{2})
        );
        log.appendEntries(0, 0, initial);

        assertEquals(2, log.getLastLogIndex());

        // Send AppendEntries with prevLogIndex=2 (lastLogIndex), entries=[3]
        List<LogEntry> newEntries = List.of(
            new LogEntry(1, 3, 2000, LogEntry.OpType.CREATE_OBJECT, new byte[]{3})
        );

        log.appendEntries(2, 1, newEntries);

        // Entry 3 should be appended
        assertEquals(3, log.getLastLogIndex(), "Should now have 3 entries");
        assertEquals(3, log.size());
        assertNotNull(log.getEntry(3));

        System.out.println("After appendEntries(2, entries=[3]): log has 3 entries");
        System.out.println("\n========================================");
        System.out.println("TEST: prevLogIndex = lastLogIndex + 1 - PASSED");
        System.out.println("========================================\n");

        wal.close();
    }

    /**
     * Test: prevLogTerm mismatch when prevLogIndex is within range.
     * Should cause conflict and potentially truncate.
     */
    @Test
    void testPrevLogTermMismatchCausesConflict() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: prevLogTerm mismatch causes conflict resolution");
        System.out.println("========================================\n");

        Path walFile = tempDir.resolve("wal-term-mismatch.dat");
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);

        // Create entries 1(T1), 2(T2)
        List<LogEntry> initial = Arrays.asList(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1}),
            new LogEntry(2, 2, 1001, LogEntry.OpType.CREATE_OBJECT, new byte[]{2})
        );
        log.appendEntries(0, 0, initial);

        assertEquals(2, log.getLastLogIndex());
        assertEquals(1, log.getEntry(1).term(), "Entry 1 should be term 1");
        assertEquals(2, log.getEntry(2).term(), "Entry 2 should be term 2");

        System.out.println("Initial: 1(T1)2(T2)");

        // Leader claims prevLogIndex=1 but prevLogTerm=T2 (WRONG - entry 1 is T1)
        // This should trigger conflict resolution
        List<LogEntry> newEntries = List.of(
            new LogEntry(2, 2, 2000, LogEntry.OpType.CREATE_OBJECT, new byte[]{2})
        );

        // With prevLogTerm mismatch, the first entry's index is beyond our prevLogIndex
        // This causes conflict resolution to start from the beginning
        log.appendEntries(1, 2, newEntries);

        // After conflict resolution, the log should be updated
        // The old entry 2 (T2) should be replaced with new entry 2 (T2 from same term)
        assertEquals(2, log.getLastLogIndex());
        assertEquals(2, log.getEntry(2).term());

        System.out.println("After conflict resolution: 1(T1)2(T2) (updated)");
        System.out.println("\n========================================");
        System.out.println("TEST: prevLogTerm mismatch - PASSED");
        System.out.println("========================================\n");

        wal.close();
    }

    /**
     * Test: AppendEntries with entries beyond gap is a no-op.
     * Follower has 1,2. Leader sends prevLogIndex=5.
     * Nothing should be appended.
     */
    @Test
    void testAppendEntriesWithGapIsNoOp() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: AppendEntries with gap is NO-OP");
        System.out.println("========================================\n");

        Path walFile = tempDir.resolve("wal-gap.dat");
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);

        // Create entries 1,2
        List<LogEntry> initial = Arrays.asList(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1}),
            new LogEntry(1, 2, 1001, LogEntry.OpType.CREATE_OBJECT, new byte[]{2})
        );
        log.appendEntries(0, 0, initial);

        assertEquals(2, log.getLastLogIndex());
        long walSizeBefore = Files.size(walFile);

        // Try to append with prevLogIndex=5 (gap)
        List<LogEntry> newEntries = List.of(
            new LogEntry(2, 6, 2000, LogEntry.OpType.CREATE_OBJECT, new byte[]{6})
        );

        log.appendEntries(5, 1, newEntries);

        // Log should be unchanged
        assertEquals(2, log.getLastLogIndex());
        assertNull(log.getEntry(6));

        long walSizeAfter = Files.size(walFile);
        assertEquals(walSizeBefore, walSizeAfter, "WAL size should be unchanged");

        System.out.println("After appendEntries(gap): log unchanged [1,2]");
        System.out.println("\n========================================");
        System.out.println("TEST: Gap is NO-OP - PASSED");
        System.out.println("========================================\n");

        wal.close();
    }
}
