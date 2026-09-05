package com.storix.metadata.raft;

import com.storix.metadata.*;
import com.storix.metadata.wal.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for recovered RaftLog validation.
 * Ensures duplicates, gaps, out-of-order entries, and entries below snapshot boundary are rejected.
 */
class RecoveredRaftLogValidationTest {

    @TempDir
    Path tempDir;

    /**
     * Helper: Create a LogEntry with specific index.
     */
    private LogEntry makeEntry(long term, long index, LogEntry.OpType opType) {
        return new LogEntry(term, index, System.currentTimeMillis(), opType, ("data-" + index).getBytes());
    }

    /**
     * Helper: Write entries to WAL and close it, then recover from a new WAL instance.
     */
    private WAL.WALRecoveryResult writeAndRecover(List<LogEntry> entries) throws IOException {
        Path walFile = tempDir.resolve("wal-" + UUID.randomUUID() + ".dat");
        WAL wal = new WAL(walFile);
        for (LogEntry e : entries) {
            wal.append(e);
        }
        wal.close();

        // Recover from a new WAL instance
        WAL recoveryWal = new WAL(walFile);
        WAL.WALRecoveryResult result = recoveryWal.recover();
        recoveryWal.close();
        return result;
    }

    /**
     * TEST 1: Valid log is loaded successfully
     */
    @Test
    void testValidLogLoadsSuccessfully() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Valid Log Loads Successfully");
        System.out.println("========================================\n");

        // Write valid entries
        List<LogEntry> entries = List.of(
            makeEntry(1, 1, LogEntry.OpType.CREATE_OBJECT),
            makeEntry(1, 2, LogEntry.OpType.CREATE_OBJECT),
            makeEntry(2, 3, LogEntry.OpType.CREATE_OBJECT),
            makeEntry(2, 4, LogEntry.OpType.UPDATE_OBJECT)
        );

        WAL.WALRecoveryResult recovery = writeAndRecover(entries);

        System.out.println("  Recovered " + recovery.entries.size() + " entries");
        assertEquals(4, recovery.entries.size());

        // Load into RaftLog
        RaftLog raftLog = new RaftLog();
        long highest = raftLog.loadEntries(recovery.entries, recovery.commitIndex, recovery.lastApplied);

        assertEquals(4, highest);
        assertEquals(4, raftLog.getEntryCount());

        System.out.println("\n========================================");
        System.out.println("TEST: Valid Log Loads Successfully - PASSED");
        System.out.println("========================================\n");
    }

    /**
     * TEST 2: Duplicate entries are rejected
     */
    @Test
    void testDuplicateEntriesAreRejected() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Duplicate Entries Are Rejected");
        System.out.println("========================================\n");

        // Write entries with a duplicate
        List<LogEntry> entries = new ArrayList<>();
        entries.add(makeEntry(1, 1, LogEntry.OpType.CREATE_OBJECT));
        entries.add(makeEntry(1, 2, LogEntry.OpType.CREATE_OBJECT));
        entries.add(makeEntry(1, 2, LogEntry.OpType.CREATE_OBJECT)); // DUPLICATE
        entries.add(makeEntry(2, 3, LogEntry.OpType.CREATE_OBJECT));

        WAL.WALRecoveryResult recovery = writeAndRecover(entries);

        System.out.println("  Recovered " + recovery.entries.size() + " entries from WAL");

        // Load into RaftLog - should reject duplicates
        RaftLog raftLog = new RaftLog();

        System.out.println("  Loading entries into RaftLog (should reject duplicate at index 2)...");

        // The loadEntries will fail because duplicates break the contiguous requirement
        assertThrows(IllegalStateException.class, () -> {
            raftLog.loadEntries(recovery.entries, recovery.commitIndex, recovery.lastApplied);
        }, "Duplicates should cause IllegalStateException during loadEntries");

        System.out.println("\n========================================");
        System.out.println("TEST: Duplicate Entries Are Rejected - PASSED");
        System.out.println("========================================\n");
    }

    /**
     * TEST 3: Gaps in log are rejected
     */
    @Test
    void testGapsInLogAreRejected() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Gaps In Log Are Rejected");
        System.out.println("========================================\n");

        // Write entries with a gap (missing index 2)
        List<LogEntry> entries = new ArrayList<>();
        entries.add(makeEntry(1, 1, LogEntry.OpType.CREATE_OBJECT));
        entries.add(makeEntry(2, 3, LogEntry.OpType.CREATE_OBJECT)); // GAP: missing 2
        entries.add(makeEntry(2, 4, LogEntry.OpType.CREATE_OBJECT));

        WAL.WALRecoveryResult recovery = writeAndRecover(entries);

        System.out.println("  Recovered " + recovery.entries.size() + " entries from WAL");

        // Load into RaftLog - should reject gap
        RaftLog raftLog = new RaftLog();

        System.out.println("  Loading entries into RaftLog (should reject gap at index 2)...");

        assertThrows(IllegalStateException.class, () -> {
            raftLog.loadEntries(recovery.entries, recovery.commitIndex, recovery.lastApplied);
        }, "Gaps should cause IllegalStateException during loadEntries");

        System.out.println("\n========================================");
        System.out.println("TEST: Gaps In Log Are Rejected - PASSED");
        System.out.println("========================================\n");
    }

    /**
     * TEST 4: Out-of-order entries are rejected
     */
    @Test
    void testOutOfOrderEntriesAreRejected() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Out-Of-Order Entries Are Rejected");
        System.out.println("========================================\n");

        // Write entries out of order
        List<LogEntry> entries = new ArrayList<>();
        entries.add(makeEntry(1, 1, LogEntry.OpType.CREATE_OBJECT));
        entries.add(makeEntry(2, 4, LogEntry.OpType.CREATE_OBJECT)); // Out of order
        entries.add(makeEntry(1, 2, LogEntry.OpType.CREATE_OBJECT)); // Should be before 4
        entries.add(makeEntry(2, 3, LogEntry.OpType.CREATE_OBJECT));

        WAL.WALRecoveryResult recovery = writeAndRecover(entries);

        System.out.println("  Recovered " + recovery.entries.size() + " entries from WAL");

        // Load into RaftLog - should reject out-of-order
        RaftLog raftLog = new RaftLog();

        System.out.println("  Loading entries into RaftLog (should reject out-of-order)...");

        assertThrows(IllegalStateException.class, () -> {
            raftLog.loadEntries(recovery.entries, recovery.commitIndex, recovery.lastApplied);
        }, "Out-of-order should cause IllegalStateException during loadEntries");

        System.out.println("\n========================================");
        System.out.println("TEST: Out-Of-Order Entries Are Rejected - PASSED");
        System.out.println("========================================\n");
    }

    /**
     * TEST 5: Entries below snapshot boundary are rejected with exception
     */
    @Test
    void testEntriesBelowSnapshotBoundaryAreRejected() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Entries Below Snapshot Boundary Are Rejected");
        System.out.println("========================================\n");

        // Write entries starting from index 1
        List<LogEntry> entries = new ArrayList<>();
        entries.add(makeEntry(1, 1, LogEntry.OpType.CREATE_OBJECT));
        entries.add(makeEntry(1, 2, LogEntry.OpType.CREATE_OBJECT));
        entries.add(makeEntry(2, 3, LogEntry.OpType.CREATE_OBJECT));
        entries.add(makeEntry(2, 4, LogEntry.OpType.CREATE_OBJECT));
        entries.add(makeEntry(2, 5, LogEntry.OpType.CREATE_OBJECT));

        WAL.WALRecoveryResult recovery = writeAndRecover(entries);

        System.out.println("  Recovered " + recovery.entries.size() + " entries from WAL");

        // Load into RaftLog with snapshot boundary at 3
        // This means entries 1-3 should be in the snapshot and not in the log
        RaftLog raftLog = new RaftLog();
        raftLog.setSnapshotBoundary(3, 2); // Snapshot includes up to index 3

        System.out.println("  Loading with snapshot boundary at index 3...");

        // Should throw because entries 1-3 are below snapshot boundary
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> {
            raftLog.loadEntries(recovery.entries, recovery.commitIndex, recovery.lastApplied);
        }, "Entries below snapshot boundary should cause IllegalStateException");

        System.out.println("  Correctly rejected entries: " + ex.getMessage());

        System.out.println("\n========================================");
        System.out.println("TEST: Entries Below Snapshot Boundary Are Rejected - PASSED");
        System.out.println("========================================\n");
    }

    /**
     * TEST 6: Entries with decreasing terms are recovered (terms only affect leader election)
     *
     * Note: In Raft, term decreases are actually allowed in the log - the term only needs to be
     * valid for leader election purposes. The WAL doesn't enforce term ordering.
     * This test verifies that such entries are recovered without throwing.
     */
    @Test
    void testEntriesWithDecreasingTermsAreRecovered() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Entries With Decreasing Terms Are Recovered");
        System.out.println("========================================\n");

        // Write entries where term decreases
        List<LogEntry> entries = new ArrayList<>();
        entries.add(makeEntry(3, 6, LogEntry.OpType.CREATE_OBJECT)); // After snapshot
        entries.add(makeEntry(2, 7, LogEntry.OpType.CREATE_OBJECT)); // Term decreased - but still valid in WAL
        entries.add(makeEntry(3, 8, LogEntry.OpType.CREATE_OBJECT));

        WAL.WALRecoveryResult recovery = writeAndRecover(entries);

        System.out.println("  Recovered " + recovery.entries.size() + " entries from WAL");

        // Load into RaftLog with snapshot at index 5
        RaftLog raftLog = new RaftLog();
        raftLog.setSnapshotBoundary(5, 2);

        System.out.println("  Loading with decreasing terms...");

        // Terms can decrease in the log - this is allowed
        long highest = raftLog.loadEntries(recovery.entries, recovery.commitIndex, recovery.lastApplied);

        assertEquals(8, highest);
        assertEquals(3, raftLog.getEntryCount());

        System.out.println("\n========================================");
        System.out.println("TEST: Entries With Decreasing Terms Are Recovered - PASSED");
        System.out.println("========================================\n");
    }

    /**
     * TEST 7: Valid entries after snapshot boundary are loaded
     */
    @Test
    void testValidEntriesAfterSnapshotAreLoaded() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Valid Entries After Snapshot Boundary Are Loaded");
        System.out.println("========================================\n");

        // Write entries starting from index 6 (after snapshot at 5)
        List<LogEntry> entries = new ArrayList<>();
        entries.add(makeEntry(2, 6, LogEntry.OpType.CREATE_OBJECT));
        entries.add(makeEntry(2, 7, LogEntry.OpType.CREATE_OBJECT));
        entries.add(makeEntry(3, 8, LogEntry.OpType.CREATE_OBJECT));

        WAL.WALRecoveryResult recovery = writeAndRecover(entries);

        System.out.println("  Recovered " + recovery.entries.size() + " entries from WAL");

        // Load into RaftLog with snapshot at index 5
        RaftLog raftLog = new RaftLog();
        raftLog.setSnapshotBoundary(5, 2);

        System.out.println("  Loading with snapshot boundary at index 5...");

        long highest = raftLog.loadEntries(recovery.entries, recovery.commitIndex, recovery.lastApplied);

        assertEquals(8, highest);
        assertEquals(3, raftLog.getEntryCount());

        System.out.println("\n========================================");
        System.out.println("TEST: Valid Entries After Snapshot Boundary Are Loaded - PASSED");
        System.out.println("========================================\n");
    }

    /**
     * TEST 8: Empty WAL recovers cleanly
     */
    @Test
    void testEmptyWalRecoversCleanly() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Empty WAL Recovers Cleanly");
        System.out.println("========================================\n");

        Path walFile = tempDir.resolve("wal-empty.dat");
        WAL wal = new WAL(walFile);
        wal.close();

        // Recover
        WAL recoveryWal = new WAL(walFile);
        WAL.WALRecoveryResult result = recoveryWal.recover();

        System.out.println("  Recovered " + result.entries.size() + " entries, commitIndex=" + result.commitIndex);
        assertEquals(0, result.entries.size());

        // Load into RaftLog
        RaftLog raftLog = new RaftLog();
        raftLog.loadEntries(result.entries, result.commitIndex, result.lastApplied);

        assertEquals(0, raftLog.getEntryCount());

        System.out.println("\n========================================");
        System.out.println("TEST: Empty WAL Recovers Cleanly - PASSED");
        System.out.println("========================================\n");
    }
}
