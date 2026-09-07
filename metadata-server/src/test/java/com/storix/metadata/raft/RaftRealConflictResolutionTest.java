package com.storix.metadata.raft;

import com.storix.metadata.*;
import com.storix.metadata.wal.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for REAL conflict resolution through the actual Raft AppendEntries path.
 *
 * Verifies:
 * 1. Per-entry conflict detection through handleAppendEntries
 * 2. Conflict suffix truncation via handleAppendEntries
 * 3. WAL consistency after conflict resolution
 * 4. Exact state after restart
 */
class RaftRealConflictResolutionTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @TempDir
    Path tempDir;

    /**
     * Test: Real conflict resolution through RaftNode.handleAppendEntries()
     *
     * Initial state:
     *   Leader: 1(T1)2(T1)3(T2)
     *   Follower: 1(T1)2(T1)3(T3)4(T3)
     *
     * Send AppendEntries(prevLogIndex=2, prevLogTerm=T1, entries=[3(T2)])
     *
     * Expected:
     *   Follower becomes: 1(T1)2(T1)3(T2)
     *   Entry 4 removed
     */
    @Test
    void testRealConflictResolutionThroughRaft() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Real conflict resolution through Raft path");
        System.out.println("========================================\n");

        Path raftDir = tempDir.resolve("raft-conflict");
        Files.createDirectories(raftDir);
        Path walFile = raftDir.resolve("wal.dat");
        Path metaFile = tempDir.resolve("meta-conflict.json");

        // Setup follower
        ClusterConfig config = new ClusterConfig("test", "follower", "127.0.0.1", 66000, null);
        MetadataStore store = new MetadataStore(metaFile);
        GenerationManager genMgr = new GenerationManager(raftDir);
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);
        RaftNode follower = new RaftNode(config, raftDir, log, wal);
        follower.setGenerationManager(genMgr);
        follower.setMetadataStore(store);
        MetadataStateMachine sm = new MetadataStateMachine(store);
        follower.setLogEntryApplier(entry -> {
            try { sm.apply(entry); }
            catch (Exception e) { throw new RuntimeException(e); }
        });

        // Simulate follower having conflicting state: 1(T1)2(T1)3(T3)4(T3)
        // Use appendEntries to set up the state
        List<LogEntry> conflictEntries = Arrays.asList(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, objectMapper.writeValueAsBytes(new ObjectMetadata("obj1", 100L, 4096))),
            new LogEntry(1, 2, 1001, LogEntry.OpType.CREATE_OBJECT, objectMapper.writeValueAsBytes(new ObjectMetadata("obj2", 100L, 4096))),
            new LogEntry(3, 3, 1002, LogEntry.OpType.CREATE_OBJECT, objectMapper.writeValueAsBytes(new ObjectMetadata("obj3", 100L, 4096))),
            new LogEntry(3, 4, 1003, LogEntry.OpType.CREATE_OBJECT, objectMapper.writeValueAsBytes(new ObjectMetadata("obj4", 100L, 4096)))
        );
        log.appendEntries(0, 0, conflictEntries);

        assertEquals(4, log.getLastLogIndex());
        assertEquals(1, log.getEntry(1).term());
        assertEquals(1, log.getEntry(2).term());
        assertEquals(3, log.getEntry(3).term(), "Entry 3 should be term 3");
        assertEquals(3, log.getEntry(4).term(), "Entry 4 should be term 3");

        System.out.println("Initial follower state: 1(T1)2(T1)3(T3)4(T3)");

        // Leader sends AppendEntries to fix the conflict
        // Leader has: 1(T1)2(T1)3(T2)
        // Follower should become: 1(T1)2(T1)3(T2)
        List<LogEntry> correctEntries = List.of(
            new LogEntry(2, 3, 2000, LogEntry.OpType.CREATE_OBJECT, objectMapper.writeValueAsBytes(new ObjectMetadata("obj3-correct", 100L, 4096)))
        );

        // Send through actual handleAppendEntries
        RaftMessage.AppendEntriesResponse response = follower.handleAppendEntries(
            2, "leader", 2, 1, correctEntries, 0
        );

        assertTrue(response.success(), "AppendEntries should succeed");

        // Verify: entry 3 replaced, entry 4 removed
        assertEquals(3, log.getLastLogIndex(), "Should have 3 entries after conflict resolution");
        assertEquals(1, log.getEntry(1).term());
        assertEquals(1, log.getEntry(2).term());
        assertEquals(2, log.getEntry(3).term(), "Entry 3 should now be term 2");
        assertNull(log.getEntry(4), "Entry 4 should be removed");

        System.out.println("After conflict resolution: 1(T1)2(T1)3(T2)");
        System.out.println("Entry 4 correctly removed");

        // Verify WAL consistency
        wal.close();

        WAL recoveredWal = new WAL(walFile);
        RaftLog recoveredLog = new RaftLog(recoveredWal);

        assertEquals(3, recoveredLog.getLastLogIndex(), "Recovered log should have 3 entries");
        assertEquals(1, recoveredLog.getEntry(1).term());
        assertEquals(1, recoveredLog.getEntry(2).term());
        assertEquals(2, recoveredLog.getEntry(3).term(), "Entry 3 should be term 2 after recovery");
        assertNull(recoveredLog.getEntry(4), "Entry 4 should not exist after recovery");

        recoveredWal.close();

        System.out.println("After restart: state matches exactly");
        System.out.println("\n========================================");
        System.out.println("TEST: Real conflict resolution - PASSED");
        System.out.println("========================================\n");
    }

    /**
     * Test: Multiple conflicts in single AppendEntries.
     *
     * Follower: 3(T3)4(T3)5(T3)
     * Leader: 3(T2)4(T2)5(T2)
     *
     * Send AppendEntries(prevLogIndex=2, entries=[3(T2)4(T2)5(T2)])
     *
     * Expected: All entries replaced
     */
    @Test
    void testMultipleConflictResolution() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Multiple conflict resolution");
        System.out.println("========================================\n");

        Path raftDir = tempDir.resolve("raft-multi-conflict");
        Files.createDirectories(raftDir);
        Path walFile = raftDir.resolve("wal.dat");
        Path metaFile = tempDir.resolve("meta-multi.json");

        ClusterConfig config = new ClusterConfig("test", "follower", "127.0.0.1", 66001, null);
        MetadataStore store = new MetadataStore(metaFile);
        GenerationManager genMgr = new GenerationManager(raftDir);
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);
        RaftNode follower = new RaftNode(config, raftDir, log, wal);
        follower.setGenerationManager(genMgr);
        follower.setMetadataStore(store);

        // Set up conflicting state: 1(T1)2(T1)3(T3)4(T3)5(T3)
        // We need entries 1 and 2 for handleAppendEntries to accept the conflict resolution
        List<LogEntry> conflictEntries = Arrays.asList(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1}),
            new LogEntry(1, 2, 1001, LogEntry.OpType.CREATE_OBJECT, new byte[]{2}),
            new LogEntry(3, 3, 1002, LogEntry.OpType.CREATE_OBJECT, new byte[]{3}),
            new LogEntry(3, 4, 1003, LogEntry.OpType.CREATE_OBJECT, new byte[]{4}),
            new LogEntry(3, 5, 1004, LogEntry.OpType.CREATE_OBJECT, new byte[]{5})
        );
        log.appendEntries(0, 0, conflictEntries);

        assertEquals(5, log.getLastLogIndex());
        assertEquals(3, log.getEntry(3).term());

        System.out.println("Initial follower: 1(T1)2(T1)3(T3)4(T3)5(T3)");

        // Leader sends correct entries
        List<LogEntry> correctEntries = Arrays.asList(
            new LogEntry(2, 3, 2000, LogEntry.OpType.CREATE_OBJECT, new byte[]{3}),
            new LogEntry(2, 4, 2001, LogEntry.OpType.CREATE_OBJECT, new byte[]{4}),
            new LogEntry(2, 5, 2002, LogEntry.OpType.CREATE_OBJECT, new byte[]{5})
        );

        RaftMessage.AppendEntriesResponse response = follower.handleAppendEntries(
            2, "leader", 2, 1, correctEntries, 0
        );

        assertTrue(response.success());

        // Verify all entries replaced
        assertEquals(5, log.getLastLogIndex());
        assertEquals(2, log.getEntry(3).term(), "Entry 3 should be term 2");
        assertEquals(2, log.getEntry(4).term(), "Entry 4 should be term 2");
        assertEquals(2, log.getEntry(5).term(), "Entry 5 should be term 2");

        System.out.println("After resolution: 3(T2)4(T2)5(T2)");

        wal.close();

        // Verify after restart
        WAL recoveredWal = new WAL(walFile);
        RaftLog recoveredLog = new RaftLog(recoveredWal);

        assertEquals(5, recoveredLog.getLastLogIndex());
        assertEquals(2, recoveredLog.getEntry(3).term());
        assertEquals(2, recoveredLog.getEntry(4).term());
        assertEquals(2, recoveredLog.getEntry(5).term());

        recoveredWal.close();

        System.out.println("\n========================================");
        System.out.println("TEST: Multiple conflict resolution - PASSED");
        System.out.println("========================================\n");
    }

    /**
     * Test: No conflict - entries should be appended without truncation.
     *
     * Follower: 1(T1)2(T1)
     * Leader sends: AppendEntries(prevLogIndex=2, entries=[3(T2)])
     *
     * Expected: Entry 3 appended, no truncation
     */
    @Test
    void testNoConflictAppend() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: No conflict - append new entry");
        System.out.println("========================================\n");

        Path raftDir = tempDir.resolve("raft-no-conflict");
        Files.createDirectories(raftDir);
        Path walFile = raftDir.resolve("wal.dat");
        Path metaFile = tempDir.resolve("meta-noconflict.json");

        ClusterConfig config = new ClusterConfig("test", "follower", "127.0.0.1", 66002, null);
        MetadataStore store = new MetadataStore(metaFile);
        GenerationManager genMgr = new GenerationManager(raftDir);
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);
        RaftNode follower = new RaftNode(config, raftDir, log, wal);
        follower.setGenerationManager(genMgr);
        follower.setMetadataStore(store);

        // Set up initial state
        List<LogEntry> initialEntries = Arrays.asList(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1}),
            new LogEntry(1, 2, 1001, LogEntry.OpType.CREATE_OBJECT, new byte[]{2})
        );
        log.appendEntries(0, 0, initialEntries);

        assertEquals(2, log.getLastLogIndex());

        System.out.println("Initial follower: 1(T1)2(T1)");

        // Leader sends new entry
        List<LogEntry> newEntries = List.of(
            new LogEntry(2, 3, 2000, LogEntry.OpType.CREATE_OBJECT, new byte[]{3})
        );

        RaftMessage.AppendEntriesResponse response = follower.handleAppendEntries(
            2, "leader", 2, 1, newEntries, 0
        );

        assertTrue(response.success());

        // Verify entry 3 appended, no truncation
        assertEquals(3, log.getLastLogIndex());
        assertEquals(1, log.getEntry(1).term());
        assertEquals(1, log.getEntry(2).term());
        assertEquals(2, log.getEntry(3).term());

        System.out.println("After append: 1(T1)2(T1)3(T2)");
        System.out.println("\n========================================");
        System.out.println("TEST: No conflict append - PASSED");
        System.out.println("========================================\n");

        wal.close();
    }

    /**
     * Test: Duplicate entry (already exists) should not cause issues.
     *
     * Follower: 1(T1)2(T1)3(T2)
     * Leader sends: AppendEntries(prevLogIndex=3, entries=[3(T2)]) (duplicate)
     *
     * Expected: No change, success response
     */
    @Test
    void testDuplicateEntryNoChange() throws Exception {
        System.out.println("\n========================================");
        System.out.println("TEST: Duplicate entry - no change");
        System.out.println("========================================\n");

        Path raftDir = tempDir.resolve("raft-dup");
        Files.createDirectories(raftDir);
        Path walFile = raftDir.resolve("wal.dat");
        Path metaFile = tempDir.resolve("meta-dup.json");

        ClusterConfig config = new ClusterConfig("test", "follower", "127.0.0.1", 66003, null);
        MetadataStore store = new MetadataStore(metaFile);
        GenerationManager genMgr = new GenerationManager(raftDir);
        WAL wal = new WAL(walFile);
        RaftLog log = new RaftLog(wal);
        RaftNode follower = new RaftNode(config, raftDir, log, wal);
        follower.setGenerationManager(genMgr);
        follower.setMetadataStore(store);

        // Set up state
        List<LogEntry> initialEntries = Arrays.asList(
            new LogEntry(1, 1, 1000, LogEntry.OpType.CREATE_OBJECT, new byte[]{1}),
            new LogEntry(1, 2, 1001, LogEntry.OpType.CREATE_OBJECT, new byte[]{2}),
            new LogEntry(2, 3, 2000, LogEntry.OpType.CREATE_OBJECT, new byte[]{3})
        );
        log.appendEntries(0, 0, initialEntries);

        assertEquals(3, log.getLastLogIndex());

        System.out.println("Initial follower: 1(T1)2(T1)3(T2)");

        // Leader sends duplicate entry 3
        List<LogEntry> duplicateEntries = List.of(
            new LogEntry(2, 3, 2000, LogEntry.OpType.CREATE_OBJECT, new byte[]{3})
        );

        RaftMessage.AppendEntriesResponse response = follower.handleAppendEntries(
            2, "leader", 3, 2, duplicateEntries, 0
        );

        assertTrue(response.success());

        // Verify no change
        assertEquals(3, log.getLastLogIndex());
        assertEquals(1, log.getEntry(1).term());
        assertEquals(1, log.getEntry(2).term());
        assertEquals(2, log.getEntry(3).term());

        System.out.println("After duplicate: 1(T1)2(T1)3(T2) (unchanged)");

        // Verify WAL has no duplicate
        wal.close();

        WAL recoveredWal = new WAL(walFile);
        RaftLog recoveredLog = new RaftLog(recoveredWal);

        assertEquals(3, recoveredLog.getLastLogIndex());

        recoveredWal.close();

        System.out.println("\n========================================");
        System.out.println("TEST: Duplicate entry - PASSED");
        System.out.println("========================================\n");
    }
}
