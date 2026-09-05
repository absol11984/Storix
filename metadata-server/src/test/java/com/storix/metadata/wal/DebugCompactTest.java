package com.storix.metadata.wal;

import com.storix.metadata.raft.LogEntry;
import com.storix.metadata.raft.RaftLog;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.io.*;
import java.nio.file.*;

import static org.junit.jupiter.api.Assertions.*;

class DebugCompactTest {
    @TempDir
    Path dataDir;
    Path walFile;

    @BeforeEach
    void setUp() {
        walFile = dataDir.resolve("wal.dat");
    }

    @Test
    void debugCompact() throws Exception {
        System.out.println("=== DEBUG COMPACT TEST ===");
        
        // Create WAL and write 20 entries
        WAL wal1 = new WAL(walFile);
        RaftLog raftLog1 = new RaftLog(wal1);
        
        for (int i = 1; i <= 20; i++) {
            LogEntry entry = LogEntry.create(1, LogEntry.OpType.CREATE_OBJECT, ("data" + i).getBytes()).withIndex(i);
            raftLog1.append(entry);
        }
        raftLog1.advanceCommitIndex(20);
        wal1.persistCommitIndex(20, 20);
        wal1.close();
        
        System.out.println("After writing 20 entries, file size: " + Files.size(walFile));
        
        // Create new WAL, add 10 more entries
        WAL wal2 = new WAL(walFile);
        RaftLog raftLog2 = new RaftLog(wal2);
        
        WAL.WALRecoveryResult result = wal2.recover();
        System.out.println("After recover: " + result.entries.size() + " entries");
        raftLog2.loadEntries(result.entries, result.commitIndex, result.lastApplied);
        
        for (int i = 21; i <= 30; i++) {
            LogEntry entry = LogEntry.create(1, LogEntry.OpType.CREATE_OBJECT, ("data" + i).getBytes()).withIndex(i);
            raftLog2.append(entry);
        }
        raftLog2.advanceCommitIndex(30);
        wal2.persistCommitIndex(30, 30);
        wal2.close();
        
        System.out.println("After writing 30 entries, file size: " + Files.size(walFile));
        
        // Print hex dump of first 100 bytes
        byte[] content = Files.readAllBytes(walFile);
        System.out.print("First 100 bytes (hex): ");
        for (int i = 0; i < Math.min(100, content.length); i++) {
            System.out.printf("%02x ", content[i]);
            if ((i + 1) % 16 == 0) System.out.println();
        }
        System.out.println();
        
        // Now compact
        WAL wal3 = new WAL(walFile);
        System.out.println("Before compact, attempting to recover...");
        
        // Try to recover first
        try {
            WAL.WALRecoveryResult recoverResult = wal3.recover();
            System.out.println("Recover successful: " + recoverResult.entries.size() + " entries");
        } catch (Exception e) {
            System.out.println("Recover failed: " + e.getMessage());
            e.printStackTrace();
        }
        
        System.out.println("Now compacting...");
        wal3.compact(20, 20, 1, null);
        wal3.close();
        
        System.out.println("After compact, file size: " + Files.size(walFile));
        
        // Verify
        WAL walCheck = new WAL(walFile);
        WAL.WALRecoveryResult checkResult = walCheck.recover();
        System.out.println("Check: " + checkResult.entries.size() + " entries, status=" + checkResult.status);
        walCheck.close();
    }
}
