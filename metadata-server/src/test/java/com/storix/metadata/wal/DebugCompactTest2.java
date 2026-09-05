package com.storix.metadata.wal;

import com.storix.metadata.raft.LogEntry;
import com.storix.metadata.raft.RaftLog;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.io.*;
import java.nio.file.*;

import static org.junit.jupiter.api.Assertions.*;

class DebugCompactTest2 {
    @TempDir
    Path dataDir;
    Path walFile;

    @BeforeEach
    void setUp() {
        walFile = dataDir.resolve("wal.dat");
    }

    @Test
    void debugCompactDetailed() throws Exception {
        System.out.println("=== DEBUG COMPACT DETAILED ===");
        
        // Create WAL and write 20 entries
        WAL wal1 = new WAL(walFile);
        RaftLog raftLog1 = new RaftLog(wal1);
        
        for (int i = 1; i <= 20; i++) {
            LogEntry entry = LogEntry.create(1, LogEntry.OpType.CREATE_OBJECT, ("data" + i).getBytes()).withIndex(i);
            raftLog1.append(entry);
        }
        raftLog1.advanceCommitIndex(20);
        wal1.persistCommitIndex(20, 20);
        
        System.out.println("wal1: Before close, file size: " + Files.size(walFile));
        wal1.close();
        System.out.println("wal1: After close, file size: " + Files.size(walFile));
        
        // Verify file content before reopening
        byte[] content1 = Files.readAllBytes(walFile);
        System.out.printf("wal1 closed: magic=0x%016x (expected 0x0000000057414c01)%n", 
            ((long)content1[0] << 56) | ((long)content1[1] << 48) | ((long)content1[2] << 40) | ((long)content1[3] << 32) |
            ((long)content1[4] << 24) | ((long)content1[5] << 16) | ((long)content1[6] << 8) | (long)content1[7]);
        
        // Create new WAL and add 10 more entries
        WAL wal2 = new WAL(walFile);
        System.out.println("wal2: After reopen, file size: " + Files.size(walFile));
        
        RaftLog raftLog2 = new RaftLog(wal2);
        
        WAL.WALRecoveryResult result = wal2.recover();
        System.out.println("wal2: After recover: " + result.entries.size() + " entries, commitIndex=" + result.commitIndex);
        raftLog2.loadEntries(result.entries, result.commitIndex, result.lastApplied);
        
        for (int i = 21; i <= 30; i++) {
            LogEntry entry = LogEntry.create(1, LogEntry.OpType.CREATE_OBJECT, ("data" + i).getBytes()).withIndex(i);
            raftLog2.append(entry);
        }
        raftLog2.advanceCommitIndex(30);
        wal2.persistCommitIndex(30, 30);
        
        System.out.println("wal2: Before close, file size: " + Files.size(walFile));
        wal2.close();
        System.out.println("wal2: After close, file size: " + Files.size(walFile));
        
        // Verify file content
        byte[] content2 = Files.readAllBytes(walFile);
        System.out.printf("wal2 closed: magic=0x%016x (expected 0x0000000057414c01)%n", 
            ((long)content2[0] << 56) | ((long)content2[1] << 48) | ((long)content2[2] << 40) | ((long)content2[3] << 32) |
            ((long)content2[4] << 24) | ((long)content2[5] << 16) | ((long)content2[6] << 8) | (long)content2[7]);
    }
}
