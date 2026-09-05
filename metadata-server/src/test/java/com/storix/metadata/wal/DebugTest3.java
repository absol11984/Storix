package com.storix.metadata.wal;

import com.storix.metadata.raft.LogEntry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;

class DebugTest3 {
    @TempDir
    Path dataDir;

    @Test
    void test1() throws Exception {
        System.out.println("=== Test 1 ===");
        Path walFile = dataDir.resolve("wal1.dat");
        
        // Write and verify
        WAL wal1 = new WAL(walFile);
        for (int i = 1; i <= 20; i++) {
            LogEntry entry = LogEntry.create(1, LogEntry.OpType.CREATE_OBJECT, ("d" + i).getBytes()).withIndex(i);
            wal1.append(entry);
        }
        wal1.persistTerm(1, null);
        wal1.persistCommitIndex(20, 20);
        // Don't close yet - verify first
        var r = wal1.recover();
        System.out.println("Test1 (before close): " + r.entries.size() + " entries");
        wal1.close();
        
        // Reopen and verify
        WAL wal2 = new WAL(walFile);
        var r2 = wal2.recover();
        System.out.println("Test1 (after reopen): " + r2.entries.size() + " entries");
        wal2.close();
    }

    @Test
    void test2() throws Exception {
        System.out.println("=== Test 2 ===");
        Path walFile = dataDir.resolve("wal2.dat");
        
        // Write 20 entries
        WAL wal1 = new WAL(walFile);
        for (int i = 1; i <= 20; i++) {
            LogEntry entry = LogEntry.create(1, LogEntry.OpType.CREATE_OBJECT, ("d" + i).getBytes()).withIndex(i);
            wal1.append(entry);
        }
        wal1.persistTerm(1, null);
        wal1.persistCommitIndex(20, 20);
        wal1.close();
        
        // Compact to 10
        WAL walCompact = new WAL(walFile);
        walCompact.compact(10, 10, 1, null);
        walCompact.close();
        
        // Reopen and verify
        WAL wal2 = new WAL(walFile);
        var r = wal2.recover();
        System.out.println("Test2 after compact: " + r.entries.size() + " entries");
        System.out.println("First index: " + (r.entries.isEmpty() ? "N/A" : r.entries.get(0).index()));
        System.out.println("Last index: " + (r.entries.isEmpty() ? "N/A" : r.entries.get(r.entries.size()-1).index()));
        wal2.close();
    }
}
