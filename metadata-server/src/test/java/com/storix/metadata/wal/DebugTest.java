package com.storix.metadata.wal;

import com.storix.metadata.raft.LogEntry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.io.*;
import java.nio.file.*;

import static org.junit.jupiter.api.Assertions.*;

class DebugTest {
    @TempDir
    Path dataDir;
    Path walFile;

    @BeforeEach
    void setUp() {
        walFile = dataDir.resolve("wal.dat");
    }

    @Test
    void debugRecovery() throws Exception {
        System.out.println("=== DEBUG TEST ===");
        
        // Create WAL and write entries
        WAL wal = new WAL(walFile);
        System.out.println("After WAL creation, file size: " + Files.size(walFile));
        
        LogEntry e1 = LogEntry.create(1, LogEntry.OpType.CREATE_OBJECT, "data1".getBytes()).withIndex(1);
        LogEntry e2 = LogEntry.create(1, LogEntry.OpType.UPDATE_OBJECT, "data2".getBytes()).withIndex(2);
        
        wal.append(e1);
        System.out.println("After first append, file size: " + Files.size(walFile));
        
        wal.append(e2);
        System.out.println("After second append, file size: " + Files.size(walFile));
        
        wal.close();
        System.out.println("After close, file size: " + Files.size(walFile));
        
        // Print hex dump of first 100 bytes
        byte[] header = Files.readAllBytes(walFile);
        System.out.println("File size: " + header.length);
        System.out.print("First 100 bytes (hex): ");
        for (int i = 0; i < Math.min(100, header.length); i++) {
            System.out.printf("%02x ", header[i]);
            if ((i + 1) % 16 == 0) System.out.println();
        }
        System.out.println();
        
        // Now try to recover
        WAL wal2 = new WAL(walFile);
        try {
            WAL.WALRecoveryResult result = wal2.recover();
            System.out.println("Recovery successful: " + result.status);
            System.out.println("Entries: " + result.entries.size());
        } catch (Exception e) {
            System.out.println("Recovery failed: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            e.printStackTrace();
        }
        wal2.close();
    }
}
