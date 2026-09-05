package com.storix.metadata.wal;

import com.storix.metadata.raft.LogEntry;
import com.storix.metadata.raft.RaftLog;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.io.*;
import java.nio.file.*;

import static org.junit.jupiter.api.Assertions.*;

class DebugHeaderTest {
    @TempDir
    Path dataDir;
    Path walFile;

    @BeforeEach
    void setUp() {
        walFile = dataDir.resolve("wal.dat");
    }

    @Test
    void debugHeader() throws Exception {
        System.out.println("=== DEBUG HEADER TEST ===");
        
        // Create WAL - this should write header
        WAL wal1 = new WAL(walFile);
        System.out.println("After WAL creation, file size: " + Files.size(walFile));
        
        // Print first 20 bytes
        byte[] content = Files.readAllBytes(walFile);
        System.out.print("First 20 bytes (hex): ");
        for (int i = 0; i < Math.min(20, content.length); i++) {
            System.out.printf("%02x ", content[i]);
        }
        System.out.println();
        
        // Check magic number
        if (content.length >= 16) {
            long magic = 0;
            for (int i = 0; i < 8; i++) {
                magic = (magic << 8) | (content[i] & 0xFF);
            }
            System.out.printf("Magic: 0x%016x (expected 0x%016x)%n", magic, 0x57414c01L);
        }
        
        wal1.close();
    }
    
    @Test
    void debugHeaderWithEntry() throws Exception {
        System.out.println("=== DEBUG HEADER WITH ENTRY TEST ===");
        
        // Create WAL and write one entry
        WAL wal1 = new WAL(walFile);
        System.out.println("After WAL creation, file size: " + Files.size(walFile));
        
        // Print first 20 bytes BEFORE entry
        byte[] content1 = Files.readAllBytes(walFile);
        System.out.print("Before entry - First 20 bytes (hex): ");
        for (int i = 0; i < Math.min(20, content1.length); i++) {
            System.out.printf("%02x ", content1[i]);
        }
        System.out.println();
        
        // Write an entry
        LogEntry entry = LogEntry.create(1, LogEntry.OpType.CREATE_OBJECT, "test".getBytes()).withIndex(1);
        wal1.append(entry);
        
        System.out.println("After append, file size: " + Files.size(walFile));
        
        // Print first 20 bytes AFTER entry
        byte[] content2 = Files.readAllBytes(walFile);
        System.out.print("After entry - First 20 bytes (hex): ");
        for (int i = 0; i < Math.min(20, content2.length); i++) {
            System.out.printf("%02x ", content2[i]);
        }
        System.out.println();
        
        // Check magic number
        if (content2.length >= 16) {
            long magic = 0;
            for (int i = 0; i < 8; i++) {
                magic = (magic << 8) | (content2[i] & 0xFF);
            }
            System.out.printf("Magic: 0x%016x (expected 0x%016x)%n", magic, 0x57414c01L);
        }
        
        wal1.close();
    }
}
