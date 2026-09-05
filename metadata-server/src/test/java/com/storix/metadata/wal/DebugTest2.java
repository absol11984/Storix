package com.storix.metadata.wal;

import com.storix.metadata.raft.LogEntry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.io.*;
import java.nio.file.*;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class DebugTest2 {
    @TempDir
    Path dataDir;
    Path walFile;

    @BeforeEach
    void setUp() {
        walFile = dataDir.resolve("wal.dat");
    }

    @Test
    void debugRecoveryDetailed() throws Exception {
        System.out.println("=== DETAILED DEBUG ===");
        
        // Create WAL and write entries
        WAL wal = new WAL(walFile);
        
        LogEntry e1 = LogEntry.create(1, LogEntry.OpType.CREATE_OBJECT, "data1".getBytes()).withIndex(1);
        
        wal.append(e1);
        wal.close();
        
        // Read raw file
        byte[] content = Files.readAllBytes(walFile);
        System.out.println("File size: " + content.length);
        
        // Print all bytes in hex
        System.out.println("All bytes (hex):");
        for (int i = 0; i < content.length; i++) {
            System.out.printf("%02x ", content[i]);
            if ((i + 1) % 16 == 0) {
                System.out.printf(" | %d-%d%n", i - 15, i);
            }
        }
        System.out.println();
        
        // Manually parse to verify
        System.out.println("Manual parse:");
        ByteBuffer buf = ByteBuffer.wrap(content);
        
        // Header
        long magic = buf.getLong();
        System.out.printf("Magic: 0x%016x (expected 0x%016x)%n", magic, 0x57414c01L);
        int version = buf.getInt();
        System.out.printf("Version: %d (expected 3)%n", version);
        int endMarker = buf.getInt();
        System.out.printf("EndMarker: 0x%08x (expected 0xdeadbeef)%n", endMarker, 0xDEADBEEF);
        
        // First record
        byte type = buf.get();
        System.out.printf("Type: %d (expected 2)%n", type);
        long term = buf.getLong();
        System.out.printf("Term: %d (expected 1)%n", term);
        long index = buf.getLong();
        System.out.printf("Index: %d (expected 1)%n", index);
        long timestamp = buf.getLong();
        System.out.printf("Timestamp: %d%n", timestamp);
        byte opType = buf.get();
        System.out.printf("OpType: %d (expected 1)%n", opType);
        int dataLen = buf.getInt();
        System.out.printf("DataLen: %d (expected 5)%n", dataLen);
        
        byte[] data = new byte[dataLen];
        buf.get(data);
        System.out.printf("Data: %s (expected 'data1')%n", new String(data));
        
        int checksum = buf.getInt();
        System.out.printf("Checksum: 0x%08x%n", checksum);
        
        System.out.println("Manual parse completed successfully!");
        
        // Now try WAL.recover()
        System.out.println("\nTrying WAL.recover()...");
        WAL wal2 = new WAL(walFile);
        try {
            WAL.WALRecoveryResult result = wal2.recover();
            System.out.println("SUCCESS: " + result.status + ", entries: " + result.entries.size());
        } catch (Exception e) {
            System.out.println("FAILED: " + e.getClass().getSimpleName());
            e.printStackTrace();
        }
        wal2.close();
    }
}
