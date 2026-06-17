package com.drmq.broker.raft;

import com.drmq.protocol.DRMQProtocol.RaftEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class RaftLogTest {

    @TempDir
    Path tempDir;

    private RaftLog raftLog;

    @BeforeEach
    void setUp() throws IOException {
        raftLog = new RaftLog(tempDir);
    }

    @Test
    void testAppendAndGet() throws IOException {
        RaftEntry entry1 = RaftEntry.newBuilder().setTerm(1).setIndex(1).build();
        RaftEntry entry2 = RaftEntry.newBuilder().setTerm(1).setIndex(2).build();

        raftLog.append(entry1);
        raftLog.append(entry2);

        assertEquals(2, raftLog.getLastIndex());
        assertEquals(1, raftLog.getLastTerm());

        RaftEntry fetched = raftLog.getEntry(1);
        assertNotNull(fetched);
        assertEquals(1, fetched.getIndex());
    }

    @Test
    void testDiskCompaction() throws IOException {
        for (int i = 1; i <= 10; i++) {
            raftLog.append(RaftEntry.newBuilder().setTerm(1).setIndex(i).build());
        }

        assertEquals(10, raftLog.getLastIndex());
        assertEquals(1, raftLog.getStartIndex());

        // Compact up to index 5
        raftLog.compact(5);

        assertEquals(6, raftLog.getStartIndex());
        assertEquals(10, raftLog.getLastIndex());
        
        // Assert that old entries are no longer available
        assertNull(raftLog.getEntry(4));
        assertNull(raftLog.getEntry(5));
        
        // Assert that new entries are still available
        assertNotNull(raftLog.getEntry(6));
        assertEquals(6, raftLog.getEntry(6).getIndex());
        assertEquals(10, raftLog.getEntry(10).getIndex());
    }

    @Test
    void testTruncateFrom() throws IOException {
        for (int i = 1; i <= 10; i++) {
            raftLog.append(RaftEntry.newBuilder().setTerm(1).setIndex(i).build());
        }

        assertEquals(10, raftLog.getLastIndex());

        // Truncate from index 7
        raftLog.truncateFrom(7);

        assertEquals(6, raftLog.getLastIndex());
        assertNull(raftLog.getEntry(7));
        assertNull(raftLog.getEntry(10));

        // Ensure we can append after truncation
        raftLog.append(RaftEntry.newBuilder().setTerm(2).setIndex(7).build());
        assertEquals(7, raftLog.getLastIndex());
        assertEquals(2, raftLog.getLastTerm());
    }
}
