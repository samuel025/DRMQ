package com.drmq.broker.raft;

import com.drmq.protocol.DRMQProtocol.RaftEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Persistent Raft log — the ordered sequence of commands that all nodes agree on.
 *
 * Every proposed message or offset commit is first appended here as a RaftEntry,
 * then replicated to followers. Once a majority of nodes have the entry, it is
 * considered "committed" and applied to the state machine (MessageStore/OffsetManager).
 *
 * Indexing: The Raft log is 1-indexed (index 0 is a sentinel meaning "no entry").
 * Internally, entries are stored in a 0-indexed ArrayList, so raft index N maps
 * to list index N-1. 
 */
public class RaftLog {
    private static final Logger logger = LoggerFactory.getLogger(RaftLog.class);

    private final Path logPath;
    private final List<RaftEntry> entries;       
    private final List<Long> filePositions;     
    private RandomAccessFile raf;
    private long startIndex = 1;

    public RaftLog(Path dataDir) throws IOException {
        Path raftDir = dataDir.resolve("raft");
        Files.createDirectories(raftDir);
        this.logPath = raftDir.resolve("raft.log");
        this.entries = new ArrayList<>();
        this.filePositions = new ArrayList<>();
        this.raf = new RandomAccessFile(logPath.toFile(), "rw");
        recover();
    }

    /**
     * Recovery: read all entries from the log file on disk.
     */
    private void recover() throws IOException {
        long fileLength = raf.length();
        if (fileLength == 0) {
            logger.info("Raft log is empty, starting fresh");
            return;
        }

        raf.seek(0);
        int count = 0;
        while (raf.getFilePointer() < fileLength) {
            long entryStart = raf.getFilePointer();
            try {
                int length = raf.readInt();
                if (length <= 0 || length > 10 * 1024 * 1024) {
                    logger.warn("Corrupt entry at pos {}, truncating", entryStart);
                    raf.setLength(entryStart);
                    break;
                }
                byte[] data = new byte[length];
                raf.readFully(data);
                RaftEntry entry = RaftEntry.parseFrom(data);
                entries.add(entry);
                filePositions.add(entryStart);
                count++;
            } catch (EOFException e) {
                logger.warn("Unexpected EOF during raft log recovery, truncating");
                raf.setLength(entryStart);
                break;
            }
        }
        if (!entries.isEmpty()) {
            startIndex = entries.get(0).getIndex();
        }
        logger.info("Raft log recovered: {} entries, startIndex={}, lastIndex={}, lastTerm={}",
                count, startIndex, getLastIndex(), getLastTerm());
    }

    /**
     * Append an entry to the log. Writes to disk immediately.
     */
    public synchronized void append(RaftEntry entry) throws IOException {
        long entryStart = raf.length();
        raf.seek(entryStart);
        byte[] data = entry.toByteArray();
        raf.writeInt(data.length);
        raf.write(data);
        raf.getFD().sync(); 
        entries.add(entry);
        filePositions.add(entryStart);
        logger.debug("Appended raft entry: index={}, term={}", entry.getIndex(), entry.getTerm());
    }

    /**
     * Append a list of entries to the log. Writes to disk immediately with a single fsync.
     */
    public synchronized void append(List<RaftEntry> batch) throws IOException {
        if (batch.isEmpty()) return;
        
        long entryStart = raf.length();
        raf.seek(entryStart);
        
        List<Long> newPositions = new ArrayList<>(batch.size());
        for (RaftEntry entry : batch) {
            newPositions.add(raf.getFilePointer());
            byte[] data = entry.toByteArray();
            raf.writeInt(data.length);
            raf.write(data);
        }
        
        raf.getFD().sync(); 
        
        entries.addAll(batch);
        filePositions.addAll(newPositions);
        logger.debug("Appended {} raft entries (indices {} to {})", 
                batch.size(), batch.get(0).getIndex(), batch.get(batch.size() - 1).getIndex());
    }

    /**
     * Get entry at the given Raft index (1-indexed).
     * Returns null if index is out of bounds.
     */
    public synchronized RaftEntry getEntry(long index) {
        if (index < startIndex || index > getLastIndex()) {
            return null;
        }
        return entries.get((int) (index - startIndex));
    }

    /**
     * Get all entries from the given Raft index (inclusive) to the end.
     */
    /** Maximum number of log entries sent in a single AppendEntries RPC.
     * Prevents oversized frames when catching up a lagging follower. */
    public static final int MAX_ENTRIES_PER_RPC = 500;

    public synchronized List<RaftEntry> getEntriesFrom(long fromIndex) {
        return getEntriesFrom(fromIndex, MAX_ENTRIES_PER_RPC);
    }

    public synchronized List<RaftEntry> getEntriesFrom(long fromIndex, int maxEntries) {
        if (fromIndex < startIndex || fromIndex > getLastIndex() + 1) {
            return Collections.emptyList();
        }
        if (fromIndex == getLastIndex() + 1) {
            return Collections.emptyList();
        }
        int from = (int) (fromIndex - startIndex);
        int to   = Math.min(from + maxEntries, entries.size());
        return new ArrayList<>(entries.subList(from, to));
    }

    /**
     * Get the Raft index of the last entry, or the last compacted index if the log is empty.
     */
    public synchronized long getLastIndex() {
        if (entries.isEmpty()) return Math.max(0, startIndex - 1);
        return entries.get(entries.size() - 1).getIndex();
    }

    /**
     * Set the start index manually, used during recovery if the log is completely empty
     * but the node has previously applied a snapshot.
     */
    public synchronized void setStartIndex(long index) {
        this.startIndex = index;
    }

    /**
     * Get the starting index of the Raft log (useful to know if log was compacted).
     */
    public synchronized long getStartIndex() {
        return startIndex;
    }

    /**
     * Get the term of the last entry, or 0 if the log is empty.
     */
    public synchronized long getLastTerm() {
        if (entries.isEmpty()) return 0;
        return entries.get(entries.size() - 1).getTerm();
    }

    /**
     * Get the term of the entry at the given index.
     * Returns 0 if index is 0 or out of range.
     */
    public synchronized long getTermAt(long index) {
        if (index == 0) return 0;
        RaftEntry entry = getEntry(index);
        return entry != null ? entry.getTerm() : 0;
    }

    /**
     * Get the total number of entries in the log.
     */
    public synchronized int size() {
        return entries.size();
    }

    /**
     * Truncate all entries from the given index onwards (inclusive).
     * Used when a follower detects conflicting entries from a new leader .
     *
     * Crash-safe: uses setLength() to truncate the file to the byte position
     * of the first removed entry, which is atomic on most filesystems.
     */
    public synchronized void truncateFrom(long fromIndex) throws IOException {
        if (fromIndex < startIndex || fromIndex > getLastIndex() + 1) {
            return;
        }
        if (fromIndex == getLastIndex() + 1) {
            return;
        }

        int removeFromListIndex = (int) (fromIndex - startIndex);
        long truncateToPosition = filePositions.get(removeFromListIndex);

        logger.warn("Truncating raft log from index {} (removing {} entries, truncating file to byte {})",
                fromIndex, entries.size() - removeFromListIndex, truncateToPosition);

        raf.setLength(truncateToPosition);
        raf.getFD().sync();

        entries.subList(removeFromListIndex, entries.size()).clear();
        filePositions.subList(removeFromListIndex, filePositions.size()).clear();
    }

    /**
     * Compact the Raft log by removing entries from memory up to the given index,
     * and rewriting the log file on disk to reclaim space.
     */
    public void compact(long upToIndex) throws IOException {
        int removeCount;
        List<RaftEntry> remainingEntries;
        
        synchronized(this) {
            if (upToIndex <= startIndex || upToIndex > getLastIndex()) {
                return;
            }
            removeCount = (int) (upToIndex - startIndex + 1);
            remainingEntries = new ArrayList<>(entries.subList(removeCount, entries.size()));
        }
        
        java.io.File tempFile = new java.io.File(logPath.getParent().toFile(), logPath.getFileName().toString() + ".tmp");
        List<Long> newPositions = new ArrayList<>(remainingEntries.size());
        
        try (RandomAccessFile tempRaf = new RandomAccessFile(tempFile, "rw")) {
            for (RaftEntry entry : remainingEntries) {
                newPositions.add(tempRaf.getFilePointer());
                byte[] data = entry.toByteArray();
                tempRaf.writeInt(data.length);
                tempRaf.write(data);
            }
            tempRaf.getFD().sync();
        }
        
        synchronized(this) {
            int currentExpectedSize = remainingEntries.size() + removeCount;
            if (entries.size() < currentExpectedSize) {
                // Truncation happened during compaction! Abort to avoid restoring truncated entries.
                tempFile.delete();
                return;
            }
            
            int addedCount = entries.size() - currentExpectedSize;
            if (addedCount > 0) {
                List<RaftEntry> newlyAdded = entries.subList(entries.size() - addedCount, entries.size());
                try (RandomAccessFile tempRaf = new RandomAccessFile(tempFile, "rw")) {
                    tempRaf.seek(tempRaf.length());
                    for (RaftEntry entry : newlyAdded) {
                        newPositions.add(tempRaf.getFilePointer());
                        byte[] data = entry.toByteArray();
                        tempRaf.writeInt(data.length);
                        tempRaf.write(data);
                    }
                    tempRaf.getFD().sync();
                }
            }

            // Swap files
            raf.close();
            try {
                java.nio.file.Files.move(tempFile.toPath(), logPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            } catch (Exception e) {
                try {
                    raf = new RandomAccessFile(logPath.toFile(), "rw");
                    raf.seek(raf.length());
                } catch (Exception ignore) {
                }
                throw e;
            }

            try {
                raf = new RandomAccessFile(logPath.toFile(), "rw");
            } catch (IOException e) {
                throw new IOException("Failed to reopen compacted log: " + logPath, e);
            }
            raf.seek(raf.length());
            
            entries.subList(0, removeCount).clear();
            filePositions.clear();
            filePositions.addAll(newPositions);
            startIndex = upToIndex + 1;
            
            logger.debug("Compacted Raft log on disk up to index {}", upToIndex);
        }
    }

    /**
     * Close the log file.
     */
    public synchronized void close() throws IOException {
        if (raf != null) {
            raf.close();
        }
    }
}

