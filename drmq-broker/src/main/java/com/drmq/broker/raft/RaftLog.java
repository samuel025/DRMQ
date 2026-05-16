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
    public synchronized List<RaftEntry> getEntriesFrom(long fromIndex) {
        if (fromIndex < startIndex || fromIndex > getLastIndex() + 1) {
            return Collections.emptyList();
        }
        if (fromIndex == getLastIndex() + 1) {
            return Collections.emptyList();
        }
        return new ArrayList<>(entries.subList((int) (fromIndex - startIndex), entries.size()));
    }

    /**
     * Get the Raft index of the last entry, or 0 if the log is empty.
     */
    public synchronized long getLastIndex() {
        if (entries.isEmpty()) return 0;
        return entries.get(entries.size() - 1).getIndex();
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
     * Compact the Raft log by removing entries from memory up to the given index.
     * This prevents unbounded memory growth. The MessageStore serves as the permanent store.
     */
    public synchronized void compact(long upToIndex) {
        if (upToIndex <= startIndex || upToIndex > getLastIndex()) {
            return;
        }
        int removeCount = (int) (upToIndex - startIndex);
        entries.subList(0, removeCount).clear();
        filePositions.subList(0, removeCount).clear();
        startIndex = upToIndex;
        logger.debug("Compacted Raft log up to index {}", upToIndex);
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

