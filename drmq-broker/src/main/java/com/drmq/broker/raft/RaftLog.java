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
 *
 * File format: [4-byte length][serialized RaftEntry protobuf] repeated.
 * Same length-prefixed framing as the message WAL (LogSegment).
 *
 * Crash safety:
 * - Every append is followed by fsync (durability before returning)
 * - Truncation uses setLength() to the byte offset of the first removed entry,
 *   which is atomic on most filesystems (Raft §5.3)
 */
public class RaftLog {
    private static final Logger logger = LoggerFactory.getLogger(RaftLog.class);

    private final Path logPath;
    private final List<RaftEntry> entries;       // In-memory copy; 0-indexed (entry at list index i has raft index i+1)
    private final List<Long> filePositions;      // Byte offset of each entry in the file (parallel to entries)
    private RandomAccessFile raf;

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
        logger.info("Raft log recovered: {} entries, lastIndex={}, lastTerm={}",
                count, getLastIndex(), getLastTerm());
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
        raf.getFD().sync();  // Durability: fsync after every append
        entries.add(entry);
        filePositions.add(entryStart);
        logger.debug("Appended raft entry: index={}, term={}", entry.getIndex(), entry.getTerm());
    }

    /**
     * Get entry at the given Raft index (1-indexed).
     * Returns null if index is out of bounds.
     */
    public synchronized RaftEntry getEntry(long index) {
        if (index < 1 || index > entries.size()) {
            return null;
        }
        return entries.get((int) (index - 1));
    }

    /**
     * Get all entries from the given Raft index (inclusive) to the end.
     */
    public synchronized List<RaftEntry> getEntriesFrom(long fromIndex) {
        if (fromIndex < 1 || fromIndex > entries.size()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(entries.subList((int) (fromIndex - 1), entries.size()));
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
     * Used when a follower detects conflicting entries from a new leader (Raft §5.3).
     *
     * Crash-safe: uses setLength() to truncate the file to the byte position
     * of the first removed entry, which is atomic on most filesystems.
     */
    public synchronized void truncateFrom(long fromIndex) throws IOException {
        if (fromIndex < 1 || fromIndex > entries.size()) {
            return; // Nothing to truncate
        }

        int removeFromListIndex = (int) (fromIndex - 1);
        long truncateToPosition = filePositions.get(removeFromListIndex);

        logger.warn("Truncating raft log from index {} (removing {} entries, truncating file to byte {})",
                fromIndex, entries.size() - removeFromListIndex, truncateToPosition);

        // Truncate the file to the byte offset of the first removed entry.
        // This is atomic on most filesystems — no data is rewritten.
        raf.setLength(truncateToPosition);
        raf.getFD().sync();

        // Remove from in-memory lists
        entries.subList(removeFromListIndex, entries.size()).clear();
        filePositions.subList(removeFromListIndex, filePositions.size()).clear();
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

