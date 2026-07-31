package com.drmq.broker.raft;

import com.drmq.protocol.RaftEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Persistent Raft log — the ordered sequence of commands that all nodes agree on.
 * Uses mmap (FileChannel.map) for zero-copy off-heap persistence.
 */
public class RaftLog {
    private static final Logger logger = LoggerFactory.getLogger(RaftLog.class);

    private static final int INITIAL_MAPPED_SIZE = 16 * 1024 * 1024; // 16 MB

    private final Path logPath;
    private final List<RaftEntry> entries;       
    private final List<Long> filePositions;     
    
    private FileChannel fileChannel;
    private MappedByteBuffer mappedBuffer;
    private long logicalFileSize = 0;
    
    private long startIndex = 1;
    private final boolean fsyncEnabled;

    public RaftLog(Path dataDir, boolean fsyncEnabled) throws IOException {
        this.fsyncEnabled = fsyncEnabled;
        Path raftDir = dataDir.resolve("raft");
        Files.createDirectories(raftDir);
        this.logPath = raftDir.resolve("raft.log");
        this.entries = new ArrayList<>();
        this.filePositions = new ArrayList<>();
        
        openChannelAndMap();
        recover();
    }
    
    private void openChannelAndMap() throws IOException {
        this.fileChannel = FileChannel.open(logPath, 
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        long fileSize = fileChannel.size();
        long mapSize = Math.max(fileSize, INITIAL_MAPPED_SIZE);
        this.mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, mapSize);
    }
    
    private void ensureCapacity(int additionalBytes) throws IOException {
        if (logicalFileSize + additionalBytes > mappedBuffer.capacity()) {
            long newCapacity = Math.max(mappedBuffer.capacity() * 2L, logicalFileSize + additionalBytes + INITIAL_MAPPED_SIZE);
            if (newCapacity > Integer.MAX_VALUE) {
                if (logicalFileSize + additionalBytes > Integer.MAX_VALUE) {
                    throw new IOException("Raft log exceeded 2GB limit of MappedByteBuffer segment");
                }
                newCapacity = Integer.MAX_VALUE;
            }
            int pos = mappedBuffer.position();
            mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, newCapacity);
            mappedBuffer.position(pos);
        }
    }

    private void recover() throws IOException {
        long fileLength = fileChannel.size();
        if (fileLength == 0) {
            logger.info("Raft log is empty, starting fresh");
            return;
        }

        mappedBuffer.position(0);
        int count = 0;
        
        while (mappedBuffer.position() < fileLength) {
            long entryStart = mappedBuffer.position();
            try {
                if (mappedBuffer.remaining() < 4) break;
                
                int length = mappedBuffer.getInt();
                if (length <= 0 || length > 10 * 1024 * 1024) {
                    if (length != 0) {
                        logger.warn("Corrupt entry at pos {}, truncating", entryStart);
                    }
                    logicalFileSize = entryStart;
                    break;
                }
                if (mappedBuffer.remaining() < length) {
                    logger.warn("Incomplete entry at pos {}, truncating", entryStart);
                    logicalFileSize = entryStart;
                    break;
                }
                
                byte[] data = new byte[length];
                mappedBuffer.get(data);
                
                RaftEntry entry = RaftEntry.parseFrom(data);
                entries.add(entry.toBuilder().clearPayload().build());
                filePositions.add(entryStart);
                count++;
                logicalFileSize = mappedBuffer.position();
            } catch (Exception e) {
                logger.warn("Error during raft log recovery at pos {}, truncating", entryStart);
                logicalFileSize = entryStart;
                break;
            }
        }
        
        // Truncate file physical size to logical file size to remove garbage
        fileChannel.truncate(logicalFileSize);
        mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Math.max(logicalFileSize, INITIAL_MAPPED_SIZE));
        mappedBuffer.position((int) logicalFileSize);
        
        if (!entries.isEmpty()) {
            startIndex = entries.get(0).getIndex();
        }
        logger.info("Raft log recovered: {} entries, startIndex={}, lastIndex={}, lastTerm={}, logicalFileSize={}",
                count, startIndex, getLastIndex(), getLastTerm(), logicalFileSize);
    }

    public synchronized void append(RaftEntry entry) throws IOException {
        byte[] data = entry.toByteArray();
        ensureCapacity(4 + data.length);
        
        long entryStart = logicalFileSize;
        mappedBuffer.position((int) entryStart);
        
        mappedBuffer.putInt(data.length);
        mappedBuffer.put(data);
        
        logicalFileSize = mappedBuffer.position();
        
        if (fsyncEnabled) {
            mappedBuffer.force();
        }
        
        entries.add(entry.toBuilder().clearPayload().build());
        filePositions.add(entryStart);
        logger.debug("Appended raft entry: index={}, term={}", entry.getIndex(), entry.getTerm());
    }

    public synchronized void append(List<RaftEntry> batch) throws IOException {
        if (batch.isEmpty()) return;
        
        int totalRequired = 0;
        List<byte[]> serializedBatch = new ArrayList<>(batch.size());
        for (RaftEntry entry : batch) {
            byte[] data = entry.toByteArray();
            serializedBatch.add(data);
            totalRequired += 4 + data.length;
        }
        
        ensureCapacity(totalRequired);
        
        mappedBuffer.position((int) logicalFileSize);
        List<Long> newPositions = new ArrayList<>(batch.size());
        
        for (byte[] data : serializedBatch) {
            newPositions.add((long) mappedBuffer.position());
            mappedBuffer.putInt(data.length);
            mappedBuffer.put(data);
        }
        
        logicalFileSize = mappedBuffer.position();
        
        if (fsyncEnabled) {
            mappedBuffer.force();
        }
        
        for (RaftEntry e : batch) {
            entries.add(e.toBuilder().clearPayload().build());
        }
        filePositions.addAll(newPositions);
        logger.debug("Appended {} raft entries (indices {} to {})", 
                batch.size(), batch.get(0).getIndex(), batch.get(batch.size() - 1).getIndex());
    }

    public RaftEntry getEntry(long index) {
        byte[] data;
        synchronized (this) {
            if (index < startIndex || index > getLastIndex()) {
                return null;
            }
            int listIndex = (int) (index - startIndex);
            try {
                long pos = filePositions.get(listIndex);
                
                int originalPos = mappedBuffer.position();
                mappedBuffer.position((int) pos);
                
                int length = mappedBuffer.getInt();
                data = new byte[length];
                mappedBuffer.get(data);
                
                mappedBuffer.position(originalPos);
            } catch (Exception e) {
                logger.error("Failed to read raft entry at index {} from mapped buffer", index, e);
                return null;
            }
        }
        try {
            return RaftEntry.parseFrom(data);
        } catch (Exception e) {
            logger.error("Failed to parse raft entry at index {}", index, e);
            return null;
        }
    }

    public static final int MAX_ENTRIES_PER_RPC = 500;

    public synchronized List<RaftEntry> getEntriesFrom(long fromIndex) {
        return getEntriesFrom(fromIndex, MAX_ENTRIES_PER_RPC);
    }

    public List<RaftEntry> getEntriesFrom(long fromIndex, int maxEntries) {
        List<byte[]> rawDataList = new ArrayList<>();
        int maxBytes = 8 * 1024 * 1024; // 8 MB limit per RPC
        
        synchronized (this) {
            if (fromIndex < startIndex || fromIndex > getLastIndex() + 1) {
                return Collections.emptyList();
            }
            if (fromIndex == getLastIndex() + 1) {
                return Collections.emptyList();
            }
            int from = (int) (fromIndex - startIndex);
            long currentBytes = 0;
            
            try {
                int originalPos = mappedBuffer.position();
                mappedBuffer.position((int) (long) filePositions.get(from));
                
                int to = from;
                while (to < entries.size() && to - from < maxEntries) {
                    int length = mappedBuffer.getInt();
                    if (to > from && currentBytes + length > maxBytes) {
                        break; 
                    }
                    byte[] data = new byte[length];
                    mappedBuffer.get(data);
                    rawDataList.add(data);
                    
                    currentBytes += length;
                    to++;
                }
                mappedBuffer.position(originalPos);
            } catch (Exception e) {
                logger.error("Failed to read raft entries from mapped buffer", e);
            }
        }
        
        List<RaftEntry> result = new ArrayList<>(rawDataList.size());
        for (byte[] data : rawDataList) {
            try {
                result.add(RaftEntry.parseFrom(data));
            } catch (Exception e) {
                logger.error("Failed to parse raft entry", e);
            }
        }
        return result;
    }

    public synchronized long getLastIndex() {
        if (entries.isEmpty()) return Math.max(0, startIndex - 1);
        return entries.get(entries.size() - 1).getIndex();
    }

    public synchronized void setStartIndex(long index) {
        this.startIndex = index;
    }

    public synchronized long getStartIndex() {
        return startIndex;
    }

    public synchronized long getLastTerm() {
        if (entries.isEmpty()) return 0;
        return entries.get(entries.size() - 1).getTerm();
    }

    public synchronized long getLogicalFileSize() {
        return logicalFileSize;
    }

    public synchronized long getTermAt(long index) {
        if (index == 0) return 0;
        if (index < startIndex || index > getLastIndex()) {
            return 0;
        }
        int listIndex = (int) (index - startIndex);
        return entries.get(listIndex).getTerm();
    }

    public synchronized int size() {
        return entries.size();
    }

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

        logicalFileSize = truncateToPosition;
        fileChannel.truncate(logicalFileSize);
        // Remap to adjust boundaries
        mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Math.max(logicalFileSize, INITIAL_MAPPED_SIZE));
        mappedBuffer.position((int) logicalFileSize);
        
        if (fsyncEnabled) {
            mappedBuffer.force();
        }

        entries.subList(removeFromListIndex, entries.size()).clear();
        filePositions.subList(removeFromListIndex, filePositions.size()).clear();
    }

    public void compact(long upToIndex) throws IOException {
        int removeCount;
        int initialSize;
        
        synchronized(this) {
            if (upToIndex <= startIndex || upToIndex > getLastIndex()) {
                return;
            }
            removeCount = (int) (upToIndex - startIndex + 1);
            initialSize = entries.size();
        }
        
        java.io.File tempFile = new java.io.File(logPath.getParent().toFile(), logPath.getFileName().toString() + ".tmp");
        List<Long> newPositions = new ArrayList<>(initialSize - removeCount);
        
        long newLogicalFileSize = 0;
        try (FileChannel tempChannel = FileChannel.open(tempFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            
            MappedByteBuffer tempMapped = tempChannel.map(FileChannel.MapMode.READ_WRITE, 0, Math.max(logicalFileSize, INITIAL_MAPPED_SIZE));
            
            long startReadPos;
            java.nio.MappedByteBuffer readerBuffer;
            synchronized(this) {
                if (removeCount < initialSize) {
                    startReadPos = filePositions.get(removeCount);
                } else {
                    startReadPos = logicalFileSize;
                }
                readerBuffer = (java.nio.MappedByteBuffer) mappedBuffer.duplicate();
            }
            
            readerBuffer.position((int) startReadPos);
            
            for (int i = removeCount; i < initialSize; i++) {
                newPositions.add((long) tempMapped.position());
                int length = readerBuffer.getInt();
                byte[] data = new byte[length];
                readerBuffer.get(data);
                
                tempMapped.putInt(length);
                tempMapped.put(data);
            }
            
            newLogicalFileSize = tempMapped.position();
            if (fsyncEnabled) tempMapped.force();
            tempChannel.truncate(newLogicalFileSize);
        }
        
        while (true) {
            int currentSize;
            int addedCount;
            byte[] addedData = null;

            synchronized(this) {
                if (entries.size() < initialSize) {
                    tempFile.delete();
                    return;
                }
                currentSize = entries.size();
                addedCount = currentSize - initialSize;

                if (addedCount == 0) {
                    fileChannel.close();
                    try {
                        java.nio.file.Files.move(tempFile.toPath(), logPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
                    } catch (Exception e) {
                        try {
                            openChannelAndMap();
                        } catch (Exception ignore) {
                        }
                        throw e;
                    }

                    try {
                        openChannelAndMap();
                        logicalFileSize = newLogicalFileSize;
                        mappedBuffer.position((int) logicalFileSize);
                    } catch (IOException e) {
                        throw new IOException("Failed to reopen compacted log: " + logPath, e);
                    }

                    entries.subList(0, removeCount).clear();
                    filePositions.clear();
                    filePositions.addAll(newPositions);
                    startIndex = upToIndex + 1;

                    logger.debug("Compacted Raft log on disk up to index {}", upToIndex);
                    return;
                }

                int dataLengthToRead = (int) (logicalFileSize - filePositions.get(initialSize));
                addedData = new byte[dataLengthToRead];
                int originalPos = mappedBuffer.position();
                mappedBuffer.position((int) (long) filePositions.get(initialSize));
                mappedBuffer.get(addedData);
                mappedBuffer.position(originalPos);
            }

            try (FileChannel tempChannel = FileChannel.open(tempFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                long mapSize = Math.max(newLogicalFileSize + addedData.length, INITIAL_MAPPED_SIZE);
                MappedByteBuffer tempMapped = tempChannel.map(FileChannel.MapMode.READ_WRITE, 0, mapSize);
                tempMapped.position((int) newLogicalFileSize);

                java.nio.ByteBuffer addedBuf = java.nio.ByteBuffer.wrap(addedData);
                for (int i = 0; i < addedCount; i++) {
                    newPositions.add((long) tempMapped.position());
                    int length = addedBuf.getInt();
                    tempMapped.putInt(length);

                    byte[] data = new byte[length];
                    addedBuf.get(data);
                    tempMapped.put(data);
                }

                newLogicalFileSize = tempMapped.position();
                if (fsyncEnabled) tempMapped.force();
                tempChannel.truncate(newLogicalFileSize);
            }
            initialSize = currentSize;
        }
    }

    public synchronized void close() throws IOException {
        if (fileChannel != null && fileChannel.isOpen()) {
            fileChannel.truncate(logicalFileSize);
            fileChannel.close();
        }
    }
}
