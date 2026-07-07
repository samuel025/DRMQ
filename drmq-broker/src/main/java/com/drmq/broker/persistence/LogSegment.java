package com.drmq.broker.persistence;

import com.drmq.broker.BrokerMetrics;
import com.drmq.protocol.DRMQProtocol.StoredMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * A single log file (WAL segment) for one topic.
 * File format: [4-byte message length][StoredMessage protobuf bytes] repeated.
 */
public class LogSegment implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(LogSegment.class);
    private static final int MAX_MESSAGE_SIZE = 10 * 1024 * 1024; // 10MB limit

    private final Path filePath;
    private final FileChannel fileChannel;
    private final long baseOffset;
    private volatile long currentSize; 

    public LogSegment(Path filePath) throws IOException {
        this.filePath = filePath;
        String fileName = filePath.getFileName().toString();
        try {
            this.baseOffset = Long.parseLong(fileName.substring(0, fileName.indexOf('.')));
        } catch (NumberFormatException e) {
            throw new IOException("Invalid log segment filename format: " + fileName, e);
        }
        this.fileChannel = FileChannel.open(filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        this.currentSize = fileChannel.size();
        logger.info("Opened log segment: {} (size: {} bytes)", this.filePath, currentSize);
    }

    /**
     * Append a message to the log segment.
     * @param message The message to append.
     * @return The starting position of the message in the file.
     * @throws IOException If a write error occurs.
     * @throws IllegalArgumentException If message exceeds MAX_MESSAGE_SIZE.
     */
    public synchronized long append(StoredMessage message) throws IOException {
        long startNanos = System.nanoTime();
        byte[] messageBytes = message.toByteArray();
        
        if (messageBytes.length > MAX_MESSAGE_SIZE) {
            throw new IllegalArgumentException(
                "Message size " + messageBytes.length + " bytes exceeds maximum allowed size " + 
                MAX_MESSAGE_SIZE + " bytes. Message cannot be persisted.");
        }
        
        ByteBuffer buffer = ByteBuffer.allocate(4 + messageBytes.length);
        buffer.putInt(messageBytes.length);
        buffer.put(messageBytes);
        buffer.flip();

        long position = currentSize;
        while (buffer.hasRemaining()) {
            fileChannel.write(buffer, position + buffer.position());
        }
        
        currentSize += buffer.limit();
        BrokerMetrics.get().recordLogAppend(buffer.limit(), System.nanoTime() - startNanos);
        return position;
    }

    /**
     * Append a batch of messages atomically with a single fsync (Group Commit).
     * All messages are written sequentially, then fileChannel.force(true) is called once.
     * If a write fails mid-batch, the segment is truncated back to its original size.
     *
     * @param messages The list of messages to append.
     * @return A list of starting byte positions for each message in the batch.
     * @throws IOException If a write or fsync error occurs.
     */
    public synchronized List<Long> appendBatch(List<StoredMessage> messages) throws IOException {
        long startNanos = System.nanoTime();
        long originalSize = currentSize;
        List<Long> positions = new ArrayList<>(messages.size());
        int totalBytesWritten = 0;

        try {
            for (StoredMessage message : messages) {
                byte[] messageBytes = message.toByteArray();

                if (messageBytes.length > MAX_MESSAGE_SIZE) {
                    throw new IllegalArgumentException(
                        "Message size " + messageBytes.length + " bytes exceeds maximum allowed size " +
                        MAX_MESSAGE_SIZE + " bytes. Message cannot be persisted.");
                }

                ByteBuffer buffer = ByteBuffer.allocate(4 + messageBytes.length);
                buffer.putInt(messageBytes.length);
                buffer.put(messageBytes);
                buffer.flip();

                long position = currentSize;
                positions.add(position);

                while (buffer.hasRemaining()) {
                    fileChannel.write(buffer, position + buffer.position());
                }
                currentSize += buffer.limit();
                totalBytesWritten += buffer.limit();
            }

            // TRUE GROUP COMMIT: single fsync for the entire batch
            fileChannel.force(true);

        } catch (IOException e) {
            // Roll back partial writes on failure
            logger.warn("Batch write failed at position {}. Truncating segment {} back to {}",
                    currentSize, filePath, originalSize);
            try {
                fileChannel.truncate(originalSize);
                currentSize = originalSize;
            } catch (IOException truncateEx) {
                logger.error("Failed to truncate segment {} after batch write failure", filePath, truncateEx);
            }
            throw e;
        }

        BrokerMetrics.get().recordLogAppend(totalBytesWritten, System.nanoTime() - startNanos);
        return positions;
    }

    /**
     * Read a message from the specified position.
     * Handles short reads and validates message length to prevent OOM and corruption.
     * @param position The position to read from.
     * @return The stored message.
     * @throws IOException If a read error occurs or data is corrupt.
     */
    public StoredMessage read(long position) throws IOException {
        long startNanos = System.nanoTime();
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        int bytesRead = 0;
        while (lengthBuffer.hasRemaining()) {
            int read = fileChannel.read(lengthBuffer, position + bytesRead);
            if (read == -1) {
                throw new CorruptRecordException("Unexpected EOF while reading message length at position " + position);
            }
            bytesRead += read;
        }
        lengthBuffer.flip();
        int length = lengthBuffer.getInt();
        
        if (length <= 0) {
            throw new CorruptRecordException("Invalid message length " + length + " at position " + position + 
                                  ". Possible data corruption.");
        }
        if (length > MAX_MESSAGE_SIZE) {
            throw new CorruptRecordException("Message length " + length + " exceeds maximum allowed size " + 
                                  MAX_MESSAGE_SIZE + " at position " + position + 
                                  ". Possible data corruption or OOM attack.");
        }

        ByteBuffer messageBuffer = ByteBuffer.allocate(length);
        bytesRead = 0;
        while (messageBuffer.hasRemaining()) {
            int read = fileChannel.read(messageBuffer, position + 4 + bytesRead);
            if (read == -1) {
                throw new CorruptRecordException("Unexpected EOF while reading message body at position " + 
                                      (position + 4) + ", expected " + length + " bytes, got " + bytesRead);
            }
            bytesRead += read;
        }
        messageBuffer.flip();

        StoredMessage message;
        try {
            message = StoredMessage.parseFrom(messageBuffer.array());
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new CorruptRecordException("Failed to parse message at position " + position, e);
        }
        BrokerMetrics.get().recordLogRead(4L + length, System.nanoTime() - startNanos);
        return message;
    }

    public long getSize() {
        return currentSize;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public Path getFilePath() {
        return filePath;
    }

    public long getLastModified() throws IOException {
        return java.nio.file.Files.getLastModifiedTime(filePath).toMillis();
    }

    public void delete() throws IOException {
        close();
        java.nio.file.Files.deleteIfExists(filePath);
        logger.info("Deleted log segment: {}", filePath);
    }

    public synchronized void truncate(long size) throws IOException {
        if (size < currentSize) {
            logger.warn("Truncating log segment {} from {} to {}", filePath, currentSize, size);
            fileChannel.truncate(size);
            fileChannel.force(true);
            this.currentSize = size;
        }
    }

    @Override
    public void close() throws IOException {
        if (fileChannel.isOpen()) {
            fileChannel.close();
        }
    }
}
