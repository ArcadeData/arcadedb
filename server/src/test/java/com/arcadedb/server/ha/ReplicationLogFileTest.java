package com.arcadedb.server.ha;

import com.arcadedb.database.Binary;
import com.arcadedb.engine.WALFile;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileNotFoundException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class ReplicationLogFileTest {
    @TempDir
    Path tempDir;

    private ReplicationLogFile logFile;
    private String filePath;

    @BeforeEach
    public void setup() throws FileNotFoundException {
        filePath = tempDir.resolve("replication.log").toString();
        logFile = new ReplicationLogFile(filePath);
    }

    @AfterEach
    public void tearDown() {
        if (logFile != null) {
            logFile.close();
        }
    }

    @Test
    public void testNewLogFileHasInitialValues() {
        // Constants for initial values
        final long INITIAL_MESSAGE_NUMBER = -1L;
        final long INITIAL_LOG_SIZE = 0L;

        // Verify that a newly created log file is properly initialized with default values
        assertThat(logFile.getLastMessageNumber()).isEqualTo(INITIAL_MESSAGE_NUMBER);
        assertThat(logFile.getSize()).isEqualTo(INITIAL_LOG_SIZE);
    }


    @Test
    public void testAppendMessage() {
        // Create and append a message
        Binary payload = new Binary(new byte[] {1, 2, 3, 4});
        ReplicationMessage message = new ReplicationMessage(0L, payload);

        boolean result = logFile.appendMessage(message);

        assertThat(result).isTrue();
        assertThat(logFile.getLastMessageNumber()).isEqualTo(0L);
        assertThat(logFile.getSize()).isPositive();
    }

    @Test
    public void testAppendMultipleMessages() {
        // Append first message
        Binary payload1 = new Binary(new byte[] {1, 2, 3, 4});
        ReplicationMessage message1 = new ReplicationMessage(0L, payload1);
        boolean result1 = logFile.appendMessage(message1);

        // Append second message
        Binary payload2 = new Binary(new byte[] {5, 6, 7, 8});
        ReplicationMessage message2 = new ReplicationMessage(1L, payload2);
        boolean result2 = logFile.appendMessage(message2);

        assertThat(result1).isTrue();
        assertThat(result2).isTrue();
        assertThat(logFile.getLastMessageNumber()).isEqualTo(1L);
    }

    @Test
    public void testGetLastMessage() {
        // Append a message
        Binary payload = new Binary(new byte[] {1, 2, 3, 4});
        ReplicationMessage message = new ReplicationMessage(0L, payload);
        logFile.appendMessage(message);

        // Get the last message
        ReplicationMessage lastMessage = logFile.getLastMessage();

        assertThat(lastMessage).isNotNull();
        assertThat(lastMessage.messageNumber).isEqualTo(0L);
        assertThat(lastMessage.payload.size()).isEqualTo(4);
    }

    @Test
    public void testFindMessagePosition() {
        // Append messages
        Binary payload1 = new Binary(new byte[] {1, 2, 3, 4});
        ReplicationMessage message1 = new ReplicationMessage(0L, payload1);
        logFile.appendMessage(message1);

        Binary payload2 = new Binary(new byte[] {5, 6, 7, 8});
        ReplicationMessage message2 = new ReplicationMessage(1L, payload2);
        logFile.appendMessage(message2);

        // Find position of the first message
        long position = logFile.findMessagePosition(0L);

        assertThat(position).isGreaterThanOrEqualTo(0);
    }

    @Test
    public void testGetMessage() {
        // Append a message
        Binary payload = new Binary(new byte[] {1, 2, 3, 4});
        ReplicationMessage message = new ReplicationMessage(0L, payload);
        logFile.appendMessage(message);

        // Find position of the message
        long position = logFile.findMessagePosition(0L);

        // Get the message at that position
        Pair<ReplicationMessage, Long> result = logFile.getMessage(position);

        assertThat(result).isNotNull();
        assertThat(result.getFirst().messageNumber).isEqualTo(0L);
        assertThat(result.getFirst().payload.size()).isEqualTo(4);
    }

    @Test
    public void testWrongMessageOrder() {
        // Append first message
        Binary payload1 = new Binary(new byte[] {1, 2, 3, 4});
        ReplicationMessage message1 = new ReplicationMessage(0L, payload1);
        logFile.appendMessage(message1);

        // Try to append a message with wrong order (same message number)
        Binary payload2 = new Binary(new byte[] {5, 6, 7, 8});
        ReplicationMessage message2 = new ReplicationMessage(0L, payload2);

        assertThat(logFile.isWrongMessageOrder(message2)).isTrue();

        // Try to append a message with wrong order (older message number)
        Binary payload3 = new Binary(new byte[] {9, 10, 11, 12});
        ReplicationMessage message3 = new ReplicationMessage(-1L, payload3);

        assertThat(logFile.isWrongMessageOrder(message3)).isTrue();

        // Try to append a message with wrong order (skipped message number)
        Binary payload4 = new Binary(new byte[] {13, 14, 15, 16});
        ReplicationMessage message4 = new ReplicationMessage(2L, payload4);

        assertThat(logFile.isWrongMessageOrder(message4)).isTrue();

        // Try to append a message with correct order
        Binary payload5 = new Binary(new byte[] {17, 18, 19, 20});
        ReplicationMessage message5 = new ReplicationMessage(1L, payload5);

        assertThat(logFile.isWrongMessageOrder(message5)).isFalse();
    }

    @Test
    public void testSetFlushPolicy() {
        assertThat(logFile.getFlushPolicy()).isEqualTo(WALFile.FlushType.NO);

        logFile.setFlushPolicy(WALFile.FlushType.YES_FULL);
        assertThat(logFile.getFlushPolicy()).isEqualTo(WALFile.FlushType.YES_FULL);
    }

    @Test
    public void testSetMaxArchivedChunks() {
        assertThat(logFile.getMaxArchivedChunks()).isEqualTo(200);

        logFile.setMaxArchivedChunks(100);
        assertThat(logFile.getMaxArchivedChunks()).isEqualTo(100);
    }

    @Test
    public void testSetArchiveChunkCallback() {
        final boolean[] callbackCalled = {false};

        ReplicationLogFile.ReplicationLogArchiveCallback callback = (chunkFile, chunkId) -> {
            callbackCalled[0] = true;
        };

        logFile.setArchiveChunkCallback(callback);

        // We can't easily test the callback is called since it requires filling a chunk
        // This just verifies the callback can be set
    }
}
