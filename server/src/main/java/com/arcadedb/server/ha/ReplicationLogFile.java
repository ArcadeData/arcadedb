/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.server.ha;

import com.arcadedb.database.Binary;
import com.arcadedb.engine.WALFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.log.ServerLogger;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LockContext;
import com.arcadedb.utility.Pair;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.logging.Level;

/**
 * Replication Log File. Writes the messages to send to a remote node on reconnection.
 * Since April 4, 2020, multiple chunk files are managed. The message position is still a long, by simulating the position of a continuous large file rather
 * than small chunks. This reduces the impacts on the rest of the components. The chunk size is 64MB, so no message can be bigger than that.
 * <p>
 * TODO: CONSIDER STRIPING MSG IF FROM THE HEADER BECAUSE THE MESSAGE NUMBER IS CONTAINED IN BOTH THE HEADER (+0 POSITION) AND THE PAYLOAD (+1 POSITION)
 * ( MSG ID + COMMAND( CMD ID + MSG ID + SERIALIZATION ) )
 */
public class ReplicationLogFile extends LockContext {
  private final        ServerLogger                  serverLogger;
  private final        String                        filePath;
  private              FileChannel                   lastChunkChannel;
  private              FileChannel                   searchChannel        = null;
  private              long                          searchChannelChunkId = -1;
  private static final int                           BUFFER_HEADER_SIZE   = Binary.LONG_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE;
  private final        ByteBuffer                    bufferHeader         = ByteBuffer.allocate(BUFFER_HEADER_SIZE);
  private static final int                           BUFFER_FOOTER_SIZE   = Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE;
  private final        ByteBuffer                    bufferFooter         = ByteBuffer.allocate(BUFFER_FOOTER_SIZE);
  private static final long                          MAGIC_NUMBER         = 93719829258702l;
  private              long                          lastMessageNumber    = -1;
  private final        long                          CHUNK_SIZE           = 64 * 1024 * 1024;
  private              long                          chunkNumber          = 0L;
  private              WALFile.FLUSH_TYPE            flushPolicy          = WALFile.FLUSH_TYPE.NO;
  private              ReplicationLogArchiveCallback archiveChunkCallback = null;
  private              long                          totalArchivedChunks  = 0;
  private              long                          maxArchivedChunks    = 200;

  private static final Comparator<File> LOG_COMPARATOR = (file1, file2) -> {
    int seq1 = Integer.parseInt(file1.getName().substring(file1.getName().lastIndexOf(".") + 1));
    int seq2 = Integer.parseInt(file2.getName().substring(file2.getName().lastIndexOf(".") + 1));
    return seq1 - seq2;
  };

  public interface ReplicationLogArchiveCallback {
    void archiveChunk(File chunkFile, int chunkId);
  }

  public static class Entry {
    public final long   messageNumber;
    public final Binary payload;
    public final int    length;

    public Entry(final long messageNumber, final Binary payload, final int length) {
      this.messageNumber = messageNumber;
      this.payload = payload;
      this.length = length;
    }
  }

  public ReplicationLogFile(final String filePath, final ServerLogger serverLogger) throws FileNotFoundException {
    this.serverLogger = serverLogger;
    this.filePath = filePath;

    final File f = new File(filePath);
    if (!f.exists())
      f.getParentFile().mkdirs();

    openLastFileChunk(f);

    final ReplicationMessage lastMessage = getLastMessage();
    if (lastMessage != null)
      lastMessageNumber = lastMessage.messageNumber;
  }

  public void close() {
    executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        lastChunkChannel.force(true);
        lastChunkChannel.close();
        lastChunkChannel = null;

        if (searchChannel != null) {
          searchChannel.close();
          searchChannel = null;
        }
        return null;
      }
    });
  }

  // NEVER USED
  public void drop() {
    close();
    new File(filePath).delete();
  }

  public long getLastMessageNumber() {
    return lastMessageNumber;
  }

  public boolean appendMessage(final ReplicationMessage message) {
    return (boolean) executeInLock(new Callable<Object>() {
      @Override
      public Object call() {
        try {
          if (!checkMessageOrder(message))
            return false;

          // UPDATE LAST MESSAGE NUMBER
          lastMessageNumber = message.messageNumber;

          final byte[] content = message.payload.toByteArray();

          final int entrySize = BUFFER_HEADER_SIZE + content.length + BUFFER_FOOTER_SIZE;

          if (entrySize > CHUNK_SIZE)
            throw new IllegalArgumentException("Cannot store in replication file messages bigger than " + FileUtils.getSizeAsString(CHUNK_SIZE));

          if (lastChunkChannel.size() + entrySize > CHUNK_SIZE)
            archiveChunk();

          // WRITE HEADER
          bufferHeader.clear();
          bufferHeader.putLong(message.messageNumber);
          bufferHeader.putInt(content.length);
          bufferHeader.rewind();
          lastChunkChannel.write(bufferHeader, lastChunkChannel.size());

          // WRITE PAYLOAD
          lastChunkChannel.write(ByteBuffer.wrap(content), lastChunkChannel.size());

          // WRITE FOOTER
          bufferFooter.clear();
          bufferFooter.putInt(entrySize);
          bufferFooter.putLong(MAGIC_NUMBER);
          bufferFooter.rewind();

          lastChunkChannel.write(bufferFooter, lastChunkChannel.size());

          switch (flushPolicy) {
          case YES_FULL:
            lastChunkChannel.force(true);
            break;
          case YES_NOMETADATA:
            lastChunkChannel.force(false);
            break;
          }

          return true;

        } catch (Exception e) {
          throw new ReplicationLogException("Error on writing message " + message.messageNumber + " to the replication log", e);
        }
      }
    });
  }

  public long findMessagePosition(final long messageNumberToFind) {
    // TODO: CHECK THE LAST MESSAGE AND DECIDE WHERE TO START EITHER FROM THE HEAD OR FROM THE TAIL

    return (long) executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        // LOOK FOR THE RIGHT FILE
        long chunkId = chunkNumber;

        while (chunkId > -1) {
          if (!openChunk(chunkId))
            return -1l;

          bufferHeader.clear();
          searchChannel.read(bufferHeader, 0);
          bufferHeader.rewind();

          final long chunkBeginMessageNumber = bufferHeader.getLong();
          if (messageNumberToFind == chunkBeginMessageNumber)
            // MESSAGE FOUND AS FIRST MESSAGE OF THE CHUNK
            return (long) (chunkId * CHUNK_SIZE);
          else if (messageNumberToFind > chunkBeginMessageNumber)
            // CHUNK FOUND
            break;

          --chunkId;
        }

        final long fileSize = searchChannel.size();

        for (long pos = 0; pos < fileSize; ) {
          bufferHeader.clear();
          searchChannel.read(bufferHeader, pos);
          bufferHeader.rewind();

          final long messageNumber = bufferHeader.getLong();
          if (messageNumber == messageNumberToFind)
            // FOUND
            return pos + ((long) chunkId * CHUNK_SIZE);

          if (messageNumber > messageNumberToFind)
            // NOT IN LOG ANYMORE
            return -1l;

          final int contentLength = bufferHeader.getInt();

          pos += BUFFER_HEADER_SIZE + contentLength + BUFFER_FOOTER_SIZE;
        }

        return -1l;
      }
    });
  }

  private boolean openChunk(final long chunkId) throws IOException {
    if (chunkId != searchChannelChunkId) {
      if (searchChannel != null)
        searchChannel.close();

      final File chunkFile = new File(filePath + "." + chunkId);
      if (!chunkFile.exists()) {
        // CHUNK NOT FOUND (= NOT AVAILABLE, PROBABLY DELETED BECAUSE TOO OLD)
        searchChannel = null;
        searchChannelChunkId = -1L;
        LogManager.instance().log(this, Level.WARNING, "Replication log chunk file %d was not found", null, chunkId);
        return false;
      }

      searchChannel = new RandomAccessFile(chunkFile, "rw").getChannel();
      searchChannelChunkId = chunkId;
    }
    return true;
  }

  public Pair<ReplicationMessage, Long> getMessage(final long positionInFile) {
    return (Pair<ReplicationMessage, Long>) executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        if (positionInFile < 0)
          throw new ReplicationLogException("Invalid position (" + positionInFile + ") in replication log file of size " + getSize());

        if (positionInFile > (searchChannel.size() - BUFFER_HEADER_SIZE - BUFFER_FOOTER_SIZE) + (chunkNumber * CHUNK_SIZE))
          throw new ReplicationLogException("Invalid position (" + positionInFile + ") in replication log file of size " + getSize());

        final int chunkId = (int) (positionInFile / CHUNK_SIZE);
        if (!openChunk(chunkId))
          throw new ReplicationLogException("Cannot find replication log file with chunk id " + chunkId);

        final long posInChunk = positionInFile % CHUNK_SIZE;

        // READ THE HEADER
        bufferHeader.clear();
        searchChannel.read(bufferHeader, posInChunk);
        bufferHeader.rewind();

        final long messageNumber = bufferHeader.getLong();
        final int contentLength = bufferHeader.getInt();

//        LogManager.instance()
//            .log(this, Level.FINE, "Read log message chunk=%d pos=%d msgNumber=%d length=%d chunkSize=%d", null, chunkId, posInChunk, messageNumber,
//                contentLength, searchChannel.size());

        // READ THE PAYLOAD
        final ByteBuffer bufferPayload = ByteBuffer.allocate(contentLength);
        searchChannel.read(bufferPayload, posInChunk + BUFFER_HEADER_SIZE);

        // READ THE FOOTER
        bufferFooter.clear();
        searchChannel.read(bufferFooter, posInChunk + BUFFER_HEADER_SIZE + contentLength);
        bufferFooter.rewind();

        final int entrySize = bufferFooter.getInt();
        final long magicNumber = bufferFooter.getLong();

        if (magicNumber != MAGIC_NUMBER)
          throw new ReplicationLogException("Corrupted replication log file at position " + positionInFile);

        final long nextPos;
        if (posInChunk + BUFFER_HEADER_SIZE + contentLength + BUFFER_FOOTER_SIZE >= searchChannel.size())
          // END OF CHUNK, SET NEXT POSITION AT THE BEGINNING OF THE NEXT CHUNK
          nextPos = (chunkId + 1L) * CHUNK_SIZE;
        else
          nextPos = positionInFile + BUFFER_HEADER_SIZE + contentLength + BUFFER_FOOTER_SIZE;

        return new Pair<>(new ReplicationMessage(messageNumber, new Binary((ByteBuffer) bufferPayload.rewind())), nextPos);
      }
    });
  }

  public boolean checkMessageOrder(final ReplicationMessage message) {
    if (lastMessageNumber > -1) {
      if (message.messageNumber < lastMessageNumber) {
        serverLogger.log(this, Level.WARNING, "Wrong sequence in message numbers. Last was %d and now receiving %d. Skip saving this entry (threadId=%d)",
            lastMessageNumber, message.messageNumber, Thread.currentThread().getId());
        return false;
      }

      if (message.messageNumber != lastMessageNumber + 1) {
        serverLogger.log(this, Level.WARNING, "Found a jump (%d) in message numbers. Last was %d and now receiving %d. Skip saving this entry (threadId=%d)",
            (message.messageNumber - lastMessageNumber), lastMessageNumber, message.messageNumber, Thread.currentThread().getId());

        return false;
      }
    }
    return true;
  }

  public ReplicationMessage getLastMessage() {
    return (ReplicationMessage) executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final long pos = lastChunkChannel.size();
        if (pos == 0)
          // EMPTY FILE
          return null;

        if (pos < BUFFER_HEADER_SIZE + BUFFER_FOOTER_SIZE) {
          // TODO: SCAN FROM THE HEAD
          throw new ReplicationLogException("Invalid position (" + pos + ") in replication log file of size " + lastChunkChannel.size());
        }

        // READ THE FOOTER
        bufferFooter.clear();
        lastChunkChannel.read(bufferFooter, pos - BUFFER_FOOTER_SIZE);
        bufferFooter.rewind();

        final int entrySize = bufferFooter.getInt();
        final long magicNumber = bufferFooter.getLong();

        if (magicNumber != MAGIC_NUMBER)
          throw new ReplicationLogException("Corrupted replication log file");

        // READ THE HEADER
        bufferHeader.clear();
        lastChunkChannel.read(bufferHeader, pos - entrySize);
        bufferHeader.rewind();

        final long messageNumber = bufferHeader.getLong();
        final int contentLength = bufferHeader.getInt();

        // READ THE PAYLOAD
        final ByteBuffer bufferPayload = ByteBuffer.allocate(contentLength);
        lastChunkChannel.read(bufferPayload, pos - entrySize + BUFFER_HEADER_SIZE);

        return new ReplicationMessage(messageNumber, new Binary((ByteBuffer) bufferPayload.rewind()));
      }
    });
  }

  public long getSize() {
    return (Long) executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        try {
          return lastChunkChannel.size() + (chunkNumber * CHUNK_SIZE);
        } catch (IOException e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on computing file size for last chunk (%d) in replication log '%s'", e, chunkNumber, filePath);
          return 0l;
        }
      }
    });
  }

  public WALFile.FLUSH_TYPE getFlushPolicy() {
    return flushPolicy;
  }

  public void setFlushPolicy(final WALFile.FLUSH_TYPE flushPolicy) {
    this.flushPolicy = flushPolicy;
  }

  public void setArchiveChunkCallback(final ReplicationLogArchiveCallback archiveChunkCallback) {
    this.archiveChunkCallback = archiveChunkCallback;
  }

  /**
   * Returns the maximum chunk files to keep.
   *
   * @return 0 for unlimited
   */
  public int getMaxArchivedChunks() {
    return (int) maxArchivedChunks;
  }

  /**
   * Sets the maximum number of chunk files to keep. Circular rewriting will be used.
   *
   * @param maxArchivedChunks use 0 for unlimited
   */
  public void setMaxArchivedChunks(final int maxArchivedChunks) {
    this.maxArchivedChunks = maxArchivedChunks;
  }

  @Override
  public String toString() {
    return filePath;
  }

  @Override
  protected RuntimeException manageExceptionInLock(final Throwable e) {
    if (e instanceof ReplicationLogException)
      throw (ReplicationLogException) e;

    return new ReplicationLogException("Error in replication log", e);
  }

  private void openLastFileChunk(final File logFile) throws FileNotFoundException {
    final String prefix = logFile.getName() + ".";
    final List<File> fileChunks = Arrays.asList(logFile.getParentFile().listFiles((f) -> f.getName().startsWith(prefix)));
    fileChunks.sort(LOG_COMPARATOR);

    totalArchivedChunks = fileChunks.isEmpty() ? 0 : fileChunks.size() - 1;

    final File lastFile = fileChunks.isEmpty() ? new File(logFile.getAbsolutePath() + ".0") : fileChunks.get(fileChunks.size() - 1);

    this.lastChunkChannel = new RandomAccessFile(lastFile, "rw").getChannel();

    chunkNumber = Long.parseLong(lastFile.getName().substring(lastFile.getName().lastIndexOf(".") + 1));
  }

  private void archiveChunk() throws IOException {
    // CREATE A NEW CHUNK FILE
    lastChunkChannel.force(true);
    lastChunkChannel.close();
    lastChunkChannel = null;

    if (archiveChunkCallback != null) {
      final File archivedFile = new File(filePath + "." + chunkNumber);
      try {
        archiveChunkCallback.archiveChunk(archivedFile, (int) chunkNumber);
      } catch (Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Error in replication log archive callback invoked on file '%s'", e, archivedFile);
      }
    }

    if (maxArchivedChunks > 0 && ++totalArchivedChunks > maxArchivedChunks) {
      // REMOVE THE OLDEST
      final File file2Remove = new File(filePath + "." + (chunkNumber - maxArchivedChunks));
      if (file2Remove.exists())
        file2Remove.delete();
      --totalArchivedChunks;
    }

    final File f = new File(filePath + "." + (chunkNumber + 1));
    lastChunkChannel = new RandomAccessFile(f, "rw").getChannel();
    ++chunkNumber;
  }
}
