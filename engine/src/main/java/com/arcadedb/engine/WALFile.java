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

package com.arcadedb.engine;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.LockContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class WALFile extends LockContext {
  public enum FLUSH_TYPE {
    NO, YES_NOMETADATA, YES_FULL
  }

  // TXID (long) + TIMESTAMP (long) + PAGES (int) + SEGMENT_SIZE (int)
  private static final int TX_HEADER_SIZE = Binary.LONG_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE;
  // SEGMENT_SIZE (int) + MAGIC_NUMBER (long)
  private static final int TX_FOOTER_SIZE = Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE;

  // FILE_ID (int) + PAGE_NUMBER (int) + DELTA_FROM (int) + DELTA_TO (int) + CURR_PAGE_VERSION (int)+ CURR_PAGE_SIZE (int)
  private static final int PAGE_HEADER_SIZE =
      Binary.INT_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE
          + Binary.INT_SERIALIZED_SIZE;

  public static final long MAGIC_NUMBER = 9371515385058702l;

  private final    String        filePath;
  private          FileChannel   channel;
  private volatile boolean       active       = true;
  private volatile boolean       open;
  private          AtomicInteger pagesToFlush = new AtomicInteger();

  private long statsPagesWritten = 0;
  private long statsBytesWritten = 0;

  // STATIC BUFFERS USED FOR RECOVERY
  private final ByteBuffer bufferLong = ByteBuffer.allocate(Binary.LONG_SERIALIZED_SIZE);
  private final ByteBuffer bufferInt  = ByteBuffer.allocate(Binary.INT_SERIALIZED_SIZE);

  public static class WALTransaction {
    public long      txId;
    public long      timestamp;
    public WALPage[] pages;
    public long      startPositionInLog;
    public long      endPositionInLog;
  }

  public static class WALPage {
    public int    fileId;
    public int    pageNumber;
    public int    changesFrom;
    public int    changesTo;
    public Binary currentContent;
    public int    currentPageVersion;
    public int    currentPageSize;

    @Override
    public String toString() {
      return "WALPage(fileId=" + fileId + " pageNumber=" + pageNumber + ")";
    }
  }

  public WALFile(final String filePath) throws FileNotFoundException {
    this.filePath = filePath;
    this.channel = new RandomAccessFile(filePath, "rw").getChannel();
    this.open = true;
  }

  public synchronized void close() throws IOException {
    this.open = false;
    channel.close();
  }

  public synchronized void drop() throws IOException {
    close();
    new File(getFilePath()).delete();
  }

  public WALTransaction getFirstTransaction() throws WALException {
    return getTransaction(0);
  }

  public int getPendingPagesToFlush() {
    return pagesToFlush.get();
  }

  /**
   * If the WAL is still active, execute the callback. This avoids to close a file where a thread is still writing to it.
   *
   * @return true if acquired, otherwise false
   */
  public synchronized boolean acquire(final Callable<Object> callable) {
    if (!active || !open)
      return false;

    try {
      callable.call();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new WALException("Error on writing to WAL file " + getFilePath(), e);
    }

    return true;
  }

  public boolean isActive() {
    return active;
  }

  public synchronized void setActive(final boolean active) {
    this.active = active;
  }

  public WALTransaction getTransaction(long pos) {
    final WALTransaction tx = new WALTransaction();

    tx.startPositionInLog = pos;

    try {
      if (pos + TX_HEADER_SIZE + TX_FOOTER_SIZE > getSize())
        // TRUNCATED FILE
        return null;

      tx.txId = readLong(pos);
      pos += Binary.LONG_SERIALIZED_SIZE;

      tx.timestamp = readLong(pos);
      pos += Binary.LONG_SERIALIZED_SIZE;

      final int pages = readInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      final int segmentSize = readInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      if (pos + segmentSize + Binary.LONG_SERIALIZED_SIZE > getSize())
        // TRUNCATED FILE
        return null;

      tx.pages = new WALPage[pages];

      for (int i = 0; i < pages; ++i) {
        if (pos > getSize())
          // INVALID
          return null;

        tx.pages[i] = new WALPage();

        tx.pages[i].fileId = readInt(pos);
        pos += Binary.INT_SERIALIZED_SIZE;

        tx.pages[i].pageNumber = readInt(pos);
        pos += Binary.INT_SERIALIZED_SIZE;

        tx.pages[i].changesFrom = readInt(pos);
        pos += Binary.INT_SERIALIZED_SIZE;

        tx.pages[i].changesTo = readInt(pos);
        pos += Binary.INT_SERIALIZED_SIZE;

        final int deltaSize = tx.pages[i].changesTo - tx.pages[i].changesFrom + 1;

        tx.pages[i].currentPageVersion = readInt(pos);
        pos += Binary.INT_SERIALIZED_SIZE;

        tx.pages[i].currentPageSize = readInt(pos);
        pos += Binary.INT_SERIALIZED_SIZE;

        final ByteBuffer buffer = ByteBuffer.allocate(deltaSize);

        tx.pages[i].currentContent = new Binary(buffer);
        channel.read(buffer, pos);

        pos += deltaSize;
      }

      final long mn = readLong(pos + Binary.INT_SERIALIZED_SIZE);
      if (mn != MAGIC_NUMBER)
        // INVALID
        return null;

      tx.endPositionInLog = pos + Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE;

      return tx;
    } catch (Exception e) {
      return null;
    }
  }

  public static Binary writeTransactionToBuffer(final List<MutablePage> pages, final long txId) {
    // COMPUTE TOTAL TXLOG SEGMENT SIZE
    int segmentSize = 0;
    for (MutablePage newPage : pages) {
      final int[] deltaRange = newPage.getModifiedRange();
      final int deltaSize = deltaRange[1] - deltaRange[0] + 1;
      segmentSize += PAGE_HEADER_SIZE + deltaSize;
    }

    final Binary bufferChanges = new Binary(TX_HEADER_SIZE + TX_FOOTER_SIZE + segmentSize);
    bufferChanges.setAutoResizable(false);

    // WRITE TX HEADER (TXID, TIMESTAMP, PAGES, SEGMENT-SIZE)
    bufferChanges.putLong(txId);
    bufferChanges.putLong(System.currentTimeMillis());
    bufferChanges.putInt(pages.size());
    bufferChanges.putInt(segmentSize);

    assert bufferChanges.position() == TX_HEADER_SIZE;

    // WRITE ALL PAGES SEGMENTS
    for (MutablePage newPage : pages) {
      final int[] deltaRange = newPage.getModifiedRange();

      assert deltaRange[0] > -1 && deltaRange[1] < newPage.getPhysicalSize();

      final int deltaSize = deltaRange[1] - deltaRange[0] + 1;

      LogManager.instance()
          .log(WALFile.class, Level.FINE, "Writing page %s v%d range %d-%d into buffer (txId=%d threadId=%d)", null, newPage.getPageId(), newPage.version + 1,
              deltaRange[0], deltaRange[1], txId, Thread.currentThread().getId());

      bufferChanges.putInt(newPage.getPageId().getFileId());
      bufferChanges.putInt(newPage.getPageId().getPageNumber());
      bufferChanges.putInt(deltaRange[0]);
      bufferChanges.putInt(deltaRange[1]);
      bufferChanges.putInt(newPage.version + 1);
      bufferChanges.putInt(newPage.getContentSize());

      bufferChanges.size(bufferChanges.position() + deltaSize);

      final ByteBuffer newPageBuffer = newPage.getContent();
      newPageBuffer.position(deltaRange[0]);
      newPageBuffer.get(bufferChanges.getContent(), bufferChanges.position(), deltaSize);

      bufferChanges.position(bufferChanges.position() + deltaSize);
    }

    // WRITE TX FOOTER (MAGIC NUMBER)
    bufferChanges.putInt(segmentSize);
    bufferChanges.putLong(MAGIC_NUMBER);

    return bufferChanges;
  }

  public void writeTransactionToFile(final DatabaseInternal database, final List<MutablePage> pages, final FLUSH_TYPE sync, final WALFile file, final long txId,
      final Binary buffer) throws IOException {

    LogManager.instance()
        .log(this, Level.FINE, "Appending WAL for txId=%d (size=%d file=%s threadId=%d)", null, txId, buffer.size(), filePath, Thread.currentThread().getId());

    file.append(buffer.getByteBuffer());

    // WRITE ALL PAGES SEGMENTS
    for (MutablePage newPage : pages) {
      // SET THE WAL FILE TO NOTIFY LATER WHEN THE PAGE HAS BEEN FLUSHED
      newPage.setWALFile(file);

      pagesToFlush.incrementAndGet();
      statsPagesWritten++;
    }

    statsBytesWritten += buffer.size();

    if (sync == FLUSH_TYPE.YES_NOMETADATA)
      channel.force(false);
    else if (sync == FLUSH_TYPE.YES_FULL)
      channel.force(true);

    database.executeCallbacks(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE);
  }

  public int getPagesToFlush() {
    return pagesToFlush.get();
  }

  public void notifyPageFlushed() {
    pagesToFlush.decrementAndGet();
  }

  public long getSize() throws IOException {
    return channel.size();
  }

  public boolean isOpen() {
    return open;
  }

  public String getFilePath() {
    return filePath;
  }

  @Override
  public String toString() {
    return filePath;
  }

  public Map<String, Object> getStats() {
    final Map<String, Object> map = new HashMap<>();
    map.put("pagesWritten", statsPagesWritten);
    map.put("bytesWritten", statsBytesWritten);
    return map;
  }

  public static FLUSH_TYPE getWALFlushType(final int txFlushType) {
    switch (txFlushType) {
    case 0:
      return WALFile.FLUSH_TYPE.NO;
    case 1:
      return WALFile.FLUSH_TYPE.YES_NOMETADATA;
    case 2:
      return WALFile.FLUSH_TYPE.YES_FULL;
    default:
      throw new ConfigurationException("Invalid TX_WAL_FLUSH setting " + txFlushType);
    }
  }

  private long readLong(final long pos) throws IOException {
    bufferLong.rewind();
    channel.read(bufferLong, pos);
    return bufferLong.getLong(0);
  }

  private int readInt(final long pos) throws IOException {
    bufferInt.rewind();
    channel.read(bufferInt, pos);
    return bufferInt.getInt(0);
  }

  protected void append(final ByteBuffer buffer) throws IOException {
    buffer.rewind();
    channel.write(buffer, channel.size());
  }
}
