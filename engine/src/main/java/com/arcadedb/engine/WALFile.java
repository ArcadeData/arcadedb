/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.engine;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LockContext;

import java.io.EOFException;
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
  public enum FlushType {
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

  public static final long MAGIC_NUMBER = 9371515385058702L;

  private final    RandomAccessFile file;
  private final    String           filePath;
  private final    FileChannel      channel;
  private volatile boolean          active            = true;
  private volatile boolean          open;
  private final    AtomicInteger    pagesToFlush      = new AtomicInteger();
  private          long             statsPagesWritten = 0;
  private          long             statsBytesWritten = 0;

  public static class WALTransaction {
    public long      txId;
    public long      timestamp;
    public WALPage[] pages;
    public long      startPositionInLog;
    public long      endPositionInLog;
    // When true, the version-gap check in applyChanges() is bypassed (used for compaction page replication)
    public boolean   forceApply = false;
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
    this.file = new RandomAccessFile(filePath, "rw");
    this.channel = file.getChannel();
    this.open = true;
  }

  public synchronized void close() throws IOException {
    this.open = false;
    if (channel != null)
      channel.close();

    if (file != null)
      file.close();
  }

  public boolean isOpen() {
    return open;
  }

  public synchronized void drop() throws IOException {
    close();
    FileUtils.deleteFile(new File(filePath));
  }

  public WALTransaction getFirstTransaction() throws WALException {
    return getTransaction(0);
  }

  public int getPendingPagesToFlush() {
    return pagesToFlush.get();
  }

  /**
   * If the WAL is still active, execute the callback. This avoids closing a file where a thread is still writing to it.
   *
   * @return true if acquired, otherwise false
   */
  public synchronized boolean acquire(final Callable<Object> callable) {
    if (!active || !open)
      return false;

    try {
      callable.call();
    } catch (final RuntimeException e) {
      throw e;
    } catch (final Exception e) {
      throw new WALException("Error on writing to WAL file " + filePath, e);
    }

    return true;
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

        // Reject obviously corrupted page headers so ByteBuffer.allocate cannot blow up with a
        // negative size and a garbage delta cannot be applied to disk. #4958: the delta must also
        // fit in the remaining file - the outer segment-size check bounds the SEGMENT by file size,
        // but a corrupt per-page header can still declare a huge deltaSize (up to Integer.MAX_VALUE)
        // and trigger a transient ~2GB allocation, or an OutOfMemoryError that escapes the
        // catch(Exception) recovery guard below, before the read would fail.
        if (deltaSize <= 0 || tx.pages[i].changesFrom < 0 || pos + deltaSize > getSize())
          return null;

        final ByteBuffer buffer = ByteBuffer.allocate(deltaSize);

        tx.pages[i].currentContent = new Binary(buffer);

        long readPos = pos;
        while (buffer.hasRemaining()) {
          final int n = channel.read(buffer, readPos);
          if (n == -1)
            return null; // truncated WAL: EOF before delta is complete
          readPos += n;
        }

        pos += deltaSize;
      }

      final long mn = readLong(pos + Binary.INT_SERIALIZED_SIZE);
      if (mn != MAGIC_NUMBER)
        // INVALID
        return null;

      tx.endPositionInLog = pos + Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE;

      return tx;
    } catch (final EOFException e) {
      // TRUNCATED FILE: same benign outcome as the explicit size checks above.
      return null;
    } catch (final IOException e) {
      throw new WALException("Error reading WAL file " + filePath, e);
    } catch (final Exception e) {
      return null;
    }
  }

  /**
   * Scans forward from {@code fromPos} for the start of the next valid transaction record. Used during
   * recovery to tell a benign torn record at the physical end of the file (the last write interrupted by
   * a crash) apart from a corrupt record in the middle of the WAL that is followed by intact transactions.
   * In the latter case stopping silently would drop committed data (issue #4508).
   * <p>
   * Every transaction ends with an 8-byte {@link #MAGIC_NUMBER} footer, so this scans for that pattern and,
   * for each occurrence, derives the candidate transaction start and validates it with {@link #getTransaction(long)}.
   * The validation rejects a {@link #MAGIC_NUMBER} that appears by coincidence inside a page payload because the
   * derived transaction must parse cleanly and its own footer must land exactly on the matched magic number.
   *
   * @param fromPos position where transaction parsing failed
   *
   * @return start position of the next valid transaction, or -1 if none is found before EOF
   */
  public long findNextValidTransactionPosition(final long fromPos) {
    try {
      final long size = getSize();
      if (fromPos < 0 || fromPos + TX_HEADER_SIZE + TX_FOOTER_SIZE > size)
        return -1;

      final int CHUNK_SIZE = 1 << 16; // 64K
      final ByteBuffer chunk = ByteBuffer.allocate(CHUNK_SIZE);

      // Begin one byte past the failed record so its broken content is skipped.
      long scanPos = fromPos + 1;
      while (scanPos + Binary.LONG_SERIALIZED_SIZE <= size) {
        chunk.clear();
        final int toRead = (int) Math.min(CHUNK_SIZE, size - scanPos);
        chunk.limit(toRead);
        int read = 0;
        while (read < toRead) {
          final int n = channel.read(chunk, scanPos + read);
          if (n == -1)
            break;
          read += n;
        }
        if (read < Binary.LONG_SERIALIZED_SIZE)
          break;

        for (int i = 0; i + Binary.LONG_SERIALIZED_SIZE <= read; i++) {
          if (chunk.getLong(i) != MAGIC_NUMBER)
            continue;

          final long magicPos = scanPos + i;
          // TX_FOOTER = SEGMENT_SIZE(int) + MAGIC_NUMBER(long): the matched magic is preceded by the segment size.
          final long footerSegmentSizePos = magicPos - Binary.INT_SERIALIZED_SIZE;
          if (footerSegmentSizePos < fromPos)
            continue;

          final int segmentSize = readInt(footerSegmentSizePos);
          if (segmentSize < 0)
            continue;

          final long txStart = footerSegmentSizePos - (long) segmentSize - TX_HEADER_SIZE;
          if (txStart < fromPos)
            continue;

          final WALTransaction tx = getTransaction(txStart);
          if (tx != null && tx.endPositionInLog == magicPos + Binary.LONG_SERIALIZED_SIZE)
            return txStart;
        }

        // Overlap by 7 bytes so a magic number straddling a chunk boundary is not missed.
        scanPos += read - (Binary.LONG_SERIALIZED_SIZE - 1);
      }
    } catch (final IOException e) {
      // Treat an I/O error during the scan as "no valid transaction found": recovery stops at fromPos.
    }
    return -1;
  }

  public static Binary writeTransactionToBuffer(final List<MutablePage> pages, final long txId) {
    // COMPUTE TOTAL TXLOG SEGMENT SIZE
    int segmentSize = 0;
    for (final MutablePage newPage : pages) {
      final int[] deltaRange = newPage.getModifiedRange();
      final int deltaSize = deltaRange[1] - deltaRange[0] + 1;

      final long totalSizeCheck = 0L + TX_HEADER_SIZE + TX_FOOTER_SIZE + segmentSize + PAGE_HEADER_SIZE + deltaSize; // USE A LONG TO CHECK THE BOUNDARIES
      if (totalSizeCheck > Integer.MAX_VALUE)
        throw new TransactionException("Transaction buffer bigger than " + FileUtils.getSizeAsString(Integer.MAX_VALUE)
            + ". Split the big transaction in smaller transactions. This transaction will be roll backed");

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
    for (final MutablePage newPage : pages) {
      final int[] deltaRange = newPage.getModifiedRange();

      assert deltaRange[0] > -1 && deltaRange[1] < newPage.getPhysicalSize();

      final int deltaSize = deltaRange[1] - deltaRange[0] + 1;
      if (deltaSize < 1)
        throw new TransactionException(
            "Invalid modified range for page " + newPage.getPageId() + " v" + newPage.version + ": deltaRange=[" + deltaRange[0]
                + "," + deltaRange[1] + "] deltaSize=" + deltaSize + " pageSize=" + newPage.getPhysicalSize());

      LogManager.instance()
          .log(WALFile.class, Level.FINE, "Writing page %s v%d range %d-%d into buffer (txId=%d threadId=%d)", null, newPage.getPageId(), newPage.version + 1,
              deltaRange[0], deltaRange[1], txId, Thread.currentThread().threadId());

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

  public void writeTransactionToFile(final DatabaseInternal database, final List<MutablePage> pages, final FlushType sync, final WALFile file, final long txId,
      final Binary buffer) throws IOException {

    LogManager.instance()
        .log(this, Level.FINE, "Appending WAL for txId=%d (size=%d file=%s threadId=%d)", null, txId, buffer.size(), filePath, Thread.currentThread().threadId());

    file.append(buffer.getByteBuffer());

    // WRITE ALL PAGES SEGMENTS
    for (final MutablePage newPage : pages) {
      // SET THE WAL FILE TO NOTIFY LATER WHEN THE PAGE HAS BEEN FLUSHED
      newPage.setWALFile(file);

      pagesToFlush.incrementAndGet();
      statsPagesWritten++;
    }

    statsBytesWritten += buffer.size();

    if (sync == FlushType.YES_NOMETADATA)
      channel.force(false);
    else if (sync == FlushType.YES_FULL)
      channel.force(true);

    database.executeCallbacks(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE);
  }

  public void notifyPageFlushed() {
    pagesToFlush.decrementAndGet();
  }

  public long getSize() throws IOException {
    return channel.size();
  }

  public String getFilePath() {
    return filePath;
  }

  @Override
  public String toString() {
    return filePath;
  }

  public Map<String, Object> getStats() {
    final Map<String, Object> map = new HashMap<>(Map.of(
        "pagesWritten", statsPagesWritten,
        "bytesWritten", statsBytesWritten));
    return map;
  }

  public static FlushType getWALFlushType(final int txFlushType) {
    switch (txFlushType) {
    case 0:
      return FlushType.NO;
    case 1:
      return FlushType.YES_NOMETADATA;
    case 2:
      return FlushType.YES_FULL;
    default:
      throw new ConfigurationException("Invalid TX_WAL_FLUSH setting " + txFlushType);
    }
  }

  private long readLong(final long pos) throws IOException {
    // #4958: per-call buffer (the shared instance buffers were unsynchronized) filled with a full-read
    // loop: a single channel.read may return early and the old code then decoded stale garbage.
    final ByteBuffer buffer = ByteBuffer.allocate(Binary.LONG_SERIALIZED_SIZE);
    readFully(buffer, pos);
    return buffer.getLong(0);
  }

  private int readInt(final long pos) throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(Binary.INT_SERIALIZED_SIZE);
    readFully(buffer, pos);
    return buffer.getInt(0);
  }

  private void readFully(final ByteBuffer buffer, final long pos) throws IOException {
    long readPos = pos;
    while (buffer.hasRemaining()) {
      final int n = channel.read(buffer, readPos);
      if (n == -1)
        throw new EOFException("EOF reading " + buffer.capacity() + " bytes at position " + pos + " of WAL file " + filePath);
      readPos += n;
    }
  }

  protected void append(final ByteBuffer buffer) throws IOException {
    buffer.rewind();
    // #4958: loop until the buffer is fully written. A single channel.write may write only part of the
    // record, leaving a torn entry that the #4508 gap detector would then flag as corruption.
    // Single-writer assumption (same as the pre-loop code that wrote at channel.size() once): appends to a
    // WALFile are externally serialized by acquire(); two concurrent appenders would both seed writePos from
    // the same channel.size() and interleave. The local writePos only tolerates PARTIAL writes, not writers.
    long writePos = channel.size();
    while (buffer.hasRemaining())
      writePos += channel.write(buffer, writePos);
  }
}
