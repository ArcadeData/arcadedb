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

import com.arcadedb.log.LogManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.spi.AbstractInterruptibleChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.zip.CRC32;

public class PaginatedComponentFile extends ComponentFile {

  private                 RandomAccessFile file;
  private                 FileChannel      channel;
  private                 int              pageSize;
  private static volatile boolean          warningPrinted = false;

  /**
   * Guards the {@link #channel}/{@link #file} fields against concurrent I/O while they are swapped.
   * I/O methods (read/write/force/...) acquire the shared READ lock, so independent pages still run
   * concurrently; {@link #close()} and {@link #rename(String)} acquire the exclusive WRITE lock so a
   * channel can never be closed or replaced from under an in-flight operation.
   */
  private final ReentrantReadWriteLock channelLock = new ReentrantReadWriteLock();

  /**
   * How many whole pages this file holds, maintained in memory instead of asked of the filesystem (#6132, item 1).
   * <p>
   * It is EXACT rather than an estimate, and the argument is short: a paginated component file is extended by
   * exactly one operation, {@link #write(MutablePage)}, which always writes one whole page at
   * {@code pageNumber * pageSize}; nothing truncates it, and nothing else writes to the channel. Seeded from the
   * real length at {@link #open}, advanced past every successful write, and re-seeded whenever the channel is
   * reopened or renamed, it therefore says exactly what {@code channel.size() / pageSize} says. The monotonic
   * {@code max} is not an approximation either: the value it maxes against is the file's own length, which only
   * grows.
   * <p>
   * Why it is worth having: {@link PageManager}'s t0 snapshot barrier reads it once per page file while holding the
   * JVM-wide page-manager lock, behind which every committer of every database in the process queues. On a database
   * with many buckets, indexes and compacted sub-indexes that was dozens to hundreds of {@code fstat} calls plus a
   * {@code channelLock} acquisition each, none of them bounded by the barrier's deadline on a stalled filesystem -
   * the last unbounded filesystem I/O left under those locks. The second reader is {@link PageManager}'s
   * new-page test on every read-cache miss, which is as hot as the engine gets.
   */
  private volatile int totalPages;

  /**
   * Updates {@link #totalPages} atomically WITHOUT an {@code AtomicInteger}: {@link #open} runs from the superclass
   * constructor, before this class's field initializers, so an object field would still be null when it has to be
   * seeded. A {@code volatile int} declared without an initializer keeps the value {@code open()} writes, and the
   * updater - a static, created once at class initialization - gives the same compare-and-set the counter was for.
   */
  private static final AtomicIntegerFieldUpdater<PaginatedComponentFile> TOTAL_PAGES_UPDATER =
      AtomicIntegerFieldUpdater.newUpdater(PaginatedComponentFile.class, "totalPages");

  public static class InterruptibleInvocationHandler implements InvocationHandler {
    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
      LogManager.instance().log(this, Level.SEVERE, "Attempt to close channel");
      return null;
    }
  }

  public PaginatedComponentFile() {
  }

  protected PaginatedComponentFile(final String filePath, final MODE mode) throws FileNotFoundException {
    super(filePath, mode);
  }

  /**
   * Reopens the channel after a {@link ClosedChannelException} while the caller holds the READ lock.
   * The read lock cannot be upgraded in place, so it is released, the exclusive WRITE lock is taken to
   * reopen the channel (double-checked so concurrent callers reopen it only once, avoiding leaked file
   * descriptors), then the read lock is reacquired before returning. The caller resumes its I/O under
   * the read lock exactly as before.
   */
  private void reopenChannelUnderWriteLock() throws FileNotFoundException {
    channelLock.readLock().unlock();
    channelLock.writeLock().lock();
    try {
      try {
        // #4930: only recover a channel closed by ACCIDENT (thread interrupt). If the file was closed on
        // purpose (close()/drop set open=false) or its OS file no longer exists (dropped by DDL while a
        // page was in flight in the flush thread), reopening would RE-CREATE the deleted file - open() uses
        // RandomAccessFile("rw") - leaving a one-page ghost file that FileManager re-registers on the next
        // open: schema/file-id confusion. Surface the closed-channel condition to the caller instead.
        if (!open)
          throw new FileNotFoundException(
              "File '" + fileName + "' was closed on purpose, refusing to reopen it after a ClosedChannelException");
        if (!new File(filePath).exists())
          throw new FileNotFoundException(
              "File '" + fileName + "' no longer exists on disk (dropped?), refusing to re-create it after a ClosedChannelException");

        if (channel == null || !channel.isOpen())
          open(filePath, mode);
      } finally {
        // DOWNGRADE: reacquire the read lock before releasing the write lock so no rename/close
        // slips in, and so the caller's finally block always has the read lock to release - even
        // when open() fails.
        channelLock.readLock().lock();
      }
    } finally {
      channelLock.writeLock().unlock();
    }
  }

  public void force(final boolean metaData) throws IOException {
    channelLock.readLock().lock();
    try {
      if (channel == null)
        return;
      try {
        channel.force(metaData);
      } catch (final ClosedChannelException e) {
        LogManager.instance().log(this, Level.SEVERE, "File '%s' was closed on force. Reopen it and retry...", null, fileName);
        // ClosedByInterruptException leaves the interrupted flag set; clear it so the reopened channel
        // is not immediately closed again, then restore it so callers are notified.
        final boolean wasInterrupted = Thread.interrupted();
        try {
          reopenChannelUnderWriteLock();
          channel.force(metaData);
        } finally {
          if (wasInterrupted)
            Thread.currentThread().interrupt();
        }
      }
    } finally {
      channelLock.readLock().unlock();
    }
  }

  @Override
  public void close() {
    channelLock.writeLock().lock();
    try {
      LogManager.instance().log(this, Level.FINE, "Closing file %s (id=%d)...", null, filePath, fileId);

      if (channel != null) {
        channel.close();
        channel = null;
      }

      if (file != null) {
        file.close();
        file = null;
      }

    } catch (final IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on closing file %s (id=%d)", e, filePath, fileId);
    } finally {
      this.open = false;
      channelLock.writeLock().unlock();
    }
  }

  /**
   * Renames the underlying OS file. {@code newFileName} is the complete file name, tail included; a name without a
   * path separator is resolved against the current parent directory.
   * <p>
   * To change only the component name and keep the {@code .fileId.pageSize.vVersion.ext} tail, call
   * {@link #renameComponent(String)}: this method cannot infer where the component name ends, and every heuristic
   * for doing so is wrong for a name that itself contains the delimiter being searched for.
   */
  public void rename(final String newFileName) throws IOException {
    channelLock.writeLock().lock();
    try {
      close();
      LogManager.instance().log(this, Level.FINE, "Renaming file %s (id=%d) to %s...", null, filePath, fileId, newFileName);

      final File newFile;
      if (newFileName.indexOf(File.separatorChar) > -1 || newFileName.indexOf('/') > -1)
        newFile = new File(newFileName);
      else
        newFile = new File(osFile.getParentFile(), newFileName);
      try {
        Files.move(osFile.getAbsoluteFile().toPath(), newFile.getAbsoluteFile().toPath(), StandardCopyOption.ATOMIC_MOVE);
        open(newFile.getAbsolutePath(), mode);
      } catch (Exception e) {
        open(filePath, mode);
        throw new IOException("Error renaming file " + filePath + " to " + newFile.getAbsolutePath(), e);
      }
    } finally {
      channelLock.writeLock().unlock();
    }
  }

  /**
   * Renames only the component name, recomposing the file name from the fields the open-time parser already
   * resolved. Nothing is inferred from either the current or the new name, so a component name containing '.' or
   * '_' round-trips like any other.
   */
  public void renameComponent(final String newComponentName) throws IOException {
    if (newComponentName == null || newComponentName.isEmpty())
      throw new IOException("Invalid component name '" + newComponentName + "'");
    if (newComponentName.indexOf(File.separatorChar) > -1 || newComponentName.indexOf('/') > -1)
      throw new IOException("Component name '" + newComponentName + "' cannot contain a path separator");

    rename(newComponentName + "." + fileId + "." + pageSize + ".v" + version + "." + fileExtension);
  }

  @Override
  public long getSize() throws IOException {
    channelLock.readLock().lock();
    try {
      return channel.size();
    } finally {
      channelLock.readLock().unlock();
    }
  }

  /**
   * Whole pages this file holds. A field read, not a syscall - see {@link #totalPages} for why the two are the same
   * number and why the difference matters (#6132).
   */
  public long getTotalPages() {
    return totalPages;
  }

  /**
   * The same number, read from the filesystem. Nothing in the engine calls this: it exists so a test can assert that
   * the in-memory counter and the file agree, which is the whole warrant for {@link #getTotalPages()} not being a
   * syscall.
   */
  public long getTotalPagesFromChannel() throws IOException {
    channelLock.readLock().lock();
    try {
      return channel.size() / pageSize;
    } finally {
      channelLock.readLock().unlock();
    }
  }

  public long calculateChecksum() throws IOException {
    channelLock.readLock().lock();
    try {
      final CRC32 crc = new CRC32();

      final ByteBuffer buffer = ByteBuffer.allocate(getPageSize());

      final long totalPages = channel.size() / pageSize;
      for (int i = 0; i < totalPages; i++) {
        buffer.clear();
        long pos = pageSize * (long) i;
        while (buffer.hasRemaining()) {
          final int r = channel.read(buffer, pos);
          if (r < 0)
            throw new IOException("Unexpected EOF calculating checksum at page " + i + " of file '" + getFileName() + "'");
          pos += r;
        }

        buffer.rewind();
        for (int j = 0; j < pageSize; j++) {
          final int read = buffer.get(j);
          crc.update(read);
        }
      }

      return crc.getValue();
    } finally {
      channelLock.readLock().unlock();
    }
  }

  /**
   * Returns the byte written. Current implementation flushes always the entire page because (1) there is not a sensible increase of
   * performance and (2) in case a page is modified multiple times before the flush now it's overwritten in the writeCache map.
   */
  public int write(final MutablePage page) throws IOException {
    final int pageNumber = page.pageId.getPageNumber();
    if (pageNumber < 0)
      throw new IllegalArgumentException("Invalid page number to write: " + pageNumber);

    channelLock.readLock().lock();
    try {
      if (channel == null)
        throw new IllegalArgumentException("Cannot write page " + pageNumber + " because the file '" + getFileName() + "' is closed");

      assert page.pageId.getFileId() == fileId;
      final ByteBuffer buffer = page.getContent();

      // NO NEED TO SYNCHRONIZE THE BUFFER BECAUSE MUTABLE PAGES ARE NOT SHARED
      buffer.clear();
      try {
        long pos = page.getPhysicalSize() * (long) pageNumber;
        while (buffer.hasRemaining())
          pos += channel.write(buffer, pos);
      } catch (final ClosedChannelException e) {
        LogManager.instance().log(this, Level.SEVERE, "File '%s' was closed on write. Reopen it and retry...", null, fileName);
        // ClosedByInterruptException leaves the interrupted flag set; clear it so the reopened channel
        // is not immediately closed again, then restore it so callers are notified.
        final boolean wasInterrupted = Thread.interrupted();
        try {
          reopenChannelUnderWriteLock();
          buffer.clear();
          long pos = page.getPhysicalSize() * (long) pageNumber;
          while (buffer.hasRemaining())
            pos += channel.write(buffer, pos);
        } finally {
          if (wasInterrupted)
            Thread.currentThread().interrupt();
        }
      }

      // AFTER THE WRITE RETURNED, NOT BEFORE: the counter then never claims a page the file does not hold yet, which
      // is the direction that matters - a reader told a page exists goes on to read it. The write above is the ONLY
      // operation that extends this file, so this is where the length changes and the only place the counter has to
      // follow it (#6132).
      final int pagesAfterThisWrite = pageNumber + 1;
      if (pagesAfterThisWrite > totalPages)
        TOTAL_PAGES_UPDATER.accumulateAndGet(this, pagesAfterThisWrite, Math::max);
    } finally {
      channelLock.readLock().unlock();
    }

    return pageSize;
//
//    final int[] range = page.getModifiedRange();
//
//    assert range[0] > -1 && range[1] < pageSize;
//
//    if (range[0] == 0 && range[1] == pageSize - 1) {
//      // FLUSH THE ENTIRE PAGE
//      buffer.rewind();
//      channel.write(buffer, (page.getPhysicalSize() * (long) page.getPageId().getPageNumber()));
//      return pageSize;
//    }
//
//    // FLUSH ONLY THE UPDATED VERSION + DELTA
//    buffer.position(range[1] + 1);
//    buffer.flip();
//    buffer.rewind(); // ALWAYS WRITE FROM 0 TO INCLUDE PAGE VERSION
//    final ByteBuffer delta = buffer.slice();
//
//    channel.write(delta, (page.getPhysicalSize() * (long) page.getPageId().getPageNumber()));
//
//    return range[1] - range[0] + 1;
  }

  public void read(final CachedPage page) throws IOException {
    final int pageNumber = page.getPageId().getPageNumber();
    if (page.getPageId().getPageNumber() < 0)
      throw new IllegalArgumentException("Invalid page number to read: " + pageNumber);

    channelLock.readLock().lock();
    try {
      if (channel == null)
        throw new IllegalArgumentException("Cannot read page " + pageNumber + " because the file '" + getFileName() + "' is closed");

      assert page.getPageId().getFileId() == fileId;
      final ByteBuffer buffer = page.getByteBuffer();
      buffer.clear();

      try {
        long pos = page.getPhysicalSize() * (long) pageNumber;
        while (buffer.hasRemaining()) {
          final int r = channel.read(buffer, pos);
          if (r < 0)
            throw new IOException("Unexpected EOF reading page " + pageNumber + " from file '" + getFileName() + "'");
          pos += r;
        }
      } catch (final ClosedChannelException e) {
        LogManager.instance().log(this, Level.SEVERE, "File '%s' was closed on read. Reopen it and retry...", null, fileName);
        // ClosedByInterruptException leaves the interrupted flag set; clear it so the reopened channel
        // is not immediately closed again, then restore it so callers are notified.
        final boolean wasInterrupted = Thread.interrupted();
        try {
          reopenChannelUnderWriteLock();
          buffer.clear();
          long pos = page.getPhysicalSize() * (long) pageNumber;
          while (buffer.hasRemaining()) {
            final int r = channel.read(buffer, pos);
            if (r < 0)
              throw new IOException("Unexpected EOF reading page " + pageNumber + " from file '" + getFileName() + "'");
            pos += r;
          }
        } finally {
          if (wasInterrupted)
            Thread.currentThread().interrupt();
        }
      }
    } finally {
      channelLock.readLock().unlock();
    }
  }

  /**
   * Reads a RUN of consecutive pages in one call into {@code buf} at its current position, which must have at least
   * {@code pages * pageSize} bytes remaining. Unlike {@link #readPage} the buffer is NOT cleared, so several runs can
   * be assembled into one larger buffer.
   * <p>
   * Used by the point-in-time snapshot reader ({@link PageSnapshot}, issue #6075) to keep the copy sequential: the
   * copy-on-write shadow removes the need for a per-page interlock, so a megabyte can be read in one syscall instead
   * of sixteen and the OS readahead works exactly as it did for the bulk {@code transferTo} the snapshot replaced.
   */
  public void readPages(final int fromPageNumber, final int pages, final ByteBuffer buf) throws IOException {
    if (fromPageNumber < 0)
      throw new IllegalArgumentException("Invalid page number to read: " + fromPageNumber);

    final int expected = pages * pageSize;
    if (buf.remaining() < expected)
      throw new IllegalArgumentException(
          "Buffer too small to read " + pages + " pages of file '" + getFileName() + "': " + buf.remaining() + " < " + expected);

    final int limit = buf.limit();
    buf.limit(buf.position() + expected);
    channelLock.readLock().lock();
    try {
      if (channel == null)
        throw new IllegalArgumentException(
            "Cannot read pages from " + fromPageNumber + " because the file '" + getFileName() + "' is closed");

      long pos = pageSize * (long) fromPageNumber;
      while (buf.hasRemaining()) {
        final int r = channel.read(buf, pos);
        if (r < 0)
          throw new IOException(
              "Unexpected EOF reading " + pages + " pages from page " + fromPageNumber + " of file '" + getFileName() + "'");
        pos += r;
      }
    } finally {
      channelLock.readLock().unlock();
      buf.limit(limit);
    }
  }

  public void readPage(final int pageNum, final ByteBuffer buf) throws IOException {
    channelLock.readLock().lock();
    try {
      buf.clear();
      long pos = pageSize * (long) pageNum;
      while (buf.hasRemaining()) {
        final int r = channel.read(buf, pos);
        if (r < 0)
          throw new IOException("Unexpected EOF reading page " + pageNum + " from file '" + getFileName() + "'");
        pos += r;
      }
    } finally {
      channelLock.readLock().unlock();
    }
  }

  public int getPageSize() {
    return pageSize;
  }

  @Override
  protected void open(final String filePath, final MODE mode) throws FileNotFoundException {
    this.filePath = filePath;

    final int lastDotPos = filePath.lastIndexOf(".");
    String filePrefix = filePath.substring(0, lastDotPos);
    this.fileExtension = filePath.substring(lastDotPos + 1);

    final int versionPos = filePrefix.lastIndexOf(".");
    if (filePrefix.charAt(versionPos + 1) == 'v') {
      // STARTING FROM 21.10.2 COMPONENTS HAVE VERSION IN THE FILE NAME
      version = Integer.parseInt(filePrefix.substring(versionPos + 2));
      filePrefix = filePrefix.substring(0, versionPos);
    }

    final int pageSizePos = filePrefix.lastIndexOf(".");
    pageSize = Integer.parseInt(filePrefix.substring(pageSizePos + 1));
    filePrefix = filePrefix.substring(0, pageSizePos);

    final int fileIdPos = filePrefix.lastIndexOf(".");
    if (fileIdPos > -1) {
      fileId = Integer.parseInt(filePrefix.substring(fileIdPos + 1));
      final int pos = filePrefix.lastIndexOf(File.separator);
      componentName = filePrefix.substring(pos + 1, filePrefix.lastIndexOf("."));
    } else {
      fileId = -1;
      final int pos = filePrefix.lastIndexOf(File.separator);
      componentName = filePrefix.substring(pos + 1);
    }

    final int lastSlash = filePath.lastIndexOf(File.separator);
    if (lastSlash > -1)
      fileName = filePath.substring(lastSlash + 1);
    else
      fileName = filePath;

    this.osFile = new File(filePath);
    this.file = new RandomAccessFile(osFile, mode == MODE.READ_WRITE ? "rw" : "r");
    this.channel = this.file.getChannel();
    doNotCloseOnInterrupt(this.channel);
    // (RE)SEED THE IN-MEMORY PAGE COUNT FROM THE REAL LENGTH: this is the only place the file's size can change
    // without this class having written it - a fresh open, a reopen after an interrupt closed the channel, or a
    // rename. set() and not max(), so a file whose content was replaced under a rename cannot leave a stale higher
    // count behind. A partial tail left by a kill is floored away exactly as PaginatedComponent does it.
    this.totalPages = (int) (osFile.length() / pageSize);
    this.open = true;
  }

  private void doNotCloseOnInterrupt(final FileChannel fc) {
    try {
      final Field field = AbstractInterruptibleChannel.class.getDeclaredField("interruptor");
      final Class<?> interruptibleClass = field.getType();
      field.setAccessible(true);
      field.set(fc, Proxy.newProxyInstance(interruptibleClass.getClassLoader(), new Class[] { interruptibleClass },
          new InterruptibleInvocationHandler()));
    } catch (final Exception e) {
      if (!warningPrinted) {
        warningPrinted = true;
        LogManager.instance().log(this, Level.FINE, "Unable to disable channel close on interrupt: %s", e.getMessage());
      }
    }
  }
}
