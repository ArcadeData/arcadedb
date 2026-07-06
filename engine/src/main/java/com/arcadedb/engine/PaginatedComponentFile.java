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

  public void rename(final String newFileName) throws IOException {
    channelLock.writeLock().lock();
    try {
      close();
      LogManager.instance().log(this, Level.FINE, "Renaming file %s (id=%d) to %s...", null, filePath, fileId, newFileName);

      final File newFile;
      if (newFileName.contains(File.separator) || (File.separatorChar != '/' && newFileName.contains("/"))) {
        // newFileName is an absolute path (e.g., from removeTempSuffix)
        newFile = new File(newFileName);
      } else if (newFileName.contains(".") && newFileName.contains("_")) {
        // newFileName is already a complete filename, but not an absolute path
        newFile = new File(osFile.getParentFile(), newFileName);
      } else {
        // newFileName is a component name, append the suffix from original file
        final int suffixStart = osFile.getName().indexOf("_");
        if (suffixStart < 0)
          throw new IOException("Original file name '" + osFile.getName() + "' does not contain '_' to extract suffix");
        final String newFilePath = newFileName + osFile.getName().substring(suffixStart);
        newFile = new File(osFile.getParentFile(), newFilePath);
      }
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

  @Override
  public long getSize() throws IOException {
    channelLock.readLock().lock();
    try {
      return channel.size();
    } finally {
      channelLock.readLock().unlock();
    }
  }

  public long getTotalPages() throws IOException {
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
