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

import java.io.*;
import java.lang.reflect.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.channels.spi.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.logging.*;
import java.util.zip.*;

public class PaginatedComponentFile extends ComponentFile {

  private                 RandomAccessFile file;
  private                 FileChannel      channel;
  private                 int              pageSize;
  private static volatile boolean          warningPrinted = false;

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

  @Override
  public void close() {
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
    }
    this.open = false;
  }

  public void rename(final String newFileName) throws IOException {
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
      Files.move(osFile.getAbsoluteFile().toPath(),
          newFile.getAbsoluteFile().toPath(),
          StandardCopyOption.ATOMIC_MOVE);
      open(newFile.getAbsolutePath(), mode);
    } catch (Exception e) {
      open(filePath, mode);
      throw new IOException("Error renaming file " + filePath + " to " + newFile.getAbsolutePath(), e);
    }
  }

  @Override
  public long getSize() throws IOException {
    return channel.size();
  }

  public long getTotalPages() throws IOException {
    return channel.size() / pageSize;
  }

  public long calculateChecksum() throws IOException {
    final CRC32 crc = new CRC32();

    final ByteBuffer buffer = ByteBuffer.allocate(getPageSize());

    final long totalPages = getTotalPages();
    for (int i = 0; i < totalPages; i++) {
      buffer.clear();
      channel.read(buffer, pageSize * (long) i);

      buffer.rewind();
      for (int j = 0; j < pageSize; j++) {
        final int read = buffer.get(j);
        crc.update(read);
      }
    }

    return crc.getValue();
  }

  /**
   * Returns the byte written. Current implementation flushes always the entire page because (1) there is not a sensible increase of
   * performance and (2) in case a page is modified multiple times before the flush now it's overwritten in the writeCache map.
   */
  public int write(final MutablePage page) throws IOException {
    final int pageNumber = page.pageId.getPageNumber();
    if (pageNumber < 0)
      throw new IllegalArgumentException("Invalid page number to write: " + pageNumber);

    assert page.pageId.getFileId() == fileId;
    final ByteBuffer buffer = page.getContent();

    // NO NEED TO SYNCHRONIZE THE BUFFER BECAUSE MUTABLE PAGES ARE NOT SHARED
    buffer.rewind();
    try {
      channel.write(buffer, (page.getPhysicalSize() * (long) pageNumber));
    } catch (final ClosedChannelException e) {
      LogManager.instance().log(this, Level.SEVERE, "File '%s' was closed on write. Reopen it and retry...", null, fileName);
      open(filePath, mode);
      buffer.rewind();
      final int written = channel.write(buffer, (page.getPhysicalSize() * (long) pageNumber));
      if (written < pageSize)
        LogManager.instance().log(this, Level.WARNING, "Written less bytes than expected: %d < %d", null, written, pageSize);
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
    if (page.getPageId().getPageNumber() < 0)
      throw new IllegalArgumentException("Invalid page number to read: " + page.getPageId().getPageNumber());

    assert page.getPageId().getFileId() == fileId;
    final ByteBuffer buffer = page.getByteBuffer();

    try {
      channel.read(buffer, page.getPhysicalSize() * (long) page.getPageId().getPageNumber());
    } catch (final ClosedChannelException e) {
      LogManager.instance().log(this, Level.SEVERE, "File '%s' was closed on read. Reopen it and retry...", null, fileName);
      open(filePath, mode);
      channel.read(buffer, page.getPhysicalSize() * (long) page.getPageId().getPageNumber());
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
      field.set(fc, Proxy.newProxyInstance(
          interruptibleClass.getClassLoader(),
          new Class[] { interruptibleClass },
          new InterruptibleInvocationHandler()));
    } catch (final Exception e) {
      if (!warningPrinted) {
        warningPrinted = true;
        LogManager.instance().log(this, Level.WARNING, "Unable to disable channel close on interrupt: %s", e.getMessage());
      }
    }
  }
}
