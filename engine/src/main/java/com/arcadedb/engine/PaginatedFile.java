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
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.zip.CRC32;

public class PaginatedFile {
  public enum MODE {
    READ_ONLY, READ_WRITE
  }

  private       RandomAccessFile file;
  private final MODE             mode;
  private       String           filePath;
  private       String           fileName;
  private       File             osFile;
  private       FileChannel      channel;
  private       int              fileId;
  private       int              pageSize;
  private       int              version = 0;      // STARTING FROM 21.10.2 COMPONENTS HAVE VERSION IN THE FILE NAME
  private       String           componentName;
  private       String           fileExtension;
  private       boolean          open;

  public PaginatedFile() {
    this.mode = MODE.READ_ONLY;
  }

  protected PaginatedFile(final String filePath, final MODE mode) throws FileNotFoundException {
    this.mode = mode;
    open(filePath, mode);
  }

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

    } catch (IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on closing file %s (id=%d)", e, filePath, fileId);
    }
    this.open = false;
  }

  public void rename(final String newFileName) throws IOException {
    close();
    LogManager.instance().log(this, Level.FINE, "Renaming file %s (id=%d) to %s...", null, filePath, fileId, newFileName);
    final File newFile = new File(newFileName);
    new File(filePath).renameTo(newFile);
    open(newFile.getAbsolutePath(), mode);
  }

  public void drop() throws IOException {
    close();
    LogManager.instance().log(this, Level.FINE, "Deleting file %s (id=%d) to %s...", null, filePath, fileId);
    Files.delete(Paths.get(getFilePath()));
  }

  public long getSize() throws IOException {
    return channel.size();
  }

  public long getTotalPages() throws IOException {
    return channel.size() / pageSize;
  }

  public void flush() throws IOException {
    channel.force(true);
  }

  public String getFileName() {
    return fileName;
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
    if (page.pageId.getPageNumber() < 0)
      throw new IllegalArgumentException("Invalid page number to write: " + page.pageId.getPageNumber());

    assert page.getPageId().getFileId() == fileId;
    final ByteBuffer buffer = page.getContent();

    buffer.rewind();
    try {
      channel.write(buffer, (page.getPhysicalSize() * (long) page.getPageId().getPageNumber()));
    } catch (ClosedChannelException e) {
      LogManager.instance().log(this, Level.SEVERE, "File '%s' was closed on write. Reopen it and retry...", null, fileName);
      open(filePath, mode);
      channel.write(buffer, (page.getPhysicalSize() * (long) page.getPageId().getPageNumber()));
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

  public void read(final ImmutablePage page) throws IOException {
    if (page.pageId.getPageNumber() < 0)
      throw new IllegalArgumentException("Invalid page number to read: " + page.pageId.getPageNumber());

    assert page.getPageId().getFileId() == fileId;
    final ByteBuffer buffer = page.getContent();
    buffer.clear();

    try {
      channel.read(buffer, page.getPhysicalSize() * (long) page.getPageId().getPageNumber());
    } catch (ClosedChannelException e) {
      LogManager.instance().log(this, Level.SEVERE, "File '%s' was closed on read. Reopen it and retry...", null, fileName);
      open(filePath, mode);
      channel.read(buffer, page.getPhysicalSize() * (long) page.getPageId().getPageNumber());
    }
  }

  public boolean isOpen() {
    return open;
  }

  public String getFilePath() {
    return filePath;
  }

  public String getComponentName() {
    return componentName;
  }

  public String getFileExtension() {
    return fileExtension;
  }

  public int getFileId() {
    return fileId;
  }

  public File getOSFile() {
    return osFile;
  }

  public void setFileId(final int fileId) {
    this.fileId = fileId;
  }

  public int getPageSize() {
    return pageSize;
  }

  public int getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return filePath;
  }

  private void open(final String filePath, final MODE mode) throws FileNotFoundException {
    this.filePath = filePath;

    String filePrefix = filePath.substring(0, filePath.lastIndexOf("."));
    this.fileExtension = filePath.substring(filePath.lastIndexOf(".") + 1);

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
      int pos = filePrefix.lastIndexOf(File.separator);
      componentName = filePrefix.substring(pos + 1, filePrefix.lastIndexOf("."));
    } else {
      fileId = -1;
      int pos = filePrefix.lastIndexOf(File.separator);
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
    this.open = true;
  }
}
