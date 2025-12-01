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
package com.arcadedb.index.vector;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import io.github.jbellis.jvector.disk.RandomAccessWriter;

import java.io.IOException;
import java.util.zip.CRC32;

/**
 * Implements JVector's RandomAccessWriter interface for writing graph topology to ArcadeDB pages.
 * This allows persisting OnHeapGraphIndex to disk pages for later loading as OnDiskGraphIndex.
 * <p>
 * NOT thread-safe: Should be used by a single thread during graph serialization.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadePageGraphWriter implements RandomAccessWriter {
  private static final int GRAPH_PAGE_HEADER_SIZE = 16; // Reserved space per page

  private final DatabaseInternal database;
  private final int              fileId;
  private final int              pageSize;
  private final int              usablePageSize; // pageSize - header

  // Current state
  private       long        currentPosition;  // Logical position in the graph data
  private       MutablePage currentPage;
  private       int         currentPageNum;
  private final CRC32       crc32;

  public ArcadePageGraphWriter(final DatabaseInternal database, final int fileId, final int pageSize) {
    this.database = database;
    this.fileId = fileId;
    this.pageSize = pageSize;
    this.usablePageSize = pageSize - BasePage.PAGE_HEADER_SIZE - GRAPH_PAGE_HEADER_SIZE;
    this.currentPosition = 0;
    this.currentPageNum = -1;
    this.crc32 = new CRC32();
  }

  @Override
  public void seek(final long position) throws IOException {
    if (position < 0)
      throw new IOException("Invalid seek position: " + position);

    this.currentPosition = position;

    // Calculate which page contains this position
    final int pageNum = (int) (position / usablePageSize);

    // Load page if not already loaded
    ensurePageLoaded(pageNum);
  }

  @Override
  public long position() {
    return currentPosition;
  }

  @Override
  public void writeInt(final int value) throws IOException {
    ensureAvailable(4);
    final int pageOffset = (int) (currentPosition % usablePageSize);
    currentPage.writeInt(GRAPH_PAGE_HEADER_SIZE + pageOffset, value);
    currentPosition += 4;

    // Update checksum
    crc32.update((value >>> 24) & 0xFF);
    crc32.update((value >>> 16) & 0xFF);
    crc32.update((value >>> 8) & 0xFF);
    crc32.update(value & 0xFF);
  }

  @Override
  public void writeLong(final long value) throws IOException {
    ensureAvailable(8);
    final int pageOffset = (int) (currentPosition % usablePageSize);
    currentPage.writeLong(GRAPH_PAGE_HEADER_SIZE + pageOffset, value);
    currentPosition += 8;

    // Update checksum
    for (int i = 7; i >= 0; i--)
      crc32.update((int) ((value >>> (i * 8)) & 0xFF));
  }

  @Override
  public void writeFloat(final float value) throws IOException {
    writeInt(Float.floatToRawIntBits(value));
  }

  @Override
  public void writeDouble(final double value) throws IOException {
    writeLong(Double.doubleToRawLongBits(value));
  }

  @Override
  public void write(final int b) throws IOException {
    ensureAvailable(1);
    final int pageOffset = (int) (currentPosition % usablePageSize);
    currentPage.writeByte(GRAPH_PAGE_HEADER_SIZE + pageOffset, (byte) b);
    currentPosition += 1;
    crc32.update(b);
  }

  @Override
  public void write(final byte[] bytes) throws IOException {
    write(bytes, 0, bytes.length);
  }

  @Override
  public void write(final byte[] bytes, int offset, int length) throws IOException {
    while (length > 0) {
      final int pageOffset = (int) (currentPosition % usablePageSize);
      final int availableInPage = usablePageSize - pageOffset;
      final int toWrite = Math.min(length, availableInPage);

      // Ensure current page is loaded
      final int pageNum = (int) (currentPosition / usablePageSize);
      ensurePageLoaded(pageNum);

      // Write to current page
      final int pageDataOffset = GRAPH_PAGE_HEADER_SIZE + pageOffset;
      currentPage.writeByteArray(pageDataOffset, bytes, offset, toWrite);

      // Update checksum
      crc32.update(bytes, offset, toWrite);

      offset += toWrite;
      currentPosition += toWrite;
      length -= toWrite;

      // If we need to write more data, it will go to the next page
      if (length > 0)
        currentPageNum = -1; // Force new page on next write
    }
  }

  @Override
  public void writeShort(final int value) throws IOException {
    ensureAvailable(2);
    final int pageOffset = (int) (currentPosition % usablePageSize);
    currentPage.writeShort(GRAPH_PAGE_HEADER_SIZE + pageOffset, (short) value);
    currentPosition += 2;

    crc32.update((value >>> 8) & 0xFF);
    crc32.update(value & 0xFF);
  }

  @Override
  public void writeChar(final int value) throws IOException {
    writeShort(value);
  }

  @Override
  public void writeByte(final int value) throws IOException {
    write(value);
  }

  @Override
  public void writeBoolean(final boolean value) throws IOException {
    write(value ? 1 : 0);
  }

  @Override
  public void writeBytes(final String s) throws IOException {
    final int len = s.length();
    for (int i = 0; i < len; i++)
      write((byte) s.charAt(i));
  }

  @Override
  public void writeChars(final String s) throws IOException {
    final int len = s.length();
    for (int i = 0; i < len; i++)
      writeChar(s.charAt(i));
  }

  @Override
  public void writeUTF(final String s) throws IOException {
    throw new UnsupportedOperationException("writeUTF not implemented");
  }

  @Override
  public void flush() {
    // No-op: pages are automatically flushed by PageManager
  }

  @Override
  public long checksum(final long start, final long end) throws IOException {
    // Return current checksum value
    // Note: This is a simplified implementation - ideally would compute checksum for specific range
    return crc32.getValue();
  }

  @Override
  public void close() {
    // No-op: pages are managed by PageManager
    currentPage = null;
  }

  private void ensurePageLoaded(final int pageNum) throws IOException {
    if (pageNum != currentPageNum) {
      final PageId pageId = new PageId(database, fileId, pageNum);

      // Try to get existing page, or create new one
      try {
        final var existingPage = database.getTransaction().getPage(pageId, pageSize);
        currentPage = database.getTransaction().getPageToModify(existingPage);
      } catch (final Exception e) {
        // Page doesn't exist, create new one
        currentPage = database.getTransaction().addPage(pageId, pageSize);
      }

      if (currentPage == null)
        throw new IOException("Failed to get/create page: " + pageId);

      currentPageNum = pageNum;

      // Initialize page header if this is a new page
      if (currentPage.readInt(0) == 0) {
        currentPage.writeInt(0, pageNum);    // Page number
        currentPage.writeLong(4, 0L);         // Reserved
        currentPage.writeInt(12, 0);          // Reserved
      }
    }
  }

  private void ensureAvailable(final int bytes) throws IOException {
    // Check if write would cross page boundary
    final int pageOffset = (int) (currentPosition % usablePageSize);
    if (pageOffset + bytes > usablePageSize) {
      // Write crosses page boundary - need to handle manually
      throw new IOException("Primitive write crossing page boundary not supported");
    }

    // Ensure page is loaded
    final int pageNum = (int) (currentPosition / usablePageSize);
    ensurePageLoaded(pageNum);
  }
}
