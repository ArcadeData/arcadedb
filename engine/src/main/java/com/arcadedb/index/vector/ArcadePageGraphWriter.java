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

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.log.LogManager;
import io.github.jbellis.jvector.disk.RandomAccessWriter;

import java.io.*;
import java.util.logging.*;
import java.util.zip.*;

/**
 * Implements JVector's RandomAccessWriter interface for writing graph topology to ArcadeDB pages.
 * This allows persisting OnHeapGraphIndex to disk pages for later loading as OnDiskGraphIndex.
 * <p>
 * NOT thread-safe: Should be used by a single thread during graph serialization.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadePageGraphWriter implements RandomAccessWriter {
  private final DatabaseInternal database;
  private final int              fileId;
  private final int              pageSize;
  private final int              usablePageSize; // pageSize - BasePage.PAGE_HEADER_SIZE

  // Current state
  private       long        currentPosition;  // Logical position in the graph data
  private       MutablePage currentPage;
  private       int         currentPageNum;
  private final CRC32       crc32;

  public ArcadePageGraphWriter(final DatabaseInternal database, final int fileId, final int pageSize) {
    this.database = database;
    this.fileId = fileId;
    this.pageSize = pageSize;
    this.usablePageSize = pageSize - BasePage.PAGE_HEADER_SIZE;
    this.currentPosition = 0;
    this.currentPageNum = -1;
    this.crc32 = new CRC32();
  }

  @Override
  public void seek(final long position) throws IOException {
    if (position < 0)
      throw new IOException("Invalid seek position: " + position);

    this.currentPosition = position;

    int pageNum = (int) (position / pageSize);
    final int pageOffset = (int) (position % pageSize);

    if (pageOffset >= usablePageSize) {
      ++pageNum;
      currentPosition = (long) pageNum * pageSize;
    }

    // Load page if not already loaded
    ensurePageLoaded(pageNum);
  }

  @Override
  public long position() {
    return currentPosition;
  }

  @Override
  public void writeInt(final int value) throws IOException {
    final int pageOffset = ensureAvailable(Binary.INT_SERIALIZED_SIZE);

    currentPage.writeInt(pageOffset, value);
    LogManager.instance()
        .log(this, Level.SEVERE, "Wrote int %d at page %d offset %d", value, currentPage.getPageId().getPageNumber(), pageOffset);

    updatePosition(Binary.INT_SERIALIZED_SIZE);

    // Update checksum
    crc32.update((value >>> 24) & 0xFF);
    crc32.update((value >>> 16) & 0xFF);
    crc32.update((value >>> 8) & 0xFF);
    crc32.update(value & 0xFF);
  }

  @Override
  public void writeLong(final long value) throws IOException {
    final int pageOffset = ensureAvailable(Binary.LONG_SERIALIZED_SIZE);

    currentPage.writeLong(pageOffset, value);

    LogManager.instance()
        .log(this, Level.SEVERE, "Wrote long %d at page %d offset %d", value, currentPage.getPageId().getPageNumber(), pageOffset);

    updatePosition(Binary.LONG_SERIALIZED_SIZE);

    // Update checksum
    for (int i = 7; i >= 0; i--)
      crc32.update((int) ((value >>> (i * 8)) & 0xFF));
  }

  @Override
  public void writeShort(final int value) throws IOException {
    final int pageOffset = ensureAvailable(Binary.SHORT_SERIALIZED_SIZE);

    currentPage.writeShort(pageOffset, (short) value);
    LogManager.instance()
        .log(this, Level.SEVERE, "Wrote short %d at page %d offset %d", value, currentPage.getPageId().getPageNumber(), pageOffset);
    updatePosition(Binary.SHORT_SERIALIZED_SIZE);

    crc32.update((value >>> 8) & 0xFF);
    crc32.update(value & 0xFF);
  }

  @Override
  public void writeFloat(final float value) throws IOException {
    final int pageOffset = ensureAvailable(Binary.FLOAT_SERIALIZED_SIZE);

    currentPage.writeFloat(pageOffset, value);
    LogManager.instance()
        .log(this, Level.SEVERE, "Wrote float %d at page %d offset %d", value, currentPage.getPageId().getPageNumber(), pageOffset);

    updatePosition(Binary.FLOAT_SERIALIZED_SIZE);

    // Update checksum
    final int intBits = Float.floatToRawIntBits(value);
    crc32.update((intBits >>> 24) & 0xFF);
    crc32.update((intBits >>> 16) & 0xFF);
    crc32.update((intBits >>> 8) & 0xFF);
    crc32.update(intBits & 0xFF);
  }

  @Override
  public void writeDouble(final double value) throws IOException {
    final int pageOffset = ensureAvailable(Binary.DOUBLE_SERIALIZED_SIZE);

    currentPage.writeDouble(pageOffset, value);
    LogManager.instance()
        .log(this, Level.SEVERE, "Wrote double %d at page %d offset %d", value, currentPage.getPageId().getPageNumber(),
            pageOffset);

    updatePosition(Binary.DOUBLE_SERIALIZED_SIZE);

    // Update checksum
    final long longBits = Double.doubleToRawLongBits(value);
    for (int i = 7; i >= 0; i--)
      crc32.update((int) ((longBits >>> (i * 8)) & 0xFF));
  }

  @Override
  public void write(final int b) throws IOException {
    final int pageOffset = ensureAvailable(Binary.BYTE_SERIALIZED_SIZE);

    currentPage.writeByte(pageOffset, (byte) b);
    LogManager.instance()
        .log(this, Level.SEVERE, "Wrote byte %d at page %d offset %d", b, currentPage.getPageId().getPageNumber(), pageOffset);

    updatePosition(Binary.BYTE_SERIALIZED_SIZE);
    crc32.update(b);
  }

  @Override
  public void write(final byte[] bytes) throws IOException {
    write(bytes, 0, bytes.length);
  }

  @Override
  public void write(final byte[] bytes, int offset, int length) throws IOException {
    LogManager.instance()
        .log(this, Level.SEVERE, "Wrote bytes %d at page %d offset %d", bytes.length, currentPage.getPageId().getPageNumber(),
            offset);

    while (length > 0) {
      int pageNum = (int) (currentPosition / usablePageSize);
      int pageOffset = (int) (currentPosition % pageSize);

      final int availableInPage = usablePageSize - pageOffset;
      if (availableInPage <= 0) {
        // MOVE TO THE NEXT PAGE
        ++pageNum;
        pageOffset = 0;
        currentPosition = (long) pageNum * pageSize;
      }

      final int toWrite = Math.min(length, availableInPage);

      ensurePageLoaded(pageNum);

      // Write to current page
      currentPage.writeByteArray(pageOffset, bytes, offset, toWrite);

      // Update checksum
      crc32.update(bytes, offset, toWrite);

      offset += toWrite;
      updatePosition(toWrite);
      length -= toWrite;

      // If we need to write more data, it will go to the next page
      if (length > 0)
        currentPageNum = -1; // Force new page on next write
    }
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
    }
  }

  private int ensureAvailable(final int bytes) throws IOException {
    int pageNum = (int) (currentPosition / pageSize);
    int pageOffset = (int) (currentPosition % pageSize);

    if (pageOffset + bytes > usablePageSize) {
      ++pageNum;
      currentPosition = (long) pageNum * pageSize;
      pageOffset = 0;
    }

    ensurePageLoaded(pageNum);
    return pageOffset;
  }

  private void updatePosition(final int bytesRead) throws IOException {
    int pageNum = (int) (currentPosition / pageSize);
    final int pageOffset = (int) (currentPosition % pageSize);

    currentPosition += bytesRead;

    if (pageOffset + bytesRead >= usablePageSize) {
      // MOVE TO THE NEXT PAGE
      ++pageNum;
      ensurePageLoaded(pageNum);
      currentPosition = (long) pageNum * pageSize;
    }
  }
}
