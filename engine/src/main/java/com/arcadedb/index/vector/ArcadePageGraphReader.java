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
import com.arcadedb.engine.PageId;
import com.arcadedb.log.LogManager;
import io.github.jbellis.jvector.disk.RandomAccessReader;

import java.io.*;
import java.nio.*;
import java.util.logging.*;

/**
 * Implements JVector's RandomAccessReader interface for reading graph topology from ArcadeDB pages.
 * This allows OnDiskGraphIndex to lazy-load graph data from disk pages on-demand.
 * <p>
 * Thread-safe: Each reader should be used by a single thread (JVector creates readers per thread).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadePageGraphReader implements RandomAccessReader {
  private final DatabaseInternal database;
  private final int              fileId;
  private final int              pageSize;
  private final long             totalBytes;  // Total graph data size
  private final int              usablePageSize; // pageSize - BasePage.PAGE_HEADER_SIZE

  // Current state
  private long     currentPosition;  // Logical position in the graph data
  private BasePage currentPage;
  private int      currentPageNum;

  public ArcadePageGraphReader(final DatabaseInternal database, final int fileId, final int pageSize, final long totalBytes) {
    this.database = database;
    this.fileId = fileId;
    this.pageSize = pageSize;
    this.totalBytes = totalBytes;
    this.usablePageSize = pageSize - BasePage.PAGE_HEADER_SIZE;
    this.currentPosition = 0;
    this.currentPageNum = -1;
  }

  @Override
  public void seek(final long position) throws IOException {
    if (position < 0 || position > totalBytes)
      throw new IOException("Invalid seek position: " + position + " (length=" + totalBytes + ")");

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
  public long getPosition() {
    return currentPosition;
  }

  @Override
  public long length() {
    return totalBytes;
  }

  @Override
  public int readInt() throws IOException {
    ensureAvailable(Binary.INT_SERIALIZED_SIZE);

    final int pageOffset = (int) (currentPosition % pageSize);

    final int value = currentPage.readInt(pageOffset);

    LogManager.instance()
        .log(this, Level.SEVERE, "Read int %d at page %d offset %d", value, currentPage.getPageId().getPageNumber(), pageOffset);

    updatePosition(Binary.INT_SERIALIZED_SIZE);
    return value;
  }

  @Override
  public float readFloat() throws IOException {
    ensureAvailable(Binary.FLOAT_SERIALIZED_SIZE);

    final int pageOffset = (int) (currentPosition % pageSize);

    final float value = currentPage.readFloat(pageOffset);

    LogManager.instance()
        .log(this, Level.SEVERE, "Read int %d at page %d offset %d", value, currentPage.getPageId().getPageNumber(), pageOffset);

    updatePosition(Binary.FLOAT_SERIALIZED_SIZE);
    return value;
  }

  @Override
  public long readLong() throws IOException {
    ensureAvailable(Binary.LONG_SERIALIZED_SIZE);

    final int pageOffset = (int) (currentPosition % pageSize);

    final long value = currentPage.readLong(pageOffset);

    LogManager.instance()
        .log(this, Level.SEVERE, "Read long %d at page %d offset %d", value, currentPage.getPageId().getPageNumber(), pageOffset);

    updatePosition(Binary.LONG_SERIALIZED_SIZE);
    return value;
  }

  @Override
  public void readFully(final byte[] bytes) throws IOException {
    readFully(bytes, 0, bytes.length);
  }

  @Override
  public void readFully(final ByteBuffer buffer) throws IOException {
    final int length = buffer.remaining();
    final byte[] temp = new byte[length];
    readFully(temp, 0, length);
    buffer.put(temp);
  }

  @Override
  public void readFully(final long[] longs) throws IOException {
    for (int i = 0; i < longs.length; i++)
      longs[i] = readLong();
  }

  @Override
  public void read(final int[] ints, final int offset, final int length) throws IOException {
    for (int i = 0; i < length; i++)
      ints[offset + i] = readInt();
  }

  @Override
  public void read(final float[] floats, final int offset, final int length) throws IOException {
    for (int i = 0; i < length; i++)
      floats[offset + i] = readFloat();
  }

  private void readFully(final byte[] bytes, final int offset, int length) throws IOException {
    LogManager.instance()
        .log(this, Level.SEVERE, "Read byte array of length %d at position %d", length, currentPosition);

    int currentOffset = offset;
    while (length > 0) {
      final int pageNum = (int) (currentPosition / pageSize);
      final int pageOffset = (int) (currentPosition % pageSize);

      final int availableInPage = usablePageSize - pageOffset;
      final int toRead = Math.min(length, availableInPage);

      ensurePageLoaded(pageNum);

      // Read from current page
      currentPage.readByteArray(pageOffset, bytes, currentOffset, toRead);

      currentOffset += toRead;
      updatePosition(toRead);
      length -= toRead;

      // If we need more data, it will be on the next page
      if (length > 0)
        currentPageNum = -1; // Force reload on next read
    }
  }

  private void ensurePageLoaded(final int pageNum) throws IOException {
    if (pageNum != currentPageNum) {
      final PageId pageId = new PageId(database, fileId, pageNum);
      // Use PageManager directly to avoid transaction context issues
      currentPage = database.getPageManager().getImmutablePage(pageId, pageSize, false, false);

      if (currentPage == null)
        throw new IOException("Page not found: " + pageId);

      currentPageNum = pageNum;
    }
  }

  private void ensureAvailable(final int bytes) throws IOException {
    if (currentPosition + bytes > totalBytes)
      throw new IOException("Read beyond end of graph data");

    int pageNum = (int) (currentPosition / pageSize);
    final int pageOffset = (int) (currentPosition % pageSize);

    if (pageOffset + bytes > usablePageSize) {
      ++pageNum;
      currentPosition = (long) pageNum * pageSize;
    }
    ensurePageLoaded(pageNum);
  }

  @Override
  public void close() {
    // No-op: pages are managed by PageManager
    currentPage = null;
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
