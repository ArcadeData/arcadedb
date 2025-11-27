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
import com.arcadedb.engine.PageId;
import io.github.jbellis.jvector.disk.RandomAccessReader;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Implements JVector's RandomAccessReader interface for reading graph topology from ArcadeDB pages.
 * This allows OnDiskGraphIndex to lazy-load graph data from disk pages on-demand.
 *
 * Thread-safe: Each reader should be used by a single thread (JVector creates readers per thread).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadePageGraphReader implements RandomAccessReader {
  private static final int GRAPH_PAGE_HEADER_SIZE = 16; // Reserved space per page

  private final DatabaseInternal database;
  private final int              fileId;
  private final int              pageSize;
  private final long             totalBytes;  // Total graph data size
  private final int              usablePageSize; // pageSize - header

  // Current state
  private long     currentPosition;  // Logical position in the graph data
  private BasePage currentPage;
  private int      currentPageNum;

  public ArcadePageGraphReader(final DatabaseInternal database, final int fileId,
                                final int pageSize, final long totalBytes) {
    this.database = database;
    this.fileId = fileId;
    this.pageSize = pageSize;
    this.totalBytes = totalBytes;
    this.usablePageSize = pageSize - GRAPH_PAGE_HEADER_SIZE;
    this.currentPosition = 0;
    this.currentPageNum = -1;
  }

  @Override
  public void seek(final long position) throws IOException {
    if (position < 0 || position > totalBytes)
      throw new IOException("Invalid seek position: " + position + " (length=" + totalBytes + ")");

    this.currentPosition = position;

    // Calculate which page contains this position
    final int pageNum = (int) (position / usablePageSize);

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
    ensureAvailable(4);
    final int pageOffset = (int) (currentPosition % usablePageSize);
    final int value = currentPage.readInt(GRAPH_PAGE_HEADER_SIZE + pageOffset);
    currentPosition += 4;
    return value;
  }

  @Override
  public float readFloat() throws IOException {
    ensureAvailable(4);
    final int pageOffset = (int) (currentPosition % usablePageSize);
    final float value = currentPage.readFloat(GRAPH_PAGE_HEADER_SIZE + pageOffset);
    currentPosition += 4;
    return value;
  }

  @Override
  public long readLong() throws IOException {
    ensureAvailable(8);
    final int pageOffset = (int) (currentPosition % usablePageSize);
    final long value = currentPage.readLong(GRAPH_PAGE_HEADER_SIZE + pageOffset);
    currentPosition += 8;
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
    int currentOffset = offset;
    while (length > 0) {
      final int pageOffset = (int) (currentPosition % usablePageSize);
      final int availableInPage = usablePageSize - pageOffset;
      final int toRead = Math.min(length, availableInPage);

      // Ensure current page is loaded
      final int pageNum = (int) (currentPosition / usablePageSize);
      ensurePageLoaded(pageNum);

      // Read from current page
      final int pageDataOffset = GRAPH_PAGE_HEADER_SIZE + pageOffset;
      currentPage.readByteArray(pageDataOffset, bytes, currentOffset, toRead);

      currentOffset += toRead;
      currentPosition += toRead;
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

    // Check if read would cross page boundary
    final int pageOffset = (int) (currentPosition % usablePageSize);
    if (pageOffset + bytes > usablePageSize) {
      // Read crosses page boundary - need to handle manually
      throw new IOException("Primitive read crossing page boundary not supported - use readFully");
    }
  }

  @Override
  public void close() {
    // No-op: pages are managed by PageManager
    currentPage = null;
  }
}
