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
import io.github.jbellis.jvector.disk.RandomAccessReader;

import java.io.*;
import java.nio.*;

/**
 * Provides a contiguous logical address space over ArcadeDB pages with headers for reading.
 * Transparently splits reads that span page boundaries, hiding the 8-byte page headers
 * from the logical address space.
 * <p>
 * This is the read counterpart to ContiguousPageWriter. Together they provide JVector
 * with a contiguous file abstraction over ArcadeDB's paged storage with headers.
 * <p>
 * Example:
 * - Logical addresses: 0, 1, 2, ..., 65527, 65528, 65529, ... (contiguous, no gaps)
 * - Physical mapping:
 *   - Logical 0-65527 → Page 0 (physical bytes 8-65535, after 8-byte header)
 *   - Logical 65528-131055 → Page 1 (physical bytes 65544-131071, after header)
 * <p>
 * Reads that span page boundaries are automatically split:
 * - readInt(position=65526) reads 2 bytes from page 0, 2 bytes from page 1
 * <p>
 * Thread-safe: Each reader should be used by a single thread (JVector creates readers per-thread).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ContiguousPageReader implements RandomAccessReader {
  private final DatabaseInternal database;
  private final int              fileId;
  private final int              pageSize;
  private final long             totalBytes;  // Total graph data size in contiguous logical space
  private final int              usablePageSize; // pageSize - BasePage.PAGE_HEADER_SIZE

  // Reusable buffer to avoid allocations (max size is 8 bytes for long/double)
  private final byte[]     buffer;
  private final ByteBuffer byteBuffer;

  // Current state
  private long     logicalPosition;  // Contiguous logical address (no gaps)
  private BasePage currentPage;
  private int      currentPageNum;

  public ContiguousPageReader(final DatabaseInternal database, final int fileId, final int pageSize, final long totalBytes) {
    this.database = database;
    this.fileId = fileId;
    this.pageSize = pageSize;
    this.totalBytes = totalBytes;
    this.usablePageSize = pageSize - BasePage.PAGE_HEADER_SIZE;
    this.logicalPosition = 0;
    this.currentPageNum = -1;

    // Allocate reusable buffer once (8 bytes for largest primitive type)
    this.buffer = new byte[Binary.LONG_SERIALIZED_SIZE];
    this.byteBuffer = ByteBuffer.wrap(buffer);
  }

  @Override
  public void seek(final long position) throws IOException {
    if (position < 0 || position > totalBytes) {
      throw new IOException("Invalid seek position: " + position + " (length=" + totalBytes + ")");
    }

    // Simply set logical position (no gaps, truly contiguous)
    this.logicalPosition = position;
  }

  @Override
  public long getPosition() {
    return logicalPosition;
  }

  @Override
  public long length() {
    return totalBytes;
  }

  @Override
  public int readInt() throws IOException {
    readFully(buffer, 0, Binary.INT_SERIALIZED_SIZE);
    byteBuffer.rewind();
    return byteBuffer.getInt();
  }

  @Override
  public float readFloat() throws IOException {
    readFully(buffer, 0, Binary.FLOAT_SERIALIZED_SIZE);
    byteBuffer.rewind();
    return byteBuffer.getFloat();
  }

  @Override
  public long readLong() throws IOException {
    readFully(buffer, 0, Binary.LONG_SERIALIZED_SIZE);
    byteBuffer.rewind();
    return byteBuffer.getLong();
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
    for (int i = 0; i < longs.length; i++) {
      longs[i] = readLong();
    }
  }

  @Override
  public void read(final int[] ints, final int offset, final int length) throws IOException {
    if (offset + length > ints.length) {
      throw new IOException(String.format(
          "Read would overflow array: offset=%d, length=%d, array.length=%d, would access index %d",
          offset, length, ints.length, offset + length - 1));
    }
    for (int i = 0; i < length; i++) {
      ints[offset + i] = readInt();
    }
  }

  @Override
  public void read(final float[] floats, final int offset, final int length) throws IOException {
    for (int i = 0; i < length; i++) {
      floats[offset + i] = readFloat();
    }
  }

  @Override
  public void close() {
    // No-op: pages are managed by PageManager
    currentPage = null;
  }

  /**
   * Reads bytes from contiguous logical address space, transparently splitting
   * reads that span page boundaries.
   */
  private void readFully(final byte[] bytes, int offset, int length) throws IOException {
    if (logicalPosition + length > totalBytes) {
      throw new IOException("Read beyond end of graph data");
    }

    // Split reads transparently across page boundaries
    while (length > 0) {
      // Map logical position to physical page (no gaps in logical space)
      final int pageNum = (int) (logicalPosition / usablePageSize);
      final int pageOffset = (int) (logicalPosition % usablePageSize);

      // How many bytes can we read from current page?
      final int availableInPage = usablePageSize - pageOffset;
      final int toRead = Math.min(length, availableInPage);

      // Ensure page is loaded
      ensurePageLoaded(pageNum);

      // Read from physical page (BasePage API handles the 8-byte header offset)
      currentPage.readByteArray(pageOffset, bytes, offset, toRead);

      // Advance logical position (truly contiguous, no gaps)
      logicalPosition += toRead;
      offset += toRead;
      length -= toRead;

      // If more data needed, next iteration reads from next page automatically
    }
  }

  /**
   * Ensures a page is loaded for reading.
   */
  private void ensurePageLoaded(final int pageNum) throws IOException {
    if (pageNum != currentPageNum) {
      final PageId pageId = new PageId(database, fileId, pageNum);
      // Use PageManager directly to avoid transaction context issues
      currentPage = database.getPageManager().getImmutablePage(pageId, pageSize, false, false);

      if (currentPage == null) {
        throw new IOException("Page not found: " + pageId);
      }

      currentPageNum = pageNum;
    }
  }
}
