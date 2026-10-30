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
import com.arcadedb.engine.CachedPage;
import com.arcadedb.engine.PageId;
import io.github.jbellis.jvector.disk.RandomAccessReader;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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
 * - Logical 0-65527 → Page 0 (physical bytes 8-65535, after 8-byte header)
 * - Logical 65528-131055 → Page 1 (physical bytes 65544-131071, after header)
 * <p>
 * Reads that span page boundaries are automatically split:
 * - readInt(position=65526) reads 2 bytes from page 0, 2 bytes from page 1
 * <p>
 * Thread-safe: Each reader should be used by a single thread (JVector creates readers per-thread).
 * <p>
 * This is the read path of every graph walk: {@code OnDiskGraphIndex.View} seeks to a node and reads its degree and its
 * neighbour ids through here for every node a search expands, and the inline vectors of a {@code storeVectorsInGraph}
 * index come the same way. A value that lies inside the page already held is therefore read straight off that page's
 * bytes, and the bulk reads ({@link #read(int[], int, int)}, {@link #read(float[], int, int)}, {@link #readFully(long[])})
 * map the position to a page once per page rather than once per element (issue #11003): the per-element path used to
 * divide the position by the usable page size twice and copy the bytes through a scratch buffer for every neighbour id of
 * every expanded node. Only a value that straddles two pages takes the byte-at-a-time path.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ContiguousPageReader implements RandomAccessReader {
  private static final VarHandle INT_BE   = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle LONG_BE  = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle FLOAT_BE = MethodHandles.byteArrayViewVarHandle(float[].class, ByteOrder.BIG_ENDIAN);

  private final DatabaseInternal database;
  private final int              fileId;
  private final int              pageSize;
  private final long             totalBytes;  // Total graph data size in contiguous logical space
  private final int              usablePageSize; // pageSize - BasePage.PAGE_HEADER_SIZE
  private final long             offset;

  // Current state
  private long   logicalPosition;  // Contiguous logical address (no gaps)
  // The page held: its bytes, where its content starts in them, and the logical range [pageStart, pageEnd) it covers,
  // cut at totalBytes. pageEnd is 0 while no page is held, so no position is ever inside it.
  private byte[] pageBytes;
  private int    pageBase;
  private long   pageStart;
  private long   pageEnd;

  public ContiguousPageReader(final DatabaseInternal database, final int fileId, final int pageSize, final long totalBytes,
      final long offset) {
    this.database = database;
    this.fileId = fileId;
    this.pageSize = pageSize;
    this.totalBytes = totalBytes;
    this.usablePageSize = pageSize - BasePage.PAGE_HEADER_SIZE;
    this.logicalPosition = 0L;
    this.offset = offset;
  }

  @Override
  public void seek(final long position) throws IOException {
    // The caller addresses the data start as logical 0; the base offset (typically 0 for
    // jvector 4.0.0-rc.7+) is applied unconditionally so the mapping stays monotonic.
    if (position < 0 || position > totalBytes - offset)
      throw new IOException("Invalid seek position: " + position + " (length=" + (totalBytes - offset) + ")");

    this.logicalPosition = position + offset;
  }

  @Override
  public long getPosition() {
    // Report the caller-space position (excluding the base offset) so seek(getPosition()) round-trips.
    return logicalPosition - offset;
  }

  @Override
  public long length() {
    return totalBytes - offset;
  }

  @Override
  public int readInt() throws IOException {
    final long position = logicalPosition;
    if (position >= pageStart && position + Integer.BYTES <= pageEnd) {
      logicalPosition = position + Integer.BYTES;
      return (int) INT_BE.get(pageBytes, pageIndex(position));
    }
    return (int) readStraddling(Integer.BYTES);
  }

  @Override
  public float readFloat() throws IOException {
    final long position = logicalPosition;
    if (position >= pageStart && position + Float.BYTES <= pageEnd) {
      logicalPosition = position + Float.BYTES;
      return (float) FLOAT_BE.get(pageBytes, pageIndex(position));
    }
    return Float.intBitsToFloat((int) readStraddling(Float.BYTES));
  }

  @Override
  public long readLong() throws IOException {
    final long position = logicalPosition;
    if (position >= pageStart && position + Long.BYTES <= pageEnd) {
      logicalPosition = position + Long.BYTES;
      return (long) LONG_BE.get(pageBytes, pageIndex(position));
    }
    return readStraddling(Long.BYTES);
  }

  @Override
  public void readFully(final byte[] bytes) throws IOException {
    readFully(bytes, 0, bytes.length);
  }

  @Override
  public void readFully(final ByteBuffer buffer) throws IOException {
    final int length = buffer.remaining();
    if (buffer.hasArray()) {
      readFully(buffer.array(), buffer.arrayOffset() + buffer.position(), length);
      buffer.position(buffer.position() + length);
      return;
    }
    final byte[] temp = new byte[length];
    readFully(temp, 0, length);
    buffer.put(temp);
  }

  @Override
  public void readFully(final long[] longs) throws IOException {
    checkReadable((long) longs.length * Long.BYTES);
    int i = 0;
    while (i < longs.length) {
      final int inPage = elementsInPage(Long.BYTES, longs.length - i);
      if (inPage == 0) {
        longs[i++] = readLong();
        continue;
      }
      int index = pageIndex(logicalPosition);
      for (final int end = i + inPage; i < end; i++, index += Long.BYTES)
        longs[i] = (long) LONG_BE.get(pageBytes, index);
      logicalPosition += (long) inPage * Long.BYTES;
    }
  }

  @Override
  public void read(final int[] ints, final int offset, final int length) throws IOException {
    if (offset + length > ints.length) {
      throw new IOException(
          String.format("Read would overflow array: offset=%d, length=%d, array.length=%d, would access index %d", offset, length,
              ints.length, offset + length - 1));
    }
    checkReadable((long) length * Integer.BYTES);
    int i = offset;
    final int last = offset + length;
    while (i < last) {
      final int inPage = elementsInPage(Integer.BYTES, last - i);
      if (inPage == 0) {
        ints[i++] = readInt();
        continue;
      }
      int index = pageIndex(logicalPosition);
      for (final int end = i + inPage; i < end; i++, index += Integer.BYTES)
        ints[i] = (int) INT_BE.get(pageBytes, index);
      logicalPosition += (long) inPage * Integer.BYTES;
    }
  }

  @Override
  public void read(final float[] floats, final int offset, final int length) throws IOException {
    checkReadable((long) length * Float.BYTES);
    int i = offset;
    final int last = offset + length;
    while (i < last) {
      final int inPage = elementsInPage(Float.BYTES, last - i);
      if (inPage == 0) {
        floats[i++] = readFloat();
        continue;
      }
      int index = pageIndex(logicalPosition);
      for (final int end = i + inPage; i < end; i++, index += Float.BYTES)
        floats[i] = (float) FLOAT_BE.get(pageBytes, index);
      logicalPosition += (long) inPage * Float.BYTES;
    }
  }

  @Override
  public void close() {
    // No-op: pages are managed by PageManager
    pageBytes = null;
    pageStart = 0L;
    pageEnd = 0L;
  }

  /**
   * Reads bytes from contiguous logical address space, transparently splitting
   * reads that span page boundaries.
   */
  private void readFully(final byte[] bytes, int offset, int length) throws IOException {
    checkReadable(length);

    // Split reads transparently across page boundaries
    while (length > 0) {
      ensurePageHolds(logicalPosition);

      // How many bytes can we read from current page?
      final int toRead = (int) Math.min(length, pageEnd - logicalPosition);

      System.arraycopy(pageBytes, pageIndex(logicalPosition), bytes, offset, toRead);

      // Advance logical position (truly contiguous, no gaps)
      logicalPosition += toRead;
      offset += toRead;
      length -= toRead;

      // If more data needed, next iteration reads from next page automatically
    }
  }

  /**
   * How many whole elements of {@code width} bytes, at most {@code wanted}, can be read from the current position without
   * leaving the page that holds it (loading that page first). 0 when the next element straddles two pages.
   */
  private int elementsInPage(final int width, final int wanted) throws IOException {
    ensurePageHolds(logicalPosition);
    return (int) Math.min(wanted, (pageEnd - logicalPosition) / width);
  }

  /** A big-endian value of {@code width} bytes that starts on one page and ends on the next, a byte at a time. */
  private long readStraddling(final int width) throws IOException {
    checkReadable(width);
    long value = 0L;
    for (int i = 0; i < width; i++) {
      ensurePageHolds(logicalPosition);
      value = (value << 8) | (pageBytes[pageIndex(logicalPosition)] & 0xFFL);
      ++logicalPosition;
    }
    return value;
  }

  private void checkReadable(final long length) throws IOException {
    if (logicalPosition + length > totalBytes)
      throw new IOException("Read beyond end of graph data");
  }

  /** Index in {@link #pageBytes} of a logical position inside the page held. */
  private int pageIndex(final long position) {
    return pageBase + (int) (position - pageStart);
  }

  /**
   * Makes the page that holds the logical {@code position} the current one. The page is read at absolute positions only, so
   * its bytes are taken from the cached page as they are, without the per-call view {@code getImmutablePage} would build.
   */
  private void ensurePageHolds(final long position) throws IOException {
    if (position >= pageStart && position < pageEnd)
      return;

    // Map logical position to physical page (no gaps in logical space)
    final int pageNum = (int) (position / usablePageSize);
    final PageId pageId = new PageId(database, fileId, pageNum);
    // Use PageManager directly to avoid transaction context issues
    final CachedPage page = database.getPageManager().getCachedPageForRead(pageId, pageSize);
    if (page == null)
      throw new IOException("Page not found: " + pageId);

    final ByteBuffer buffer = page.getByteBuffer();
    pageBytes = buffer.array();
    pageBase = buffer.arrayOffset() + BasePage.PAGE_HEADER_SIZE;
    pageStart = (long) pageNum * usablePageSize;
    // Capped at the end of the data, so a fast-path read past it falls to the checked path and fails as before
    pageEnd = Math.min(pageStart + usablePageSize, totalBytes);
  }
}
