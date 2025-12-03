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
import io.github.jbellis.jvector.disk.IndexWriter;

import java.io.*;
import java.nio.*;

import static java.util.logging.Level.*;

/**
 * Provides a contiguous logical address space over ArcadeDB pages with headers.
 * Transparently splits writes that span page boundaries, hiding the 8-byte page headers
 * from the logical address space.
 * <p>
 * This solves the fundamental issue where JVector assumes a contiguous file layout with
 * no gaps, but ArcadeDB pages have headers. Without this wrapper, JVector's offset
 * calculations (e.g., baseNodeOffsetFor) would be incorrect.
 * <p>
 * Example:
 * - Logical addresses: 0, 1, 2, ..., 65527, 65528, 65529, ... (contiguous, no gaps)
 * - Physical mapping:
 * - Logical 0-65527 → Page 0 (physical bytes 8-65535, after 8-byte header)
 * - Logical 65528-131055 → Page 1 (physical bytes 65544-131071, after header)
 * <p>
 * Writes that span page boundaries are automatically split:
 * - writeInt(position=65526, value=X) writes 2 bytes to page 0, 2 bytes to page 1
 * <p>
 * NOT thread-safe: Should be used by a single thread during graph serialization.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ContiguousPageWriter implements IndexWriter {
  private final DatabaseInternal database;
  private final int              fileId;
  private final int              pageSize;
  private final int              usablePageSize; // pageSize - BasePage.PAGE_HEADER_SIZE

  // Reusable buffer to avoid allocations (max size is 8 bytes for long/double)
  private final byte[]     buffer;
  private final ByteBuffer byteBuffer;

  // Current state
  private long        logicalPosition;  // Contiguous logical address (no gaps)
  private MutablePage currentPage;
  private int         currentPageNum;

  public ContiguousPageWriter(final DatabaseInternal database, final int fileId, final int pageSize) {
    this.database = database;
    this.fileId = fileId;
    this.pageSize = pageSize;
    this.usablePageSize = pageSize - BasePage.PAGE_HEADER_SIZE;
    this.logicalPosition = 0;
    this.currentPageNum = -1;

    // Allocate reusable buffer once (8 bytes for largest primitive type)
    this.buffer = new byte[Binary.LONG_SERIALIZED_SIZE];
    this.byteBuffer = ByteBuffer.wrap(buffer);
  }

  @Override
  public long position() {
    return logicalPosition;
  }

  @Override
  public void writeInt(final int value) throws IOException {
    //LogManager.instance().log(this, SEVERE, "Writing int value %d at logical position %d", value, logicalPosition);

    byteBuffer.rewind();
    byteBuffer.putInt(value);
    write(buffer, 0, Binary.INT_SERIALIZED_SIZE);
  }

  @Override
  public void writeLong(final long value) throws IOException {
    byteBuffer.rewind();
    byteBuffer.putLong(value);
    write(buffer, 0, Binary.LONG_SERIALIZED_SIZE);
  }

  @Override
  public void writeShort(final int value) throws IOException {
    byteBuffer.rewind();
    byteBuffer.putShort((short) value);
    write(buffer, 0, Binary.SHORT_SERIALIZED_SIZE);
  }

  @Override
  public void writeFloat(final float value) throws IOException {
// @TODO AVOID WRITING FLOATS FOR NOW, BYPASSING JVECTOR WRITING OF VECTORS
//    byteBuffer.rewind();
//    byteBuffer.putFloat(value);
//    write(buffer, 0, Binary.FLOAT_SERIALIZED_SIZE);
  }

  @Override
  public void writeDouble(final double value) throws IOException {
    byteBuffer.rewind();
    byteBuffer.putDouble(value);
    write(buffer, 0, Binary.DOUBLE_SERIALIZED_SIZE);
  }

  @Override
  public void write(final int b) throws IOException {
    buffer[0] = (byte) b;
    write(buffer, 0, 1);
  }

  @Override
  public void write(final byte[] bytes) throws IOException {
    write(bytes, 0, bytes.length);
  }

  @Override
  public void write(final byte[] bytes, int offset, int length) throws IOException {
    // Split writes transparently across page boundaries
    while (length > 0) {
      // Map logical position to physical page (no gaps in logical space)
      final int pageNum = (int) (logicalPosition / usablePageSize);
      final int pageOffset = (int) (logicalPosition % usablePageSize);

      // How many bytes can fit in current page?
      final int availableInPage = usablePageSize - pageOffset;
      final int toWrite = Math.min(length, availableInPage);

//      LogManager.instance().log(this, SEVERE, "Writing %d bytes at logical position %d (page %d, offset %d)",
//          toWrite, logicalPosition, pageNum, pageOffset);
//
      // Ensure page is loaded
      ensurePageLoaded(pageNum);

      // Write to physical page (BasePage API handles the 8-byte header offset)
      currentPage.writeByteArray(pageOffset, bytes, offset, toWrite);

      // Advance logical position (truly contiguous, no gaps)
      logicalPosition += toWrite;
      offset += toWrite;
      length -= toWrite;

      // If more data remains, next iteration writes to next page automatically
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
    for (int i = 0; i < len; i++) {
      write((byte) s.charAt(i));
    }
  }

  @Override
  public void writeChars(final String s) throws IOException {
    final int len = s.length();
    for (int i = 0; i < len; i++) {
      writeChar(s.charAt(i));
    }
  }

  @Override
  public void writeUTF(final String s) throws IOException {
    throw new UnsupportedOperationException("writeUTF not implemented");
  }

  public void flush() {
    // No-op: pages are automatically flushed by PageManager
  }

  @Override
  public void close() {
    // No-op: pages are managed by PageManager
    currentPage = null;
  }

  /**
   * Ensures a page is loaded for writing. Creates new pages as needed.
   */
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

      if (currentPage == null) {
        throw new IOException("Failed to get/create page: " + pageId);
      }

      currentPageNum = pageNum;
    }
  }
}
