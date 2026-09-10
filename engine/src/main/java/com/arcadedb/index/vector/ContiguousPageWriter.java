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
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.Component;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.log.LogManager;
import io.github.jbellis.jvector.disk.IndexWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;

import static java.util.logging.Level.FINE;

/**
 * Callback interface for handling chunk completion during large writes.
 * Allows callers to commit transactions periodically to avoid exceeding memory or size limits.
 */
@FunctionalInterface
interface ChunkCommitCallback {
  /**
   * Called when a chunk of data has been written and should be committed.
   *
   * @param bytesWritten Total bytes written in the current chunk
   * @throws IOException if commit operation fails
   */
  void onChunkComplete(long bytesWritten) throws IOException;
}

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

  /**
   * Pages the file already held when this writer was created. A page number below it addresses a page that exists,
   * whatever this writer does; one at or above it is an append. See {@link #ensurePageLoaded(int)}.
   */
  private final int existingPages;

  /**
   * Highest page number this writer has already acquired, across chunk commits. Writing is strictly sequential
   * from logical position 0, so the first time a page number is seen it is the one being appended - and only that
   * first sight may register it as new. Needed separately from {@code currentPageNum} because a chunk commit
   * resets that to -1 and the next write re-acquires the same, now committed, page.
   */
  private int highestPageAcquired = -1;

  // Chunking support (for large bulk writes with WAL disabled)
  private long                 totalBytesWritten;   // Track bytes written in current chunk
  private final long           chunkSizeBytes;      // Chunk size in bytes (0 = no chunking)
  private final ChunkCommitCallback chunkCallback;  // Callback to commit chunks

  /**
   * Constructor without chunking support (backward compatibility).
   */
  public ContiguousPageWriter(final DatabaseInternal database, final int fileId, final int pageSize) {
    this(database, fileId, pageSize, 0, null);
  }

  /**
   * Constructor with chunking support for bulk writes.
   *
   * @param database       Database instance
   * @param fileId         File ID for page allocation
   * @param pageSize       Page size in bytes
   * @param chunkSizeMB    Chunk size in MB (0 = no chunking)
   * @param chunkCallback  Callback to invoke when chunk is complete (can be null if chunkSizeMB=0)
   */
  public ContiguousPageWriter(final DatabaseInternal database, final int fileId, final int pageSize,
      final long chunkSizeMB, final ChunkCommitCallback chunkCallback) {
    this.database = database;
    this.fileId = fileId;
    this.pageSize = pageSize;
    this.usablePageSize = pageSize - BasePage.PAGE_HEADER_SIZE;
    this.logicalPosition = 0;
    this.currentPageNum = -1;
    this.totalBytesWritten = 0;
    this.chunkSizeBytes = chunkSizeMB * 1024 * 1024;
    this.chunkCallback = chunkCallback;
    this.existingPages = countExistingPages(database, fileId);

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
    byteBuffer.rewind();
    byteBuffer.putFloat(value);
    write(buffer, 0, Binary.FLOAT_SERIALIZED_SIZE);
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

      // Track bytes for chunking
      if (chunkCallback != null && chunkSizeBytes > 0) {
        totalBytesWritten += toWrite;

        // Check if chunk boundary reached
        if (totalBytesWritten >= chunkSizeBytes) {
          LogManager.instance().log(this, FINE,
              "Chunk complete: %d bytes written, invoking commit callback", totalBytesWritten);

          // Invoke callback to commit current transaction
          chunkCallback.onChunkComplete(totalBytesWritten);

          // Invalidate stale page reference: the commit above finalized the old
          // transaction so currentPage now points to a MutablePage that has already
          // been handed to the async-flush thread. Continuing to write through that
          // reference would mutate its byte array concurrently with the flush,
          // potentially corrupting the on-disk page metadata (content-size header).
          // Resetting currentPageNum forces ensurePageLoaded() to re-acquire the
          // page in the new transaction on the next iteration.
          currentPageNum = -1;
          currentPage = null;

          // Reset counter for next chunk
          totalBytesWritten = 0;
        }
      }

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
   * Ensures a page is loaded for writing, appending one to the file when the logical address has run past its end.
   * <p>
   * An appended page MUST be registered with {@link TransactionContext#addPage}, not merely read and modified
   * (issue #7362). {@code addPage} is the only thing that raises the transaction's page counter for this file, and
   * that counter is what the commit hands to {@link PaginatedComponent#updatePageCount} - so it is what makes
   * {@code getTotalPages()} answer for the pages this write just produced, both DURING the write and immediately
   * after the commit. Until this fix the append went through {@code getPage() + getPageToModify()}, which never
   * fails on a page past the end of the file: {@code getPage} asks the page manager with {@code createIfNotExists},
   * so it invents a zero-filled page and the {@code addPage} fallback below was unreachable. Every graph page
   * landed among the transaction's MODIFIED pages, no counter moved, and the component's page count was left to be
   * raised asynchronously, one page at a time, by the flush thread. A graph persisted and reloaded in the same
   * breath therefore measured itself against whatever the flusher happened to have written by then - which for a
   * multi-GB graph is a length short by gigabytes, and a JVector footer read off the middle of the payload.
   * <p>
   * "Appended" is decided by two facts and not by a failed read: the page is past what the file held when this
   * writer started, AND this writer has not already acquired it (a chunk commit resets {@code currentPageNum}, so
   * the page a chunk boundary fell inside is re-acquired right after it was committed - existing, by then).
   */
  private void ensurePageLoaded(final int pageNum) throws IOException {
    if (pageNum != currentPageNum) {
      final PageId pageId = new PageId(database, fileId, pageNum);
      final TransactionContext transaction = database.getTransaction();

      if (pageNum > highestPageAcquired && pageNum >= existingPages)
        currentPage = transaction.addPage(pageId, pageSize);
      else {
        try {
          final BasePage existingPage = transaction.getPage(pageId, pageSize);
          currentPage = transaction.getPageToModify(existingPage);
        } catch (final Exception e) {
          // Page doesn't exist, create new one
          currentPage = transaction.addPage(pageId, pageSize);
        }
      }

      if (currentPage == null) {
        throw new IOException("Failed to get/create page: " + pageId);
      }

      currentPageNum = pageNum;
      if (pageNum > highestPageAcquired)
        highestPageAcquired = pageNum;
    }
  }

  /**
   * Pages the file behind {@code fileId} holds right now, as the largest of the two counts that can answer it: the
   * physical file (which lags a committed page until the flush thread writes it) and the component's own counter
   * (which lags a page this very writer appended, until the commit). Overestimating is the safe direction - a page
   * wrongly judged to exist is merely read and modified instead of appended, which is what the whole file used to
   * do - while underestimating would let {@link TransactionContext#addPage} replace a page holding real bytes with
   * an empty one.
   * <p>
   * A file with no {@link PaginatedComponent} registered against it answers {@link Integer#MAX_VALUE}, which turns
   * the append path off entirely for it. Nothing is lost: the transaction page counter this whole change exists to
   * raise is consumed at commit as {@code getFileById(id).updatePageCount(...)}, so for a file the schema does not
   * own there is nothing on the other end to receive it - and a counter naming an unregistered file is a
   * deliberately loud failure on that path, which this must not start triggering.
   *
   * @return the page count, or {@link Integer#MAX_VALUE} when it cannot be established or has nowhere to go, so
   * nothing is ever appended blind
   */
  private static int countExistingPages(final DatabaseInternal database, final int fileId) {
    try {
      final Component component = database.getSchema().getFileByIdIfExists(fileId);
      if (!(component instanceof final PaginatedComponent paginatedComponent))
        return Integer.MAX_VALUE;

      int pages = paginatedComponent.getTotalPages();

      final ComponentFile file = database.getFileManager().getFileIfExists(fileId);
      if (file instanceof final PaginatedComponentFile paginatedFile)
        pages = Math.max(pages, (int) paginatedFile.getTotalPages());

      return pages;
    } catch (final Exception e) {
      // WARNING and not FINE: this fallback puts the file back on the pre-issue-#7362 behaviour wholesale, so an
      // exception here silently defeats the fix rather than degrading it. It has to be visible to an operator.
      LogManager.instance().log(ContiguousPageWriter.class, Level.WARNING,
          "Could not establish the page count of file %d (%s): every page will be acquired as an existing one, so the "
              + "component's page count will not account for the pages this write appends", fileId, e.getMessage());
      return Integer.MAX_VALUE;
    }
  }
}
