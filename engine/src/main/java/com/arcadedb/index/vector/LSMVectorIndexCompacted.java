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
import com.arcadedb.database.RID;
import com.arcadedb.database.TrackableBinary;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.index.IndexException;
import com.arcadedb.log.LogManager;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

import static com.arcadedb.database.Binary.BYTE_SERIALIZED_SIZE;
import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;

/**
 * Compacted vector index with immutable pages.
 * Stores merged, deduplicated vector data after LSM compaction.
 * Uses two-level structure: root pages (vectorId → pageNum) + data pages (vector entries).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LSMVectorIndexCompacted extends PaginatedComponent {
  public static final  String FILE_EXT         = "vcidx";
  public static final  int    CURRENT_VERSION  = 1;
  private static final int    PAGE_HEADER_SIZE = 4 + 4 + 1 + 4; // offsetFree + count + mutable + series

  protected final LSMVectorIndex           mainIndex;
  protected final int                      dimensions;
  protected final VectorSimilarityFunction similarityFunction;
  protected final int                      maxConnections;
  protected final int                      beamWidth;
  protected final int                      entrySize;

  /**
   * Called at creation time for compaction.
   */
  public LSMVectorIndexCompacted(final LSMVectorIndex mainIndex, final DatabaseInternal database, final String name,
      final String filePath, final int dimensions, final VectorSimilarityFunction similarityFunction,
      final int maxConnections, final int beamWidth, final int pageSize) throws IOException {
    super(database, name, filePath, FILE_EXT, ComponentFile.MODE.READ_WRITE, pageSize, CURRENT_VERSION);
    this.mainIndex = mainIndex;
    this.dimensions = dimensions;
    this.similarityFunction = similarityFunction;
    this.maxConnections = maxConnections;
    this.beamWidth = beamWidth;
    this.entrySize = 4 + 8 + 4 + (dimensions * 4) + 1; // id + position + bucketId + vector + deleted
  }

  /**
   * Called at load time (from page 0).
   */
  protected LSMVectorIndexCompacted(final LSMVectorIndex mainIndex, final DatabaseInternal database, final String name,
      final String filePath, final int id, final ComponentFile.MODE mode, final int pageSize, final int version)
      throws IOException {
    super(database, name, filePath, id, mode, pageSize, version);
    this.mainIndex = mainIndex;

    // Read metadata from page 0
    try {
      final BasePage page0 = database.getTransaction().getPage(new PageId(database, getFileId(), 0), pageSize);
      final ByteBuffer buffer = page0.getContent();

      // Skip page header (offsetFree, count, mutable, series)
      buffer.position(PAGE_HEADER_SIZE);

      // Read vector index metadata
      this.dimensions = buffer.getInt();
      this.similarityFunction = VectorSimilarityFunction.values()[buffer.getInt()];
      this.maxConnections = buffer.getInt();
      this.beamWidth = buffer.getInt();
      this.entrySize = 4 + 8 + 4 + (dimensions * 4) + 1;

    } catch (final Exception e) {
      throw new DatabaseOperationException("Error loading compacted vector index metadata", e);
    }
  }

  @Override
  public Object getMainComponent() {
    return mainIndex;
  }

  /**
   * Result of appending a vector entry during compaction.
   * Contains both the pages modified and the physical location where the entry was written.
   */
  public static class CompactionAppendResult {
    public final List<MutablePage> newPages;
    public final int pageNumber;
    public final int offset;

    public CompactionAppendResult(final List<MutablePage> newPages, final int pageNumber, final int offset) {
      this.newPages = newPages;
      this.pageNumber = pageNumber;
      this.offset = offset;
    }
  }

  /**
   * Appends a vector entry during compaction.
   * Handles page overflow by creating new pages as needed.
   *
   * @param currentPage                 The current page being written to (or null to create new)
   * @param compactedPageNumberOfSeries Counter for page series numbering
   * @param vectorId                    The vector ID
   * @param rid                         The record ID
   * @param vector                      The vector data
   * @param deleted                     Whether this entry is deleted
   *
   * @return CompactionAppendResult containing new pages and write location (pageNum, offset)
   */
  public CompactionAppendResult appendDuringCompaction(MutablePage currentPage,
      final AtomicInteger compactedPageNumberOfSeries, final int vectorId, final RID rid, final float[] vector,
      final boolean deleted) throws IOException, InterruptedException {

    final List<MutablePage> newPages = new ArrayList<>();

    if (currentPage == null) {
      // CREATE A NEW PAGE
      currentPage = createNewPage(compactedPageNumberOfSeries.getAndIncrement());
      newPages.add(currentPage);
    }

    ByteBuffer pageBuffer = currentPage.getContent();

    // Read page header (account for PAGE_HEADER_SIZE offset used by writeInt/readInt methods)
    int offsetFreeContent = currentPage.readInt(0);
    int numberOfEntries = currentPage.readInt(4);
    int pageNum = currentPage.getPageId().getPageNumber();

    // Calculate space needed
    int headerSize = getHeaderSize(pageNum);
    final int pointerTableSize = (numberOfEntries + 1) * 4; // +1 for new entry
    final int availableSpace = offsetFreeContent - (headerSize + pointerTableSize);

    if (availableSpace < entrySize) {
      // NO SPACE LEFT, CREATE A NEW PAGE AND FLUSH CURRENT ONE (NO WAL)
      // During compaction, pages are created fresh and don't need version tracking
      database.getPageManager().writePages(List.of(currentPage), true);

      currentPage = createNewPage(compactedPageNumberOfSeries.getAndIncrement());
      newPages.add(currentPage);

      // Update pageBuffer reference to point to the new page
      pageBuffer = currentPage.getContent();

      // Reset for new page (account for PAGE_HEADER_SIZE offset)
      offsetFreeContent = currentPage.readInt(0);
      numberOfEntries = 0;
      pageNum = currentPage.getPageId().getPageNumber();
      headerSize = getHeaderSize(pageNum);
    }

    // Write entry at tail (backwards from offsetFreeContent)
    final int entryOffset = offsetFreeContent - entrySize;

    // Validate we have enough space (safety check)
    if (entryOffset < 0) {
      throw new IndexException("Entry size (" + entrySize + ") exceeds available page space. " +
          "offsetFreeContent=" + offsetFreeContent + ", headerSize=" + headerSize +
          ", pageSize=" + pageSize + ", pageNum=" + pageNum);
    }

    pageBuffer.position(entryOffset);

    pageBuffer.putInt(vectorId);
    pageBuffer.putLong(rid.getPosition());
    pageBuffer.putInt(rid.getBucketId());
    for (int i = 0; i < dimensions; i++) {
      pageBuffer.putFloat(vector[i]);
    }
    pageBuffer.put((byte) (deleted ? 1 : 0));

    // Add pointer to entry in header
    pageBuffer.putInt(headerSize + (numberOfEntries * 4), entryOffset);

    // Update page header (use writeInt to account for PAGE_HEADER_SIZE offset)
    numberOfEntries++;
    offsetFreeContent = entryOffset;
    currentPage.writeInt(0, offsetFreeContent);
    currentPage.writeInt(4, numberOfEntries);

    // Return the new pages and the location where this entry was written
    return new CompactionAppendResult(newPages, pageNum, entryOffset);
  }

  /**
   * Creates a new immutable page for compacted data.
   */
  protected MutablePage createNewPage(final int compactedPageNumberOfSeries) {
    final int txPageCounter = getTotalPages();
    final MutablePage currentPage = new MutablePage(new PageId(database, getFileId(), txPageCounter), pageSize);

    int pos = 0;

    // offsetFreeContent (starts at end of page)
    currentPage.writeInt(pos, currentPage.getMaxContentSize());
    pos += INT_SERIALIZED_SIZE;

    // numberOfEntries
    currentPage.writeInt(pos, 0);
    pos += INT_SERIALIZED_SIZE;

    // mutable flag (IMMUTABLE for compacted pages)
    currentPage.writeByte(pos, (byte) 0);
    pos += BYTE_SERIALIZED_SIZE;

    // compacted page number of series
    currentPage.writeInt(pos, compactedPageNumberOfSeries);
    pos += INT_SERIALIZED_SIZE;

    // If page 0, write metadata
    if (txPageCounter == 0) {
      currentPage.writeInt(pos, dimensions);
      pos += INT_SERIALIZED_SIZE;

      currentPage.writeInt(pos, similarityFunction.ordinal());
      pos += INT_SERIALIZED_SIZE;

      currentPage.writeInt(pos, maxConnections);
      pos += INT_SERIALIZED_SIZE;

      currentPage.writeInt(pos, beamWidth);
      pos += INT_SERIALIZED_SIZE;
    }

    return currentPage;
  }

  /**
   * Gets all vector entries from this compacted index.
   * Used during merge operations.
   */
  public Map<Integer, VectorEntry> getAllVectors() {
    final Map<Integer, VectorEntry> vectors = new HashMap<>();

    try {
      final int totalPages = getTotalPages();

      for (int pageNum = 0; pageNum < totalPages; pageNum++) {
        final BasePage page = database.getTransaction().getPage(new PageId(database, getFileId(), pageNum), pageSize);
        final ByteBuffer pageBuffer = page.getContent();

        // Read page header
        final int offsetFreeContent = pageBuffer.getInt(0);
        final int numberOfEntries = pageBuffer.getInt(4);

        if (numberOfEntries == 0)
          continue;

        // Read pointer table
        final int[] pointers = new int[numberOfEntries];
        final int headerSize = getHeaderSize(pageNum);
        for (int i = 0; i < numberOfEntries; i++) {
          pointers[i] = pageBuffer.getInt(headerSize + (i * 4));
        }

        // Read entries
        for (int i = 0; i < numberOfEntries; i++) {
          pageBuffer.position(pointers[i]);

          final int id = pageBuffer.getInt();
          final long position = pageBuffer.getLong();
          final int bucketId = pageBuffer.getInt();
          final RID rid = new RID(database, bucketId, position);

          final float[] vector = new float[dimensions];
          for (int j = 0; j < dimensions; j++) {
            vector[j] = pageBuffer.getFloat();
          }

          final boolean deleted = pageBuffer.get() == 1;

          final VectorEntry entry = new VectorEntry(id, rid, vector);
          entry.deleted = deleted;

          // Last write wins (though in compacted index, should only have one entry per ID)
          vectors.put(id, entry);
        }
      }

    } catch (final Exception e) {
      throw new DatabaseOperationException("Error reading vectors from compacted index", e);
    }

    return vectors;
  }

  /**
   * Returns header size which varies by page number (page 0 has metadata).
   */
  private int getHeaderSize(final int pageNum) {
    if (pageNum == 0) {
      // page0: offsetFree + count + mutable + series + dimensions + similarity + maxConn + beamWidth
      return 4 + 4 + 1 + 4 + 4 + 4 + 4 + 4;
    } else {
      // other pages: offsetFree + count + mutable + series
      return 4 + 4 + 1 + 4;
    }
  }

  /**
   * Returns true if this is a mutable page (always false for compacted index).
   */
  public boolean isMutable(final BasePage page) {
    return page.readByte(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE) == 1;
  }

  public int getDimensions() {
    return dimensions;
  }

  public VectorSimilarityFunction getSimilarityFunction() {
    return similarityFunction;
  }

  public int getMaxConnections() {
    return maxConnections;
  }

  public int getBeamWidth() {
    return beamWidth;
  }

  /**
   * Simple vector entry class for returning data.
   */
  public static class VectorEntry {
    public final int     id;
    public final RID     rid;
    public final float[] vector;
    public       boolean deleted;

    public VectorEntry(final int id, final RID rid, final float[] vector) {
      this.id = id;
      this.rid = rid;
      this.vector = vector;
      this.deleted = false;
    }
  }
}
