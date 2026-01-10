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
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.index.IndexException;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.atomic.*;

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
  // Base header size (without page 0 metadata): offsetFree(4) + count(4) + mutable(1) + series(4) = 13
  private static final int    BASE_HEADER_SIZE = 4 + 4 + 1 + 4;

  protected final LSMVectorIndex           mainIndex;
  protected final int                      dimensions;
  protected final VectorSimilarityFunction similarityFunction;
  protected final int                      maxConnections;
  protected final int                      beamWidth;

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
    // Entry size is now variable - calculated per entry using Binary.getNumberSpace()
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

      // Skip both ArcadeDB page header (8 bytes) and LSM vector base header (BASE_HEADER_SIZE)
      buffer.position(BasePage.PAGE_HEADER_SIZE + BASE_HEADER_SIZE);

      // Read vector index metadata
      this.dimensions = buffer.getInt();
      this.similarityFunction = VectorSimilarityFunction.values()[buffer.getInt()];
      this.maxConnections = buffer.getInt();
      this.beamWidth = buffer.getInt();
      // Entry size is now variable - calculated per entry using Binary.getNumberSpace()

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
   * Contains both the pages modified and the absolute file offset where the entry was written.
   */
  public static class CompactionAppendResult {
    public final List<MutablePage> newPages;
    public final long              absoluteFileOffset;

    public CompactionAppendResult(final List<MutablePage> newPages, final long absoluteFileOffset) {
      this.newPages = newPages;
      this.absoluteFileOffset = absoluteFileOffset;
    }
  }

  /**
   * Appends a vector entry during compaction using variable-sized encoding.
   * Entries are written sequentially without pointer tables for maximum space efficiency.
   * Preserves quantized data to maintain search performance for quantization-enabled indexes.
   *
   * @param currentPage                 The current page being written to (or null to create new)
   * @param compactedPageNumberOfSeries Counter for page series numbering
   * @param currentFileOffset           Tracks absolute file offset for entries (updated by this method)
   * @param vectorId                    The vector ID
   * @param rid                         The record ID
   * @param deleted                     Whether this entry is deleted
   * @param quantType                   The quantization type
   * @param quantizedData               The quantized vector data (null if NONE)
   * @param quantMin                    Min value for INT8 quantization
   * @param quantMax                    Max value for INT8 quantization
   * @param quantMedian                 Median value for BINARY quantization
   * @param originalLength              Original vector length for BINARY quantization
   *
   * @return CompactionAppendResult containing new pages and absolute file offset of the written entry
   */
  public CompactionAppendResult appendDuringCompaction(MutablePage currentPage,
      final AtomicInteger compactedPageNumberOfSeries, final AtomicLong currentFileOffset,
      final int vectorId, final RID rid, final boolean deleted, final VectorQuantizationType quantType,
      final byte[] quantizedData, final float quantMin, final float quantMax, final float quantMedian,
      final int originalLength) throws IOException, InterruptedException {

    final List<MutablePage> newPages = new ArrayList<>();

    if (currentPage == null) {
      // CREATE A NEW PAGE
      currentPage = createNewPage(compactedPageNumberOfSeries.getAndIncrement());
      newPages.add(currentPage);
    }

    // Calculate variable entry size (space needed for this specific entry)
    final int vectorIdSize = Binary.getNumberSpace(vectorId);
    final int bucketIdSize = Binary.getNumberSpace(rid.getBucketId());
    final int positionSize = Binary.getNumberSpace(rid.getPosition());
    int entrySize = vectorIdSize + positionSize + bucketIdSize + 1 + 1; // +1 for deleted byte, +1 for quantTypeOrdinal

    // Add size for quantized data if present
    if (quantType == VectorQuantizationType.INT8 && quantizedData != null) {
      entrySize += 4; // vector length
      entrySize += quantizedData.length; // quantized bytes
      entrySize += 8; // min + max (2 floats)
    } else if (quantType == VectorQuantizationType.BINARY && quantizedData != null) {
      entrySize += 4; // original length
      entrySize += quantizedData.length; // packed bytes
      entrySize += 4; // median (float)
    }

    // Read page header using BasePage methods (accounts for PAGE_HEADER_SIZE automatically)
    int offsetFreeContent = currentPage.readInt(LSMVectorIndex.OFFSET_FREE_CONTENT);
    int numberOfEntries = currentPage.readInt(LSMVectorIndex.OFFSET_NUM_ENTRIES);
    int pageNum = currentPage.getPageId().getPageNumber();

    // Calculate space needed (no pointer table - just header + sequential entries)
    int headerSize = getHeaderSize(pageNum);
    final int availableSpace = currentPage.getMaxContentSize() - offsetFreeContent;

    if (availableSpace < entrySize) {
      // NO SPACE LEFT, CREATE A NEW PAGE AND FLUSH CURRENT ONE (NO WAL)
      // During compaction, pages are created fresh and don't need version tracking
      database.getPageManager().writePages(List.of(currentPage), true);

      currentPage = createNewPage(compactedPageNumberOfSeries.getAndIncrement());
      newPages.add(currentPage);

      // Reset for new page
      offsetFreeContent = currentPage.readInt(0);
      numberOfEntries = 0;
      pageNum = currentPage.getPageId().getPageNumber();
      headerSize = getHeaderSize(pageNum);
    }

    // Record the absolute file offset where this entry will be written
    final long entryFileOffset = currentFileOffset.get();

    // Write entry sequentially using variable-sized encoding
    int bytesWritten = 0;
    bytesWritten += currentPage.writeNumber(offsetFreeContent + bytesWritten, vectorId);
    bytesWritten += currentPage.writeNumber(offsetFreeContent + bytesWritten, rid.getBucketId());
    bytesWritten += currentPage.writeNumber(offsetFreeContent + bytesWritten, rid.getPosition());
    bytesWritten += currentPage.writeByte(offsetFreeContent + bytesWritten, (byte) (deleted ? 1 : 0));

    // Write quantization type byte (preserving quantization from source pages)
    bytesWritten += currentPage.writeByte(offsetFreeContent + bytesWritten, (byte) quantType.ordinal());

    // Write quantized vector data if present (preserving for search performance)
    if (quantType == VectorQuantizationType.INT8 && quantizedData != null) {
      // Write INT8 quantized data: length + bytes + min + max
      bytesWritten += currentPage.writeInt(offsetFreeContent + bytesWritten, quantizedData.length);
      for (byte b : quantizedData) {
        bytesWritten += currentPage.writeByte(offsetFreeContent + bytesWritten, b);
      }
      bytesWritten += currentPage.writeInt(offsetFreeContent + bytesWritten, Float.floatToIntBits(quantMin));
      bytesWritten += currentPage.writeInt(offsetFreeContent + bytesWritten, Float.floatToIntBits(quantMax));

    } else if (quantType == VectorQuantizationType.BINARY && quantizedData != null) {
      // Write BINARY quantized data: original length + packed bytes + median
      bytesWritten += currentPage.writeInt(offsetFreeContent + bytesWritten, originalLength);
      for (byte b : quantizedData) {
        bytesWritten += currentPage.writeByte(offsetFreeContent + bytesWritten, b);
      }
      bytesWritten += currentPage.writeInt(offsetFreeContent + bytesWritten, Float.floatToIntBits(quantMedian));
    }
    // When quantType is NONE, vectors remain in documents (no quantized data to write)

    // Update page header
    numberOfEntries++;
    offsetFreeContent += bytesWritten;
    currentPage.writeInt(LSMVectorIndex.OFFSET_FREE_CONTENT, offsetFreeContent);
    currentPage.writeInt(LSMVectorIndex.OFFSET_NUM_ENTRIES, numberOfEntries);

    // Advance file offset tracker
    currentFileOffset.addAndGet(bytesWritten);

    // Return the new pages and the absolute file offset where this entry was written
    return new CompactionAppendResult(newPages, entryFileOffset);
  }

  /**
   * Creates a new immutable page for compacted data.
   * Entries are written sequentially starting from headerSize (no pointer table).
   */
  protected MutablePage createNewPage(final int compactedPageNumberOfSeries) {
    final int txPageCounter = getTotalPages();
    // Create MutablePage directly (compaction happens outside transaction context)
    final MutablePage currentPage = new MutablePage(new PageId(database, getFileId(), txPageCounter), pageSize);

    // Calculate header size first
    final int headerSize = getHeaderSize(txPageCounter);

    int pos = 0;

    // offsetFreeContent starts right after header (entries grow forward sequentially)
    pos += currentPage.writeInt(pos, headerSize);
    // numberOfEntries (initially 0)
    pos += currentPage.writeInt(pos, 0);
    // mutable flag (IMMUTABLE for compacted pages)
    pos += currentPage.writeByte(pos, (byte) 0);

    // If page 0, write metadata
    if (txPageCounter == 0) {
      pos += currentPage.writeInt(pos, dimensions);
      pos += currentPage.writeInt(pos, similarityFunction.ordinal());
      pos += currentPage.writeInt(pos, maxConnections);
      currentPage.writeInt(pos, beamWidth);
    }

    // Manually update page count (following LSMTreeIndexCompacted pattern)
    updatePageCount(txPageCounter + 1);

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

        // Read page header using BasePage methods (accounts for PAGE_HEADER_SIZE automatically)
        final int offsetFreeContent = page.readInt(0);
        final int numberOfEntries = page.readInt(4);

        if (numberOfEntries == 0)
          continue;

        final int headerSize = getHeaderSize(pageNum);

        // Parse variable-sized entries sequentially (no pointer table)
        int currentOffset = headerSize;
        for (int i = 0; i < numberOfEntries; i++) {
          // Read variable-sized vectorId
          final long[] vectorIdAndSize = page.readNumberAndSize(currentOffset);
          final int id = (int) vectorIdAndSize[0];
          currentOffset += (int) vectorIdAndSize[1];

          // Read variable-sized bucketId
          final long[] bucketIdAndSize = page.readNumberAndSize(currentOffset);
          final int bucketId = (int) bucketIdAndSize[0];
          currentOffset += (int) bucketIdAndSize[1];

          // Read variable-sized position
          final long[] positionAndSize = page.readNumberAndSize(currentOffset);
          final long position = positionAndSize[0];
          currentOffset += (int) positionAndSize[1];

          final RID rid = new RID(database, bucketId, position);

          // Read deleted flag (fixed 1 byte)
          final boolean deleted = page.readByte(currentOffset) == 1;
          currentOffset += 1;

          // CRITICAL: Always read quantization type byte (matches writer that always writes it)
          final byte quantTypeOrdinal = page.readByte(currentOffset);
          currentOffset += 1;

          // Compacted pages preserve quantization from source pages for search performance
          // Skip quantized data (we'll load vectors from documents for getAllVectors)
          if (quantTypeOrdinal > 0 && quantTypeOrdinal < VectorQuantizationType.values().length) {
            final VectorQuantizationType quantType = VectorQuantizationType.values()[quantTypeOrdinal];

            if (quantType == VectorQuantizationType.INT8) {
              // Skip: vector length + quantized bytes + min + max
              final int vectorLength = page.readInt(currentOffset);
              currentOffset += 4 + vectorLength + 8;

            } else if (quantType == VectorQuantizationType.BINARY) {
              // Skip: original length + packed bytes + median
              final int origLength = page.readInt(currentOffset);
              currentOffset += 4;
              final int byteCount = (origLength + 7) / 8;
              currentOffset += byteCount + 4;
            }
          }

          // Load vector from document (for getAllVectors, we fetch from documents)
          float[] vector = null;
          if (!deleted) {
            try {
              final var record = rid.asDocument(false);
              if (record != null) {
                // Get vector from document property
                final String vectorPropertyName = mainIndex.getPropertyNames() != null && !mainIndex.getPropertyNames().isEmpty() ?
                    mainIndex.getPropertyNames().get(0) : "vector";
                final Object vectorObj = record.get(vectorPropertyName);
                if (vectorObj instanceof float[]) {
                  vector = (float[]) vectorObj;
                }
              }
            } catch (final Exception e) {
              // Skip entries where document is not accessible
            }
          }

          // Skip entries without valid vectors (unless deleted)
          if (vector != null || deleted) {
            final VectorEntry entry = new VectorEntry(id, rid, vector);
            entry.deleted = deleted;
            vectors.put(id, entry);
          }
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
      // page 0: offsetFree + count + mutable + dimensions + similarity + maxConn + beamWidth
      return 4 + 4 + 1 + 4 + 4 + 4 + 4; // 25 bytes (removed series field)
    } else {
      // other pages: offsetFree + count + mutable
      return 4 + 4 + 1; // 9 bytes = LSMVectorIndex.HEADER_BASE_SIZE (removed series field)
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
