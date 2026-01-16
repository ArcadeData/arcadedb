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
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.log.LogManager;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Level;

/**
 * Utility class for parsing vector entries from LSMVectorIndex pages.
 * Consolidates the page parsing logic used in multiple places:
 * - Loading vectors from pages during startup
 * - Building graph from scratch
 * - Scanning pages for specific vector IDs
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexPageParser {

  /**
   * Represents a single parsed vector entry from a page.
   */
  static class VectorEntry {
    final int vectorId;
    final RID rid;
    final boolean deleted;
    final long absoluteFileOffset;
    final boolean isCompacted;

    VectorEntry(final int vectorId, final RID rid, final boolean deleted,
        final long absoluteFileOffset, final boolean isCompacted) {
      this.vectorId = vectorId;
      this.rid = rid;
      this.deleted = deleted;
      this.absoluteFileOffset = absoluteFileOffset;
      this.isCompacted = isCompacted;
    }
  }

  /**
   * Parse all vector entries from a range of pages in a file.
   *
   * @param database    The database instance
   * @param fileId      The file ID to read from
   * @param totalPages  Number of pages to read
   * @param pageSize    Page size in bytes
   * @param isCompacted Whether this is a compacted index file
   * @param consumer    Consumer to process each parsed entry
   * @return Number of entries parsed
   */
  static int parsePages(final DatabaseInternal database, final int fileId, final int totalPages,
      final int pageSize, final boolean isCompacted, final Consumer<VectorEntry> consumer) {

    int entriesRead = 0;

    for (int pageNum = 0; pageNum < totalPages; pageNum++) {
      try {
        final PageId pageId = new PageId(database, fileId, pageNum);
        final BasePage page = database.getPageManager().getImmutablePage(pageId, pageSize, false, false);

        if (page == null)
          continue;

        final int numberOfEntries = page.readInt(LSMVectorIndex.OFFSET_NUM_ENTRIES);
        if (numberOfEntries == 0)
          continue;

        // Calculate header size (page 0 of compacted index has extra metadata)
        final int headerSize = calculateHeaderSize(isCompacted, pageNum);

        // Calculate absolute file offset for this page's data
        final long pageStartOffset = (long) pageNum * pageSize;

        // Parse entries
        int currentOffset = headerSize;
        for (int i = 0; i < numberOfEntries; i++) {
          final long entryFileOffset = pageStartOffset + BasePage.PAGE_HEADER_SIZE + currentOffset;

          // Read variable-sized vectorId
          final long[] vectorIdAndSize = page.readNumberAndSize(currentOffset);
          final int vectorId = (int) vectorIdAndSize[0];
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

          // Skip quantization data
          currentOffset = skipQuantizationData(page, currentOffset);

          consumer.accept(new VectorEntry(vectorId, rid, deleted, entryFileOffset, isCompacted));
          entriesRead++;
        }
      } catch (final Exception e) {
        LogManager.instance().log(LSMVectorIndexPageParser.class, Level.WARNING,
            "Error parsing page %d in file %d: %s", pageNum, fileId, e.getMessage());
      }
    }

    return entriesRead;
  }

  /**
   * Parse all entries from pages and return as a list.
   *
   * @param database    The database instance
   * @param fileId      The file ID to read from
   * @param totalPages  Number of pages to read
   * @param pageSize    Page size in bytes
   * @param isCompacted Whether this is a compacted index file
   * @return List of parsed entries
   */
  static List<VectorEntry> parseAllEntries(final DatabaseInternal database, final int fileId,
      final int totalPages, final int pageSize, final boolean isCompacted) {

    final List<VectorEntry> entries = new ArrayList<>();
    parsePages(database, fileId, totalPages, pageSize, isCompacted, entries::add);
    return entries;
  }

  /**
   * Calculate header size for a page.
   *
   * @param isCompacted Whether this is a compacted file
   * @param pageNum     The page number
   * @return Header size in bytes
   */
  static int calculateHeaderSize(final boolean isCompacted, final int pageNum) {
    if (isCompacted && pageNum == 0)
      return LSMVectorIndex.HEADER_BASE_SIZE + (4 * 4); // base + 4 ints for metadata
    return LSMVectorIndex.HEADER_BASE_SIZE;
  }

  /**
   * Skip over quantization data in a page, returning the new offset.
   *
   * @param page   The page to read from
   * @param offset Current offset in page
   * @return New offset after skipping quantization data
   */
  static int skipQuantizationData(final BasePage page, int offset) {
    final byte quantTypeOrdinal = page.readByte(offset);
    offset += 1;

    if (quantTypeOrdinal == VectorQuantizationType.INT8.ordinal()) {
      // INT8: vector length (4 bytes) + quantized bytes + min (4 bytes) + max (4 bytes)
      final int vectorLength = page.readInt(offset);
      offset += 4;
      offset += vectorLength;
      offset += 8; // min + max floats
    } else if (quantTypeOrdinal == VectorQuantizationType.BINARY.ordinal()) {
      // BINARY: original length (4 bytes) + packed bytes + median (4 bytes)
      final int originalLength = page.readInt(offset);
      offset += 4;
      final int byteCount = (originalLength + 7) / 8;
      offset += byteCount;
      offset += 4; // median float
    }
    // NONE or PRODUCT: no additional data

    return offset;
  }

  /**
   * Calculate the size of quantized vector data in bytes.
   *
   * @param dimensions       Number of vector dimensions
   * @param quantizationType Type of quantization
   * @return Size in bytes
   */
  static int calculateQuantizedDataSize(final int dimensions, final VectorQuantizationType quantizationType) {
    return switch (quantizationType) {
      case INT8 -> dimensions + 8; // bytes + min + max floats
      case BINARY -> ((dimensions + 7) / 8) + 4; // packed bytes + median float
      default -> 0;
    };
  }
}
