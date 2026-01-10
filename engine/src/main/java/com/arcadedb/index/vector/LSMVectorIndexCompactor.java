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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.log.LogManager;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

/**
 * Compactor for LSM-style vector indexes.
 * Performs K-way merge of mutable pages with last-write-wins semantics.
 * Removes deleted entries and creates compacted immutable pages.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LSMVectorIndexCompactor {

  /**
   * Performs compaction of the vector index.
   * Merges multiple mutable pages into compacted pages with deduplication.
   *
   * @param mainIndex The vector index to compact
   *
   * @return true if compaction was performed, false otherwise
   */
  public static boolean compact(final LSMVectorIndex mainIndex) throws IOException, InterruptedException {
    final DatabaseInternal database = mainIndex.getDatabase();
    final int totalPages = mainIndex.getTotalPages();

    LogManager.instance()
        .log(mainIndex, Level.INFO, "Starting compaction of vector index '%s' (totalPages=%d, mutablePages=%d)", null,
            mainIndex.getName(), totalPages, mainIndex.getCurrentMutablePages());

    try {
      if (totalPages < 1) {
        // Nothing to compact (only metadata page)
        mainIndex.setStatus(new IndexInternal.INDEX_STATUS[] { IndexInternal.INDEX_STATUS.COMPACTION_IN_PROGRESS },
            IndexInternal.INDEX_STATUS.AVAILABLE);
        return false;
      }

      // Get or create compacted sub-index
      LSMVectorIndexCompacted compactedIndex = mainIndex.getSubIndex();
      if (compactedIndex == null) {
        compactedIndex = createCompactedIndex(mainIndex);
        database.getSchema().getEmbedded().registerFile(compactedIndex);
      }

      // Calculate RAM budget
      long indexCompactionRAM = database.getConfiguration().getValueAsLong(GlobalConfiguration.INDEX_COMPACTION_RAM_MB)
          * 1024 * 1024;
      final long maxUsableRAM = Runtime.getRuntime().maxMemory() * 30 / 100;
      if (indexCompactionRAM > maxUsableRAM)
        indexCompactionRAM = maxUsableRAM;

      // Find last immutable page (skip mutable pages still being written)
      int lastImmutablePage = findLastImmutablePage(mainIndex, totalPages);
      LogManager.instance()
          .log(mainIndex, Level.INFO, "findLastImmutablePage returned: %d (totalPages=%d)", null, lastImmutablePage, totalPages);

      if (lastImmutablePage < 1) {
        // All pages are either page 0 (metadata) or mutable
        mainIndex.setStatus(new IndexInternal.INDEX_STATUS[] { IndexInternal.INDEX_STATUS.COMPACTION_IN_PROGRESS },
            IndexInternal.INDEX_STATUS.AVAILABLE);
        return false;
      }

      // Perform the merge (start from page 0 which contains vector data)
      final int pageSize = mainIndex.getPageSize();
      final int entriesCompacted = mergePages(mainIndex, compactedIndex, 0, lastImmutablePage, pageSize,
          indexCompactionRAM);

      LogManager.instance()
          .log(mainIndex, Level.INFO,
              "Completed compaction of vector index '%s' (entriesCompacted=%d, pagesCompacted=%d)", null,
              mainIndex.getName(), entriesCompacted, lastImmutablePage);

      // Atomically replace the old index with new one containing compacted data
      if (entriesCompacted > 0) {
        final LSMVectorIndexMutable newMutable = mainIndex.splitIndex(lastImmutablePage + 1, compactedIndex);
        LogManager.instance()
            .log(mainIndex, Level.INFO, "Atomic replacement completed: new fileId=%d", null, newMutable.getFileId());

        // CRITICAL: Reload VectorLocationIndex from new file structure after compaction
        // After splitIndex, the file structure has changed:
        // - Compacted vectors are in the compacted sub-index
        // - Remaining mutable pages are renumbered in the new mutable file
        // - VectorLocationIndex must be refreshed to reflect new page numbers and offsets
        LogManager.instance().log(mainIndex, Level.INFO,
            "Reloading VectorLocationIndex after compaction (old size: %d)", null,
            mainIndex.getVectorIndex().size());

        database.transaction(() -> {
          mainIndex.getVectorIndex().clear();
          mainIndex.loadVectorsFromPagesAfterCompaction();
          mainIndex.rebuildGraphAfterCompaction();
        }, true, 0);

        LogManager.instance().log(mainIndex, Level.INFO,
            "VectorLocationIndex reloaded after compaction (new size: %d)", null,
            mainIndex.getVectorIndex().size());
      }

      mainIndex.setStatus(new IndexInternal.INDEX_STATUS[] { IndexInternal.INDEX_STATUS.COMPACTION_IN_PROGRESS },
          IndexInternal.INDEX_STATUS.AVAILABLE);

      return entriesCompacted > 0;

    } catch (final Exception e) {
      mainIndex.setStatus(new IndexInternal.INDEX_STATUS[] { IndexInternal.INDEX_STATUS.COMPACTION_IN_PROGRESS },
          IndexInternal.INDEX_STATUS.AVAILABLE);
      LogManager.instance().log(mainIndex, Level.SEVERE, "Error during vector index compaction", e);
      throw new IndexException("Error during vector index compaction", e);
    }
  }

  /**
   * Creates a new compacted index file.
   */
  private static LSMVectorIndexCompacted createCompactedIndex(final LSMVectorIndex mainIndex) throws IOException {
    final String componentName = mainIndex.getName();
    final int last_ = componentName.lastIndexOf('_');
    final String newName = componentName.substring(0, last_) + "_" + System.nanoTime();

    return new LSMVectorIndexCompacted(mainIndex, mainIndex.getDatabase(), newName,
        mainIndex.getDatabase().getDatabasePath() + File.separator + newName, mainIndex.getDimensions(),
        mainIndex.getSimilarityFunction(), mainIndex.getMaxConnections(), mainIndex.getBeamWidth(),
        mainIndex.getPageSize());
  }

  /**
   * Finds the last immutable page in the index.
   * Scans pages from the end backwards and stops at the first immutable page.
   * Mutable pages (still being written) are skipped for compaction.
   */
  private static int findLastImmutablePage(final LSMVectorIndex mainIndex, final int totalPages) {
    final DatabaseInternal database = mainIndex.getDatabase();
    int lastImmutablePage = -1;

    // Start from the end and work backwards, stopping at the first immutable page
    // Note: Page 0 contains vector data (not just metadata), so we need to check it too
    for (int pageIndex = totalPages - 1; pageIndex >= 0; --pageIndex) {
      try {
        final BasePage page = database.getTransaction().getPage(new PageId(database, mainIndex.getFileId(), pageIndex),
            mainIndex.getPageSize());

        // Read the mutable flag - OFFSET_MUTABLE is absolute, not content-relative
        // So we need to read from the buffer directly without adding PAGE_HEADER_SIZE again
        final ByteBuffer buffer = page.getContent();
        buffer.position(LSMVectorIndex.OFFSET_MUTABLE);
        final byte mutable = buffer.get();

        if (mutable == 0) {
          // Found an immutable page, this is the last one to compact
          lastImmutablePage = pageIndex;
          break;
        }
        // Otherwise, this page is still mutable, skip it and continue backwards
      } catch (final Exception e) {
        LogManager.instance().log(LSMVectorIndexCompactor.class, Level.SEVERE,
            "Error accessing page %d while finding last immutable page: %s", pageIndex, e.getMessage());
        // Page might not exist yet, continue
      }
    }

    return lastImmutablePage;
  }

  /**
   * Merges multiple pages into compacted format.
   * Implements K-way merge with last-write-wins semantics.
   */
  private static int mergePages(final LSMVectorIndex mainIndex, final LSMVectorIndexCompacted compactedIndex,
      final int startPage, final int endPage, final int pageSize, final long ramBudget)
      throws IOException, InterruptedException {

    final DatabaseInternal database = mainIndex.getDatabase();
    final int dimensions = mainIndex.getDimensions();

    // Calculate how many pages we can process in RAM
    final int pagesToCompact;
    final int totalPagesToCompact = endPage - startPage + 1;
    final long totalRAMNeeded = (long) totalPagesToCompact * pageSize;

    LogManager.instance().log(LSMVectorIndexCompactor.class, Level.FINE,
        "Compaction RAM budget: " + (ramBudget / 1024 / 1024) + " MB, " +
            "totalPagesToCompact: " + totalPagesToCompact + ", " +
            "totalRAMNeeded: " + (totalRAMNeeded / 1024 / 1024) + " MB, " +
            "pageSize: " + pageSize);

    if (totalRAMNeeded > ramBudget) {
      pagesToCompact = (int) (ramBudget / pageSize);
      LogManager.instance().log(LSMVectorIndexCompactor.class, Level.WARNING,
          "WARNING: RAM budget insufficient - compacting only " + pagesToCompact + " of " + totalPagesToCompact + " pages");
      if (pagesToCompact < 1)
        return 0; // Not enough RAM even for 1 page
    } else {
      pagesToCompact = totalPagesToCompact;
      LogManager.instance()
          .log(LSMVectorIndexCompactor.class, Level.FINE, "RAM budget sufficient - compacting all " + pagesToCompact + " pages");
    }

    // Read all vector entries from pages being compacted
    // Use RID as key for deduplication (same document updated multiple times gets multiple vectorIds)
    // Keep the entry with the highest vectorId (latest write wins)
    final Map<RID, VectorEntryData> vectorMap = new HashMap<>();
    int totalEntriesRead = 0;

    for (int pageNum = startPage; pageNum < startPage + pagesToCompact && pageNum <= endPage; pageNum++) {
      try {
        final BasePage page = database.getTransaction().getPage(new PageId(database, mainIndex.getFileId(), pageNum),
            pageSize);

        // Read page header using BasePage methods (accounts for PAGE_HEADER_SIZE automatically)
        final int offsetFreeContent = page.readInt(LSMVectorIndex.OFFSET_FREE_CONTENT);
        final int numberOfEntries = page.readInt(LSMVectorIndex.OFFSET_NUM_ENTRIES);

        // Read mutable flag - OFFSET_MUTABLE is absolute, not content-relative
        final ByteBuffer pageBuffer = page.getContent();
        pageBuffer.position(LSMVectorIndex.OFFSET_MUTABLE);
        final byte mutable = pageBuffer.get();

        if (numberOfEntries == 0)
          continue;

        final int headerSize = LSMVectorIndex.HEADER_BASE_SIZE; // No pointer table

        // Validate offsetFreeContent is within page bounds
        if (offsetFreeContent < headerSize || offsetFreeContent > pageSize) {
          LogManager.instance().log(mainIndex, Level.WARNING,
              "Skipping corrupted page %d: invalid offsetFreeContent=%d (pageSize=%d, headerSize=%d)",
              pageNum, offsetFreeContent, pageSize, headerSize);
          continue;
        }

        // Parse variable-sized entries sequentially (no pointer table)
        int currentOffset = headerSize;
        for (int i = 0; i < numberOfEntries; i++) {
          try {
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

            // Read and preserve quantized vector data if quantization is enabled
            VectorQuantizationType quantType = VectorQuantizationType.NONE;
            byte[] quantizedData = null;
            float quantMin = 0.0f;
            float quantMax = 0.0f;
            float quantMedian = 0.0f;
            int originalLength = 0;

            if (quantTypeOrdinal > 0 && quantTypeOrdinal < VectorQuantizationType.values().length) {
              quantType = VectorQuantizationType.values()[quantTypeOrdinal];

              if (quantType == VectorQuantizationType.INT8) {
                // Read: vector length (4 bytes) + quantized bytes + min (4 bytes) + max (4 bytes)
                final int vectorLength = page.readInt(currentOffset);
                currentOffset += 4;

                // Read quantized bytes
                quantizedData = new byte[vectorLength];
                for (int j = 0; j < vectorLength; j++) {
                  quantizedData[j] = page.readByte(currentOffset);
                  currentOffset += 1;
                }

                // Read min and max
                quantMin = Float.intBitsToFloat(page.readInt(currentOffset));
                currentOffset += 4;
                quantMax = Float.intBitsToFloat(page.readInt(currentOffset));
                currentOffset += 4;

              } else if (quantType == VectorQuantizationType.BINARY) {
                // Read: original length (4 bytes) + packed bytes + median (4 bytes)
                originalLength = page.readInt(currentOffset);
                currentOffset += 4;

                final int byteCount = (originalLength + 7) / 8;
                quantizedData = new byte[byteCount];
                for (int j = 0; j < byteCount; j++) {
                  quantizedData[j] = page.readByte(currentOffset);
                  currentOffset += 1;
                }

                // Read median
                quantMedian = Float.intBitsToFloat(page.readInt(currentOffset));
                currentOffset += 4;
              }
            }

            // NOTE: When quantization is NONE, vectors are stored in documents.
            // When quantization is enabled (INT8/BINARY), we preserve the quantized data
            // in compacted pages to maintain search performance.

            // Last write wins: keep entry with highest vectorId for each RID
            final VectorEntryData entry = new VectorEntryData(id, rid, deleted, quantType, quantizedData, quantMin,
                quantMax, quantMedian, originalLength);
            final VectorEntryData existing = vectorMap.get(rid);
            if (existing == null || id > existing.id) {
              // This entry is newer (higher vectorId), keep it
              vectorMap.put(rid, entry);
            }
            totalEntriesRead++;

          } catch (final Exception e) {
            LogManager.instance().log(mainIndex, Level.WARNING,
                "Error parsing entry %d in page %d: %s", i, pageNum, e.getMessage());
            break; // Skip rest of page if parsing fails
          }
        }
      } catch (final Exception e) {
        // Page is corrupted or unreadable, log warning and skip
        LogManager.instance().log(mainIndex, Level.WARNING,
            "Skipping corrupted page %d during compaction: %s", pageNum, e.getMessage());
      }
    }

    // Write merged entries to compacted index (sorted by vector ID, skip deleted)
    final List<VectorEntryData> entries = new ArrayList<>(vectorMap.values());
    // Sort by vectorId to maintain some ordering in compacted pages
    entries.sort((a, b) -> Integer.compare(a.id, b.id));

    MutablePage currentPage = null;
    final AtomicInteger compactedPageSeries = new AtomicInteger(0);
    final AtomicLong currentFileOffset = new AtomicLong(0);
    int entriesWritten = 0;
    int deletedSkipped = 0;

    for (final VectorEntryData entry : entries) {

      // Skip deleted entries
      if (entry.deleted) {
        deletedSkipped++;
        continue;
      }

      // Write to compacted index with file offset tracking, preserving quantized data
      final LSMVectorIndexCompacted.CompactionAppendResult result = compactedIndex.appendDuringCompaction(
          currentPage, compactedPageSeries, currentFileOffset, entry.id, entry.rid, entry.deleted, entry.quantType,
          entry.quantizedData, entry.quantMin, entry.quantMax, entry.quantMedian, entry.originalLength);

      if (!result.newPages.isEmpty()) {
        // New page(s) were created
        currentPage = result.newPages.get(result.newPages.size() - 1); // Use the last created page
      }

      // POINTER MIGRATION: Update VectorLocationIndex with absolute file offset in compacted file
      // This ensures each server's VectorLocationIndex matches its own physical file structure
      mainIndex.getVectorIndex().addOrUpdate(
          entry.id,                    // vectorId
          true,                        // isCompacted = true (this vector is now in the compacted file)
          result.absoluteFileOffset,   // absolute file offset in compacted file
          entry.rid,                   // document RID
          false                        // deleted = false (we already skipped deleted entries)
      );

      entriesWritten++;
    }

    // Flush the last page
    if (currentPage != null) {
      database.getPageManager().updatePageVersion(currentPage, true);
      database.getPageManager().writePages(List.of(currentPage), true); // Bypass WAL for compaction
    }

    return entriesWritten;
  }

  /**
   * Temporary data structure for vector entries during compaction.
   */
  private static class VectorEntryData {
    final int                    id;
    final RID                    rid;
    final boolean                deleted;
    final VectorQuantizationType quantType;
    final byte[]                 quantizedData;  // For INT8: quantized bytes; For BINARY: packed bits
    final float                  quantMin;       // For INT8: min value
    final float                  quantMax;       // For INT8: max value
    final float                  quantMedian;    // For BINARY: median value
    final int                    originalLength; // For BINARY: original vector length

    VectorEntryData(final int id, final RID rid, final boolean deleted, final VectorQuantizationType quantType,
        final byte[] quantizedData, final float quantMin, final float quantMax, final float quantMedian,
        final int originalLength) {
      this.id = id;
      this.rid = rid;
      this.deleted = deleted;
      this.quantType = quantType;
      this.quantizedData = quantizedData;
      this.quantMin = quantMin;
      this.quantMax = quantMax;
      this.quantMedian = quantMedian;
      this.originalLength = originalLength;
    }
  }
}
