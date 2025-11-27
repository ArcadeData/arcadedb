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
import java.nio.*;
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
      if (lastImmutablePage < 1) {
        // All pages are either page 0 (metadata) or mutable
        mainIndex.setStatus(new IndexInternal.INDEX_STATUS[] { IndexInternal.INDEX_STATUS.COMPACTION_IN_PROGRESS },
            IndexInternal.INDEX_STATUS.AVAILABLE);
        return false;
      }

      // Perform the merge
      final int pageSize = mainIndex.getPageSize();
      final int entriesCompacted = mergePages(mainIndex, compactedIndex, 1, lastImmutablePage, pageSize,
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
    for (int pageIndex = totalPages - 1; pageIndex >= 1; --pageIndex) {
      try {
        final BasePage page = database.getTransaction().getPage(new PageId(database, mainIndex.getFileId(), pageIndex),
            mainIndex.getPageSize());
        final ByteBuffer buffer = page.getContent();

        // Read the mutable flag (OFFSET_MUTABLE = 8, after offsetFreeContent and numberOfEntries)
        buffer.position(LSMVectorIndex.OFFSET_MUTABLE);
        final byte mutable = buffer.get();

        if (mutable == 0) {
          // Found an immutable page, this is the last one to compact
          lastImmutablePage = pageIndex;
          break;
        }
        // Otherwise, this page is still mutable, skip it and continue backwards
      } catch (final Exception e) {
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
    final int entrySize = 4 + 8 + 4 + (dimensions * 4) + 1; // id + position + bucketId + vector + deleted

    // Calculate how many pages we can process in RAM
    final int pagesToCompact;
    final long totalRAMNeeded = (long) (endPage - startPage + 1) * pageSize;
    if (totalRAMNeeded > ramBudget) {
      pagesToCompact = (int) (ramBudget / pageSize);
      if (pagesToCompact < 1)
        return 0; // Not enough RAM even for 1 page
    } else {
      pagesToCompact = endPage - startPage + 1;
    }

    // Read all vector entries from pages being compacted
    final Map<Integer, VectorEntryData> vectorMap = new HashMap<>();
    int totalEntriesRead = 0;

    for (int pageNum = startPage; pageNum < startPage + pagesToCompact && pageNum <= endPage; pageNum++) {
      final BasePage page = database.getTransaction().getPage(new PageId(database, mainIndex.getFileId(), pageNum),
          pageSize);
      final ByteBuffer buffer = page.getContent();

      // Read page header
      final int offsetFreeContent = buffer.getInt(LSMVectorIndex.OFFSET_FREE_CONTENT);
      final int numberOfEntries = buffer.getInt(LSMVectorIndex.OFFSET_NUM_ENTRIES);
      final byte mutable = buffer.get(LSMVectorIndex.OFFSET_MUTABLE); // Read mutable flag (for validation)

      if (numberOfEntries == 0)
        continue;

      // Read pointer table (starts at HEADER_BASE_SIZE offset)
      final int[] pointers = new int[numberOfEntries];
      for (int i = 0; i < numberOfEntries; i++) {
        pointers[i] = buffer.getInt(LSMVectorIndex.HEADER_BASE_SIZE + (i * 4));
      }

      // Read entries
      for (int i = 0; i < numberOfEntries; i++) {
        buffer.position(pointers[i]);

        final int id = buffer.getInt();
        final long position = buffer.getLong();
        final int bucketId = buffer.getInt();
        final RID rid = new RID(database, bucketId, position);

        final float[] vector = new float[dimensions];
        for (int j = 0; j < dimensions; j++) {
          vector[j] = buffer.getFloat();
        }

        final boolean deleted = buffer.get() == 1;

        // Last write wins: later pages override earlier entries
        final VectorEntryData entry = new VectorEntryData(id, rid, vector, deleted);
        vectorMap.put(id, entry);
        totalEntriesRead++;
      }
    }

    LogManager.instance()
        .log(mainIndex, Level.FINE, "Read %d entries from %d pages, unique vectors: %d", null, totalEntriesRead,
            pagesToCompact, vectorMap.size());

    // Write merged entries to compacted index (sorted by vector ID, skip deleted)
    final List<Integer> sortedIds = new ArrayList<>(vectorMap.keySet());
    Collections.sort(sortedIds);

    MutablePage currentPage = null;
    final AtomicInteger compactedPageSeries = new AtomicInteger(0);
    int entriesWritten = 0;

    for (final Integer vectorId : sortedIds) {
      final VectorEntryData entry = vectorMap.get(vectorId);

      // Skip deleted entries
      if (entry.deleted)
        continue;

      // Write to compacted index
      final LSMVectorIndexCompacted.CompactionAppendResult result = compactedIndex.appendDuringCompaction(
          currentPage, compactedPageSeries, entry.id, entry.rid, entry.vector, entry.deleted);

      if (!result.newPages.isEmpty()) {
        // New page(s) were created
        currentPage = result.newPages.get(result.newPages.size() - 1); // Use the last created page
      }

      // POINTER MIGRATION: Update VectorLocationIndex with new location in compacted file
      // This ensures each server's VectorLocationIndex matches its own physical file structure
      mainIndex.getVectorIndex().addOrUpdate(
          entry.id,                    // vectorId
          true,                        // isCompacted = true (this vector is now in the compacted file)
          result.pageNumber,           // new page number in compacted file
          result.offset,               // new offset within that page
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

    LogManager.instance()
        .log(mainIndex, Level.FINE, "Wrote %d non-deleted entries to compacted index", null, entriesWritten);

    return entriesWritten;
  }

  /**
   * Temporary data structure for vector entries during compaction.
   */
  private static class VectorEntryData {
    final int     id;
    final RID     rid;
    final float[] vector;
    final boolean deleted;

    VectorEntryData(final int id, final RID rid, final float[] vector, final boolean deleted) {
      this.id = id;
      this.rid = rid;
      this.vector = vector;
      this.deleted = deleted;
    }
  }
}
