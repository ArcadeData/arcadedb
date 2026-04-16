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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ComponentFile;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #3867: Error with index rebuilding.
 * <p>
 * The bug was in ContiguousPageWriter: after a chunk-commit callback committed the
 * transaction and started a new one, the writer's currentPage/currentPageNum were not
 * invalidated. If the next write targeted the same page number, ensurePageLoaded()
 * reused the stale MutablePage from the old (committed) transaction. This caused the
 * writer to mutate the byte array of a page already in the async-flush queue, leading
 * to corrupted on-disk page metadata ("Cannot resize the buffer" during loadMetadata).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3867ChunkedGraphPersistenceTest {
  private static final String DB_PATH = "databases/test-issue-3867-chunked-persistence";

  @AfterEach
  void cleanup() {
    if (new DatabaseFactory(DB_PATH).exists())
      new DatabaseFactory(DB_PATH).open().drop();
  }

  /**
   * Reproduces the scenario from issue #3867: writes data through ContiguousPageWriter
   * with a very small chunk size so that the chunk-commit callback fires mid-page.
   * After the commit the writer must reload the page in the new transaction instead of
   * reusing the stale reference.
   * <p>
   * Without the fix, this test would either throw "Cannot resize the buffer" during
   * commit, or silently corrupt page data (writes going to the stale page would be lost
   * or cause inconsistent metadata).
   */
  @Test
  void chunkedWriteWithMidPageBoundaryShouldNotCorrupt() throws Exception {
    DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();

    Database database = factory.create();

    try (database) {
      final DatabaseInternal dbInternal = (DatabaseInternal) database;
      final int pageSize = 65536;
      final int usablePageSize = pageSize - BasePage.PAGE_HEADER_SIZE;

      // Create a graph file to write into
      final LSMVectorIndexGraphFile graphFile = new LSMVectorIndexGraphFile(
          dbInternal, "test-chunk-graph",
          dbInternal.getDatabasePath(),
          ComponentFile.MODE.READ_WRITE, pageSize);

      dbInternal.getSchema().getEmbedded().registerFile(graphFile);

      // Start a transaction with WAL disabled (matching the production scenario)
      database.begin();
      dbInternal.getTransaction().setUseWAL(false);

      // Chunk commit callback that mirrors the production code in LSMVectorIndex
      final ChunkCommitCallback chunkCallback = (bytesWritten) -> {
        database.commit();
        database.begin();
        dbInternal.getTransaction().setUseWAL(false);
      };

      // Create a writer with 1 MB chunk size. The chunk boundary at 1 MB = 1048576
      // bytes falls mid-page (page 16, offset 128), which is the scenario that
      // triggered the stale page reference bug.
      final ContiguousPageWriter chunkWriter = new ContiguousPageWriter(
          dbInternal, graphFile.getFileId(), pageSize,
          1, // 1 MB chunks
          chunkCallback);

      // Write enough data to trigger at least 3 chunk commits (> 3 MB).
      // Each writeInt writes 4 bytes, so 3 MB = 3 * 1024 * 1024 / 4 = 786432 ints
      final int totalInts = 800_000;
      for (int i = 0; i < totalInts; i++)
        chunkWriter.writeInt(i);

      final long totalBytes = chunkWriter.position();
      chunkWriter.close();

      // Final commit for any remaining data
      database.commit();

      // Verify the data can be read back correctly through ContiguousPageReader
      final ContiguousPageReader reader = new ContiguousPageReader(
          dbInternal, graphFile.getFileId(), pageSize, totalBytes, 0L);

      // The chunk boundary at 1 MB = 1048576 bytes corresponds to int index 262144.
      // After that commit, the next int (index 262144) writes to page 16, offset 128.
      // Without the fix, the stale page reference causes this write to be lost.
      // Verify the critical region around each chunk boundary.
      reader.seek(0);
      for (int i = 0; i < totalInts; i++) {
        final int actual = reader.readInt();
        assertThat(actual)
            .as("Value at index %d (logical position %d)", i, (long) i * 4)
            .isEqualTo(i);
      }

      reader.close();
    }
  }

  /**
   * Tests that the defensive content-size validation in CachedPage.loadMetadata()
   * handles corrupted pages gracefully instead of throwing an exception.
   */
  @Test
  void corruptedPageContentSizeShouldBeClamped() throws Exception {
    DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();

    Database database = factory.create();

    try (database) {
      final DatabaseInternal dbInternal = (DatabaseInternal) database;
      final int pageSize = 65536;

      // Create a graph file
      final LSMVectorIndexGraphFile graphFile = new LSMVectorIndexGraphFile(
          dbInternal, "test-corrupt-metadata",
          dbInternal.getDatabasePath(),
          ComponentFile.MODE.READ_WRITE, pageSize);

      dbInternal.getSchema().getEmbedded().registerFile(graphFile);

      // Write a page with deliberately corrupted content size metadata
      database.begin();

      final ContiguousPageWriter writer = new ContiguousPageWriter(
          dbInternal, graphFile.getFileId(), pageSize);

      // Write some data
      for (int i = 0; i < 100; i++)
        writer.writeInt(i);

      writer.close();
      database.commit();

      // Now corrupt the content size on disk by writing a value > pageSize at offset 4
      // of page 0. We do this by modifying a page directly.
      database.begin();
      final var pageId = new com.arcadedb.engine.PageId(dbInternal, graphFile.getFileId(), 0);
      final var page = dbInternal.getTransaction().getPage(pageId, pageSize);
      final var mutablePage = dbInternal.getTransaction().getPageToModify(page);

      // Write an invalid content size (larger than page size) at the content-size
      // header position (offset 4, right after the 4-byte version field).
      // This simulates the corruption that occurred in issue #3867.
      final int contentSizeOffset = 4; // Binary.INT_SERIALIZED_SIZE
      mutablePage.getContent().putInt(contentSizeOffset, pageSize + 1000);
      database.commit();

      // Wait for async flush to complete
      Thread.sleep(500);

      // Evict the page from read cache to force a disk reload
      dbInternal.getPageManager().removePageFromCache(pageId);

      // Now try to read the page - before the fix this would throw
      // "Cannot resize the buffer (autoResizable=false)"
      database.begin();
      final var reloadedPage = dbInternal.getTransaction().getPage(pageId, pageSize);
      // If we reach here, the defensive clamping worked
      assertThat(reloadedPage).isNotNull();
      database.rollback();
    }
  }
}
