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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #12139: a transaction that reads a page of an index file while a concurrent (or replicated)
 * {@code DROP INDEX} removes that file failed with {@code IllegalArgumentException: Cannot read page 0 because the file
 * '...' is closed}, which is not a database exception and is not retried. The drop marks the file dropped and closes it
 * BEFORE it leaves the file manager, so a read that resolved the file just before the drop finds its channel gone. That
 * read has to answer the retryable {@code ConcurrentModificationException("File with id 'N' has been removed")} the same
 * race already gets at commit time, because a retry succeeds.
 * <p>
 * The window is held open deterministically by the file manager's dropped-file handler, which {@code FileManager#dropFile}
 * calls after it raised the dropped flag and before it removes the file from its maps.
 */
class Issue12139DroppedIndexPageReadTest extends TestHelper {
  private static final String INDEX_NAME = "Item[v]";

  @Test
  void aTransactionReadingAPageOfAnIndexDroppedUnderItGetsARetryableException() throws Exception {
    final IndexInternal bucketIndex = createIndexedType();
    final int fileId = bucketIndex.getFileId();
    final PageId pageId = new PageId(database, fileId, 0);
    final int pageSize = bucketIndex.getPageSize();

    final DatabaseInternal db = (DatabaseInternal) database;
    final FileManager fileManager = db.getFileManager();
    final AtomicBoolean windowReached = new AtomicBoolean();
    final AtomicReference<Throwable> readInTheWindow = new AtomicReference<>();
    final ExecutorService reader = Executors.newSingleThreadExecutor();
    try {
      // THE READER'S TRANSACTION IS OPEN AND HAS ALREADY READ THE INDEX
      reader.submit(() -> {
        database.begin();
        readPage(pageId, pageSize);
        return null;
      }).get(30, TimeUnit.SECONDS);

      fileManager.setDroppedFileHandler(file -> {
        if (file.getFileId() != fileId)
          return PageManager.INSTANCE.deferFileDrop(database, file);
        try {
          file.drop();
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
        // CLOSED AND DROPPED, STILL REGISTERED: THE READER CONTINUES ITS TRANSACTION NOW
        windowReached.set(true);
        final Future<?> read = reader.submit(() -> {
          readPage(pageId, pageSize);
          return null;
        });
        try {
          read.get(30, TimeUnit.SECONDS);
        } catch (final ExecutionException e) {
          readInTheWindow.set(e.getCause());
        } catch (final Exception e) {
          readInTheWindow.set(e);
        }
        // THE FILE IS ALREADY DELETED: THE FILE MANAGER MUST NOT DELETE IT AGAIN
        return true;
      });

      database.getSchema().dropIndex(INDEX_NAME);

      assertThat(windowReached.get()).as("the drop reached the window between closing the file and unregistering it").isTrue();
      assertThat(readInTheWindow.get()).as("a page read of a file a concurrent drop closed is retryable")
          .isInstanceOf(ConcurrentModificationException.class)
          .hasMessage("File with id '" + fileId + "' has been removed")
          .hasCauseInstanceOf(IllegalArgumentException.class);
    } finally {
      fileManager.setDroppedFileHandler(file -> PageManager.INSTANCE.deferFileDrop(database, file));
      reader.submit(() -> {
        if (database.isTransactionActive())
          database.rollback();
      }).get(30, TimeUnit.SECONDS);
      reader.shutdownNow();
    }
  }

  /**
   * The control: a file closed for any reason other than a drop (the database closing under the read, for one) is not a
   * schema change a retry gets past, and keeps answering what it answered before.
   */
  @Test
  void aPageReadOfAFileClosedWithoutADropIsNotMaskedAsRetryable() {
    final IndexInternal bucketIndex = createIndexedType();
    final int fileId = bucketIndex.getFileId();
    final PageId pageId = new PageId(database, fileId, 0);
    final int pageSize = bucketIndex.getPageSize();

    final DatabaseInternal db = (DatabaseInternal) database;
    final ComponentFile file = db.getFileManager().getFile(fileId);
    db.getPageManager().removePageFromCache(pageId);
    file.close();
    try {
      database.begin();
      assertThatThrownBy(() -> readPage(pageId, pageSize))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Cannot read page 0 because the file")
          .hasMessageEndingWith("is closed");
      database.rollback();
    } finally {
      // THE FILE IS STILL REGISTERED: REOPEN THE DATABASE SO THE REST OF THE TEST LIFECYCLE SEES AN OPEN FILE
      reopenDatabase();
    }
  }

  private IndexInternal createIndexedType() {
    database.command("sql", "CREATE DOCUMENT TYPE Item");
    database.command("sql", "CREATE PROPERTY Item.v INTEGER");
    database.command("sql", "CREATE INDEX ON Item (v) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument("Item").set("v", i).save();
    });
    // EVERY PAGE ON DISK, NOTHING CACHED: THE READS BELOW GO TO THE FILE
    reopenDatabase();

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(INDEX_NAME);
    for (final IndexInternal sub : typeIndex.getIndexesOnBuckets())
      if (sub.countEntries() > 0)
        return sub;
    throw new IllegalStateException("No bucket index holds an entry");
  }

  private void readPage(final PageId pageId, final int pageSize) throws IOException {
    ((DatabaseInternal) database).getTransaction().getPage(pageId, pageSize);
  }
}
