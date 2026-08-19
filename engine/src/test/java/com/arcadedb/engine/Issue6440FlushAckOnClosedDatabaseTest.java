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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6440: {@link PageManager#flushPage} abandons a page whose database has already
 * closed WITHOUT releasing its WAL ack.
 * <p>
 * A page is written to WAL synchronously during commit ({@code WALFile.writeTransactionToFile} increments
 * {@code WALFile.pagesToFlush} for every page it writes), then handed to the async flush pipeline to be written
 * to the actual data file; only that second, asynchronous write releases the ack
 * ({@code WALFile.notifyPageFlushed}, via {@code MutablePage.takeWALFile()}). {@code TransactionManager.close()}
 * waits (up to 20 x 100 ms) for every WAL file's {@code pagesToFlush} to reach zero before it will drop or clean
 * up the file.
 * <p>
 * {@code LocalDatabase.closeDurableParts} sets {@code open = false} BEFORE that wait runs, and {@code flushPage}
 * checks {@code database.isOpen()} first and returns immediately - without writing the page AND without acking
 * it - the moment that race is lost. Losing it is exactly what a {@code drop()} (or a plain {@code close()})
 * immediately following a commit produces: whether the async flush thread reaches this page before or after
 * {@code open} flips is a pure scheduling race, decided by nothing the caller controls. When it loses, the page
 * is abandoned - correct, since there is nothing left to write it to - but the counter it left behind can never
 * reach zero, so {@code TransactionManager.close()}'s retry loop burns its ENTIRE 2000 ms budget waiting on a
 * count that nothing will ever satisfy, before giving up and forcing the close anyway.
 * <p>
 * Reproduced black-box (not part of this test, timing-dependent) by rapidly creating, writing to, and dropping a
 * database at a fixed path in a tight loop: even with the JVM otherwise idle this raced roughly 1 time in 10; in
 * a JVM busy running thousands of other tests first (issue #6440's original report, {@code GAVEligibilityTest}
 * inside the full opencypher suite) it raced on most of the class's 43 iterations, turning a ~2 second class
 * into one taking 35-60+ seconds. This test drives the exact interleaving deterministically instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6440FlushAckOnClosedDatabaseTest {
  private static final int FILE_ID   = 4_104;
  private static final int PAGE_SIZE = 64;

  /**
   * THE ISSUE. {@code flushPage} must ack a page's WAL file even when it abandons the write because the
   * database already closed.
   */
  @Test
  void flushPageAcksTheWalFileWhenTheDatabaseIsAlreadyClosed() throws Exception {
    final String path = "target/databases/" + getClass().getSimpleName() + "-closed";
    final Database db = TestHelper.createDatabase(path);

    final File walFileDir = new File(path);
    walFileDir.mkdirs();
    final WALFile walFile = new WALFile(new File(walFileDir, "issue6440.wal").getAbsolutePath());
    try {
      final MutablePage page = page((DatabaseInternal) db, 0);
      page.setWALFile(walFile);
      // Mirrors what WALFile.writeTransactionToFile does for each page it writes, without touching disk I/O:
      // this page was durably WAL-appended during commit and is now awaiting the async data-file write.
      bumpPendingPagesToFlush(walFile);
      assertThat(walFile.getPendingPagesToFlush()).isEqualTo(1);

      // The database closes - exactly what races the async flush thread in the real failure - before the
      // flush pipeline got to this page.
      db.close();
      assertThat(db.isOpen()).isFalse();

      PageManager.INSTANCE.flushPage(page);

      assertThat(walFile.getPendingPagesToFlush()).as(
          "abandoning a page because its database is already closed must still release its WAL ack").isZero();
    } finally {
      walFile.close();
    }
  }

  /**
   * Sanity check of the normal path: flushing a page of an OPEN database still acks it exactly once, through
   * the pre-existing success path - the fix must not double-ack (takeWALFile() is exactly-once by design).
   */
  @Test
  void flushPageAcksTheWalFileExactlyOnceForAnOpenDatabase() throws Exception {
    final String path = "target/databases/" + getClass().getSimpleName() + "-open";
    final Database db = TestHelper.createDatabase(path);
    try {
      final File walFileDir = new File(path);
      final WALFile walFile = new WALFile(new File(walFileDir, "issue6440b.wal").getAbsolutePath());
      try {
        final MutablePage page = page((DatabaseInternal) db, 0);
        page.setWALFile(walFile);
        bumpPendingPagesToFlush(walFile);

        // A page for a file that does not exist takes the "file dropped" branch, which already acks once and
        // returns without touching the (still open) database's schema - the simplest way to exercise a real,
        // successful ack path without a full commit.
        PageManager.INSTANCE.flushPage(page);

        assertThat(walFile.getPendingPagesToFlush()).isZero();
        assertThat(page.takeWALFile()).as("the reference is consumed exactly once, a second take finds nothing")
            .isNull();
      } finally {
        walFile.close();
      }
    } finally {
      db.drop();
    }
  }

  /**
   * The {@link PageManagerFlushThread}-level counterpart of the two tests above: {@code flushPagesFromQueueToDisk}'s
   * own "database closed before this batch could be processed" branch must ack too, not just {@code flushPage()}
   * in isolation - the queue-draining loop reaches that branch first and never even calls {@code flushPage()}.
   */
  @Test
  void flushPagesFromQueueToDiskAcksABatchWhoseDatabaseIsAlreadyClosed() throws Exception {
    final String path = "target/databases/" + getClass().getSimpleName() + "-queue-closed";
    final Database db = TestHelper.createDatabase(path);

    final File walFileDir = new File(path);
    final WALFile walFile = new WALFile(new File(walFileDir, "issue6440c.wal").getAbsolutePath());
    try {
      final PageManagerFlushThread flush = detachedFlushThread();
      final MutablePage page = page((DatabaseInternal) db, 0);
      page.setWALFile(walFile);
      bumpPendingPagesToFlush(walFile);

      // Indexed and queued while the database is still open, exactly as a real commit does.
      flush.pageIndex.putAll(new ArrayList<>(List.of(page)));
      flush.offerBatch(new PageManagerFlushThread.PagesToFlush(new ArrayList<>(List.of(page))), false);

      // The database closes before the (here, manually driven) queue-drain loop gets to this batch.
      db.close();

      flush.flushPagesFromQueueToDisk(null, 20L);

      assertThat(walFile.getPendingPagesToFlush()).as(
          "a queued batch whose database closed before it could be processed must still be acked").isZero();
    } finally {
      walFile.close();
    }
  }

  /**
   * {@code resumeFlushing()}'s equivalent on the suspend/backup-resume path: a batch deferred while suspended,
   * whose database closed before the suspension was released, must be acked when the resume finally runs - not
   * just detached from the deferred backlog and dropped.
   */
  @Test
  void resumeFlushingAcksADeferredBatchWhoseDatabaseIsAlreadyClosed() throws Exception {
    final String path = "target/databases/" + getClass().getSimpleName() + "-resume-closed";
    final Database db = TestHelper.createDatabase(path);

    final File walFileDir = new File(path);
    final WALFile walFile = new WALFile(new File(walFileDir, "issue6440d.wal").getAbsolutePath());
    try {
      final PageManagerFlushThread flush = detachedFlushThread();
      assertThat(flush.setSuspended((Database) db, true)).isTrue();

      final MutablePage page = page((DatabaseInternal) db, 0);
      page.setWALFile(walFile);
      bumpPendingPagesToFlush(walFile);

      flush.pageIndex.putAll(new ArrayList<>(List.of(page)));
      flush.offerBatch(new PageManagerFlushThread.PagesToFlush(new ArrayList<>(List.of(page))), false);
      // Suspended, so the drain defers this batch instead of writing it.
      flush.flushPagesFromQueueToDisk(null, 20L);
      assertThat(flush.hasDeferredBatches((DatabaseInternal) db)).isTrue();

      db.close();

      // The last (only) suspender releasing runs resumeFlushing(db) synchronously on this thread.
      flush.setSuspended((Database) db, false);

      assertThat(walFile.getPendingPagesToFlush()).as(
          "a deferred batch whose database closed while suspended must still be acked on resume").isZero();
    } finally {
      walFile.close();
    }
  }

  private static PageManagerFlushThread detachedFlushThread() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.PAGE_FLUSH_QUEUE, 8);
    return new PageManagerFlushThread(PageManager.INSTANCE, cfg);
  }

  private static MutablePage page(final DatabaseInternal database, final int pageNumber) {
    return new MutablePage(new PageId(database, FILE_ID, pageNumber), PAGE_SIZE, new byte[PAGE_SIZE], 0, 0);
  }

  /** Mirrors what {@code WALFile.writeTransactionToFile} does for each page it writes, without touching disk I/O. */
  private static void bumpPendingPagesToFlush(final WALFile walFile) throws Exception {
    final java.lang.reflect.Field f = WALFile.class.getDeclaredField("pagesToFlush");
    f.setAccessible(true);
    ((java.util.concurrent.atomic.AtomicInteger) f.get(walFile)).incrementAndGet();
  }
}
