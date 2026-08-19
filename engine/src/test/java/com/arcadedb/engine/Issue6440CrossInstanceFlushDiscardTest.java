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
 * Regression test for issue #6440: a test (or application) that repeatedly {@code drop()}s and re-{@code create()}s
 * a database at the SAME path stalls every {@code close()}/{@code drop()} for up to
 * {@code TransactionManager}'s full 20 x 100 ms retry budget, because
 * {@link PageManagerFlushThread#removeAllPagesOfDatabase} and {@link FlushPageIndex#removeAllOfDatabase} match
 * pending pages by {@code Database.equals()}, which - like the rest of this class's per-database bookkeeping -
 * compares by database PATH, not by instance (see the {@code PagesToFlush.slotCharged} javadoc, and issues #6281 /
 * #6303, which defended {@code slotsInUse} and the suspend/deferred bookkeeping against the exact same aliasing but
 * left this queue scan and the index purge it drives untouched).
 * <p>
 * The failure shape: database A is closing while a NEW instance B, opened at A's same path (exactly what a JUnit
 * {@code @BeforeEach}/{@code @AfterEach} pair produces test after test), already has its own pages queued for
 * flush. {@code A.equals(B)} is {@code true} (same path), so {@code removeAllPagesOfDatabase(A)} wrongly matches
 * and discards B's still-pending pages - clearing them from the batch and the flush index WITHOUT ever writing them
 * or acking their WAL file. B's own {@code WALFile.pagesToFlush} counter for that page can then never reach zero:
 * {@code TransactionManager.close()}'s retry loop burns its whole budget waiting on a count that no longer
 * corresponds to anything in the pipeline, then gives up and force-drops anyway.
 * <p>
 * Reproduced black-box (not part of this test, timing-dependent) by opening and dropping a database at a fixed path
 * in a tight loop: roughly 1 in 8-10 iterations, even with the JVM otherwise idle, pays the full ~2000 ms stall.
 * These tests drive the exact interleaving white-box instead, because - like #6303's - it is a race the public API
 * cannot be made to produce on demand.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6440CrossInstanceFlushDiscardTest extends TestHelper {
  private static final int FILE_ID   = 4_103;
  private static final int PAGE_SIZE = 64;

  /**
   * THE ISSUE. Closing database A must not discard the still-pending pages of a database B that merely shares A's
   * path.
   */
  @Test
  void closingADatabaseMustNotDiscardAStillOpenSamePathSiblingsQueuedPage() throws Exception {
    final String path = "target/databases/" + getClass().getSimpleName() + "-recycled";
    final PageManagerFlushThread flush = detachedFlushThread();

    final Database closing = TestHelper.createDatabase(path);
    closing.drop();

    final Database open = TestHelper.createDatabase(path);
    try {
      assertThat(open).as("same-path instances compare equal under LocalDatabase.equals()").isEqualTo(closing);
      assertThat(open).isNotSameAs(closing);

      // "open"'s own commit queued a page that has not been flushed yet.
      final MutablePage pending = page((DatabaseInternal) open, 0);
      flush.scheduleFlushOfPages(new ArrayList<>(List.of(pending)));
      assertThat(flush.pageIndex.hasPendingOf((DatabaseInternal) open)).isTrue();

      // "closing"'s belated cleanup runs now, exactly as TransactionManager.close()'s teardown does. Since
      // closing.equals(open), the buggy queue scan resolved this against "open"'s live batch.
      flush.removeAllPagesOfDatabase(closing);

      assertThat(flush.pageIndex.get(pending.getPageId())).as(
          "a batch of a DIFFERENT, still-open instance must survive a same-path sibling's cleanup").isSameAs(pending);
      assertThat(flush.pageIndex.hasPendingOf((DatabaseInternal) open)).as(
          "the live instance's own pending count must be untouched by the sibling's cleanup").isTrue();

      // The page is still genuinely in the pipeline, so it can be flushed normally afterward.
      flush.flushPagesFromQueueToDisk(null, 20L);
      assertThat(flush.pageIndex.hasPendingOf((DatabaseInternal) open)).isFalse();
    } finally {
      open.drop();
    }
  }

  /**
   * Defence in depth for the same aliasing, one layer down: {@link FlushPageIndex#removeAllOfDatabase} walks the
   * WHOLE JVM-wide index by path and must not evict a same-path sibling's entry either, even when
   * {@link PageManagerFlushThread#removeAllPagesOfDatabase}'s queue scan above is fixed.
   */
  @Test
  void flushIndexRemoveAllOfDatabaseMustNotEvictASamePathSiblingsEntry() throws Exception {
    final String path = "target/databases/" + getClass().getSimpleName() + "-index-recycled";

    final Database closing = TestHelper.createDatabase(path);
    closing.drop();

    final Database open = TestHelper.createDatabase(path);
    try {
      final FlushPageIndex index = new FlushPageIndex();
      final MutablePage page = page((DatabaseInternal) open, 0);
      index.put(page);

      index.removeAllOfDatabase((DatabaseInternal) closing);

      assertThat(index.get(page.getPageId())).as(
          "a same-path sibling's indexed page must survive removeAllOfDatabase() of a DIFFERENT instance")
          .isSameAs(page);
      assertThat(index.hasPendingOf((DatabaseInternal) open)).isTrue();
    } finally {
      open.drop();
    }
  }

  /**
   * A database dropped WITH its own pages still queued (no other instance involved at all) must still ack their WAL
   * file when it discards them, exactly as the sibling method {@code removePagesOfFileFromBatch} already does for a
   * single dropped file (issue #4928). Without this, a database that legitimately still has pages in flight when it
   * is force-dropped strands its own {@code WALFile.pagesToFlush} counter forever.
   */
  @Test
  void removingADatabasesOwnQueuedPageAcksItsWalFile() throws Exception {
    final String path = "target/databases/" + getClass().getSimpleName() + "-own-ack";
    final Database db = TestHelper.createDatabase(path);
    try {
      final PageManagerFlushThread flush = detachedFlushThread();
      final MutablePage pending = page((DatabaseInternal) db, 0);

      final File walFileDir = new File(path);
      walFileDir.mkdirs();
      final WALFile walFile = new WALFile(new File(walFileDir, "issue6440.wal").getAbsolutePath());
      try {
        pending.setWALFile(walFile);
        // Mirrors what WALFile.writeTransactionToFile does for each page it writes, without touching disk I/O.
        bumpPendingPagesToFlush(walFile);

        flush.scheduleFlushOfPages(new ArrayList<>(List.of(pending)));
        assertThat(walFile.getPendingPagesToFlush()).isEqualTo(1);

        flush.removeAllPagesOfDatabase((DatabaseInternal) db);

        assertThat(walFile.getPendingPagesToFlush()).as(
            "discarding a page that will never be flushed must still release its WAL ack").isEqualTo(0);
      } finally {
        walFile.close();
      }
    } finally {
      db.drop();
    }
  }

  /**
   * Constructing the flush thread directly does NOT start the background thread, so the pipeline only moves when
   * the test moves it.
   */
  private static PageManagerFlushThread detachedFlushThread() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.PAGE_FLUSH_QUEUE, 8);
    return new PageManagerFlushThread(PageManager.INSTANCE, cfg);
  }

  private static MutablePage page(final DatabaseInternal database, final int pageNumber) {
    return new MutablePage(new PageId(database, FILE_ID, pageNumber), PAGE_SIZE, new byte[PAGE_SIZE], 0, 0);
  }

  private static void bumpPendingPagesToFlush(final WALFile walFile) throws Exception {
    final java.lang.reflect.Field f = WALFile.class.getDeclaredField("pagesToFlush");
    f.setAccessible(true);
    ((java.util.concurrent.atomic.AtomicInteger) f.get(walFile)).incrementAndGet();
  }
}
