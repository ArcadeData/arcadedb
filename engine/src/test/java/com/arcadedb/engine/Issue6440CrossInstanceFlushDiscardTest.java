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
 * <p>
 * The fix keys {@link FlushPageIndex}'s {@code pending} counter map by reference identity rather than
 * {@code equals()}, on BOTH ends: {@code counterOf()} (insertion, via {@code put()}/{@code putAll()}) must resolve
 * a brand-new instance's OWN fresh counter and never a same-path sibling's still-present one, exactly as
 * {@code removeAllOfDatabase} (removal) must retire only the counter the instance being closed actually created.
 * Fixing only removal is not enough on its own: a same-path sibling's belated cleanup could otherwise still zero
 * out a live counter it never should have been able to resolve in the first place.
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
   * The map-key-collision twin of the test above (issue #6440 review, fourth pass):
   * {@link PageManagerFlushThread#removeAllPagesOfDatabase} used to evict {@code pageIndex} entries with
   * {@code pageIndex.remove(page.getPageId())} - a plain, {@code equals()}-based removal - rather than the
   * identity-safe {@code removeIfSame} the rest of the pipeline uses. When "closing" has its OWN queued batch at
   * the exact same {@code (fileId, pageNumber)} "open" later indexes, {@code Map.put()}'s "keep the old key on an
   * {@code equals()} match" behavior leaves "closing"'s original {@code PageId} as the map's stored key even
   * though the VALUE is "open"'s live page - so removing by that key, unconditionally, evicted (and left unacked)
   * a page that was never "closing"'s to begin with.
   */
  @Test
  void closingADatabaseMustNotEvictASamePathSiblingsPageItsOwnQueuedBatchCollidesWith() throws Exception {
    final String path = "target/databases/" + getClass().getSimpleName() + "-queue-collision";
    final PageManagerFlushThread flush = detachedFlushThread();

    final Database closing = TestHelper.createDatabase(path);
    closing.close();

    final Database open = TestHelper.createDatabase(path);
    try {
      // "closing" has its own still-queued batch, indexed first so its PageId becomes the map's stored key.
      final MutablePage closingPage = page((DatabaseInternal) closing, 0);
      flush.pageIndex.put(closingPage);
      flush.offerBatch(new PageManagerFlushThread.PagesToFlush(new ArrayList<>(List.of(closingPage))), false);

      // "open" indexes a page at the exact SAME (fileId, pageNumber): the map slot's value becomes open's page,
      // but the stored key is still closingPage's PageId object (Map.put() on an equals() match).
      final MutablePage openPage = page((DatabaseInternal) open, 0);
      flush.pageIndex.put(openPage);
      assertThat(flush.pageIndex.get(openPage.getPageId())).isSameAs(openPage);

      // "closing"'s belated cleanup runs, walking its OWN queued batch - which still references closingPage.
      flush.removeAllPagesOfDatabase(closing);

      assertThat(flush.pageIndex.get(openPage.getPageId())).as(
          "a same-path sibling's live page, sitting at the same map slot as this instance's OWN queued page, "
              + "must survive this instance's cleanup").isSameAs(openPage);
      assertThat(flush.pageIndex.hasPendingOf((DatabaseInternal) open)).as(
          "open's own pending count must be untouched by closing's cleanup of its own, superseded page").isTrue();
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
   * A deeper layer of the same aliasing (issue #6440 review, third pass): {@code removeAllOfDatabase}'s pages
   * purge used to filter by the RETAINED MAP KEY's database identity, not the indexed page's. {@code PageId}
   * collides as a map key across same-path instances too ({@code PageId.equals()}/{@code hashCode()} go through
   * path-based {@code Database.equals()}), and {@code Map.put()} on an {@code equals()}-match keeps the ORIGINAL
   * key object and only swaps the value - textbook Java {@code Map} behavior. So when "open" indexes a page at
   * the exact same {@code (fileId, pageNumber)} "closing" used, the map's stored key is still "closing"'s
   * original {@code PageId}, even though the VALUE is now "open"'s live page. Filtering by that retained key's
   * database would still evict "open"'s page - the same failure the earlier tests pin, one layer further down.
   */
  @Test
  void flushIndexRemoveAllOfDatabaseMustNotEvictASamePathSiblingsPageOnMapKeyCollision() throws Exception {
    final String path = "target/databases/" + getClass().getSimpleName() + "-page-collision";

    final Database closing = TestHelper.createDatabase(path);
    closing.drop();

    final Database open = TestHelper.createDatabase(path);
    try {
      final FlushPageIndex index = new FlushPageIndex();

      // Same (fileId, pageNumber) on purpose: this is what makes the two PageIds collide as one map key.
      final MutablePage closingPage = page((DatabaseInternal) closing, 0);
      index.put(closingPage);
      final MutablePage openPage = page((DatabaseInternal) open, 0);
      index.put(openPage);

      // open's page is the current value at that key - closing's PageId object may still be the stored key.
      assertThat(index.get(openPage.getPageId())).isSameAs(openPage);

      index.removeAllOfDatabase((DatabaseInternal) closing);

      assertThat(index.get(openPage.getPageId())).as(
          "a same-path sibling's live page must survive removeAllOfDatabase() of a DIFFERENT instance even when "
              + "it collided with that instance's page at the same (fileId, pageNumber) map key").isSameAs(openPage);
    } finally {
      open.drop();
    }
  }

  /**
   * The counter-accounting half of the same map-key collision: {@code put()}/{@code putAll()}'s "a later TX
   * superseded a page still queued" branch (issue #4544) used to decrement the INCOMING page's counter whenever
   * {@code pages.put()} returned a non-null replaced value - correct within one database, wrong across two: the
   * replaced value can belong to a DIFFERENT same-path instance (the same collision as the test above), and
   * decrementing the wrong counter phantom-inflates a live sibling's pending count forever while leaving the
   * instance that actually owned the replaced page one too high.
   */
  @Test
  void flushIndexPutMustReleaseTheReplacedPagesOwnCounterNotTheIncomingPages() throws Exception {
    final String path = "target/databases/" + getClass().getSimpleName() + "-collision-counters";

    final Database closing = TestHelper.createDatabase(path);
    closing.drop();

    final Database open = TestHelper.createDatabase(path);
    try {
      final FlushPageIndex index = new FlushPageIndex();

      final MutablePage closingPage = page((DatabaseInternal) closing, 0);
      index.put(closingPage);
      assertThat(index.pendingOf((DatabaseInternal) closing)).isEqualTo(1);

      // Same (fileId, pageNumber): closingPage's entry is replaced, not added alongside.
      final MutablePage openPage = page((DatabaseInternal) open, 0);
      index.put(openPage);

      assertThat(index.pendingOf((DatabaseInternal) open)).as(
          "open's own freshly-indexed page must still count as pending - it must not have been decremented "
              + "away just because it happened to replace a different instance's page at the same map key")
          .isEqualTo(1);
      assertThat(index.pendingOf((DatabaseInternal) closing)).as(
          "closing's superseded page must be released from ITS OWN counter, not left permanently inflated")
          .isEqualTo(0);
    } finally {
      open.drop();
    }
  }

  /**
   * The insertion-side twin of the test above: a same-path sibling's counter must never be resolved on the way
   * IN either. {@code counterOf()} used to be a plain {@code pending.get(database)}/{@code computeIfAbsent}, and
   * since {@code LocalDatabase.equals()} compares by path, that could hand a brand-new database's FIRST page the
   * counter object a DIFFERENT, not-yet-fully-forgotten same-path instance created - "closing" here: closed and
   * drained, so its own counter is back to zero, but its map ENTRY survives until {@code removeAllOfDatabase}
   * actually runs. If "open"'s first page increments that shared entry instead of a fresh one, "closing"'s
   * eventual (belated) cleanup then zeroes out "open"'s live, un-flushed count - the same "backup stamps its t0
   * over a half-written batch" failure this class's own javadoc warns about, just reached from the other side.
   */
  @Test
  void flushIndexCounterOfMustNotAliasASamePathSiblingsCounterOnInsertion() throws Exception {
    final String path = "target/databases/" + getClass().getSimpleName() + "-counter-recycled";

    final Database closing = TestHelper.createDatabase(path);
    closing.drop();

    final Database open = TestHelper.createDatabase(path);
    try {
      final FlushPageIndex index = new FlushPageIndex();

      // Seed "closing"'s counter WITHOUT going through removeAllOfDatabase, mirroring the real race: its page
      // arrived and left the pipeline normally (counter created, then decremented back to zero by remove()),
      // but the counter's MAP ENTRY survives - only removeAllOfDatabase forgets it, and that has not run yet.
      final MutablePage closingPage = page((DatabaseInternal) closing, 0);
      index.put(closingPage);
      index.remove(closingPage.getPageId());
      assertThat(index.isTracked((DatabaseInternal) closing)).isTrue();
      assertThat(index.pendingOf((DatabaseInternal) closing)).isZero();

      // "open"'s first page arrives while "closing"'s entry is still present at the same path.
      final MutablePage openPage = page((DatabaseInternal) open, 1);
      index.put(openPage);

      assertThat(index.pendingOf((DatabaseInternal) open)).as(
          "open's own page must increment open's own counter, not a same-path sibling's stale one").isEqualTo(1);

      // "closing"'s belated cleanup finally runs.
      index.removeAllOfDatabase((DatabaseInternal) closing);

      assertThat(index.pendingOf((DatabaseInternal) open)).as(
          "a same-path sibling's belated cleanup must not zero out open's live, un-flushed counter").isEqualTo(1);
      assertThat(index.get(openPage.getPageId())).isSameAs(openPage);
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
