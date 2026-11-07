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
import com.arcadedb.log.WarningCapture;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Discarding and acknowledging queued pages when a database is closed or a file is dropped, including same-path sibling instances.
 * <p>
 * Each nested class is the regression suite of one issue (the number is in the nested class name).
 */
class PageManagerFlushDiscardTest {

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
  @Nested
  class CrossInstanceFlushDiscard6440 extends TestHelper {
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
  @Nested
  class FlushAckOnClosedDatabase6440 {
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

  /**
   * Regression test for issue #6931: the dropped-FILE flush purge kept both same-path aliasing patterns that #6440
   * removed from the dropped-DATABASE purge, on adjacent lines of the same file that documents why they exist.
   * <p>
   * Two {@code LocalDatabase} instances can be live at one path at once (a restore, a re-provision, or simply a test
   * whose {@code @BeforeEach}/{@code @AfterEach} hammers one fixed path). Same path means the same schema and the same
   * file ids, so both can index {@code PageId(fileId=F, pageNumber=N)} - and since {@code LocalDatabase.equals()} and,
   * through it, {@code PageId.equals()}/{@code hashCode()} compare by PATH, the two collide as one map key, with
   * {@code Map.put()}'s "keep the old key on an equals() match" behavior leaving the FIRST instance's key object in
   * place while the VALUE becomes the second instance's page.
   * <p>
   * {@link PageManagerFlushThread#removeAllPagesOfFile} therefore had two ways to damage a live sibling:
   * <ul>
   * <li>{@code removePagesOfFileFromBatch} gated on {@code database.equals(pagesToFlush.database)}, so a file drop on
   * one instance purged the OTHER instance's queued batch - discarding pages that were never written AND releasing
   * their WAL acks, leaving nothing to replay them from: a silent write loss;</li>
   * <li>it then evicted the index entry with the single-key, {@code equals()}-based {@code pageIndex.remove(pageId)}
   * overload - the very overload the comment three methods up forbids - which can evict a same-path sibling's live
   * page out of the colliding map slot.</li>
   * </ul>
   * One layer down, {@link FlushPageIndex#removeAllOfFile} filtered on the RETAINED MAP KEY (the shape
   * {@code removeAllOfDatabase} was switched away from) and handed that stale key to {@code remove(PageId)}, which
   * releases the KEY owner's counter rather than the removed VALUE owner's - so the live instance's pending count
   * drifts HIGH and never returns to zero: the "hangs close() forever on a pipeline that is in fact empty" failure
   * named in {@link FlushPageIndex}'s own class javadoc.
   * <p>
   * The fix mirrors #6440 exactly: match batches by reference identity, remove index entries with
   * {@code removeIfSame}, and filter the index walk on the indexed VALUE's own {@link PageId}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CrossInstanceFileFlushDiscard6931 extends TestHelper {
    private static final int FILE_ID   = 6_931;
    private static final int PAGE_SIZE = 64;

    /**
     * THE ISSUE, first half. Dropping a file on instance A must not purge the queued batch of an instance B that
     * merely shares A's path - neither from the batch nor from the WAL ack that protects it.
     */
    @Test
    void droppingAFileMustNotDiscardAStillOpenSamePathSiblingsQueuedPage() throws Exception {
      final String path = "target/databases/" + getClass().getSimpleName() + "-recycled";
      final PageManagerFlushThread flush = detachedFlushThread();

      final Database dropping = TestHelper.createDatabase(path);
      dropping.drop();

      final Database open = TestHelper.createDatabase(path);
      try {
        assertThat(open).as("same-path instances compare equal under LocalDatabase.equals()").isEqualTo(dropping);
        assertThat(open).isNotSameAs(dropping);

        final MutablePage pending = page((DatabaseInternal) open, 0);
        final WALFile walFile = walFile(path, "issue6931-recycled.wal");
        try {
          pending.setWALFile(walFile);
          bumpPendingPagesToFlush(walFile);

          flush.scheduleFlushOfPages(new ArrayList<>(List.of(pending)));
          assertThat(flush.pageIndex.hasPendingOf((DatabaseInternal) open)).isTrue();

          // "dropping"'s file purge runs now, exactly as PageManager.deleteFile does. Since dropping.equals(open),
          // the buggy batch scan resolved this against "open"'s live batch.
          flush.removeAllPagesOfFile(dropping, FILE_ID);

          assertThat(flush.pageIndex.get(pending.getPageId())).as(
              "a batch of a DIFFERENT, still-open instance must survive a same-path sibling's file drop")
              .isSameAs(pending);
          assertThat(flush.pageIndex.hasPendingOf((DatabaseInternal) open)).as(
              "the live instance's own pending count must be untouched by the sibling's file drop").isTrue();
          assertThat(walFile.getPendingPagesToFlush()).as(
              "the live instance's page was never written, so its WAL ack must NOT have been released").isEqualTo(1);

          // The page is still genuinely in the pipeline, so it can be flushed normally afterward.
          flush.flushPagesFromQueueToDisk(null, 20L);
          assertThat(flush.pageIndex.hasPendingOf((DatabaseInternal) open)).isFalse();
        } finally {
          walFile.close();
        }
      } finally {
        open.drop();
      }
    }

    /**
     * THE ISSUE, second half: the map-key-collision twin. {@code removePagesOfFileFromBatch} evicted index entries
     * with {@code pageIndex.remove(page.getPageId())} - a plain, {@code equals()}-based removal - rather than the
     * identity-safe {@code removeIfSame} the rest of the pipeline uses. When the instance dropping the file has its
     * OWN queued page at the exact {@code (fileId, pageNumber)} a same-path sibling later indexed, the map's stored
     * key is still the dropper's original {@code PageId} even though the VALUE is the sibling's live page - so
     * removing by that key, unconditionally, evicted a page that was never the dropper's to begin with.
     */
    @Test
    void droppingAFileMustNotEvictASamePathSiblingsPageItsOwnQueuedBatchCollidesWith() throws Exception {
      final String path = "target/databases/" + getClass().getSimpleName() + "-queue-collision";
      final PageManagerFlushThread flush = detachedFlushThread();

      final Database dropping = TestHelper.createDatabase(path);
      dropping.close();

      final Database open = TestHelper.createDatabase(path);
      try {
        // "dropping" has its own still-queued page of that file, indexed first so its PageId becomes the stored key.
        final MutablePage droppingPage = page((DatabaseInternal) dropping, 0);
        flush.pageIndex.put(droppingPage);
        flush.offerBatch(new PageManagerFlushThread.PagesToFlush(new ArrayList<>(List.of(droppingPage))), false);

        // "open" indexes a page at the exact SAME (fileId, pageNumber): the slot's value becomes open's page, but the
        // stored key is still droppingPage's PageId object (Map.put() on an equals() match).
        final MutablePage openPage = page((DatabaseInternal) open, 0);
        flush.pageIndex.put(openPage);
        assertThat(flush.pageIndex.get(openPage.getPageId())).isSameAs(openPage);

        flush.removeAllPagesOfFile(dropping, FILE_ID);

        assertThat(flush.pageIndex.get(openPage.getPageId())).as(
            "a same-path sibling's live page, sitting at the same map slot as this instance's OWN queued page, "
                + "must survive this instance's file drop").isSameAs(openPage);
        assertThat(flush.pageIndex.hasPendingOf((DatabaseInternal) open)).as(
            "open's own pending count must be untouched by dropping's purge of its own, superseded page").isTrue();
      } finally {
        open.drop();
      }
    }

    /**
     * Defence in depth for the same aliasing, one layer down: {@link FlushPageIndex#removeAllOfFile} walks the WHOLE
     * JVM-wide index and must not evict a same-path sibling's entry either, even once the batch scan above is fixed.
     */
    @Test
    void flushIndexRemoveAllOfFileMustNotEvictASamePathSiblingsEntry() throws Exception {
      final String path = "target/databases/" + getClass().getSimpleName() + "-index-recycled";

      final Database dropping = TestHelper.createDatabase(path);
      dropping.drop();

      final Database open = TestHelper.createDatabase(path);
      try {
        final FlushPageIndex index = new FlushPageIndex();
        final MutablePage page = page((DatabaseInternal) open, 0);
        index.put(page);

        index.removeAllOfFile((DatabaseInternal) dropping, FILE_ID);

        assertThat(index.get(page.getPageId())).as(
            "a same-path sibling's indexed page must survive removeAllOfFile() of a DIFFERENT instance")
            .isSameAs(page);
        assertThat(index.pendingOf((DatabaseInternal) open)).isEqualTo(1);
      } finally {
        open.drop();
      }
    }

    /**
     * The counter-accounting half of the map-key collision, and the one that fires on a perfectly LEGITIMATE purge:
     * {@code removeAllOfFile} filtered {@code pages.keySet()} and handed the retained key to {@code remove(PageId)},
     * which releases {@code pageId.getDatabase()}'s counter. When the stored key belongs to a same-path sibling while
     * the value is the live instance's own page, the live instance drops its page but keeps its count (drift HIGH -
     * a close that never completes), while the sibling's counter is decremented for a page it no longer owns.
     */
    @Test
    void flushIndexRemoveAllOfFileMustReleaseTheIndexedPagesOwnCounterNotTheRetainedKeysOwner() throws Exception {
      final String path = "target/databases/" + getClass().getSimpleName() + "-file-collision-counters";

      final Database stale = TestHelper.createDatabase(path);
      stale.drop();

      final Database open = TestHelper.createDatabase(path);
      try {
        final FlushPageIndex index = new FlushPageIndex();

        // Indexed first, so ITS PageId object stays the map's key for that slot forever after.
        final MutablePage stalePage = page((DatabaseInternal) stale, 0);
        index.put(stalePage);
        // Same (fileId, pageNumber): stalePage's entry is replaced, not added alongside.
        final MutablePage openPage = page((DatabaseInternal) open, 0);
        index.put(openPage);

        assertThat(index.get(openPage.getPageId())).isSameAs(openPage);
        assertThat(index.pendingOf((DatabaseInternal) open)).isEqualTo(1);
        assertThat(index.pendingOf((DatabaseInternal) stale)).isZero();

        // "open" drops the file itself - its own, entirely legitimate cleanup.
        index.removeAllOfFile((DatabaseInternal) open, FILE_ID);

        assertThat(index.get(openPage.getPageId())).as("open's own page must be gone from the index").isNull();
        assertThat(index.pendingOf((DatabaseInternal) open)).as(
            "the removal must release the counter of the page it actually removed, or open's count drifts high and "
                + "its close() waits forever on a pipeline that is empty").isZero();
        assertThat(index.pendingOf((DatabaseInternal) stale)).as(
            "the stale key owner's counter must not be decremented for a page it no longer owns").isZero();
      } finally {
        open.drop();
      }
    }

    /**
     * The non-regression half: narrowing the two matches must NOT turn the purge into a no-op for the instance that
     * actually dropped the file. Its own queued page must leave the batch, leave the index, release its counter and
     * release its WAL ack, exactly as before.
     */
    @Test
    void droppingAFileStillPurgesTheDroppingInstancesOwnQueuedPage() throws Exception {
      final String path = "target/databases/" + getClass().getSimpleName() + "-own-purge";
      final PageManagerFlushThread flush = detachedFlushThread();

      final Database db = TestHelper.createDatabase(path);
      try {
        final MutablePage pending = page((DatabaseInternal) db, 0);
        final WALFile walFile = walFile(path, "issue6931-own-purge.wal");
        try {
          pending.setWALFile(walFile);
          bumpPendingPagesToFlush(walFile);

          final PageManagerFlushThread.PagesToFlush batch = new PageManagerFlushThread.PagesToFlush(
              new ArrayList<>(List.of(pending)));
          flush.pageIndex.put(pending);
          flush.offerBatch(batch, false);
          assertThat(flush.pageIndex.pendingOf((DatabaseInternal) db)).isEqualTo(1);

          flush.removeAllPagesOfFile(db, FILE_ID);

          assertThat(batch.pages).as("the dropped file's pages must leave the batch").isEmpty();
          assertThat(flush.pageIndex.get(pending.getPageId())).as("and the index").isNull();
          assertThat(flush.pageIndex.pendingOf((DatabaseInternal) db)).as("and release their pending count").isZero();
          assertThat(walFile.getPendingPagesToFlush()).as(
              "a page that will never be written must still release its WAL ack").isZero();
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

    private static WALFile walFile(final String path, final String name) throws Exception {
      final File dir = new File(path);
      dir.mkdirs();
      return new WALFile(new File(dir, name).getAbsolutePath());
    }

    /** Mirrors what WALFile.writeTransactionToFile does for each page it writes, without touching disk I/O. */
    private static void bumpPendingPagesToFlush(final WALFile walFile) throws Exception {
      final Field f = WALFile.class.getDeclaredField("pagesToFlush");
      f.setAccessible(true);
      ((AtomicInteger) f.get(walFile)).incrementAndGet();
    }
  }

  /**
   * Issue #7363: during a long ingest the async flush thread repeatedly reported, at SEVERE and at WARNI, that it
   * could not write pages to a sub-index file an index compaction had since closed:
   * <pre>
   * SEVER [PageManagerFlushThread] Error on processing page flush requests
   * java.lang.IllegalArgumentException: Cannot write page 8 because the file '...umtidx' is closed
   * WARNI [PageManagerFlushThread] Error on flushing page 'PageId(citation_graph/27/9) v=1' to disk
   * com.arcadedb.exception.DatabaseMetadataException: Cannot flush pages on disk because file '...umtidx' is closed
   * </pre>
   * Those pages are superseded by construction - the file they belong to has been replaced and deleted, so there is
   * nowhere for them to go and nothing to lose - and {@code flushPage} has always had a quiet FINE path for exactly
   * that, taken when the file has already left the {@code FileManager}. What it did not have was a way to recognise
   * the same situation one instant EARLIER, while the file is on its way out: still registered, closed or closing.
   * So the identical, harmless event was reported at the two levels an operator watches for real corruption.
   * <p>
   * The {@code IllegalArgumentException} variant was worse than noise. It is unchecked, so it escaped every per-page
   * catch in the flush loop, abandoning the rest of the batch: those pages kept their {@code pageIndex} entries and
   * their WAL acks forever, and {@code waitAllPagesOfDatabaseAreFlushed} then burned its whole budget on the next
   * close.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DroppedFileFlushIsQuiet7363 extends TestHelper {
    private static final int PAGE_SIZE = 65536;

    /**
     * The exact window the report caught: the file has been dropped (closed, deleted) but is still registered, so
     * every "does this file exist" check the flush path makes still says yes.
     */
    @Test
    void aPageOfADroppedButStillRegisteredFileIsDiscardedQuietly() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;

      final PaginatedComponentFile file = scratchFile(db, "issue7363-dropped");
      final MutablePage page = new MutablePage(new PageId(db, file.getFileId(), 0), PAGE_SIZE);
      page.writeInt(0, 0xDEAD);

      file.drop();

      assertThat(file.isDropped()).as("drop() has to say so, and say so before it closes the channel").isTrue();
      assertThat(db.getFileManager().existsFile(file.getFileId()))
          .as("dropping the file object alone leaves it registered - the window the report caught").isTrue();

      final List<WarningCapture.LogLine> lines = WarningCapture.capture(Level.WARNING,
          () -> assertThatCode(() -> PageManager.INSTANCE.flushPage(page))
              .as("a page addressed to a deleted file must not fail the flush of the batch it is in")
              .doesNotThrowAnyException());

      assertThat(lines)
          .as("a superseded page is a FINE event, not a SEVERE/WARNI one on a path watched for corruption; got: %s",
              lines)
          .noneMatch(line -> line.message().contains("Cannot write page")
              || line.message().contains("Cannot flush pages on disk")
              || line.message().contains("Error on flushing page")
              || line.message().contains("Error on processing page flush requests"));
    }

    /**
     * The same page, once the file has also left the {@code FileManager}: quiet before this change too, and it has
     * to stay quiet - the two are the same event observed on either side of one non-atomic drop.
     */
    @Test
    void aPageOfAFullyUnregisteredFileStaysQuietToo() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;

      final PaginatedComponentFile file = scratchFile(db, "issue7363-unregistered");
      final MutablePage page = new MutablePage(new PageId(db, file.getFileId(), 0), PAGE_SIZE);

      db.getFileManager().dropFile(file.getFileId());
      assertThat(db.getFileManager().existsFile(file.getFileId())).isFalse();

      final List<WarningCapture.LogLine> lines = WarningCapture.capture(Level.WARNING,
          () -> assertThatCode(() -> PageManager.INSTANCE.flushPage(page)).doesNotThrowAnyException());

      assertThat(lines).as("got: %s", lines)
          .noneMatch(line -> line.message().contains("Cannot write page")
              || line.message().contains("Cannot flush pages on disk"));
    }

    /**
     * A file that is merely CLOSED and not dropped is a different thing entirely - the engine writing to a live
     * component whose channel it lost - and must still be reported. Silencing that along with the dropped case would
     * trade a false alarm for a missed one.
     */
    @Test
    void aPageOfAClosedButNotDroppedFileIsStillReported() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;

      final PaginatedComponentFile file = scratchFile(db, "issue7363-closed");
      final MutablePage page = new MutablePage(new PageId(db, file.getFileId(), 0), PAGE_SIZE);

      file.close();
      assertThat(file.isDropped()).isFalse();

      assertThatCode(() -> PageManager.INSTANCE.flushPage(page))
          .as("nothing says these pages are superseded, so this failure must reach the caller")
          .hasMessageContaining("is closed");

      // Leave nothing half-open behind for the fixture's own close.
      db.getFileManager().dropFile(file.getFileId());
    }

    /**
     * A page-backed file registered with the {@code FileManager} but backed by no schema component: enough for the
     * flush path, which resolves the file by id, and detached from everything the fixture's own close touches.
     */
    private static PaginatedComponentFile scratchFile(final DatabaseInternal db, final String componentName)
        throws Exception {
      final int fileId = db.getFileManager().newFileId();
      final String path = db.getDatabasePath() + "/" + componentName + "." + fileId + "." + PAGE_SIZE + ".v0.scratch";
      return (PaginatedComponentFile) db.getFileManager()
          .getOrCreateFile(componentName, path, ComponentFile.MODE.READ_WRITE);
    }
  }

  /**
   * https://github.com/ArcadeData/arcadedb/issues/8139
   * <p>
   * {@code WALFile.retryAfterReopen()} restores the interrupt flag after the retry, deliberately, so the next channel
   * operation on the reopened channel is closed and reopened again. It logged every reopen at {@code SEVERE}, so a
   * recovery scan on an interrupted thread emitted one {@code SEVERE} line per read. The first reopen is still
   * {@code SEVERE}; the rest of the burst goes to {@code FINE} and is counted.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ChannelReopenLogFlood8139 extends TestHelper {

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType("Doc");
    }

    @Test
    void anInterruptedRecoveryReadLogsOneSevereLineAndCountsEveryReopen() throws Exception {
      final File path = new File(database.getDatabasePath(), "txlog_8139_read.wal");
      final WALFile walFile = new WALFile(path.getAbsolutePath());
      try {
        final List<MutablePage> pages = Collections.emptyList();
        walFile.append(WALFile.writeTransactionToBuffer(pages, 4242L).getByteBuffer());

        final WALFile.WALTransaction[] tx = new WALFile.WALTransaction[1];
        final List<WarningCapture.LogLine> lines = WarningCapture.capture(Level.FINE, () -> {
          Thread.currentThread().interrupt();
          try {
            tx[0] = walFile.getFirstTransaction();
          } finally {
            Thread.interrupted();
          }
        });

        final List<WarningCapture.LogLine> reopenLines = lines.stream().filter(l -> l.message().contains("txlog_8139_read.wal"))
            .toList();

        assertThat(tx[0]).isNotNull();
        assertThat(tx[0].txId).isEqualTo(4242L);
        assertThat(walFile.getReopenCount()).as("the restored interrupt flag closes the channel again on every read")
            .isGreaterThan(1);
        assertThat(reopenLines.stream().filter(l -> l.level() == Level.SEVERE).count()).as("lines: %s", reopenLines)
            .isEqualTo(1);
        assertThat(reopenLines).as("every reopen is still traceable at FINE").hasSize((int) walFile.getReopenCount());
      } finally {
        walFile.close();
      }
    }

    @Test
    void severeAgainAfterAQuietIntervalWithTheFoldedCount() {
      final AtomicLong now = new AtomicLong(1_000L);
      final ChannelReopenLog log = new ChannelReopenLog(now::get);

      assertThat(log.record()).as("the first reopen is SEVERE").isZero();
      assertThat(log.record()).isEqualTo(-1);
      assertThat(log.record()).isEqualTo(-1);

      now.addAndGet(ChannelReopenLog.SEVERE_INTERVAL_NANOS - 1);
      assertThat(log.record()).as("still inside the interval").isEqualTo(-1);

      now.addAndGet(1);
      assertThat(log.record()).as("a later burst is SEVERE again and says how many were folded").isEqualTo(3);
      assertThat(log.record()).isEqualTo(-1);
      assertThat(log.getTotal()).isEqualTo(6);
    }
  }
}
