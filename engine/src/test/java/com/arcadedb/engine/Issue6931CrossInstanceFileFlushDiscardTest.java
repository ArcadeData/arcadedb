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
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue6931CrossInstanceFileFlushDiscardTest extends TestHelper {
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
