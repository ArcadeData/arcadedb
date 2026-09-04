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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6133.
 * <p>
 * "Does this database still have pages in the flush pipeline?" used to be answered by walking the JVM-WIDE flush index
 * and comparing every key's database, so the cost of the question scaled with the backlog of every OTHER open database
 * - and the question is polled every 10 ms for the whole duration of a drain by close, rename, index compaction,
 * backup suspension and the snapshot t0 barrier, the last of which polls it holding the JVM-wide page-manager lock.
 * The answer is now an O(1) per-database counter maintained inside {@link FlushPageIndex}.
 * <p>
 * A counter is only as good as its exactness, which is what this test is about: one that drifts HIGH hangs
 * {@code close()} forever on a pipeline that is in fact empty, and one that drifts LOW lets a backup stamp its t0 over
 * a half-written batch. Every route a page can take OUT of the pipeline is exercised below - flushed (with and without
 * a newer instance superseding it), explicitly removed by the replay detach, purged with its file, purged with its
 * database - and after each one the counter is compared against the brute-force scan it replaced.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6133PendingPagesPerDatabaseTest extends TestHelper {
  private static final int PAGE_SIZE = 1024;

  /**
   * Every mutation of the index keeps the per-database count in agreement with the map, and the two databases account
   * for their pages independently.
   */
  @Test
  void everyRouteOutOfThePipelineKeepsTheCountExact() {
    final DatabaseInternal db1 = (DatabaseInternal) database;
    final Database db2 = TestHelper.createDatabase("target/databases/" + getClass().getSimpleName() + "-sibling");
    try {
      final FlushPageIndex index = new FlushPageIndex();

      // db1: 5 pages on file 7, 3 on file 8. db2: 4 pages on file 7 - a same-numbered file of another database must
      // not be confused with db1's, which is the whole point of keying the count by database.
      final List<MutablePage> file7 = pages(db1, 7, 5);
      final List<MutablePage> file8 = pages(db1, 8, 3);
      final List<MutablePage> sibling = pages((DatabaseInternal) db2, 7, 4);
      index.putAll(file7);
      index.putAll(file8);
      index.putAll(sibling);
      assertCountsAgree(index, db1, 8);
      assertCountsAgree(index, db2, 4);

      // A later transaction supersedes a page still queued (the two-instance case of #4544): the index holds ONE
      // entry for that pageId, so the count must not grow.
      final MutablePage superseded = file7.get(0);
      final MutablePage newer = new MutablePage(superseded.getPageId(), PAGE_SIZE, new byte[PAGE_SIZE], 1, 0);
      index.put(newer);
      assertCountsAgree(index, db1, 8);

      // The superseded copy reaching the disk must NOT release the entry - it still belongs to the newer instance,
      // which is still pending - nor its count.
      assertThat(index.removeIfSame(superseded)).as("a superseded copy must not evict the newer entry").isFalse();
      assertCountsAgree(index, db1, 8);

      // The indexed instance reaching the disk releases both.
      assertThat(index.removeIfSame(newer)).isTrue();
      assertCountsAgree(index, db1, 7);

      // Flushing a page that is not indexed at all (already superseded AND already dropped) releases nothing.
      assertThat(index.removeIfSame(newer)).isFalse();
      assertCountsAgree(index, db1, 7);

      // The replay detach takes a copy out by pageId.
      assertThat(index.remove(file8.get(0).getPageId())).isSameAs(file8.get(0));
      assertCountsAgree(index, db1, 6);
      assertThat(index.remove(file8.get(0).getPageId())).as("a second detach of the same page finds nothing").isNull();
      assertCountsAgree(index, db1, 6);

      // A dropped file purges its pages, and only its own: db2's file 7 is a different file.
      index.removeAllOfFile(db1, 7);
      assertCountsAgree(index, db1, 2);
      assertCountsAgree(index, db2, 4);

      // A dropped/closed database purges the rest, and forgets its counter with them.
      index.removeAllOfDatabase(db1);
      assertCountsAgree(index, db1, 0);
      assertCountsAgree(index, db2, 4);
      assertThat(index.isEmpty()).isFalse();

      index.removeAllOfDatabase(db2);
      assertCountsAgree(index, db2, 0);
      assertThat(index.isEmpty()).as("the index is empty once both databases are gone").isTrue();
    } finally {
      db2.drop();
    }
  }

  /**
   * The count a drain polls is this database's own: a sibling database with a large backlog in the same JVM-wide
   * pipeline must neither be counted into it nor make the drain wait for it.
   */
  @Test
  void aSiblingBacklogIsNeitherCountedNorWaitedFor() throws Exception {
    final DatabaseInternal db1 = (DatabaseInternal) database;
    final Database db2 = TestHelper.createDatabase("target/databases/" + getClass().getSimpleName() + "-backlog");
    try {
      // Constructing the flush thread directly does NOT start the background thread (that happens in
      // PageManager.startup()), so the pipeline can be loaded up deterministically and never drains.
      final PageManagerFlushThread flush = new PageManagerFlushThread(PageManager.INSTANCE, db1.getConfiguration());

      final int backlog = 50_000;
      flush.pageIndex.putAll(pages((DatabaseInternal) db2, 3, backlog));

      assertThat(flush.pageIndex.pendingOf(db2)).isEqualTo(backlog);
      assertThat(flush.hasPendingPagesOfDatabase(db2)).isTrue();
      assertThat(flush.pageIndex.pendingOf(db1)).as("a sibling's backlog is not this database's").isZero();
      assertThat(flush.hasPendingPagesOfDatabase(db1)).isFalse();

      // The drain of db1 returns immediately even though the pipeline is far from empty.
      assertThat(flush.waitAllPagesOfDatabaseAreFlushed(db1)).isTrue();
      assertThat(flush.waitPendingPagesOfDatabaseUntil(db1, System.currentTimeMillis() + 1_000)).isTrue();

      // One page of db1 in flight is enough for the same drain to report the pipeline busy.
      final MutablePage own = pages(db1, 3, 1).get(0);
      flush.pageIndex.put(own);
      assertThat(flush.hasPendingPagesOfDatabase(db1)).isTrue();
      assertThat(flush.waitPendingPagesOfDatabaseUntil(db1, System.currentTimeMillis() + 50)).isFalse();

      flush.removeFromFlushIndex(own);
      assertThat(flush.hasPendingPagesOfDatabase(db1)).isFalse();

      flush.pageIndex.removeAllOfDatabase(db2);
      assertThat(flush.pageIndex.isEmpty()).isTrue();
    } finally {
      db2.drop();
    }
  }

  /**
   * The accounting survives real contention. Each thread owns a disjoint page range and drives it through the whole
   * life cycle - queued, superseded by a later transaction, flushed, detached - so any lost update in the counter
   * shows up as a disagreement with the map once the threads are joined and the index is quiescent again. It is
   * asserted quiescent on purpose: a comparison sampled WHILE the mutations run proves nothing either way, because
   * the two reads straddle other threads' mutations.
   */
  @Test
  void theCountSurvivesConcurrentMutation() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final FlushPageIndex index = new FlushPageIndex();

    final int threads = 8;
    final int perThread = 2_000;
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread[] workers = new Thread[threads];

    for (int t = 0; t < threads; t++) {
      final int id = t;
      workers[t] = new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < perThread; i++) {
            final PageId pageId = new PageId(db, 100 + id, i);
            final MutablePage queued = new MutablePage(pageId, PAGE_SIZE, new byte[PAGE_SIZE], 0, 0);
            index.put(queued);
            // A later transaction supersedes it while it is still queued: one entry, still one page pending.
            final MutablePage newer = new MutablePage(pageId, PAGE_SIZE, new byte[PAGE_SIZE], 1, 0);
            index.put(newer);
            // The superseded copy reaches the disk first and must release nothing...
            index.removeIfSame(queued);
            // ...then the indexed one leaves, by the flush on even pages and by the replay detach on odd ones.
            if (i % 2 == 0)
              index.removeIfSame(newer);
            else
              index.remove(pageId);
          }
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        }
      }, "issue6133-mutator-" + t);
      workers[t].start();
    }

    start.countDown();
    for (final Thread worker : workers)
      worker.join(TimeUnit.MINUTES.toMillis(2));

    assertThat(failure.get()).isNull();
    assertCountsAgree(index, db, 0);
    assertThat(index.isEmpty()).isTrue();
  }

  /**
   * The end-to-end invariant on the real pipeline: after a burst of concurrent commits the count settles at exactly
   * zero, in agreement with the map. A count left ABOVE zero here is a close() that would never return; one left
   * BELOW is a backup free to stamp its t0 over a page still on its way to the disk.
   */
  @Test
  void theCountSettlesAtZeroAfterConcurrentCommits() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getSchema().createDocumentType("Doc");

    final FlushPageIndex index = PageManager.INSTANCE.getFlushThread().pageIndex;

    final Thread[] writers = new Thread[4];
    for (int t = 0; t < writers.length; t++) {
      final int id = t;
      writers[t] = new Thread(() -> {
        for (int i = 0; i < 250; i++) {
          final int value = i;
          db.transaction(() -> db.newDocument("Doc").set("writer", id).set("v", value).save());
        }
      }, "issue6133-writer-" + t);
      writers[t].start();
    }
    for (final Thread writer : writers)
      writer.join(TimeUnit.MINUTES.toMillis(2));

    assertThat(PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(db)).isTrue();
    assertThat(index.pendingOf(db)).as("everything reached the disk, so nothing is pending").isZero();
    assertThat(index.scanPendingOf(db)).isZero();
  }

  /**
   * A plain, successful close must make the JVM-wide flush thread forget the database, not just drain it.
   * <p>
   * The purge that clears the per-database bookkeeping - the pending-page counter added here, and with it the
   * pre-existing suspend locks, deferred batches and flush-progress counter - used to run only when the close was a
   * DROP or when the flush wait gave up. On the common path nothing cleared them, so every closed database stayed
   * pinned as a map key for the life of {@code PageManager.INSTANCE}: one dead {@code LocalDatabase}, and everything
   * it references, per close, without bound.
   */
  @Test
  void aCleanCloseForgetsTheDatabase() {
    final String path = "target/databases/" + getClass().getSimpleName() + "-close";
    final PageManagerFlushThread flush = PageManager.INSTANCE.getFlushThread();

    final DatabaseInternal closed = (DatabaseInternal) TestHelper.createDatabase(path);
    try {
      closed.getSchema().createDocumentType("Doc");
      closed.transaction(() -> closed.newDocument("Doc").set("v", 1).save());
      // Both entries exist now: the counter from the commit, the progress counter from the wait below.
      assertThat(PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(closed)).isTrue();
      assertThat(flush.pageIndex.isTracked(closed)).isTrue();
      assertThat(flush.flushedPagesPerDatabase).containsKey(closed);

      closed.close();

      assertThat(flush.pageIndex.isTracked(closed))
          .as("a clean close must release the pending-page counter, not pin the closed database as its key").isFalse();
      assertThat(flush.flushedPagesPerDatabase)
          .as("and the flush-progress counter with it").doesNotContainKey(closed);
      assertThat(flush.pageIndex.scanPendingOf(closed)).isZero();
    } finally {
      if (closed.isOpen())
        closed.close();
      new DatabaseFactory(path).open().drop();
    }
  }

  private static List<MutablePage> pages(final DatabaseInternal database, final int fileId, final int total) {
    final List<MutablePage> pages = new ArrayList<>(total);
    for (int i = 0; i < total; i++) {
      final PageId pageId = new PageId(database, fileId, 1_000_000 + i);
      pages.add(new MutablePage(pageId, PAGE_SIZE, new byte[PAGE_SIZE], 0, 0));
    }
    return pages;
  }

  private static void assertCountsAgree(final FlushPageIndex index, final Database database, final int expected) {
    assertThat(index.pendingOf(database)).as("pending count of '%s'", database.getName()).isEqualTo(expected);
    assertThat(index.scanPendingOf(database)).as("scan of the index for '%s'", database.getName()).isEqualTo(expected);
  }

}
