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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6303, item 1: the per-database bookkeeping of {@link PageManagerFlushThread} must not be
 * reachable through a batch whose database has been closed.
 * <p>
 * {@code LocalDatabase} compares by database PATH, not by instance, so every map in that class keyed by a
 * {@code Database} answers about whatever database is open at that path NOW. Almost every caller holds a live
 * instance it owns and cannot be caught by that, with ONE exception: the flush thread reads a batch's own
 * {@code database} field after polling it, and a batch can outlive the database that queued it (it is polled, then
 * the database closes, then the suspension check runs). #6281 defended {@code slotsInUse}; the suspension flag, the
 * deferred-batch map, the deferred-RAM charge and the flush-progress counter were left aliasing.
 * <p>
 * These tests drive that exact interleaving white-box, because it is a race the public API cannot be made to produce
 * on demand: a batch of a closed database is put in the pipeline by hand and the pipeline is stepped one poll at a
 * time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6303StaleBatchBookkeepingTest extends TestHelper {
  /** A file that does not exist, so a fabricated page can be driven through the pipeline without touching a real one. */
  private static final int FILE_ID   = 4_101;
  private static final int PAGE_SIZE = 64;

  /**
   * THE ISSUE. A batch left behind by a closed database must not be deferred into the backlog of one reopened at the
   * same path, nor charged to its deferred RAM.
   * <p>
   * Before the fix the flush thread asked {@code isSuspended(staleInstance)}, which resolved the NEW database's flag:
   * a batch of a database that no longer exists was deferred into a backlog it never contributed to and charged bytes
   * to a database that had queued nothing. The batch is not written either way - there is nothing left to write it to
   * - so the only observable effect was accounting drift in an unrelated database, which is precisely the shape that
   * takes the longest to explain.
   */
  @Test
  void aBatchOfAClosedDatabaseIsNotDeferredIntoTheBacklogOfOneReopenedAtTheSamePath() throws Exception {
    final String path = "target/databases/" + getClass().getSimpleName() + "-recycled";
    final PageManagerFlushThread flush = detachedFlushThread();

    final Database closed = TestHelper.createDatabase(path);
    final PageManagerFlushThread.PagesToFlush straggler = new PageManagerFlushThread.PagesToFlush(
        new ArrayList<>(List.of(page((DatabaseInternal) closed, 0))));
    closed.drop();
    assertThat(closed.isOpen()).isFalse();

    final Database reopened = TestHelper.createDatabase(path);
    try {
      assertThat(reopened).isEqualTo(closed);
      assertThat(reopened).isNotSameAs(closed);

      // The reopened database is suspended, exactly as a backup or an HA snapshot ship would leave it. This is the
      // flag the stale batch used to consult.
      assertThat(flush.setSuspended(reopened, true)).isTrue();
      try {
        // The straggler enters the pipeline as one polled just before its database closed would: it never went
        // through reserveQueueSlot, so it is charged one here.
        flush.offerBatch(straggler, false);
        flush.flushPagesFromQueueToDisk(null, 20L);

        assertThat(flush.hasDeferredBatches(reopened)).as(
                "a batch of a database that no longer exists must not join the backlog of one merely sharing its path")
            .isFalse();
        assertThat(flush.getDeferredRAMBytesOf(reopened)).as(
            "...nor be charged to its deferred RAM: it queued nothing").isZero();
        assertThat(flush.deferredRAMBytes.get()).as("...nor to the JVM-wide total the #4728 cap is read from")
            .isZero();
      } finally {
        flush.setSuspended(reopened, false);
      }

      assertThat(flush.deferredRAMDriftRepairs.get()).as(
          "the accounting must balance by construction, not by the repair net of #6200").isZero();
    } finally {
      reopened.drop();
    }
  }

  /**
   * The progress signal of #4928 is now reported on the counter the BATCH carries rather than on one looked up per
   * page, so this pins that the refactor still counts what the bounded waits read - one bump per page that leaves the
   * pipeline, on the database the batch belongs to and on no other.
   * <p>
   * The lookup it replaces was keyed by the page's own database, which compares by PATH, so it was the same aliasing
   * as above one indirection further down. The check hoisted into the suspend monitor makes it unreachable through
   * the flush loop - a batch of a closed database no longer gets that far - so this is defence in depth rather than
   * the fix, and it is strictly less work than what it replaces: one map lookup per batch instead of one per page, on
   * the flush thread's hot loop.
   */
  @Test
  void progressIsCountedPerPageOnTheDatabaseTheBatchBelongsTo() throws Exception {
    final PageManagerFlushThread flush = detachedFlushThread();
    final DatabaseInternal live = (DatabaseInternal) database;
    final Database other = TestHelper.createDatabase("target/databases/" + getClass().getSimpleName() + "-other");
    try {
      flush.scheduleFlushOfPages(new ArrayList<>(List.of(page(live, 0), page(live, 1), page(live, 2))));
      flush.flushPagesFromQueueToDisk(null, 20L);

      assertThat(flush.flushedPagesPerDatabase.get(live)).isNotNull();
      assertThat(flush.flushedPagesPerDatabase.get(live).get()).as("one bump per page that left the pipeline")
          .isEqualTo(3);
      final AtomicLong othersProgress = flush.flushedPagesPerDatabase.get(other);
      assertThat(othersProgress == null ? 0L : othersProgress.get()).as(
          "and nothing at all for a database that queued nothing").isZero();
    } finally {
      other.drop();
    }
  }

  /**
   * Constructing the flush thread directly does NOT start the background thread, so the pipeline only moves when the
   * test moves it.
   */
  private static PageManagerFlushThread detachedFlushThread() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.PAGE_FLUSH_QUEUE, 8);
    return new PageManagerFlushThread(PageManager.INSTANCE, cfg);
  }

  private static MutablePage page(final DatabaseInternal database, final int pageNumber) {
    return new MutablePage(new PageId(database, FILE_ID, pageNumber), PAGE_SIZE, new byte[PAGE_SIZE], 0, 0);
  }
}
