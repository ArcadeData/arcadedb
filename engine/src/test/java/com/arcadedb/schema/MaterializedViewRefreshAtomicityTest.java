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
package com.arcadedb.schema;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A full refresh used to be a sequence of committed transactions rather than one: {@code TRUNCATE TYPE ... UNSAFE}
 * commits the caller's transaction from the inside - once per {@code arcadedb.truncateBatchSize} records, once more
 * before it rebuilds the indexes it dropped, and the {@code dropIndex} itself is committed before any record is
 * touched. So the emptied view became visible to every other reader long before the repopulate finished. Two
 * consequences, both silent: a reader saw the view empty or half built for the whole runtime of the defining query,
 * and a refresh that failed after the truncate destroyed the previous snapshot instead of leaving it in place.
 * <p>
 * Both triggers are covered below, since neither fires on the trivial case: an index on the backing type (the
 * dropIndex commit, at any size) and a view larger than one truncate batch (the in-scan commit).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MaterializedViewRefreshAtomicityTest extends TestHelper {

  /**
   * The data-loss half. A refresh that throws part way through the repopulate must leave the view exactly as it was,
   * not empty: the view's status reports staleness, never "the rows are gone", so an operator has nothing to go on,
   * and for a MANUAL view there may be no later refresh to put them back.
   */
  @Test
  void aFailedRefreshOnAnIndexedViewLeavesThePreviousSnapshotIntact() {
    createSourceWith(5);
    createView("FailedRefreshView");
    createIndexOn("FailedRefreshView", "NOTUNIQUE");

    assertThat(countOf("FailedRefreshView")).isEqualTo(5);

    // More source rows, so a successful refresh would be visibly different from the snapshot on disk.
    insertSourceRows(5, 3);

    failRefreshOnRecord("FailedRefreshView", 3);

    assertThat(countOf("FailedRefreshView"))
        .as("a failed refresh must preserve the previous snapshot, not empty the view")
        .isEqualTo(5);
    assertThat(idsOf("FailedRefreshView")).containsExactly(0L, 1L, 2L, 3L, 4L);
    assertThat(idsFromIndexOf("FailedRefreshView"))
        .as("and the index must still agree with the records")
        .containsExactly(0L, 1L, 2L, 3L, 4L);

    // ...and the view is still refreshable afterwards.
    database.getSchema().getMaterializedView("FailedRefreshView").refresh();
    assertThat(countOf("FailedRefreshView")).isEqualTo(8);
    assertThat(idsFromIndexOf("FailedRefreshView")).containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L);
  }

  /**
   * The same guarantee for the other trigger: a view bigger than one truncate batch, with no index in play.
   */
  @Test
  void aFailedRefreshOnAViewLargerThanOneTruncateBatchLeavesThePreviousSnapshotIntact() {
    final int rows = GlobalConfiguration.TRUNCATE_BATCH_SIZE.getValueAsInteger() + 100;
    createSourceWith(rows);
    createView("LargeFailedRefreshView");

    assertThat(countOf("LargeFailedRefreshView")).isEqualTo(rows);

    insertSourceRows(rows, 3);
    failRefreshOnRecord("LargeFailedRefreshView", 3);

    assertThat(countOf("LargeFailedRefreshView"))
        .as("a failed refresh must preserve the previous snapshot, not empty the view")
        .isEqualTo(rows);
  }

  /**
   * The visibility half: for the whole runtime of the defining query - which for a view worth materialising is
   * exactly the expensive case, and for a PERIODIC view is every tick - a concurrent reader must keep seeing the
   * previous snapshot, never a prefix of the new one and never zero rows.
   */
  @Test
  void aConcurrentReaderNeverSeesTheViewPartiallyBuilt() throws Exception {
    createSourceWith(5);
    createView("VisibilityView");
    createIndexOn("VisibilityView", "NOTUNIQUE");

    assertThat(countOf("VisibilityView")).isEqualTo(5);
    insertSourceRows(5, 3);

    final CountDownLatch refreshReachedFirstInsert = new CountDownLatch(1);
    final CountDownLatch readerObserved = new CountDownLatch(1);
    final DocumentType backing = database.getSchema().getType("VisibilityView");
    final AtomicInteger created = new AtomicInteger();
    final BeforeRecordCreateListener pause = record -> {
      if (created.incrementAndGet() == 1) {
        refreshReachedFirstInsert.countDown();
        try {
          readerObserved.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return true;
    };
    backing.getEvents().registerListener(pause);

    final AtomicReference<Throwable> refreshFailure = new AtomicReference<>();
    final Thread refresher = new Thread(() -> {
      try {
        database.getSchema().getMaterializedView("VisibilityView").refresh();
      } catch (final Throwable e) {
        refreshFailure.set(e);
      }
    }, "mv-refresh-under-test");
    refresher.start();

    final long observed;
    try {
      assertThat(refreshReachedFirstInsert.await(30, TimeUnit.SECONDS))
          .as("the refresh reached its first insert").isTrue();
      observed = countOf("VisibilityView");
    } finally {
      readerObserved.countDown();
      refresher.join(TimeUnit.SECONDS.toMillis(30));
      backing.getEvents().unregisterListener(pause);
    }

    assertThat(refreshFailure.get()).isNull();
    assertThat(observed)
        .as("a reader must see the previous snapshot for the whole refresh, not an empty or half-built view")
        .isEqualTo(5);
    assertThat(countOf("VisibilityView")).as("and the finished refresh is visible").isEqualTo(8);
  }

  /**
   * A UNIQUE index on the backing type is the case where clearing the view and repopulating it inside one
   * transaction has to survive the same key being removed and re-added before the uniqueness check runs.
   */
  @Test
  void aViewWithAUniqueIndexRefreshesRepeatedly() {
    createSourceWith(5);
    createView("UniqueIndexView");
    createIndexOn("UniqueIndexView", "UNIQUE");

    for (int pass = 0; pass < 3; pass++) {
      database.getSchema().getMaterializedView("UniqueIndexView").refresh();
      assertThat(idsOf("UniqueIndexView")).containsExactly(0L, 1L, 2L, 3L, 4L);
      assertThat(idsFromIndexOf("UniqueIndexView")).containsExactly(0L, 1L, 2L, 3L, 4L);
    }

    insertSourceRows(5, 2);
    database.getSchema().getMaterializedView("UniqueIndexView").refresh();
    assertThat(idsFromIndexOf("UniqueIndexView")).containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L);
  }

  private void failRefreshOnRecord(final String viewName, final int failOnNthCreate) {
    final DocumentType backing = database.getSchema().getType(viewName);
    final AtomicInteger created = new AtomicInteger();
    final BeforeRecordCreateListener boom = record -> {
      if (created.incrementAndGet() == failOnNthCreate)
        throw new IllegalStateException("simulated failure part way through the repopulate");
      return true;
    };
    backing.getEvents().registerListener(boom);
    try {
      assertThatThrownBy(() -> database.getSchema().getMaterializedView(viewName).refresh())
          .as("the failure still reaches the caller")
          .isInstanceOf(IllegalStateException.class);
    } finally {
      backing.getEvents().unregisterListener(boom);
    }
  }

  private void createIndexOn(final String viewName, final String uniqueness) {
    database.command("sql", "CREATE PROPERTY `" + viewName + "`.id INTEGER").close();
    database.command("sql", "CREATE INDEX ON `" + viewName + "` (id) " + uniqueness).close();
  }

  private void createView(final String viewName) {
    database.transaction(() -> database.getSchema().buildMaterializedView()
        .withName(viewName)
        .withQuery("SELECT id FROM RefreshSource ORDER BY id")
        .create());
  }

  private void createSourceWith(final int rows) {
    database.transaction(() -> database.getSchema().createDocumentType("RefreshSource"));
    insertSourceRows(0, rows);
  }

  private void insertSourceRows(final int from, final int count) {
    database.transaction(() -> {
      for (int i = from; i < from + count; i++)
        database.newDocument("RefreshSource").set("id", i).save();
    });
  }

  private long countOf(final String viewName) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM `" + viewName + "`")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private List<Long> idsOf(final String viewName) {
    final List<Long> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM `" + viewName + "` ORDER BY id")) {
      while (rs.hasNext())
        ids.add(((Number) rs.next().getProperty("id")).longValue());
    }
    return ids;
  }

  /**
   * Reads through the index rather than the buckets, so an index left disagreeing with the records - stale entries
   * for rows that were rolled back, missing entries for rows that survived - cannot pass as correct data. The plan
   * is asserted as well as the rows: without that, a planner change that stopped choosing the index would silently
   * turn this into a second bucket scan, and the test would keep passing while checking nothing new.
   */
  private List<Long> idsFromIndexOf(final String viewName) {
    final String query = "SELECT id FROM `" + viewName + "` WHERE id >= 0 ORDER BY id";
    final List<Long> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 3))
          .as("the assertion is only worth making if it is served by the index")
          .contains("FETCH FROM INDEX");
      while (rs.hasNext())
        ids.add(((Number) rs.next().getProperty("id")).longValue());
    }
    return ids;
  }
}
