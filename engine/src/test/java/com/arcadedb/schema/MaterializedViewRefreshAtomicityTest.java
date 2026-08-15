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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.engine.FileManager;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
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

    final CountDownLatch refreshReachedFirstWrite = new CountDownLatch(1);
    final CountDownLatch readerObserved = new CountDownLatch(1);
    final DocumentType backing = database.getSchema().getType("VisibilityView");
    final AtomicInteger written = new AtomicInteger();
    // On the update, not the create: the refresh writes the new rows over the previous snapshot's records and only
    // creates the shortfall, so the first write of a pass is an update whenever the view already had a row.
    final BeforeRecordUpdateListener pause = record -> {
      if (written.incrementAndGet() == 1) {
        refreshReachedFirstWrite.countDown();
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
      assertThat(refreshReachedFirstWrite.await(30, TimeUnit.SECONDS))
          .as("the refresh reached its first write").isTrue();
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
   * A UNIQUE index on the backing type is where rewriting the view inside one transaction has to survive the same
   * key being written over the record that already holds it, and - once the view outgrows the previous snapshot -
   * being added on a new record in the same transaction that carries the rest.
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

  /**
   * Rewriting in place pairs each new row with a previous record BY POSITION, so a defining query whose order moves
   * between passes makes the unique keys change hands: the record that held key 4 is handed key 0, while the record
   * that held key 0 is handed key 4, all inside one transaction. Under the truncate this could not arise - the index
   * was dropped before any row was touched and rebuilt only once every row was in place - so it is new ground, and a
   * spurious {@code DuplicatedKeyException} here would be exactly the kind of failure that only shows up on the one
   * view whose query has no stable sort.
   * <p>
   * The permutation is driven rather than hoped for: the view sorts on a column the test rewrites between passes, so
   * every one of the five keys lands on a different record than it did before.
   */
  @Test
  void aViewWithAUniqueIndexSurvivesItsRowsChangingOrderBetweenRefreshes() {
    database.transaction(() -> database.getSchema().createDocumentType("RefreshSource"));
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.newDocument("RefreshSource").set("id", i).set("ord", i).save();
    });
    database.transaction(() -> database.getSchema().buildMaterializedView()
        .withName("ReorderedView")
        .withQuery("SELECT id FROM RefreshSource ORDER BY ord")
        .create());
    createIndexOn("ReorderedView", "UNIQUE");

    assertThat(idsOf("ReorderedView")).containsExactly(0L, 1L, 2L, 3L, 4L);

    // Same five keys, reversed order: every record is handed a key that another record still holds.
    for (int pass = 0; pass < 3; pass++) {
      final int direction = pass;
      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT FROM RefreshSource")) {
          while (rs.hasNext()) {
            final MutableDocument doc = rs.next().getRecord().orElseThrow().asDocument().modify();
            final int id = ((Number) doc.get("id")).intValue();
            doc.set("ord", direction % 2 == 0 ? 4 - id : id).save();
          }
        }
      });

      database.getSchema().getMaterializedView("ReorderedView").refresh();

      // In record order, not sorted: this is what proves the keys actually changed hands rather than the test
      // asserting a permutation it never provoked.
      assertThat(idsInRecordOrderOf("ReorderedView"))
          .as("pass %d handed every record a key another record was still holding", pass)
          .containsExactlyElementsOf(direction % 2 == 0 ?
              List.of(4L, 3L, 2L, 1L, 0L) :
              List.of(0L, 1L, 2L, 3L, 4L));
      assertThat(idsOf("ReorderedView")).as("pass %d", pass).containsExactly(0L, 1L, 2L, 3L, 4L);
      assertThat(idsFromIndexOf("ReorderedView"))
          .as("the unique index agrees with the records after pass %d", pass)
          .containsExactly(0L, 1L, 2L, 3L, 4L);
    }
  }

  /**
   * The cost the atomicity is not allowed to have. The truncate this replaced dropped every index on the backing
   * type and rebuilt it empty, so an indexed view's index never grew however often it refreshed. Clearing the view
   * with a delete instead would give every row a new RID on every pass, and {@code TransactionIndexContext}
   * collapses a REMOVE followed by an ADD only on the same (key, RID) - so each pass would leave a full set of real
   * tombstones behind. Measured over 120 passes of this fixture: 256KB flat for the truncate, 256KB to 3.9MB and
   * still climbing for delete-and-recreate. Writing the rows over the previous snapshot's records keeps the key
   * values unchanged, which {@code DocumentIndexer.updateDocument} skips outright, so nothing is written to the
   * index at all.
   * <p>
   * Measured on the file size rather than the mutable page count: auto-compaction saws the page count back down at
   * {@code arcadedb.indexCompactionMinPagesSchedule}, so it hides the growth by folding it into ever more compacted
   * series. The bound of twice the first pass's size is deliberately loose - this guards against the growth pattern
   * coming back, it does not measure it - and delete-and-recreate is already past it by pass 60.
   */
  @Test
  @Tag("slow")
  void refreshingAnIndexedViewRepeatedlyDoesNotGrowTheIndex() throws Exception {
    createSourceWith(2000);
    createView("SoakView");
    createIndexOn("SoakView", "NOTUNIQUE");

    final long afterFirst = indexBytesOf("SoakView");
    assertThat(afterFirst).as("the index is on disk to begin with").isPositive();

    for (int pass = 0; pass < 60; pass++)
      database.getSchema().getMaterializedView("SoakView").refresh();

    assertThat(idsFromIndexOf("SoakView")).hasSize(2000);
    assertThat(indexBytesOf("SoakView"))
        .as("60 refreshes of an unchanged indexed view must not grow its index")
        .isLessThanOrEqualTo(afterFirst * 2);
  }

  private long indexBytesOf(final String viewName) throws Exception {
    final FileManager files = ((DatabaseInternal) database).getFileManager();
    long bytes = 0;
    for (final TypeIndex index : database.getSchema().getType(viewName).getAllIndexes(false))
      for (final IndexInternal bucketIndex : index.getIndexesOnBuckets())
        for (final int fileId : bucketIndex.getFileIds())
          bytes += files.getFile(fileId).getSize();
    return bytes;
  }

  /**
   * Fails the refresh on its {@code failOnNthWrite}-th write to the backing type. Counted over creates AND updates,
   * so the failure lands part way through the pass whether the row is written over one of the previous snapshot's
   * records or created because the new snapshot is longer - and stays there if that balance ever changes.
   */
  private void failRefreshOnRecord(final String viewName, final int failOnNthWrite) {
    final DocumentType backing = database.getSchema().getType(viewName);
    final AtomicInteger written = new AtomicInteger();
    final BeforeRecordCreateListener boomOnCreate = record -> {
      if (written.incrementAndGet() == failOnNthWrite)
        throw new IllegalStateException("simulated failure part way through the refresh");
      return true;
    };
    final BeforeRecordUpdateListener boomOnUpdate = record -> {
      if (written.incrementAndGet() == failOnNthWrite)
        throw new IllegalStateException("simulated failure part way through the refresh");
      return true;
    };
    backing.getEvents().registerListener(boomOnCreate);
    backing.getEvents().registerListener(boomOnUpdate);
    try {
      assertThatThrownBy(() -> database.getSchema().getMaterializedView(viewName).refresh())
          .as("the failure still reaches the caller")
          .isInstanceOf(IllegalStateException.class);
    } finally {
      backing.getEvents().unregisterListener(boomOnCreate);
      backing.getEvents().unregisterListener(boomOnUpdate);
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

  /** The ids as the records hold them, unsorted, so a permutation of the rows is visible rather than normalised away. */
  private List<Long> idsInRecordOrderOf(final String viewName) {
    final List<Long> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM `" + viewName + "`")) {
      while (rs.hasNext())
        ids.add(((Number) rs.next().getProperty("id")).longValue());
    }
    return ids;
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
