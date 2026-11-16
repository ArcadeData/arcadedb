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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucketTestAccess;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11755: the index-side reads against one committed state ({@link ConsistentKeyLookup} and the point lookup, re-read and
 * reconcile of {@code GetValueFromIndexEntryStep}) waited for a publication through the page-manager lock and ran their last attempt
 * under it. The replicated apply of a replica publishes without that lock (#10941), so on a replica the wait returned at once and the
 * last attempt read with a publication open, pairing index entries and records of two commits. A publication held open here is what
 * the replicated apply does ({@code TransactionManager.applyChangesInternal}).
 */
class IndexLookupReplicaPublicationTest extends TestHelper {
  private static final int RECORDS = 100;

  private boolean danglingEntryLeft;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Item");
      type.createProperty("k", Type.INTEGER);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k");
      for (int i = 0; i < RECORDS; i++)
        database.newDocument("Item").set("k", i).save();
    });
  }

  @Override
  protected void endTest() {
    if (danglingEntryLeft)
      // the dangling entry the reconcile tests leave on purpose is pruned, so the integrity check of the teardown passes
      database.command("sql", "CHECK DATABASE FIX").close();
  }

  private DatabaseInternal internal() {
    return (DatabaseInternal) database;
  }

  private TypeIndex index() {
    return (TypeIndex) database.getSchema().getIndexByName("Item[k]");
  }

  /** Runs {@code action} with a publication held open the whole time, as a replicated apply that does not end would. */
  private void withPublicationOpen(final Runnable action) {
    LocalBucketTestAccess.beginPublication(internal());
    try {
      action.run();
    } finally {
      LocalBucketTestAccess.endPublication(internal());
    }
  }

  /**
   * Opens a publication and ends it from another thread a little later, as a replicated apply that ends while the read waits; returns
   * the thread to join.
   */
  private Thread publicationEndingSoon() throws InterruptedException {
    LocalBucketTestAccess.beginPublication(internal());
    final CountDownLatch started = new CountDownLatch(1);
    final Thread ender = new Thread(() -> {
      started.countDown();
      try {
        Thread.sleep(30);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        LocalBucketTestAccess.endPublication(internal());
      }
    });
    ender.start();
    assertThat(started.await(30, TimeUnit.SECONDS)).isTrue();
    return ender;
  }

  // ---- ConsistentKeyLookup ----

  /**
   * Regression test for issue #11755: with a publication open for the whole lookup, no attempt and no last resort reads one
   * committed state, so the lookup must not answer. Before the fix the wait returned at once on the open publication and the last
   * attempt read under the page-manager lock, which the publication does not take, and returned what it read.
   */
  @Test
  void consistentKeyLookupDoesNotAnswerWhileAPublicationStaysOpen() {
    withPublicationOpen(() -> assertThatThrownBy(() -> ConsistentKeyLookup.lookup(internal(), index(), List.<Object[]>of(new Object[] { 7 })))
        .isInstanceOf(ConcurrentModificationException.class));
  }

  /** Regression test for issue #11755: a publication that ends while the lookup waits for it is waited out, and the lookup answers. */
  @Test
  void consistentKeyLookupWaitsOutAPublicationThatEnds() throws Exception {
    final Thread ender = publicationEndingSoon();
    try {
      final List<IndexCursorEntry> entries = ConsistentKeyLookup.lookup(internal(), index(), List.<Object[]>of(new Object[] { 7 }));
      assertThat(entries).hasSize(1);
      assertThat(entries.getFirst().record.asDocument().getInteger("k")).isEqualTo(7);
    } finally {
      ender.join(30_000);
    }
    assertThat(internal().getPageManager().getPublicationSequence() & 1).isZero();
  }

  /** Regression test for issue #11755: {@link ConsistentKeyLookup#getForQuery} goes through the same lookup. */
  @Test
  void consistentKeyLookupForQueryDoesNotAnswerWhileAPublicationStaysOpen() {
    withPublicationOpen(() -> assertThatThrownBy(() -> ConsistentKeyLookup.getForQuery(database, index(), new Object[] { 7 }))
        .isInstanceOf(ConcurrentModificationException.class));
  }

  // ---- GetValueFromIndexEntryStep: point lookup ----

  /**
   * Regression test for issue #11755: the SQL point lookup read against one committed state ({@code loadPointLookup}) must not answer
   * while a publication stays open. Before the fix it returned the rows its last attempt read under the page-manager lock.
   */
  @Test
  void sqlPointLookupDoesNotAnswerWhileAPublicationStaysOpen() {
    withPublicationOpen(() -> assertThatThrownBy(() -> collect("SELECT FROM Item WHERE k = 7"))
        .isInstanceOf(ConcurrentModificationException.class)
        .hasStackTraceContaining("loadPointLookup"));
  }

  /** Regression test for issue #11755: the SQL point lookup waits out a publication that ends, and answers. */
  @Test
  void sqlPointLookupWaitsOutAPublicationThatEnds() throws Exception {
    final Thread ender = publicationEndingSoon();
    final List<Result> rows;
    try {
      rows = collect("SELECT FROM Item WHERE k = 7");
    } finally {
      ender.join(30_000);
    }
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<Integer>getProperty("k")).isEqualTo(7);
  }

  // ---- GetValueFromIndexEntryStep: reconcile of a range-scan entry without a record ----

  /**
   * Regression test for issue #11755: a range-scan entry whose record is gone is looked up again against one committed state
   * ({@code reconcile}); with a publication open the whole time it must not be settled. Before the fix the last attempt ran under
   * the page-manager lock and settled it with whatever the open publication had put in the cache.
   */
  @Test
  void rangeScanReconcileDoesNotSettleWhileAPublicationStaysOpen() {
    removeRecordLeavingItsEntry(12);
    withPublicationOpen(() -> assertThatThrownBy(() -> collect("SELECT FROM Item WHERE k >= 10 AND k <= 14"))
        .isInstanceOf(ConcurrentModificationException.class)
        .hasStackTraceContaining("reconcile"));
  }

  /** Regression test for issue #11755: the reconcile waits out a publication that ends, and drops the dangling entry. */
  @Test
  void rangeScanReconcileWaitsOutAPublicationThatEnds() throws Exception {
    removeRecordLeavingItsEntry(12);
    final Thread ender = publicationEndingSoon();
    final List<Result> rows;
    try {
      rows = collect("SELECT FROM Item WHERE k >= 10 AND k <= 14");
    } finally {
      ender.join(30_000);
    }
    final List<Integer> keys = new ArrayList<>();
    for (final Result row : rows)
      keys.add(row.getProperty("k"));
    assertThat(keys).containsExactlyInAnyOrder(10, 11, 13, 14);
  }

  /** Control: without a publication the range scan drops the dangling entry, so the tests above reach the reconcile. */
  @Test
  void rangeScanWithoutPublicationDropsTheDanglingEntry() {
    removeRecordLeavingItsEntry(12);
    final List<Integer> keys = new ArrayList<>();
    for (final Result row : collect("SELECT FROM Item WHERE k >= 10 AND k <= 14"))
      keys.add(row.getProperty("k"));
    assertThat(keys).containsExactlyInAnyOrder(10, 11, 13, 14);
  }

  /** Deletes the record of key {@code k} from its bucket alone, leaving its index entry behind as a dangling one. */
  private void removeRecordLeavingItsEntry(final int k) {
    final RID rid;
    try (final ResultSet rs = database.query("sql", "SELECT FROM Item WHERE k = ?", k)) {
      rid = rs.next().getIdentity().orElseThrow();
    }
    database.transaction(() -> database.getSchema().getBucketById(rid.getBucketId()).deleteRecord(rid));
    danglingEntryLeft = true;
    try (final IndexCursor cursor = index().get(new Object[] { k })) {
      assertThat(cursor.hasNext()).as("the entry of key " + k + " must survive the record").isTrue();
    }
  }

  private List<Result> collect(final String sql) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }
}
