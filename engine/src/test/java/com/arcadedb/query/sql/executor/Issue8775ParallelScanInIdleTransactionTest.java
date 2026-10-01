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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8775: a filtered scan ran on the calling thread as soon as a transaction was active, even one that had
 * written nothing. The workers of a parallel scan read the committed pages, which is exactly what such a transaction
 * sees, so they may serve it. Once the transaction has written anything, or pins the pages it reads
 * (REPEATABLE_READ), the scan stays on the calling thread, which sees the transaction's own changes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8775ParallelScanInIdleTransactionTest extends TestHelper {
  private static final int    ROWS   = 20_000;
  private static final String QUERY  = "SELECT count(*) AS n FROM E WHERE grp = 5";
  private static final String CYPHER = "MATCH (e:E) WHERE e.grp = 5 RETURN count(*) AS n";

  private long expected;

  @Override
  protected void beginTest() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    database.getSchema().createVertexType("E", 4);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex("E").set("id", i, "grp", i % 100).save();
    });
    expected = ROWS / 100;
  }

  @Test
  void idleTransactionScansInParallel() {
    database.begin();
    try {
      assertThat(sqlPlan()).contains("(parallel)");
      assertThat(cypherPlan()).contains("[parallel]");
      assertThat(count()).isEqualTo(expected);
    } finally {
      database.rollback();
    }
  }

  @Test
  void idleTransactionAfterAReadScansInParallel() {
    database.begin();
    try {
      assertThat(count()).isEqualTo(expected);
      assertThat(sqlPlan()).contains("(parallel)");
    } finally {
      database.rollback();
    }
  }

  @Test
  void transactionThatWroteStaysSequentialAndSeesItsWrites() {
    database.begin();
    try {
      database.newVertex("E").set("id", -1, "grp", 5).save();
      assertThat(sqlPlan()).doesNotContain("(parallel)");
      assertThat(cypherPlan()).doesNotContain("[parallel]");
      assertThat(count()).isEqualTo(expected + 1);
    } finally {
      database.rollback();
    }
  }

  @Test
  void transactionThatCreatedEdgesStaysSequential() {
    database.getSchema().createEdgeType("Link");
    database.getSchema().buildEdgeType().withName("OneWay").withBidirectional(false).create();
    database.begin();
    try {
      final Vertex c = database.iterateType("E", false).next().asVertex();
      final Vertex d = database.iterateType("E", false).next().asVertex();
      assertThat(sqlPlan()).contains("(parallel)");
      c.newEdge("Link", d).save();
      assertThat(sqlPlan()).doesNotContain("(parallel)");
    } finally {
      database.rollback();
    }
    database.begin();
    try {
      final Vertex c = database.iterateType("E", false).next().asVertex();
      final Vertex d = database.iterateType("E", false).next().asVertex();
      c.newEdge("OneWay", d).save();
      assertThat(sqlPlan()).doesNotContain("(parallel)");
    } finally {
      database.rollback();
    }
  }

  @Test
  void transactionThatAppendedEdgesWithMergeEnabledStaysSequential() {
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE, true);
    database.getSchema().createEdgeType("Merged");
    database.begin();
    try {
      final Vertex c = database.iterateType("E", false).next().asVertex();
      final Vertex d = database.iterateType("E", false).next().asVertex();
      assertThat(sqlPlan()).contains("(parallel)");
      c.newEdge("Merged", d).save();
      assertThat(sqlPlan()).doesNotContain("(parallel)");
    } finally {
      database.rollback();
    }
  }

  @Test
  void transactionThatCreatedALightweightEdgeStaysSequential() {
    database.getSchema().buildEdgeType().withName("Light").withLightweight(true).withBidirectional(false).create();
    database.begin();
    try {
      final Vertex c = database.iterateType("E", false).next().asVertex();
      final Vertex d = database.iterateType("E", false).next().asVertex();
      c.newEdge("Light", d).save();
      assertThat(sqlPlan()).doesNotContain("(parallel)");
    } finally {
      database.rollback();
    }
  }

  @Test
  void transactionThatUpdatedStaysSequentialAndSeesItsUpdate() {
    database.begin();
    try {
      database.command("sql", "UPDATE E SET grp = 5 WHERE id = 1");
      assertThat(sqlPlan()).doesNotContain("(parallel)");
      assertThat(count()).isEqualTo(expected + 1);
    } finally {
      database.rollback();
    }
  }

  @Test
  void transactionThatDeletedStaysSequential() {
    database.begin();
    try {
      database.command("sql", "DELETE FROM E WHERE id = 5");
      assertThat(sqlPlan()).doesNotContain("(parallel)");
      assertThat(count()).isEqualTo(expected - 1);
    } finally {
      database.rollback();
    }
  }

  @Test
  void repeatableReadStaysSequential() {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(sqlPlan()).doesNotContain("(parallel)");
      assertThat(count()).isEqualTo(expected);
    } finally {
      database.rollback();
    }
  }

  @Test
  void writeDuringIterationDoesNotChangeTheScanThatAlreadyStarted() {
    // A scan is decided at its first pull, and its workers read committed pages: what the transaction writes while it
    // drains is not fed back into it (no statement snapshot, though: a foreign commit can reach pages read later)
    database.begin();
    try {
      long rows = 0;
      try (final ResultSet rs = database.query("sql", "SELECT FROM E WHERE grp = 5")) {
        while (rs.hasNext()) {
          rs.next();
          if (rows++ == 0)
            database.newVertex("E").set("id", -1, "grp", 5).save();
        }
      }
      assertThat(rows).isEqualTo(expected);
      // the next query starts after the write: it sees it, on the calling thread
      assertThat(count()).isEqualTo(expected + 1);
    } finally {
      database.rollback();
    }
  }

  @Test
  void callerDoesNotScanUnitsItselfOnceTheTransactionHasWritten() {
    // Units the caller would claim read through the transaction and would see its writes, unlike the workers' units:
    // every row of a scan must come from the same view, so a transaction that wrote since the first pull leaves all
    // the units to the workers
    database.begin();
    try {
      long rows = 0;
      try (final ResultSet rs = database.query("sql", "SELECT FROM E WHERE grp = 5")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          rows++;
          // the update moves every row out of the filter: a unit the caller scanned through the transaction would miss them
          if (rows == 1)
            database.command("sql", "UPDATE E SET grp = 7 WHERE grp = 5");
          assertThat(row.<Integer>getProperty("grp")).isEqualTo(5);
        }
      }
      assertThat(rows).isEqualTo(expected);
    } finally {
      database.rollback();
    }
  }

  @Test
  void rowsDeletedDuringIterationAreNotHandedOut() {
    // The workers read committed pages, which still hold a row the transaction has deleted: the scan drops it on its way
    // out, as the sequential scan, which reads through the transaction, would not have returned it
    database.begin();
    try {
      long rows = 0;
      try (final ResultSet rs = database.query("sql", "SELECT FROM E WHERE grp = 5")) {
        while (rs.hasNext()) {
          rs.next();
          if (rows++ == 0)
            database.command("sql", "DELETE FROM E WHERE grp = 5");
        }
      }
      // only the row handed out before the delete
      assertThat(rows).isEqualTo(1);
      assertThat(count()).isZero();
    } finally {
      database.rollback();
    }
  }

  @Test
  void deletingEveryReturnedRowWhileIteratingLeavesNothingBehind() {
    // the common loop: iterate a query and delete each record it returns, through the record API
    database.begin();
    try {
      long deleted = 0;
      try (final ResultSet rs = database.query("sql", "SELECT FROM E WHERE grp = 5")) {
        while (rs.hasNext()) {
          rs.next().getRecord().get().delete();
          deleted++;
        }
      }
      assertThat(deleted).isEqualTo(expected);
      assertThat(count()).isZero();
    } finally {
      database.rollback();
    }
  }

  @Test
  void rowsDeletedAfterTheFirstPullCanBeTouchedAgainWithoutFailing() {
    database.begin();
    try {
      long rows = 0;
      long touched = 0;
      try (final ResultSet rs = database.query("sql", "SELECT FROM E WHERE grp = 5")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          if (rows++ == 0)
            database.command("sql", "DELETE FROM E WHERE grp = 5");
          // a stale row: the update of a record the transaction deleted finds nothing and does not fail
          try (final ResultSet update = database.command("sql", "UPDATE " + row.getIdentity().get() + " SET touched = true")) {
            touched += ((Number) update.next().getProperty("count")).longValue();
          }
        }
      }
      assertThat(rows).isEqualTo(1);
      // the first row was deleted by the statement before it was touched: every update finds nothing
      assertThat(touched).isZero();
    } finally {
      database.rollback();
    }
  }

  @Test
  void unitsAsLargeAsTheWholeBucketStayCorrectInATransaction() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 0);
    database.begin();
    try {
      assertThat(count()).isEqualTo(expected);
      database.newVertex("E").set("id", -1, "grp", 5).save();
      assertThat(count()).isEqualTo(expected + 1);
    } finally {
      database.rollback();
    }
  }

  @Test
  void limitInATransactionReturnsItsRows() {
    database.begin();
    try {
      try (final ResultSet rs = database.query("sql", "SELECT FROM E WHERE grp = 5 LIMIT 3")) {
        long n = 0;
        while (rs.hasNext()) {
          rs.next();
          n++;
        }
        assertThat(n).isEqualTo(3);
      }
    } finally {
      database.rollback();
    }
  }

  @Test
  void rowUpdatedDuringIterationComesBackAsCommitted() {
    database.begin();
    try {
      long rows = 0;
      try (final ResultSet rs = database.query("sql", "SELECT FROM E WHERE grp = 5")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          if (rows++ == 0)
            database.command("sql", "UPDATE E SET grp = 7 WHERE grp = 5");
          // pinned: the scan hands out the committed content, never the transaction's update
          assertThat(row.<Integer>getProperty("grp")).isEqualTo(5);
        }
      }
      assertThat(rows).isEqualTo(expected);
    } finally {
      database.rollback();
    }
  }

  @Test
  void transactionThatOnlyReadKeepsItsParallelScan() {
    database.begin();
    try {
      assertThat(((DatabaseInternal) database).getTransaction().isReadOnlyView()).isTrue();
      try (final ResultSet rs = database.query("sql", "SELECT FROM E WHERE id = 1")) {
        assertThat(rs.hasNext()).isTrue();
      }
      assertThat(((DatabaseInternal) database).getTransaction().isReadOnlyView()).isTrue();
      assertThat(sqlPlan()).contains("(parallel)");
    } finally {
      database.rollback();
    }
  }

  @Test
  void indexRangeInATransactionAnswersTheSameAsOutsideOne() {
    database.command("sql", "CREATE PROPERTY E.grp INTEGER");
    database.command("sql", "CREATE INDEX ON E (grp) NOTUNIQUE");
    final String range = "SELECT count(*) AS n FROM E WHERE grp >= 10 AND grp < 90";
    final long outside = single(range);
    assertThat(outside).isEqualTo(expected * 80);

    database.begin();
    try {
      assertThat(single(range)).isEqualTo(outside);
      database.newVertex("E").set("id", -1, "grp", 50).save();
      // dirty: sequential through the transaction, and it sees its own write
      assertThat(single(range)).isEqualTo(outside + 1);
    } finally {
      database.rollback();
    }
  }

  private long single(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  @Test
  void databaseSettingTurnsTheParallelScanOffInATransaction() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_IN_TRANSACTION, false);
    database.begin();
    try {
      assertThat(sqlPlan()).doesNotContain("(parallel)");
      assertThat(count()).isEqualTo(expected);
    } finally {
      database.rollback();
    }
    // outside a transaction the scan is still parallel
    assertThat(sqlPlan()).contains("(parallel)");
  }

  @Test
  void transactionOverrideWinsOverTheSetting() {
    database.begin();
    try {
      ((DatabaseInternal) database).getTransaction().setParallelScanForThisTransaction(false);
      assertThat(sqlPlan()).doesNotContain("(parallel)");
      assertThat(cypherPlan()).doesNotContain("[parallel]");
    } finally {
      database.rollback();
    }
    // the override does not outlive its transaction
    database.begin();
    try {
      assertThat(sqlPlan()).contains("(parallel)");
    } finally {
      database.rollback();
    }

    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_IN_TRANSACTION, false);
    database.begin();
    try {
      ((DatabaseInternal) database).getTransaction().setParallelScanForThisTransaction(true);
      assertThat(sqlPlan()).contains("(parallel)");
    } finally {
      database.rollback();
    }
  }

  @Test
  void selfFeedingUpdateInIdleTransactionMatchesTheSequentialAnswer() {
    final String update = "UPDATE E SET grp = 5 WHERE grp = 6";
    final long parallelUpdated = updatedIn(update, true);
    final long sequentialUpdated = updatedIn(update, false);
    assertThat(parallelUpdated).isEqualTo(expected).isEqualTo(sequentialUpdated);
  }

  @Test
  void deleteInIdleTransactionMatchesTheSequentialAnswer() {
    assertThat(deletedIn(true)).isEqualTo(expected).isEqualTo(deletedIn(false));
  }

  @Test
  void commitFromAnotherThreadIsSeenByTheIdleTransaction() throws InterruptedException {
    database.begin();
    try {
      assertThat(count()).isEqualTo(expected);
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread writer = new Thread(() -> {
        try {
          database.begin();
          database.newVertex("E").set("id", -2, "grp", 5).save();
          database.commit();
        } catch (final Throwable e) {
          failure.set(e);
        }
      });
      writer.start();
      writer.join();
      assertThat(failure.get()).isNull();
      assertThat(sqlPlan()).contains("(parallel)");
      assertThat(count()).isEqualTo(expected + 1);
    } finally {
      database.rollback();
    }
  }

  private long updatedIn(final String update, final boolean parallel) {
    return inRolledBackTransaction(parallel, () -> {
      try (final ResultSet rs = database.command("sql", update)) {
        return ((Number) rs.next().getProperty("count")).longValue();
      }
    });
  }

  private long deletedIn(final boolean parallel) {
    return inRolledBackTransaction(parallel, () -> {
      try (final ResultSet rs = database.command("sql", "DELETE FROM E WHERE grp = 5")) {
        return ((Number) rs.next().getProperty("count")).longValue();
      }
    });
  }

  private long inRolledBackTransaction(final boolean parallel, final LongSupplier body) {
    final boolean previous = database.getConfiguration().getValueAsBoolean(GlobalConfiguration.QUERY_PARALLEL_SCAN);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, parallel);
    try {
      database.begin();
      try {
        return body.getAsLong();
      } finally {
        database.rollback();
      }
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, previous);
    }
  }

  private long count() {
    try (final ResultSet rs = database.query("sql", QUERY)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  private String sqlPlan() {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + QUERY)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  private String cypherPlan() {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + CYPHER)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }
}
