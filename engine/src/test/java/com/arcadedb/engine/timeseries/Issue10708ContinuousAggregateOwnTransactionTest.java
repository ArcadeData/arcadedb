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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.ContinuousAggregateImpl;
import com.arcadedb.schema.ContinuousAggregateRefresher;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.MaterializedViewStatus;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10708, the continuous-aggregate refresh is not part of the caller's transaction, in either direction.
 * <ul>
 *   <li>#10688: a refresh run while a transaction is open ({@code REFRESH CONTINUOUS AGGREGATE}, {@code ContinuousAggregate.refresh()},
 *   the initial refresh of {@code CREATE CONTINUOUS AGGREGATE}) joined it, then installed and saved the watermark as if its rows had
 *   committed. A rollback of the caller's transaction took the rows back and left the watermark: the buckets behind it were lost for
 *   good, with the aggregate reported {@code VALID}. The refresh now commits in a transaction of its own.</li>
 *   <li>#10687: a write to a TIMESERIES type ({@code INSERT}, {@code DELETE}, {@code TRUNCATE TYPE}) is not part of the statement's
 *   transaction (#7410), but the refresh it owes the aggregates was registered as an after-commit callback of that transaction, so a
 *   rollback discarded it. The refresh now runs when the transaction ends, whichever way it ends.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10708ContinuousAggregateOwnTransactionTest extends TestHelper {
  private static final long   HOUR     = 3_600_000L;
  /** An hour boundary, so that the buckets of the default grid start where the test says they do. */
  private static final long   H0       = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);
  private static final String DEFINING = "SELECT ts.timeBucket('1h', ts) AS hr, sum(v) AS total FROM S GROUP BY hr";

  private void createSource() {
    database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE)");
  }

  private void createAggregate() {
    database.command("sql", "CREATE CONTINUOUS AGGREGATE A AS " + DEFINING).close();
  }

  private void insert(final long ts, final double v) {
    database.command("sql", "INSERT INTO S SET ts = ?, h = 'a', v = ?", ts, v).close();
  }

  /**
   * An append that skips the refresh every ingest path owes since #10686, so nothing refreshes the aggregate: it is left behind the
   * source.
   */
  private void appendWithoutRefresh(final long ts, final double v) throws IOException {
    ((LocalTimeSeriesType) database.getSchema().getType("S")).getEngine()
        .appendSamplesWithoutRefresh(new long[] { ts }, new Object[] { "a" }, new Object[] { v });
  }

  private ContinuousAggregateImpl aggregate() {
    return (ContinuousAggregateImpl) database.getSchema().getContinuousAggregate("A");
  }

  /** {@code SELECT FROM A}, as hour offset from H0 to total. */
  private Map<Long, Double> aggregateRows() {
    return rows("SELECT hr, total FROM A");
  }

  /** The defining query run over the source, what the aggregate has to equal. */
  private Map<Long, Double> sourceRows() {
    return rows(DEFINING);
  }

  private Map<Long, Double> rows(final String query) {
    final Map<Long, Double> result = new TreeMap<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final Double previous = result.put((DateUtils.toEpochMillis(row.getProperty("hr")) - H0) / HOUR,
            ((Number) row.getProperty("total")).doubleValue());
        assertThat(previous).as("one row per bucket").isNull();
      }
    }
    return result;
  }

  /** Four hourly samples, with the aggregate created after them: one refresh reads them all. */
  private void sourceWithFourHours() {
    createSource();
    for (int i = 0; i < 4; i++)
      insert(H0 + i * HOUR, i + 1);
  }

  /** An aggregate that is behind its source: three of four hours were appended with nothing refreshing it. */
  private void aggregateBehindItsSource() throws IOException {
    createSource();
    insert(H0, 1);
    createAggregate();
    for (int i = 1; i < 4; i++)
      appendWithoutRefresh(H0 + i * HOUR, i + 1);
    assertThat(aggregateRows()).containsExactly(Map.entry(0L, 1.0));
  }

  private void assertConverged() {
    assertThat(aggregateRows()).isEqualTo(sourceRows());
  }

  // ---- #10688: a refresh run inside an open transaction commits on its own

  @Test
  void createInsideARolledBackTransactionKeepsItsRows() {
    sourceWithFourHours();

    database.begin();
    createAggregate();
    database.rollback();

    // the aggregate is DDL and outlives the rollback, and so do the rows that vouch for its watermark
    assertThat(aggregate()).isNotNull();
    assertThat(aggregate().getStatus()).isEqualTo(MaterializedViewStatus.VALID.name());
    assertThat(aggregateRows()).containsExactly(Map.entry(0L, 1.0), Map.entry(1L, 2.0), Map.entry(2L, 3.0), Map.entry(3L, 4.0));
    assertConverged();

    // and stays converged through further refreshes and a new sample
    database.command("sql", "REFRESH CONTINUOUS AGGREGATE A").close();
    insert(H0 + 5 * HOUR, 9);
    assertConverged();
  }

  @Test
  void sqlRefreshInsideARolledBackTransactionKeepsItsRows() throws IOException {
    aggregateBehindItsSource();

    database.begin();
    database.command("sql", "REFRESH CONTINUOUS AGGREGATE A").close();
    database.rollback();

    assertConverged();
    database.command("sql", "REFRESH CONTINUOUS AGGREGATE A").close();
    assertConverged();
  }

  @Test
  void apiRefreshInsideARolledBackTransactionKeepsItsRows() throws IOException {
    aggregateBehindItsSource();

    database.begin();
    aggregate().refresh();
    database.rollback();

    assertConverged();
    aggregate().refresh();
    assertConverged();
  }

  @Test
  void refreshInsideATransactionIsCommittedBeforeTheTransactionEnds() throws IOException {
    aggregateBehindItsSource();

    database.begin();
    aggregate().refresh();
    // the caller's transaction is still the one open, untouched by the refresh's own
    assertThat(database.isTransactionActive()).isTrue();
    assertThat(((DatabaseInternal) database).getTransaction().getModifiedPages()).isZero();
    database.rollback();

    assertConverged();
  }

  @Test
  void refreshInsideACommittedTransactionConverges() throws IOException {
    aggregateBehindItsSource();

    database.transaction(() -> database.command("sql", "REFRESH CONTINUOUS AGGREGATE A").close());

    assertConverged();
  }

  @Test
  void refreshAfterTheCallerWroteKeepsTheCallersWork() throws IOException {
    aggregateBehindItsSource();
    database.command("sql", "CREATE DOCUMENT TYPE Other");

    database.begin();
    database.newDocument("Other").set("x", 1).save();
    aggregate().refresh();
    database.newDocument("Other").set("x", 2).save();
    database.commit();

    assertConverged();
    assertThat(database.countType("Other", false)).isEqualTo(2);
  }

  // ---- #10687: a TIMESERIES write refreshes its aggregates however the transaction ends

  @Test
  void insertAheadOfTheWatermarkInARolledBackTransactionRefreshes() {
    createSource();
    for (int i = 0; i < 3; i++)
      insert(H0 + i * HOUR, i + 1);
    createAggregate();
    final long before = aggregate().getRefreshCount();

    database.begin();
    insert(H0 + 3 * HOUR, 9);
    // not before the transaction ends
    assertThat(aggregate().getRefreshCount()).isEqualTo(before);
    database.rollback();

    // the sample is durable whatever the rollback (#7410), and the aggregate reflects it
    assertThat(sourceRows()).containsEntry(3L, 9.0);
    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertConverged();
  }

  @Test
  void insertBehindTheWatermarkInARolledBackTransactionRefreshes() {
    createSource();
    for (int i = 0; i < 3; i++)
      insert(H0 + i * HOUR, i + 1);
    createAggregate();

    database.begin();
    insert(H0 + 1_000, 50);
    database.rollback();

    assertThat(sourceRows()).containsEntry(0L, 51.0);
    assertConverged();
    assertThat(aggregate().getInvalidation()).isNull();
  }

  @Test
  void deleteInARolledBackTransactionRefreshes() {
    createSource();
    for (int i = 0; i < 3; i++)
      insert(H0 + i * HOUR, i + 1);
    createAggregate();

    database.begin();
    database.command("sql", "DELETE FROM S WHERE ts < ?", H0 + HOUR).close();
    database.rollback();

    // the bucket whose samples are gone is gone from the aggregate too
    assertThat(sourceRows()).doesNotContainKey(0L);
    assertConverged();
  }

  @Test
  void truncateInARolledBackTransactionRefreshes() {
    createSource();
    for (int i = 0; i < 3; i++)
      insert(H0 + i * HOUR, i + 1);
    createAggregate();

    database.begin();
    database.command("sql", "TRUNCATE TYPE S").close();
    database.rollback();

    assertThat(sourceRows()).isEmpty();
    assertConverged();
  }

  @Test
  void writesInACommittedTransactionRefreshOnceAtTheEnd() {
    createSource();
    for (int i = 0; i < 3; i++)
      insert(H0 + i * HOUR, i + 1);
    createAggregate();
    final long before = aggregate().getRefreshCount();

    database.transaction(() -> {
      insert(H0 + 1_000, 50);
      insert(H0 + 3 * HOUR, 9);
      database.command("sql", "DELETE FROM S WHERE ts >= ? AND ts < ?", H0 + HOUR, H0 + 2 * HOUR).close();
      assertThat(aggregate().getRefreshCount()).isEqualTo(before);
    });

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertConverged();
  }

  @Test
  void writesInARolledBackTransactionRefreshOnceAtTheEnd() {
    createSource();
    for (int i = 0; i < 3; i++)
      insert(H0 + i * HOUR, i + 1);
    createAggregate();
    final long before = aggregate().getRefreshCount();

    database.begin();
    insert(H0 + 1_000, 50);
    insert(H0 + 3 * HOUR, 9);
    database.rollback();

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertConverged();
  }

  /**
   * A statement that fails inside a transaction is taken back to where it began, and with it what it registered on the transaction:
   * the samples it appended before the failure are stored all the same, and still owed a refresh when the transaction ends.
   */
  @Test
  void aFailedInsertInsideATransactionStillRefreshesAtTheEnd() {
    createSource();
    insert(H0, 1);
    createAggregate();

    database.begin();
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO S CONTENT [" //
        + "{\"ts\": " + (H0 + HOUR) + ", \"h\": \"a\", \"v\": 2}," //
        + "{\"ts\": \"not a timestamp\", \"h\": \"a\", \"v\": 3}]").close()).isInstanceOf(RuntimeException.class);
    assertThat(database.isTransactionActive()).isTrue();
    database.commit();

    assertThat(sourceRows()).containsEntry(1L, 2.0);
    assertConverged();
  }

  @Test
  void theRefreshIsNotLeftOnTheContextForTheNextTransaction() {
    createSource();
    insert(H0, 1);
    createAggregate();
    database.command("sql", "CREATE DOCUMENT TYPE Other");

    database.begin();
    insert(H0 + HOUR, 2);
    database.rollback();
    final long afterRollback = aggregate().getRefreshCount();

    assertThat(((DatabaseInternal) database).getTransaction()
        .hasAfterCompletionCallbackKey(ContinuousAggregateRefresher.refreshCallbackKey("A"))).isFalse();

    // a commit that touches nothing of S must not refresh the aggregate
    database.transaction(() -> database.newDocument("Other").set("x", 1).save());
    assertThat(aggregate().getRefreshCount()).isEqualTo(afterRollback);
  }

  // ---- the after-completion callback, on its own

  /**
   * The callback runs with the transaction ended, and a transaction it begins is one of its own, on a context of its own: the
   * context that just concluded keeps its commit count, which the caller of a failed commit reads next (#7916).
   */
  @Test
  void anAfterCompletionCallbackWritesInATransactionOfItsOwn() {
    database.command("sql", "CREATE DOCUMENT TYPE Other");

    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    final long commitsBefore = tx.getCommitCount();
    final AtomicBoolean activeInCallback = new AtomicBoolean(true);
    assertThat(tx.addAfterCompletionCallbackIfAbsent("k", () -> {
      activeInCallback.set(database.isTransactionActive());
      database.transaction(() -> database.newDocument("Other").set("x", 1).save(), false);
    })).isTrue();
    assertThat(tx.addAfterCompletionCallbackIfAbsent("k", () -> {
      throw new IllegalStateException("registered once per key");
    })).isFalse();
    database.newDocument("Other").set("x", 2).save();
    database.rollback();

    assertThat(activeInCallback.get()).isFalse();
    assertThat(tx.getCommitCount()).isEqualTo(commitsBefore);
    assertThat(database.isTransactionActive()).isFalse();
    try (final ResultSet rs = database.query("sql", "SELECT x FROM Other")) {
      assertThat(rs.next().<Integer>getProperty("x")).isEqualTo(1);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /**
   * A commit that fails on a conflict rolls the transaction back, which runs the callback, and the block is retried: the callback's
   * own commit is not taken for a part of the block that was already published (#7916), which would have refused the retry.
   */
  @Test
  void aConflictingCommitIsRetriedAfterTheCallbackRan() {
    database.command("sql", "CREATE DOCUMENT TYPE Other");
    final RID[] created = new RID[1];
    database.transaction(() -> created[0] = database.newDocument("Other").set("x", 0).save().getIdentity());
    final RID rid = created[0];
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicInteger completions = new AtomicInteger();

    database.transaction(() -> {
      final int attempt = attempts.incrementAndGet();
      ((DatabaseInternal) database).getTransaction().addAfterCompletionCallbackIfAbsent("k", () -> {
        completions.incrementAndGet();
        database.transaction(() -> database.newDocument("Other").set("x", -1).save(), false);
      });
      database.lookupByRID(rid, true).asDocument().modify().set("x", attempt).save();
      if (attempt == 1) {
        // another thread commits the same record first: this commit fails with a conflict
        final Thread concurrent = new Thread(
            () -> database.transaction(() -> database.lookupByRID(rid, true).asDocument().modify().set("x", 100).save()));
        concurrent.start();
        try {
          concurrent.join();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }
    });

    assertThat(attempts.get()).isEqualTo(2);
    assertThat(completions.get()).isEqualTo(2);
    assertThat(database.lookupByRID(rid, true).asDocument().getInteger("x")).isEqualTo(2);
  }

  /** An Error escaping an after-commit callback still propagates, and the transaction, committed, still runs what its end owes. */
  @Test
  void anErrorInAnAfterCommitCallbackDoesNotSkipTheAfterCompletionCallbacks() {
    final class CallbackError extends Error {
    }

    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    final AtomicBoolean completed = new AtomicBoolean();
    tx.addAfterCommitCallback(() -> {
      throw new CallbackError();
    });
    tx.addAfterCompletionCallbackIfAbsent("k", () -> completed.set(true));

    assertThatThrownBy(() -> database.commit()).hasRootCauseInstanceOf(CallbackError.class);

    assertThat(completed.get()).isTrue();
    assertThat(database.isTransactionActive()).isFalse();
  }

  /**
   * A block retried after a conflict: each attempt's append is durable (#7410), the second attempt appends again, and each attempt
   * owes its own refresh - the one rolled back too. The aggregate follows the source through both.
   */
  @Test
  void aRetriedBlockRefreshesOncePerAttempt() {
    createSource();
    insert(H0, 1);
    createAggregate();
    database.command("sql", "CREATE DOCUMENT TYPE Other");
    final RID[] created = new RID[1];
    database.transaction(() -> created[0] = database.newDocument("Other").set("x", 0).save().getIdentity());
    final RID rid = created[0];
    final long before = aggregate().getRefreshCount();
    final AtomicInteger attempts = new AtomicInteger();

    database.transaction(() -> {
      final int attempt = attempts.incrementAndGet();
      insert(H0 + HOUR, 2);
      database.lookupByRID(rid, true).asDocument().modify().set("x", attempt).save();
      if (attempt == 1) {
        final Thread concurrent = new Thread(
            () -> database.transaction(() -> database.lookupByRID(rid, true).asDocument().modify().set("x", 100).save()));
        concurrent.start();
        try {
          concurrent.join();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }
      assertThat(aggregate().getRefreshCount()).as("not before the attempt ends").isEqualTo(before + attempt - 1);
    });

    assertThat(attempts.get()).isEqualTo(2);
    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 2);
    // both attempts appended: the samples are not part of the transaction
    assertThat(sourceRows()).containsEntry(1L, 4.0);
    assertConverged();
  }

  /** A nested transaction of the caller's own: the refresh runs when the transaction the write ran in ends, not the outer one. */
  @Test
  void aWriteInANestedTransactionRefreshesWhenThatTransactionEnds() {
    createSource();
    insert(H0, 1);
    createAggregate();

    database.begin();
    database.begin();
    insert(H0 + HOUR, 2);
    database.rollback();
    assertConverged();
    database.rollback();

    assertConverged();
  }
}
