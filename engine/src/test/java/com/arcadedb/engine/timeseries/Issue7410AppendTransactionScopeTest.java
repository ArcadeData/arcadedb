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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7410: {@link TimeSeriesEngine#appendSamples} documented that, inside an enclosing
 * transaction, "the shard's internal begin/commit nests into that transaction, so the mutable-bucket page writes
 * are published and replicated by the enclosing commit as a single, in-order transaction". The code never did
 * that: {@code TimeSeriesShard.appendSamples} commits its own transaction whatever the caller has open, for the
 * reason its javadoc now gives in full - an ArcadeDB nested {@code begin/commit} is an independent transaction
 * rather than a savepoint.
 * <p>
 * These are characterization tests: they pin the behaviour the corrected javadoc now states, one per entry point
 * that reaches the shard append. They fail against the <i>old documented</i> contract, not against the code - the
 * defect #7410 reports is exactly that the two disagreed. A rollback of the enclosing transaction is the probe,
 * because it is the one observation that separates "published by the enclosing commit" from "already committed":
 * only the first can be undone.
 * <p>
 * The DOCUMENT row written in the same transaction is the control in {@link #sqlInsertIntoTimeSeriesTypeSurvivesTheRollback()}:
 * it proves the statements ran inside a live transaction that really did roll back, so "the sample survived"
 * cannot be explained by the transaction never having existed.
 * <p>
 * {@code Issue7370GrpcTimeSeriesInTransactionIT.timeSeriesAppendsAreNotPartOfTheEnclosingTransaction} pins the
 * same contract over the wire; this class pins it embedded, where the engine and SQL entry points live.
 */
class Issue7410AppendTransactionScopeTest extends TestHelper {

  private static final int SHARDS = 2;

  /**
   * Written into the same transaction as every append below and never into any other. Its fate after the
   * rollback is the control: it proves the transaction was open, really contained the statements, and really
   * rolled back - so "the sample survived" cannot be explained by the transaction never having existed.
   */
  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Witness");
  }

  /**
   * Entry point 1: {@link TimeSeriesEngine#appendSamples(long[], Object[][])}, which delegates to the
   * {@link TimeSeriesRowSource} overload and from there to a single shard.
   */
  @Test
  void appendSamplesSurvivesTheRollbackOfTheEnclosingTransaction() throws Exception {
    final TimeSeriesEngine engine = createEngine("append_rollback");
    try {
      database.begin();
      database.command("sql", "INSERT INTO Witness SET name = 'w'");
      engine.appendSamples(new long[] { 1_000L }, new Object[][] { { 11.0 } });
      assertThat(countOfWitnesses())
          .as("the control row is visible inside its own open transaction")
          .isEqualTo(1L);
      database.rollback();

      assertThat(countOfWitnesses())
          .as("the rollback did take the DOCUMENT row, so the transaction was real")
          .isZero();
      assertThat(timestampsOf(engine))
          .as("#7410: appendSamples commits its own shard transaction, so the enclosing rollback cannot undo it")
          .containsExactly(1_000L);
    } finally {
      engine.close();
    }
  }

  /**
   * Entry point 2: {@link TimeSeriesEngine#appendBatch(long[], Object[][])}, whose in-thread fallback under an
   * enclosing transaction (issue #4957) does not change the transaction scope of the shard writes it makes -
   * it only keeps them off the shard executor.
   */
  @Test
  void appendBatchSurvivesTheRollbackOfTheEnclosingTransaction() throws Exception {
    final TimeSeriesEngine engine = createEngine("batch_rollback");
    try {
      final int n = 6;
      final long[] timestamps = new long[n];
      final Object[][] columnValues = new Object[1][n];
      for (int i = 0; i < n; i++) {
        timestamps[i] = 2_000L + i;
        columnValues[0][i] = (double) i;
      }

      database.begin();
      database.command("sql", "INSERT INTO Witness SET name = 'w'");
      engine.appendBatch(timestamps, columnValues);
      assertThat(countOfWitnesses())
          .as("the control row is visible inside its own open transaction")
          .isEqualTo(1L);
      database.rollback();

      assertThat(countOfWitnesses())
          .as("the rollback did take the DOCUMENT row, so the transaction was real")
          .isZero();
      assertThat(timestampsOf(engine))
          .as("#7410: every shard sub-batch committed its own transaction before the enclosing rollback ran")
          .containsExactly(2_000L, 2_001L, 2_002L, 2_003L, 2_004L, 2_005L);
    } finally {
      engine.close();
    }
  }

  /**
   * Entry point 3: {@code SaveElementStep.saveToTimeSeries}, reached by {@code INSERT INTO <timeseries type>}.
   * This is the path a SQL user meets, and the one where the divergence is user-visible: the INSERT is not
   * atomic with the rest of its own transaction.
   */
  @Test
  void sqlInsertIntoTimeSeriesTypeSurvivesTheRollback() {
    database.command("sql", "CREATE TIMESERIES TYPE Reading TIMESTAMP ts FIELDS (value DOUBLE)");

    database.begin();
    database.command("sql", "INSERT INTO Witness SET name = 'w'");
    database.command("sql", "INSERT INTO Reading SET ts = 3000, value = 30.0");
    assertThat(countOfWitnesses())
        .as("the control row is visible inside its own open transaction")
        .isEqualTo(1L);
    database.rollback();

    assertThat(countOfWitnesses())
        .as("the rollback did take the DOCUMENT row, so the transaction was real")
        .isZero();
    assertThat(countOfReadings())
        .as("#7410: INSERT INTO a TIMESERIES type is not atomic with the transaction that contains it")
        .isEqualTo(1L);
  }

  /** Counts the control rows, from outside any transaction of this test's making. */
  private long countOfWitnesses() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM Witness")) {
      return ((Number) rs.next().getProperty("cnt")).longValue();
    }
  }

  /** Counts the samples of the SQL-created series, through the SQL reader a user would use. */
  private long countOfReadings() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM Reading")) {
      return ((Number) rs.next().getProperty("cnt")).longValue();
    }
  }

  /** Every sample timestamp the engine holds, merge-sorted across shards, over a range wider than any test writes. */
  private static List<Long> timestampsOf(final TimeSeriesEngine engine) throws Exception {
    return engine.query(0L, 1_000_000L, null, null).stream().map(row -> (Long) row[0]).toList();
  }

  /**
   * Builds a two-shard engine directly, as {@code Issue4957AppendBatchTransactionTest} does. The constructor
   * initialises each shard's header page in a transaction of its own, so it is wrapped in one here.
   */
  private TimeSeriesEngine createEngine(final String typeName) throws Exception {
    final List<ColumnDefinition> cols = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, typeName, cols, SHARDS);
    database.commit();
    return engine;
  }
}
