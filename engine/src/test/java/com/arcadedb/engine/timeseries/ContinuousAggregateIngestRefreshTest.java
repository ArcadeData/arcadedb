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
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.ContinuousAggregateImpl;
import com.arcadedb.schema.ContinuousAggregateRefresher;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.MaterializedViewStatus;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Continuous aggregate refresh triggered by ingest, own transactions and deletes on the source time series type.
 */
class ContinuousAggregateIngestRefreshTest extends TestHelper {
  private static final long HOUR = 3_600_000L;
  /** An hour boundary, so that the buckets of the default grid start where the test says they do. */
  private static final long H0   = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);

  private record Bucket(long start, String tag, double total) {
  }

  private void createSourceAndAggregate() {
    database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE)");
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1h', ts) AS hr, h, sum(v) AS total FROM S GROUP BY hr, h");
  }

  private void insert(final long ts, final String tag, final double v) {
    database.command("sql", "INSERT INTO S SET ts = ?, h = ?, v = ?", ts, tag, v).close();
  }

  private ContinuousAggregateImpl aggregate() {
    return (ContinuousAggregateImpl) database.getSchema().getContinuousAggregate("A");
  }

  private List<Bucket> buckets() {
    final List<Bucket> result = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM A ORDER BY hr, h")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        result.add(new Bucket(DateUtils.toEpochMillis(row.getProperty("hr")), row.getProperty("h"),
            ((Number) row.getProperty("total")).doubleValue()));
      }
    }
    return result;
  }

  // Issue #10632: a SQL {@code INSERT} into a TIMESERIES type registered the refresh of the continuous aggregates over the type as an
  // after-commit callback of the current transaction. With no transaction open (an embedded autocommit {@code database.command()})
  // that callback never ran when the statement ended, so the aggregate was not refreshed by the INSERT at all; it stayed on the
  // inactive transaction context and ran at the next unrelated commit instead.
  // <p>
  // With no transaction open the aggregates are now refreshed once when the statement ends, also for a multi-row INSERT.
  // #10632
  // ---- the repro of the issue

  @Test
  void anAutocommitInsertRefreshesTheAggregate() {
    createSourceAndAggregate();
    assertThat(database.isTransactionActive()).isFalse();
    final long before = aggregate().getRefreshCount();

    insert(H0 + 1_000, "a", 10);

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10));

    insert(H0 + HOUR + 1_000, "a", 20);

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 2);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0 + HOUR, "a", 20));
  }

  // #10632
  /** The issue's own sequence: two autocommit INSERTs, a REFRESH, then a further autocommit INSERT. */
  @Test
  void anAutocommitInsertAfterAnExplicitRefreshRefreshesTheAggregate() {
    createSourceAndAggregate();
    insert(H0 + 1_000, "a", 10);
    insert(H0 + HOUR + 1_000, "a", 20);
    database.command("sql", "REFRESH CONTINUOUS AGGREGATE A").close();
    final long before = aggregate().getRefreshCount();

    insert(H0 + HOUR + 2_000, "a", 5);

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0 + HOUR, "a", 25));
  }

  // #10632
  // ---- once per statement, not once per row

  @Test
  void aMultiRowAutocommitInsertRefreshesOncePerStatement() {
    createSourceAndAggregate();
    final long before = aggregate().getRefreshCount();

    database.command("sql", "INSERT INTO S CONTENT [" //
        + "{\"ts\": " + (H0 + 1_000) + ", \"h\": \"a\", \"v\": 1}," //
        + "{\"ts\": " + (H0 + 2_000) + ", \"h\": \"a\", \"v\": 2}," //
        + "{\"ts\": " + (H0 + HOUR + 1_000) + ", \"h\": \"b\", \"v\": 4}]").close();

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 3), new Bucket(H0 + HOUR, "b", 4));
  }

  // #10632
  // ---- the transactional path keeps its after-commit refresh

  @Test
  void anInsertInsideATransactionRefreshesOnceAtCommit() {
    createSourceAndAggregate();
    final long before = aggregate().getRefreshCount();

    database.transaction(() -> {
      insert(H0 + 1_000, "a", 10);
      insert(H0 + 2_000, "a", 5);
      // not before the commit
      assertThat(aggregate().getRefreshCount()).isEqualTo(before);
    });

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 15));
  }

  // #10632
  // ---- nothing is left behind on the inactive transaction context

  @Test
  void anAutocommitInsertLeavesNoCallbackForTheNextUnrelatedCommit() {
    createSourceAndAggregate();
    // the type the unrelated commit below writes to: created before the INSERT, so that its schema change is not that commit
    database.command("sql", "CREATE DOCUMENT TYPE Other");

    insert(H0 + 1_000, "a", 10);
    final long afterInsert = aggregate().getRefreshCount();

    assertThat(((DatabaseInternal) database).getTransaction()
        .hasCallbackKey(ContinuousAggregateRefresher.refreshCallbackKey("A"))).isFalse();
    // where a transaction registers it since #10687
    assertThat(((DatabaseInternal) database).getTransaction()
        .hasAfterCompletionCallbackKey(ContinuousAggregateRefresher.refreshCallbackKey("A"))).isFalse();

    // a commit that touches nothing of S must not refresh the aggregate
    database.transaction(() -> database.newDocument("Other").set("x", 1).save());

    assertThat(aggregate().getRefreshCount()).isEqualTo(afterInsert);
  }

  // #10632
  // ---- the other statement shapes that reach the same step

  /** With no transaction the samples appended before a failing row are stored, and their aggregate owes them a refresh. */
  @Test
  void anAutocommitInsertThatFailsAfterAppendingStillRefreshesTheAggregate() {
    createSourceAndAggregate();
    final long before = aggregate().getRefreshCount();

    assertThatThrownBy(() -> database.command("sql", "INSERT INTO S CONTENT [" //
        + "{\"ts\": " + (H0 + 1_000) + ", \"h\": \"a\", \"v\": 1}," //
        + "{\"ts\": \"not a timestamp\", \"h\": \"a\", \"v\": 2}]").close()).isInstanceOf(RuntimeException.class);

    assertThat(database.isTransactionActive()).isFalse();
    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 1));
  }

  // #10632
  @Test
  void anAutocommitInsertFromASelectRefreshesOncePerStatement() {
    createSourceAndAggregate();
    database.command("sql", "CREATE DOCUMENT TYPE Src");
    database.transaction(() -> {
      database.newDocument("Src").set("ts", H0 + 1_000).set("h", "a").set("v", 1.0).save();
      database.newDocument("Src").set("ts", H0 + 2_000).set("h", "a").set("v", 2.0).save();
      database.newDocument("Src").set("ts", H0 + HOUR + 1_000).set("h", "b").set("v", 4.0).save();
    });
    final long before = aggregate().getRefreshCount();

    database.command("sql", "INSERT INTO S FROM SELECT ts, h, v FROM Src").close();

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 3), new Bucket(H0 + HOUR, "b", 4));
  }

  // #10632
  /** A script without BEGIN runs each INSERT with no transaction open: each refreshes the aggregate when it ends. */
  @Test
  void eachInsertOfAScriptWithNoTransactionRefreshesTheAggregate() {
    createSourceAndAggregate();
    final long before = aggregate().getRefreshCount();

    database.command("sqlscript", //
        "INSERT INTO S SET ts = " + (H0 + 1_000) + ", h = 'a', v = 1;" //
            + "INSERT INTO S SET ts = " + (H0 + HOUR + 1_000) + ", h = 'a', v = 2;").close();

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 2);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 1), new Bucket(H0 + HOUR, "a", 2));
  }

  private void createSourceAndAggregate(final int shards) {
    database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE) SHARDS " + shards);
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1h', ts) AS hr, h, sum(v) AS total FROM S GROUP BY hr, h");
  }

  private TimeSeriesEngine engine(final String typeName) {
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
  }

  private static String lineProtocol(final String tag, final double v, final long tsMs) {
    return "S,h=" + tag + " v=" + v + " " + (tsMs * 1_000_000L);
  }

  // Issue #10686: only a SQL statement refreshed a continuous aggregate. The line protocol and the Prometheus remote write (both
  // through {@link TimeSeriesGateway#write} or {@link TimeSeriesStorableRows#appendTo}), the gRPC ingest RPCs, the JSONL importer, the
  // embedded engine API and the async executor appended the samples and left the aggregate on the buckets of its last refresh, with
  // no status or metric saying so.
  // <p>
  // Every path is asserted on its own: a census asserting only that SOME path refreshed would pass on the SQL arm alone. Each sample
  // lands in a NEW hour bucket, ahead of the watermark, so this is the plain incremental case and not the late-sample one.
  // #10686
  // ---- the issue's sequence: every path in turn, the refresh count asserted after each

  @Test
  void everyIngestPathRefreshesTheAggregate() throws Exception {
    createSourceAndAggregate(1);
    final long created = aggregate().getRefreshCount();

    database.command("sql", "INSERT INTO S SET ts = ?, h = 'a', v = 10", H0 + 1_000).close();
    assertThat(aggregate().getRefreshCount()).isEqualTo(created + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10));

    // the line protocol, the Prometheus-shaped gateway and the gRPC ingest RPCs share this chokepoint
    final TimeSeriesGateway.WriteReport report = TimeSeriesGateway.write((DatabaseInternal) database,
        LineProtocolParser.parse(lineProtocol("a", 20, H0 + HOUR + 1_000), LineProtocolParser.Precision.NANOSECONDS));
    assertThat(report.written()).isEqualTo(1);
    assertThat(aggregate().getRefreshCount()).isEqualTo(created + 2);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0 + HOUR, "a", 20));

    engine("S").appendSamples(new long[] { H0 + 2 * HOUR + 1_000 }, new Object[] { "a" }, new Object[] { 30.0 });
    assertThat(aggregate().getRefreshCount()).isEqualTo(created + 3);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0 + HOUR, "a", 20),
        new Bucket(H0 + 2 * HOUR, "a", 30));

    database.async().appendSamples("S", new long[] { H0 + 3 * HOUR + 1_000 }, new Object[] { "a" }, new Object[] { 40.0 });
    database.async().waitCompletion();
    assertThat(aggregate().getRefreshCount()).isEqualTo(created + 4);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0 + HOUR, "a", 20),
        new Bucket(H0 + 2 * HOUR, "a", 30), new Bucket(H0 + 3 * HOUR, "a", 40));
  }

  // #10686
  // ---- the gateway: once per call, however many samples and measurements it carries

  @Test
  void aGatewayWriteRefreshesOncePerCall() throws Exception {
    createSourceAndAggregate(4);
    final long before = aggregate().getRefreshCount();

    final String body = String.join("\n", lineProtocol("a", 1, H0 + 1_000), lineProtocol("a", 2, H0 + 2_000),
        lineProtocol("b", 4, H0 + HOUR + 1_000), lineProtocol("a", 8, H0 + 2 * HOUR + 1_000));
    TimeSeriesGateway.write((DatabaseInternal) database, LineProtocolParser.parse(body, LineProtocolParser.Precision.NANOSECONDS));

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 3), new Bucket(H0 + HOUR, "b", 4),
        new Bucket(H0 + 2 * HOUR, "a", 8));
  }

  // #10686
  /** The Prometheus remote write and the JSONL importer convert the rows and append them with {@code appendTo}. */
  @Test
  void storableRowsAppendedToTheEngineRefreshTheAggregate() throws Exception {
    createSourceAndAggregate(2);
    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("S");
    final long before = aggregate().getRefreshCount();

    final long[] timestamps = { H0 + 1_000, H0 + HOUR + 1_000 };
    final String[] tags = { "a", "b" };
    final double[] values = { 5, 7 };
    final TimeSeriesStorableRows rows = TimeSeriesStorableRows.convert(database, "S", type, timestamps, 2, false,
        (row, valueColumn, col) -> col.getRole() == ColumnDefinition.ColumnRole.TAG ? tags[row] : values[row], new HashSet<>());
    rows.appendTo(type.getEngine());

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 5), new Bucket(H0 + HOUR, "b", 7));
  }

  // #10686
  // ---- the embedded engine API, every overload

  @Test
  void anEngineAppendOfARowSourceRefreshesTheAggregate() throws Exception {
    createSourceAndAggregate(1);
    final TimeSeriesEngine engine = engine("S");
    final long before = aggregate().getRefreshCount();

    final TimeSeriesBatch batch = engine.newBatch(1);
    final int row = batch.addRow(H0 + 1_000);
    batch.setString(row, 0, "a");
    batch.setDouble(row, 1, 3);
    engine.appendSamples(batch);

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 3));
  }

  // #10686
  /** The parallel fan-out over the shards: refreshed once after every shard has written, not once per shard. */
  @Test
  void aMultiShardBatchRefreshesOnceAfterEveryShardWrote() throws Exception {
    createSourceAndAggregate(4);
    final TimeSeriesEngine engine = engine("S");
    final long before = aggregate().getRefreshCount();

    final int n = 40;
    final long[] timestamps = new long[n];
    final Object[] tags = new Object[n];
    final Object[] values = new Object[n];
    for (int i = 0; i < n; i++) {
      timestamps[i] = H0 + (i / 10) * HOUR + 1_000 + i;
      tags[i] = "a";
      values[i] = 1.0;
    }
    assertThat(database.isTransactionActive()).isFalse();
    engine.appendBatch(timestamps, new Object[][] { tags, values });

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0 + HOUR, "a", 10),
        new Bucket(H0 + 2 * HOUR, "a", 10), new Bucket(H0 + 3 * HOUR, "a", 10));
  }

  // #10686
  @Test
  void aBatchOfOneSampleRefreshesOnce() throws Exception {
    createSourceAndAggregate(2);
    final long before = aggregate().getRefreshCount();

    engine("S").appendBatch(new long[] { H0 + 1_000 }, new Object[][] { { "a" }, { 6.0 } });

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 6));
  }

  // #10686
  /** With a transaction open the refresh waits for its commit, once per aggregate however many appends it saw. */
  @Test
  void anEngineAppendInsideATransactionRefreshesOnceAtCommit() {
    createSourceAndAggregate(2);
    final TimeSeriesEngine engine = engine("S");
    final long before = aggregate().getRefreshCount();

    database.transaction(() -> {
      try {
        engine.appendSamples(new long[] { H0 + 1_000 }, new Object[] { "a" }, new Object[] { 1.0 });
        engine.appendBatch(new long[] { H0 + 2_000, H0 + HOUR + 1_000 }, new Object[][] { { "a", "a" }, { 2.0, 4.0 } });
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
      assertThat(aggregate().getRefreshCount()).isEqualTo(before);
    });

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 3), new Bucket(H0 + HOUR, "a", 4));
  }

  // #10686
  // ---- the async executor, both overloads

  @Test
  void asyncAppendsRefreshTheAggregate() {
    createSourceAndAggregate(2);
    final TimeSeriesEngine engine = engine("S");
    final long before = aggregate().getRefreshCount();

    database.async().appendSamples("S", new long[] { H0 + 1_000 }, new Object[] { "a" }, new Object[] { 1.0 });
    final TimeSeriesBatch batch = engine.newBatch(1);
    final int row = batch.addRow(H0 + HOUR + 1_000);
    batch.setString(row, 0, "a");
    batch.setDouble(row, 1, 2);
    database.async().appendSamples("S", batch);
    database.async().waitCompletion();

    assertThat(aggregate().getRefreshCount()).isGreaterThan(before);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 1), new Bucket(H0 + HOUR, "a", 2));
  }

  // #10686
  /**
   * The shard append commits its own transaction and leaves the worker's batch transaction open, so the refresh each task asks for
   * is registered on that batch once per aggregate and runs once when it commits: not once per task.
   */
  @Test
  void asyncAppendsOfOneBatchRefreshOnceAtItsCommit() {
    createSourceAndAggregate(1);
    final long before = aggregate().getRefreshCount();
    final int previousCommitEvery = database.async().getCommitEvery();
    database.async().setCommitEvery(1_000);
    try {
      // one shard: every task lands on the same worker, so in the same batch
      for (int i = 0; i < 5; i++)
        database.async().appendSamples("S", new long[] { H0 + i * HOUR + 1_000 }, new Object[] { "a" }, new Object[] { 1.0 });
      database.async().waitCompletion();
    } finally {
      database.async().setCommitEvery(previousCommitEvery);
    }

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 1), new Bucket(H0 + HOUR, "a", 1), new Bucket(H0 + 2 * HOUR, "a", 1),
        new Bucket(H0 + 3 * HOUR, "a", 1), new Bucket(H0 + 4 * HOUR, "a", 1));
  }

  // #10686
  // ---- scoped to the aggregates over the type appended to

  @Test
  void anAppendToAnotherTypeDoesNotRefreshTheAggregate() throws Exception {
    createSourceAndAggregate(1);
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE)");
    final long before = aggregate().getRefreshCount();

    engine("T").appendSamples(new long[] { H0 + 1_000 }, new Object[] { "a" }, new Object[] { 1.0 });

    assertThat(aggregate().getRefreshCount()).isEqualTo(before);
  }

  // #10686
  /** The aggregate follows a rename of its source, and so does the refresh the engine of the renamed type asks for. */
  @Test
  void anAppendAfterARenameOfTheSourceRefreshesTheAggregate() throws Exception {
    createSourceAndAggregate(1);
    database.command("sql", "ALTER TYPE S NAME S2");
    final long before = aggregate().getRefreshCount();

    engine("S2").appendSamples(new long[] { H0 + 1_000 }, new Object[] { "a" }, new Object[] { 9.0 });

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 9));
  }

  // #10686
  // ---- the removals of the engine API refresh too: the refresh moved from the DELETE and TRUNCATE statements into the engine

  @Test
  void anEngineDeleteRefreshesTheAggregate() throws Exception {
    createSourceAndAggregate(2);
    final TimeSeriesEngine engine = engine("S");
    engine.appendBatch(new long[] { H0 + 1_000, H0 + HOUR + 1_000, H0 + 2 * HOUR + 1_000 },
        new Object[][] { { "a", "a", "a" }, { 1.0, 2.0, 4.0 } });
    final long before = aggregate().getRefreshCount();

    assertThat(engine.deleteSamples(H0, H0 + HOUR - 1, null)).isEqualTo(1L);

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0 + HOUR, "a", 2), new Bucket(H0 + 2 * HOUR, "a", 4));
  }

  // #10686
  @Test
  void anEngineDeleteThatRemovesNothingDoesNotRefresh() throws Exception {
    createSourceAndAggregate(1);
    final TimeSeriesEngine engine = engine("S");
    engine.appendSamples(new long[] { H0 + 1_000 }, new Object[] { "a" }, new Object[] { 1.0 });
    final long before = aggregate().getRefreshCount();

    assertThat(engine.deleteSamples(H0 + 5 * HOUR, H0 + 6 * HOUR, null)).isZero();

    assertThat(aggregate().getRefreshCount()).isEqualTo(before);
  }

  // #10686
  @Test
  void anEngineTruncateRefreshesTheAggregate() throws Exception {
    createSourceAndAggregate(2);
    final TimeSeriesEngine engine = engine("S");
    engine.appendBatch(new long[] { H0 + 1_000, H0 + HOUR + 1_000 }, new Object[][] { { "a", "a" }, { 1.0, 2.0 } });
    final long before = aggregate().getRefreshCount();

    assertThat(engine.truncate()).isEqualTo(2L);

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).isEmpty();
  }

  // #10686
  /** The SQL statements now reach the refresh through the engine: once per statement still. */
  @Test
  void sqlDeleteAndTruncateStillRefreshOncePerStatement() throws Exception {
    createSourceAndAggregate(2);
    engine("S").appendBatch(new long[] { H0 + 1_000, H0 + HOUR + 1_000, H0 + 2 * HOUR + 1_000 },
        new Object[][] { { "a", "a", "a" }, { 1.0, 2.0, 4.0 } });
    final long before = aggregate().getRefreshCount();

    database.command("sql", "DELETE FROM S WHERE ts < ?", H0 + HOUR).close();
    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0 + HOUR, "a", 2), new Bucket(H0 + 2 * HOUR, "a", 4));

    database.command("sql", "TRUNCATE TYPE S").close();
    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 2);
    assertThat(buckets()).isEmpty();
  }

  // #10686
  // ---- the SQL path keeps refreshing once per statement, not once per appended row

  @Test
  void aMultiRowAutocommitInsertStillRefreshesOncePerStatement() {
    createSourceAndAggregate(2);
    final long before = aggregate().getRefreshCount();

    database.command("sql", "INSERT INTO S CONTENT [" //
        + "{\"ts\": " + (H0 + 1_000) + ", \"h\": \"a\", \"v\": 1}," //
        + "{\"ts\": " + (H0 + 2_000) + ", \"h\": \"a\", \"v\": 2}," //
        + "{\"ts\": " + (H0 + HOUR + 1_000) + ", \"h\": \"b\", \"v\": 4}]").close();

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 3), new Bucket(H0 + HOUR, "b", 4));
  }

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

  // Issue #10708, the continuous-aggregate refresh is not part of the caller's transaction, in either direction.
  // <ul>
  //   <li>#10688: a refresh run while a transaction is open ({@code REFRESH CONTINUOUS AGGREGATE}, {@code ContinuousAggregate.refresh()},
  //   the initial refresh of {@code CREATE CONTINUOUS AGGREGATE}) joined it, then installed and saved the watermark as if its rows had
  //   committed. A rollback of the caller's transaction took the rows back and left the watermark: the buckets behind it were lost for
  //   good, with the aggregate reported {@code VALID}. The refresh now commits in a transaction of its own.</li>
  //   <li>#10687: a write to a TIMESERIES type ({@code INSERT}, {@code DELETE}, {@code TRUNCATE TYPE}) is not part of the statement's
  //   transaction (#7410), but the refresh it owes the aggregates was registered as an after-commit callback of that transaction, so a
  //   rollback discarded it. The refresh now runs when the transaction ends, whichever way it ends.</li>
  // </ul>
  // #10708
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

  // #10708
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

  // #10708
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

  // #10708
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

  // #10708
  @Test
  void refreshInsideACommittedTransactionConverges() throws IOException {
    aggregateBehindItsSource();

    database.transaction(() -> database.command("sql", "REFRESH CONTINUOUS AGGREGATE A").close());

    assertConverged();
  }

  // #10708
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

  // #10708
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

  // #10708
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

  // #10708
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

  // #10708
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

  // #10708
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

  // #10708
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

  // #10708
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

  // #10708
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

  // #10708
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

  // #10708
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

  // #10708
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

  // #10708
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

  // #10708
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

  private void refresh() {
    database.command("sql", "REFRESH CONTINUOUS AGGREGATE A").close();
  }

  private int delete(final String where) {
    try (final ResultSet rs = database.command("sql", "DELETE FROM S" + (where != null ? " WHERE " + where : ""))) {
      return ((Number) rs.next().getProperty("count")).intValue();
    }
  }

  // Issue #10428: {@code DELETE FROM <timeseries type> WHERE ...} and {@code TRUNCATE TYPE <timeseries type>} removed samples from the
  // type, but a continuous aggregate over it kept the buckets it had already materialized, and a refresh did not rebuild them: the
  // incremental refresh only recomputes the bucket open at the watermark and the newer ones, so every bucket behind the watermark kept
  // the deleted samples for good.
  // <p>
  // The delete now records, on every aggregate over the type, the time range it removed samples from. The next refresh recomputes the
  // buckets of that range - and only those, the ones behind it and the ones between it and the watermark are not touched - and the
  // statement schedules that refresh as an INSERT does. The record is part of the aggregate's definition in the schema, so it survives
  // a restart and, under HA, is shipped with the delete.
  // #10428
  // ---- the repro of the issue

  @Test
  void aDeleteBehindTheWatermarkIsReflectedByTheNextRefresh() {
    createSourceAndAggregate();
    insert(H0, "a", 10);
    insert(H0 + 2 * HOUR, "a", 20); // two hours later, so the first bucket is behind the watermark
    refresh();
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0 + 2 * HOUR, "a", 20));

    assertThat(delete("ts = " + H0)).isEqualTo(1);
    refresh();

    assertThat(buckets()).containsExactly(new Bucket(H0 + 2 * HOUR, "a", 20));
  }

  // #10428
  @Test
  void aDeleteRefreshesTheAggregateByItselfLikeAnInsertDoes() {
    createSourceAndAggregate();
    insert(H0, "a", 10);
    insert(H0 + 2 * HOUR, "a", 20);
    refresh();

    delete("ts = " + H0);

    // no REFRESH here
    assertThat(buckets()).containsExactly(new Bucket(H0 + 2 * HOUR, "a", 20));
  }

  // #10428
  @Test
  void aDeleteInsideATransactionRefreshesWhenItCommits() {
    createSourceAndAggregate();
    insert(H0, "a", 10);
    insert(H0 + 2 * HOUR, "a", 20);
    refresh();

    database.transaction(() -> delete("ts = " + H0));

    assertThat(buckets()).containsExactly(new Bucket(H0 + 2 * HOUR, "a", 20));
  }

  // #10428
  @Test
  void aDeleteOfSomeSamplesOfABucketRecomputesItsTotal() {
    createSourceAndAggregate();
    insert(H0 + 1_000, "a", 10);
    insert(H0 + 2_000, "a", 5);
    insert(H0 + 2 * HOUR, "a", 20);
    refresh();
    assertThat(buckets().get(0).total()).isEqualTo(15.0);

    delete("ts = " + (H0 + 1_000));

    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 5), new Bucket(H0 + 2 * HOUR, "a", 20));
  }

  // #10428
  @Test
  void aDeleteByTagKeepsTheRowsOfTheOtherTags() {
    createSourceAndAggregate();
    insert(H0 + 1_000, "a", 10);
    insert(H0 + 2_000, "b", 7);
    insert(H0 + 3 * HOUR, "a", 20);
    refresh();

    delete("h = 'a'");

    assertThat(buckets()).containsExactly(new Bucket(H0, "b", 7));
  }

  // #10428
  @Test
  void aDeleteOfEverySampleOfABucketRemovesTheBucket() {
    createSourceAndAggregate();
    insert(H0, "a", 10);
    insert(H0 + HOUR, "a", 20);
    insert(H0 + 2 * HOUR, "a", 30);
    refresh();

    delete("ts >= " + H0 + " AND ts < " + (H0 + HOUR));

    assertThat(buckets()).containsExactly(new Bucket(H0 + HOUR, "a", 20), new Bucket(H0 + 2 * HOUR, "a", 30));
  }

  // #10428
  // ---- TRUNCATE TYPE

  @Test
  void truncateTypeEmptiesTheAggregate() {
    createSourceAndAggregate();
    insert(H0, "a", 10);
    insert(H0 + 2 * HOUR, "a", 20);
    refresh();
    assertThat(buckets()).hasSize(2);

    database.command("sql", "TRUNCATE TYPE S").close();

    assertThat(buckets()).isEmpty();
    // and it keeps working: a sample written afterwards is aggregated
    insert(H0 + 5 * HOUR, "a", 3);
    refresh();
    assertThat(buckets()).containsExactly(new Bucket(H0 + 5 * HOUR, "a", 3));
  }

  // #10428
  // ---- only the buckets the delete reached are rebuilt

  @Test
  void onlyTheBucketsOfTheDeletedRangeAreRebuilt() {
    createSourceAndAggregate();
    for (int hour = 0; hour < 4; hour++)
      insert(H0 + hour * HOUR + 1_000, "a", 10 + hour);
    refresh();
    // buckets 0..3, the watermark is at bucket 3
    final String[] before = new String[4];
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid, hr FROM A ORDER BY hr")) {
      for (int i = 0; rs.hasNext(); i++)
        before[i] = rs.next().getProperty("rid").toString();
    }

    delete("ts = " + (H0 + HOUR + 1_000)); // bucket 1

    final String[] after = new String[3];
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid, hr FROM A ORDER BY hr")) {
      for (int i = 0; rs.hasNext(); i++)
        after[i] = rs.next().getProperty("rid").toString();
    }
    assertThat(buckets().stream().map(Bucket::total)).containsExactly(10.0, 12.0, 13.0);
    // bucket 0 is behind the range, bucket 2 is between it and the watermark: neither is rewritten
    assertThat(after[0]).isEqualTo(before[0]);
    assertThat(after[1]).isEqualTo(before[2]);
  }

  // #10428
  @Test
  void aDeleteThatRemovesNothingLeavesTheAggregateAlone() {
    createSourceAndAggregate();
    insert(H0, "a", 10);
    insert(H0 + 2 * HOUR, "a", 20);
    refresh();
    final String firstRid;
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM A ORDER BY hr")) {
      firstRid = rs.next().getProperty("rid").toString();
    }

    assertThat(delete("ts = " + (H0 + HOUR))).isZero();

    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0 + 2 * HOUR, "a", 20));
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM A ORDER BY hr")) {
      assertThat(rs.next().getProperty("rid").toString()).isEqualTo(firstRid);
    }
  }

  // #10428
  // ---- a grid that does not start at the epoch

  @Test
  void aBucketOfAShiftedGridIsRebuiltWholeNotFromAPartOfItsSamples() {
    database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE)");
    // days that start at 08:00 UTC
    database.command("sql", "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1d', ts, {'offset': '8h'}) AS hr, h, sum(v) AS total "
        + "FROM S GROUP BY hr, h");
    final long day0 = H0 - (H0 % (24 * HOUR)) + 8 * HOUR; // a bucket start of that grid
    insert(day0 + 1 * HOUR, "a", 1);
    insert(day0 + 5 * HOUR, "a", 2);
    insert(day0 + 20 * HOUR, "a", 4);
    insert(day0 + 24 * HOUR + HOUR, "a", 8);
    insert(day0 + 48 * HOUR + HOUR, "a", 16);
    refresh();
    assertThat(buckets().stream().map(Bucket::total)).containsExactly(7.0, 8.0, 16.0);

    // a delete that starts in the middle of the first bucket of the grid
    delete("ts = " + (day0 + 5 * HOUR));

    assertThat(buckets()).containsExactly(new Bucket(day0, "a", 5), new Bucket(day0 + 24 * HOUR, "a", 8),
        new Bucket(day0 + 48 * HOUR, "a", 16));
  }

  // #10428
  @Test
  void aBucketOfATimezoneGridIsRebuiltWholeNotFromAPartOfItsSamples() {
    database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE)");
    // days that start at local midnight in UTC+08:00, which is 16:00 UTC
    database.command("sql", "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1d', ts, {'timezone': '+08:00'}) AS hr, h, "
        + "sum(v) AS total FROM S GROUP BY hr, h");
    final long day0 = H0 - (H0 % (24 * HOUR)) + 16 * HOUR; // a bucket start of that grid
    insert(day0 + 1 * HOUR, "a", 1);
    insert(day0 + 5 * HOUR, "a", 2);
    insert(day0 + 20 * HOUR, "a", 4);
    insert(day0 + 24 * HOUR + HOUR, "a", 8);
    insert(day0 + 48 * HOUR + HOUR, "a", 16);
    refresh();
    assertThat(buckets().stream().map(Bucket::total)).containsExactly(7.0, 8.0, 16.0);

    // a delete that starts in the middle of the first bucket of the grid
    delete("ts = " + (day0 + 5 * HOUR));

    assertThat(buckets()).containsExactly(new Bucket(day0, "a", 5), new Bucket(day0 + 24 * HOUR, "a", 8),
        new Bucket(day0 + 48 * HOUR, "a", 16));
  }

  // #10428
  // ---- failures

  @Test
  void aDeleteInATransactionThatRollsBackStillRefreshesTheAggregate() {
    createSourceAndAggregate();
    insert(H0, "a", 10);
    insert(H0 + 2 * HOUR, "a", 20);
    refresh();

    // the samples are not part of the transaction, so the rollback does not bring them back, and the refresh they are owed runs
    // when the transaction ends however it ends (issue #10687: it was an after-commit callback, which the rollback dropped)
    database.begin();
    delete("ts = " + H0);
    assertThat(pendingFrom()).as("recorded at once").isEqualTo(H0);
    database.rollback();

    assertThat(buckets()).containsExactly(new Bucket(H0 + 2 * HOUR, "a", 20));
    assertThat(pendingFrom()).isEqualTo(Long.MIN_VALUE);
  }

  // #10428
  @Test
  void aRefreshThatFailsWhileRecomputingTheRangeLeavesTheBucketsAndTheRecordAsTheyWere() {
    createSourceAndAggregate();
    insert(H0 + 1_000, "a", 10);
    insert(H0 + 2_000, "a", 8);
    insert(H0 + 2 * HOUR, "a", 20);
    refresh();
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 18), new Bucket(H0 + 2 * HOUR, "a", 20));
    // the backing type refuses a total under 15
    database.command("sql", "CREATE PROPERTY A.total DOUBLE").close();
    database.command("sql", "ALTER PROPERTY A.total MIN 15").close();

    // the delete leaves the first bucket with 8: the refresh it schedules deletes the old row of the bucket and then fails to insert
    // the new one, so it must roll back as a whole
    delete("ts = " + (H0 + 1_000));

    assertThat(buckets()).as("the old row of the bucket is still there").containsExactly(new Bucket(H0, "a", 18),
        new Bucket(H0 + 2 * HOUR, "a", 20));
    assertThat(pendingFrom()).as("and the range is still recorded").isEqualTo(H0 + 1_000);

    // once the backing type accepts the row, the next refresh finishes the job
    database.command("sql", "ALTER PROPERTY A.total MIN 1").close();
    refresh();

    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 8), new Bucket(H0 + 2 * HOUR, "a", 20));
    assertThat(pendingFrom()).isEqualTo(Long.MIN_VALUE);
  }

  // #10428
  @Test
  void aDeleteThatLandsWhileARefreshRunsIsAppliedWhenThatRefreshEnds() throws Exception {
    createSourceAndAggregate();
    insert(H0, "a", 10);
    insert(H0 + HOUR, "a", 20);
    insert(H0 + 2 * HOUR, "a", 30);
    refresh();

    // a first delete whose own refresh stands aside, so that the refresh below has a range to recompute
    final ContinuousAggregateImpl aggregate = (ContinuousAggregateImpl) database.getSchema().getContinuousAggregate("A");
    assertThat(aggregate.tryBeginRefresh()).isTrue();
    delete("ts = " + H0);
    aggregate.endRefresh();

    // the refresh stops when it deletes the old row of the first bucket, after it read the range of the first delete
    final CountDownLatch inRefresh = new CountDownLatch(1);
    final CountDownLatch resume = new CountDownLatch(1);
    final AtomicBoolean first = new AtomicBoolean(true);
    database.getSchema().getType("A").getEvents().registerListener((BeforeRecordDeleteListener) record -> {
      if (first.getAndSet(false)) {
        inRefresh.countDown();
        try {
          resume.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return true;
    });
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread refresher = new Thread(() -> {
      try {
        refresh();
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    refresher.start();
    try {
      assertThat(inRefresh.await(30, TimeUnit.SECONDS)).isTrue();

      // a second delete, in a bucket the refresh in progress did not read: its own refresh finds the aggregate busy
      delete("ts = " + (H0 + HOUR));
      assertThat(pendingFrom()).as("recorded, and not yet applied").isEqualTo(H0);
    } finally {
      resume.countDown();
      refresher.join(30_000);
    }

    assertThat(failure.get()).isNull();
    // the refresh that was in progress ran again when it ended: nothing asked for a third
    assertThat(buckets()).containsExactly(new Bucket(H0 + 2 * HOUR, "a", 30));
    assertThat(pendingFrom()).isEqualTo(Long.MIN_VALUE);
  }

  /** The start of the range the aggregate records as invalidated, or {@code Long.MIN_VALUE} when it records none. */
  private long pendingFrom() {
    final JSONObject definition = ((LocalSchema) database.getSchema()).toJSON().getJSONObject("continuousAggregates").getJSONObject("A");
    return definition.getLong("invalidatedFromTs", Long.MIN_VALUE);
  }

  // #10428
  // ---- the record lives in the schema

  @Test
  void theRangeIsPartOfTheDefinitionOfTheAggregateAndSurvivesARestart() {
    createSourceAndAggregate();
    insert(H0, "a", 10);
    insert(H0 + 2 * HOUR, "a", 20);
    refresh();

    // a refresh in flight makes the one the delete schedules stand aside, so the record is still there to be seen
    final ContinuousAggregateImpl aggregate = (ContinuousAggregateImpl) database.getSchema().getContinuousAggregate("A");
    assertThat(aggregate.tryBeginRefresh()).isTrue();
    try {
      delete("ts = " + H0);
      final JSONObject definition = ((LocalSchema) database.getSchema()).toJSON().getJSONObject("continuousAggregates").getJSONObject("A");
      assertThat(definition.getLong("invalidatedFromTs", Long.MIN_VALUE)).isEqualTo(H0);
      assertThat(definition.getLong("invalidatedToTs", Long.MIN_VALUE)).isEqualTo(H0);
    } finally {
      aggregate.endRefresh();
    }

    reopenDatabase();
    refresh();

    assertThat(buckets()).containsExactly(new Bucket(H0 + 2 * HOUR, "a", 20));
    // and the refresh that rebuilt it cleared the record
    final JSONObject definition = ((LocalSchema) database.getSchema()).toJSON().getJSONObject("continuousAggregates").getJSONObject("A");
    assertThat(definition.has("invalidatedFromTs")).isFalse();
    assertThat(definition.has("invalidatedToTs")).isFalse();
  }

  // #10428
  @Test
  void everyAggregateOverTheTypeIsRebuilt() {
    createSourceAndAggregate();
    database.command("sql", "CREATE CONTINUOUS AGGREGATE B AS SELECT ts.timeBucket('1h', ts) AS hr, count(*) AS n FROM S GROUP BY hr");
    insert(H0, "a", 10);
    insert(H0 + 2 * HOUR, "a", 20);
    refresh();
    database.command("sql", "REFRESH CONTINUOUS AGGREGATE B").close();

    delete("ts = " + H0);

    assertThat(buckets()).containsExactly(new Bucket(H0 + 2 * HOUR, "a", 20));
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM B")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
    }
  }
}
