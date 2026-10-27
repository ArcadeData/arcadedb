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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.ContinuousAggregateImpl;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10686: only a SQL statement refreshed a continuous aggregate. The line protocol and the Prometheus remote write (both
 * through {@link TimeSeriesGateway#write} or {@link TimeSeriesStorableRows#appendTo}), the gRPC ingest RPCs, the JSONL importer, the
 * embedded engine API and the async executor appended the samples and left the aggregate on the buckets of its last refresh, with
 * no status or metric saying so.
 * <p>
 * Every path is asserted on its own: a census asserting only that SOME path refreshed would pass on the SQL arm alone. Each sample
 * lands in a NEW hour bucket, ahead of the watermark, so this is the plain incremental case and not the late-sample one.
 */
class Issue10686ContinuousAggregateNonSqlIngestRefreshTest extends TestHelper {
  private static final long HOUR = 3_600_000L;
  /** An hour boundary, so that the buckets of the default grid start where the test says they do. */
  private static final long H0   = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);

  private record Bucket(long start, String tag, double total) {
  }

  private void createSourceAndAggregate(final int shards) {
    database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE) SHARDS " + shards);
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1h', ts) AS hr, h, sum(v) AS total FROM S GROUP BY hr, h");
  }

  private ContinuousAggregateImpl aggregate() {
    return (ContinuousAggregateImpl) database.getSchema().getContinuousAggregate("A");
  }

  private TimeSeriesEngine engine(final String typeName) {
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
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

  private static String lineProtocol(final String tag, final double v, final long tsMs) {
    return "S,h=" + tag + " v=" + v + " " + (tsMs * 1_000_000L);
  }

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

  @Test
  void aBatchOfOneSampleRefreshesOnce() throws Exception {
    createSourceAndAggregate(2);
    final long before = aggregate().getRefreshCount();

    engine("S").appendBatch(new long[] { H0 + 1_000 }, new Object[][] { { "a" }, { 6.0 } });

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 6));
  }

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

  // ---- scoped to the aggregates over the type appended to

  @Test
  void anAppendToAnotherTypeDoesNotRefreshTheAggregate() throws Exception {
    createSourceAndAggregate(1);
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE)");
    final long before = aggregate().getRefreshCount();

    engine("T").appendSamples(new long[] { H0 + 1_000 }, new Object[] { "a" }, new Object[] { 1.0 });

    assertThat(aggregate().getRefreshCount()).isEqualTo(before);
  }

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

  @Test
  void anEngineDeleteThatRemovesNothingDoesNotRefresh() throws Exception {
    createSourceAndAggregate(1);
    final TimeSeriesEngine engine = engine("S");
    engine.appendSamples(new long[] { H0 + 1_000 }, new Object[] { "a" }, new Object[] { 1.0 });
    final long before = aggregate().getRefreshCount();

    assertThat(engine.deleteSamples(H0 + 5 * HOUR, H0 + 6 * HOUR, null)).isZero();

    assertThat(aggregate().getRefreshCount()).isEqualTo(before);
  }

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
}
