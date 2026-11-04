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
import com.arcadedb.engine.timeseries.LineProtocolParser.Sample;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.ContinuousAggregateImpl;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10502: the incremental refresh of a continuous aggregate recomputes only the bucket open at its watermark and the newer
 * ones, so a sample written with a timestamp behind the watermark was never aggregated into its bucket.
 * <p>
 * An append now records, on every aggregate over the type whose watermark it is behind, the range of its late samples, as a delete
 * does since #10428, and the next refresh recomputes the buckets of that range. The check is in the shard append every ingest path
 * goes through, so SQL {@code INSERT}, the engine API, the async API and the line protocol gateway all record it; these tests drive
 * each of them.
 */
class Issue10502ContinuousAggregateLateSampleTest extends TestHelper {
  private static final long HOUR = 3_600_000L;
  /** An hour boundary, so that the buckets of the default grid start where the test says they do. */
  private static final long H0   = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);

  private record Bucket(long start, String tag, double total) {
  }

  private void createSource() {
    database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE)");
  }

  private void createSourceAndAggregate() {
    createSource();
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1h', ts) AS hr, h, sum(v) AS total FROM S GROUP BY hr, h");
  }

  private void insert(final long ts, final String tag, final double v) {
    database.command("sql", "INSERT INTO S SET ts = ?, h = ?, v = ?", ts, tag, v).close();
  }

  private void refresh() {
    database.command("sql", "REFRESH CONTINUOUS AGGREGATE A").close();
  }

  private TimeSeriesEngine engine() {
    return ((LocalTimeSeriesType) database.getSchema().getType("S")).getEngine();
  }

  private ContinuousAggregateImpl aggregate() {
    return (ContinuousAggregateImpl) database.getSchema().getContinuousAggregate("A");
  }

  private long schemaVersion() {
    return ((LocalSchema) database.getSchema()).getVersion();
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

  /** Two buckets two hours apart: the watermark is at the second, the first is behind it and the one between has no row. */
  private void twoBucketsAndARefresh() {
    insert(H0 + 1_000, "a", 10);
    insert(H0 + 2 * HOUR + 1_000, "a", 20);
    refresh();
    assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 2 * HOUR);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0 + 2 * HOUR, "a", 20));
  }

  // ---- the repro of the issue: SQL INSERT

  @Test
  void anInsertBehindTheWatermarkReachesItsBucket() {
    createSourceAndAggregate();
    twoBucketsAndARefresh();

    insert(H0 + 2_000, "a", 5);
    refresh();

    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 15), new Bucket(H0 + 2 * HOUR, "a", 20));
    assertThat(aggregate().getInvalidation()).isNull();
  }

  @Test
  void anInsertIntoABucketWithNoRowBehindTheWatermarkCreatesTheBucket() {
    createSourceAndAggregate();
    twoBucketsAndARefresh();

    insert(H0 + HOUR + 1_000, "a", 7);
    refresh();

    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0 + HOUR, "a", 7),
        new Bucket(H0 + 2 * HOUR, "a", 20));
  }

  @Test
  void anInsertOlderThanEveryBucketCreatesTheBucket() {
    createSourceAndAggregate();
    twoBucketsAndARefresh();

    insert(H0 - 5 * HOUR + 1_000, "a", 3);
    refresh();

    assertThat(buckets()).containsExactly(new Bucket(H0 - 5 * HOUR, "a", 3), new Bucket(H0, "a", 10),
        new Bucket(H0 + 2 * HOUR, "a", 20));
  }

  @Test
  void anInsertOfANewTagBehindTheWatermarkAddsItsRowAndKeepsTheOthers() {
    createSourceAndAggregate();
    twoBucketsAndARefresh();

    insert(H0 + 3_000, "b", 4);
    refresh();

    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0, "b", 4), new Bucket(H0 + 2 * HOUR, "a", 20));
  }

  @Test
  void anInsertInsideATransactionReachesItsBucketWhenTheTransactionCommits() {
    createSourceAndAggregate();
    twoBucketsAndARefresh();

    database.transaction(() -> insert(H0 + 2_000, "a", 5));

    // no REFRESH: an INSERT in a transaction refreshes the aggregates over its type when the transaction commits
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 15), new Bucket(H0 + 2 * HOUR, "a", 20));
  }

  @Test
  void aSampleOfAShiftedGridIsAggregatedIntoItsWholeBucket() {
    createSource();
    // days that start at 08:00 UTC: a bucket worked out on the epoch's grid would start at midnight
    database.command("sql", "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1d', ts, {'offset': '8h'}) AS hr, h, sum(v) AS total "
        + "FROM S GROUP BY hr, h");
    final long day = 24 * HOUR;
    final long d0 = 1_700_000_000_000L - Math.floorMod(1_700_000_000_000L - 8 * HOUR, day); // a day of the shifted grid
    insert(d0 + HOUR, "a", 1);        // 09:00
    insert(d0 + 20 * HOUR, "a", 2);   // 04:00 the next day, same bucket
    insert(d0 + 3 * day + HOUR, "a", 100);
    refresh();
    assertThat(buckets()).containsExactly(new Bucket(d0, "a", 3), new Bucket(d0 + 3 * day, "a", 100));

    insert(d0 + 22 * HOUR, "a", 4);   // 06:00 the next day: still the bucket of d0, which started at 08:00
    refresh();

    assertThat(buckets()).containsExactly(new Bucket(d0, "a", 7), new Bucket(d0 + 3 * day, "a", 100));
  }

  // ---- the paths that are not statements: since #10686 they refresh by themselves, and that refresh consumes the record

  @Test
  void anAppendThroughTheEngineIsAggregatedByItsOwnRefresh() throws Exception {
    createSourceAndAggregate();
    twoBucketsAndARefresh();

    engine().appendSamples(new long[] { H0 + 2_000 }, new Object[] { "a" }, new Object[] { 5.0 });

    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 15), new Bucket(H0 + 2 * HOUR, "a", 20));
    assertThat(aggregate().getInvalidation()).isNull();
  }

  /** The record alone, without the refresh of the engine API: it waits for the next refresh. */
  @Test
  void anAppendWithoutRefreshIsAggregatedByTheNextRefresh() throws Exception {
    createSourceAndAggregate();
    twoBucketsAndARefresh();

    engine().appendSamplesWithoutRefresh(new long[] { H0 + 2_000 }, new Object[] { "a" }, new Object[] { 5.0 });
    assertThat(aggregate().getInvalidation()).isNotNull();
    refresh();

    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 15), new Bucket(H0 + 2 * HOUR, "a", 20));
    assertThat(aggregate().getInvalidation()).isNull();
  }

  @Test
  void aBatchThroughTheEngineRecordsTheHullOfItsLateSamplesAcrossShards() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE) SHARDS 2");
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1h', ts) AS hr, h, sum(v) AS total FROM S GROUP BY hr, h");
    twoBucketsAndARefresh();

    // a late sample in each of the two buckets behind the watermark, and one at the watermark
    engine().appendBatch(new long[] { H0 + 2_000, H0 + HOUR + 2_000, H0 + 2 * HOUR + 2_000 },
        new Object[][] { { "a", "a", "a" }, { 1.0, 2.0, 3.0 } });

    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 11), new Bucket(H0 + HOUR, "a", 2),
        new Bucket(H0 + 2 * HOUR, "a", 23));
  }

  @Test
  void anAsyncAppendIsAggregatedByItsOwnRefresh() {
    createSourceAndAggregate();
    twoBucketsAndARefresh();

    database.async().appendSamples("S", new long[] { H0 + 2_000 }, new Object[] { "a" }, new Object[] { 5.0 });
    database.async().waitCompletion();

    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 15), new Bucket(H0 + 2 * HOUR, "a", 20));
  }

  @Test
  void aLineProtocolWriteIsAggregatedByItsOwnRefresh() throws Exception {
    createSourceAndAggregate();
    twoBucketsAndARefresh();

    TimeSeriesGateway.write((DatabaseInternal) database,
        new ArrayList<>(List.of(new Sample("S", Map.of("h", "a"), Map.of("v", 5.0), H0 + 2_000))));

    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 15), new Bucket(H0 + 2 * HOUR, "a", 20));
  }

  // ---- what records nothing

  @Test
  void aSampleAtOrAfterTheWatermarkRecordsNothingAndSavesNoSchema() throws Exception {
    createSourceAndAggregate();
    twoBucketsAndARefresh();
    final long version = schemaVersion();

    // the record alone: the refresh of the engine API saves the schema of its own when it moves the watermark
    engine().appendSamplesWithoutRefresh(new long[] { H0 + 2 * HOUR }, new Object[] { "a" }, new Object[] { 1.0 });
    engine().appendSamplesWithoutRefresh(new long[] { H0 + 5 * HOUR }, new Object[] { "a" }, new Object[] { 1.0 });

    assertThat(aggregate().getInvalidation()).isNull();
    assertThat(schemaVersion()).isEqualTo(version);
  }

  @Test
  void anAggregateNeverRefreshedRecordsNothing() throws Exception {
    createSourceAndAggregate();
    // created over an empty type: no bucket was read, so there is no watermark and the first refresh reads every sample
    assertThat(aggregate().isWatermarkSet()).isFalse();

    engine().appendSamples(new long[] { H0 }, new Object[] { "a" }, new Object[] { 1.0 });

    assertThat(aggregate().getInvalidation()).isNull();
    refresh();
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 1));
  }

  @Test
  void anAggregateOverAnotherTypeRecordsNothing() throws Exception {
    createSourceAndAggregate();
    twoBucketsAndARefresh();
    database.command("sql", "CREATE TIMESERIES TYPE Other TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE)");

    ((LocalTimeSeriesType) database.getSchema().getType("Other")).getEngine()
        .appendSamples(new long[] { H0 }, new Object[] { "a" }, new Object[] { 1.0 });

    assertThat(aggregate().getInvalidation()).isNull();
  }

  // ---- a renamed source type (#10096): the engine keeps the name it was created with, the aggregate follows the rename

  @Test
  void aLateSampleOfARenamedTypeReachesItsBucket() {
    createSourceAndAggregate();
    twoBucketsAndARefresh();
    database.command("sql", "ALTER TYPE S NAME S2");

    database.command("sql", "INSERT INTO S2 SET ts = ?, h = ?, v = ?", H0 + 2_000, "a", 5.0).close();
    refresh();

    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 15), new Bucket(H0 + 2 * HOUR, "a", 20));
  }

  @Test
  void aDeleteFromARenamedTypeIsReflectedByTheNextRefresh() {
    createSourceAndAggregate();
    twoBucketsAndARefresh();
    database.command("sql", "ALTER TYPE S NAME S2");

    database.command("sql", "DELETE FROM S2 WHERE ts = " + (H0 + 1_000)).close();
    refresh();

    assertThat(buckets()).containsExactly(new Bucket(H0 + 2 * HOUR, "a", 20));
  }

  // ---- the cost: a stream of late samples does not save the schema once per batch

  @Test
  void lateSamplesOfABucketAlreadyRecordedSaveTheSchemaOnce() throws Exception {
    createSourceAndAggregate();
    twoBucketsAndARefresh();
    final long version = schemaVersion();

    // the cost of the record alone, as an INSERT in a transaction pays it until the commit refreshes: the refresh of the engine API
    // consumes the record after each append (#10686)
    engine().appendSamplesWithoutRefresh(new long[] { H0 + 2_000 }, new Object[] { "a" }, new Object[] { 1.0 });
    final long afterFirst = schemaVersion();
    assertThat(afterFirst).isGreaterThan(version);

    // later samples of the same bucket, in later batches, ascending as a backfill writes them
    for (int i = 1; i <= 20; i++)
      engine().appendSamplesWithoutRefresh(new long[] { H0 + 2_000 + i * 60_000L }, new Object[] { "a" }, new Object[] { 1.0 });
    assertThat(schemaVersion()).isEqualTo(afterFirst);

    refresh();
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 31), new Bucket(H0 + 2 * HOUR, "a", 20));
  }

  // ---- durability

  @Test
  void theRecordOfALateSampleSurvivesARestart() throws Exception {
    createSourceAndAggregate();
    twoBucketsAndARefresh();

    // without the refresh of the engine API, which would consume the record before the restart
    engine().appendSamplesWithoutRefresh(new long[] { H0 + 2_000 }, new Object[] { "a" }, new Object[] { 5.0 });
    reopenDatabase();

    assertThat(aggregate().getInvalidation()).isNotNull();
    refresh();
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 15), new Bucket(H0 + 2 * HOUR, "a", 20));
  }
}
