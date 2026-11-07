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
import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.LineProtocolParser.Sample;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.ContinuousAggregate;
import com.arcadedb.schema.ContinuousAggregateImpl;
import com.arcadedb.schema.ContinuousAggregateRefresher;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ContinuousAggregateTest extends TestHelper {

  @Test
  void createAndInitialPopulation() {
    createSensorType();
    insertInitialData();

    final ContinuousAggregate ca = database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .create();

    assertThat(ca.getName()).isEqualTo("hourly_temps");
    assertThat(ca.getSourceTypeName()).isEqualTo("SensorReading");
    assertThat(ca.getBucketColumn()).isEqualTo("hour");
    assertThat(ca.getBucketIntervalMs()).isEqualTo(3_600_000L);
    assertThat(ca.getStatus()).isEqualTo("VALID");
    assertThat(ca.getWatermarkTs()).isGreaterThanOrEqualTo(0);

    // Verify backing type has data
    final ResultSet rs = database.query("sql", "SELECT FROM hourly_temps");
    final List<Result> results = collectResults(rs);
    assertThat(results).isNotEmpty();
  }

  @Test
  void incrementalRefreshOnInsert() {
    createSensorType();
    insertInitialData();

    database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .create();

    final long initialWatermark = database.getSchema().getContinuousAggregate("hourly_temps").getWatermarkTs();

    // Insert new data in a later hour
    database.transaction(() -> {
      database.command("sql",
          "INSERT INTO SensorReading SET ts = 7200000, sensor_id = 'A', temperature = 30.0");
      database.command("sql",
          "INSERT INTO SensorReading SET ts = 7201000, sensor_id = 'A', temperature = 32.0");
    });

    // The post-commit callback should have triggered an incremental refresh. #8152: this used to be
    // isGreaterThanOrEqualTo, which a watermark stuck at its initial 0 satisfies just as well - and it was.
    final ContinuousAggregate ca = database.getSchema().getContinuousAggregate("hourly_temps");
    assertThat(ca.getWatermarkTs()).isGreaterThan(initialWatermark);
    assertThat(ca.getWatermarkTs()).isEqualTo(7_200_000L);
    assertThat(ca.getStatus()).isEqualTo("VALID");

    // Verify the new bucket data exists
    final ResultSet rs = database.query("sql",
        "SELECT FROM hourly_temps WHERE hour >= ?", 7200000L);
    final List<Result> results = collectResults(rs);
    assertThat(results).isNotEmpty();
  }

  @Test
  void watermarkAdvances() {
    createSensorType();

    // Insert data at hour 0
    database.transaction(() ->
      database.command("sql",
          "INSERT INTO SensorReading SET ts = 100, sensor_id = 'A', temperature = 20.0"));

    database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .create();

    final long wm1 = database.getSchema().getContinuousAggregate("hourly_temps").getWatermarkTs();
    assertThat(wm1).isEqualTo(0L); // bucket start for ts=100 with 1h interval is 0

    // Insert data at hour 1
    database.transaction(() ->
      database.command("sql",
          "INSERT INTO SensorReading SET ts = 3600000, sensor_id = 'A', temperature = 25.0"));

    // #8152: STRICTLY greater. The old isGreaterThanOrEqualTo held for a watermark that never moved at all.
    final long wm2 = database.getSchema().getContinuousAggregate("hourly_temps").getWatermarkTs();
    assertThat(wm2).isGreaterThan(wm1);
    assertThat(wm2).isEqualTo(3_600_000L);
  }

  @Test
  void dropContinuousAggregate() {
    createSensorType();
    insertInitialData();

    database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .create();

    assertThat(database.getSchema().existsContinuousAggregate("hourly_temps")).isTrue();
    assertThat(database.getSchema().existsType("hourly_temps")).isTrue();

    database.getSchema().dropContinuousAggregate("hourly_temps");

    assertThat(database.getSchema().existsContinuousAggregate("hourly_temps")).isFalse();
    assertThat(database.getSchema().existsType("hourly_temps")).isFalse();
  }

  @Test
  void ifNotExistsIdempotent() {
    createSensorType();
    insertInitialData();

    database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .create();

    // Should not throw
    final ContinuousAggregate ca2 = database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .withIgnoreIfExists(true)
        .create();

    assertThat(ca2.getName()).isEqualTo("hourly_temps");
  }

  @Test
  void manualRefresh() {
    createSensorType();
    insertInitialData();

    database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .create();

    final ContinuousAggregate ca = database.getSchema().getContinuousAggregate("hourly_temps");
    final long countBefore = ca.getRefreshCount();

    ca.refresh();

    assertThat(ca.getRefreshCount()).isGreaterThan(countBefore);
    assertThat(ca.getStatus()).isEqualTo("VALID");
  }

  @Test
  void schemaPersistence() {
    createSensorType();
    insertInitialData();

    database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .create();

    // Close and reopen
    final String dbPath = database.getDatabasePath();
    database.close();
    database = factory.open();

    assertThat(database.getSchema().existsContinuousAggregate("hourly_temps")).isTrue();
    final ContinuousAggregate ca = database.getSchema().getContinuousAggregate("hourly_temps");
    assertThat(ca.getSourceTypeName()).isEqualTo("SensorReading");
    assertThat(ca.getBucketColumn()).isEqualTo("hour");
  }

  @Test
  void invalidQueryNoTimeBucket() {
    createSensorType();

    assertThatThrownBy(() ->
        database.getSchema().buildContinuousAggregate()
            .withName("bad_ca")
            .withQuery("SELECT sensor_id, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id")
            .create()
    ).isInstanceOf(SchemaException.class)
        .hasMessageContaining("ts.timeBucket");
  }

  @Test
  void invalidQueryNonTimeSeriesSource() {
    database.getSchema().buildDocumentType().withName("RegularDoc").create();

    assertThatThrownBy(() ->
        database.getSchema().buildContinuousAggregate()
            .withName("bad_ca")
            .withQuery("SELECT ts.timeBucket('1h', ts) AS hour, count(*) AS cnt FROM RegularDoc GROUP BY hour")
            .create()
    ).isInstanceOf(SchemaException.class)
        .hasMessageContaining("not a TimeSeries type");
  }

  @Test
  void invalidQueryNoGroupBy() {
    createSensorType();

    assertThatThrownBy(() ->
        database.getSchema().buildContinuousAggregate()
            .withName("bad_ca")
            .withQuery("SELECT ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading")
            .create()
    ).isInstanceOf(SchemaException.class)
        .hasMessageContaining("GROUP BY");
  }

  @Test
  void getContinuousAggregates() {
    createSensorType();
    insertInitialData();

    database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .create();

    final ContinuousAggregate[] aggregates = database.getSchema().getContinuousAggregates();
    assertThat(aggregates).hasSize(1);
    assertThat(aggregates[0].getName()).isEqualTo("hourly_temps");
  }

  @Test
  void protectSourceTypeFromDrop() {
    createSensorType();
    insertInitialData();

    database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .create();

    assertThatThrownBy(() -> database.getSchema().dropType("SensorReading"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("continuous aggregate");
  }

  private void createSensorType() {
    database.command("sql",
        "CREATE TIMESERIES TYPE SensorReading TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE)");
  }

  private void insertInitialData() {
    database.transaction(() -> {
      database.command("sql",
          "INSERT INTO SensorReading SET ts = 1000, sensor_id = 'A', temperature = 22.5");
      database.command("sql",
          "INSERT INTO SensorReading SET ts = 2000, sensor_id = 'B', temperature = 23.1");
      database.command("sql",
          "INSERT INTO SensorReading SET ts = 3000, sensor_id = 'A', temperature = 21.8");
    });
  }

  private List<Result> collectResults(final ResultSet rs) {
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());
    return results;
  }

  private static final long HOUR = 3_600_000L;

  // https://github.com/ArcadeData/arcadedb/issues/8152 and
  // https://github.com/ArcadeData/arcadedb/issues/8156
  // <p>
  // A continuous aggregate's whole refresh loop rests on one number, the watermark: rows whose bucket is at or after
  // it are deleted and recomputed, rows before it are left alone. Two defects broke that, and the second was masked by
  // the first.
  // <ol>
  //   <li><b>#8152</b> - the refresher read the bucket column with a private converter that knew {@code Date},
  //   {@code Long} and {@code Number} and fell through to {@code return 0} for everything else. The bucket column is
  //   produced by {@code ts.timeBucket()}, which answers a {@code LocalDateTime} on BOTH of its evaluation paths
  //   (#7610 and #4385), so every bucket read as 0, the watermark never moved off its initial 0, the opening
  //   {@code DELETE} was skipped and the defining query ran unfiltered. Each refresh therefore APPENDED a complete
  //   second copy of the aggregate, and a refresh is scheduled after every commit into the source type: 1, 3, 6, 10
  //   rows for four buckets. The status stayed VALID and nothing was logged.</li>
  //   <li><b>#8156</b> - the watermark filter was spliced in after the caller's {@code WHERE} with no bracket, and
  //   {@code AND} binds tighter than {@code OR}. A defining query whose WHERE is a disjunction was rewritten into a
  //   different predicate whose second disjunct escapes the watermark entirely, re-aggregating buckets OLDER than it -
  //   buckets the opening DELETE does not cover, so their rows survive and the recomputed ones land beside them.</li>
  // </ol>
  // Both show up as the same silent symptom, duplicated aggregate rows, so both are asserted the same way: the
  // aggregate must hold EXACTLY one row per (tag, bucket) pair, not merely be non-empty - which is what the existing
  // assertions checked, and why neither defect was caught.
  // #8152
  @Test
  void everyBucketAppearsExactlyOnceAfterManyRefreshes() {
    createSensorType();

    database.transaction(() -> insert(0L, "A", 20.0));
    createHourlyAggregate("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp "
        + "FROM SensorReading GROUP BY sensor_id, hour");

    // One commit per hour, each firing its own after-commit refresh.
    for (int hour = 1; hour <= 4; hour++) {
      final long ts = hour * HOUR;
      database.transaction(() -> insert(ts, "A", 20.0 + ts / HOUR));
    }

    // Five distinct buckets for one sensor. Before the fix: 1, 3, 6, 10, 15.
    assertThat(countAggregateRows()).isEqualTo(5);
    assertThat(distinctBuckets()).isEqualTo(5);
  }

  // #8152
  @Test
  void theWatermarkTracksTheNewestBucket() {
    createSensorType();

    database.transaction(() -> insert(100L, "A", 20.0));
    createHourlyAggregate("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp "
        + "FROM SensorReading GROUP BY sensor_id, hour");

    final ContinuousAggregate ca = database.getSchema().getContinuousAggregate("hourly_temps");
    // #8152: the epoch bucket is a REAL watermark, and 0 alone could not say so.
    assertThat(ca.getWatermarkTs()).isEqualTo(0L);
    assertThat(ca.isWatermarkSet()).isTrue();

    database.transaction(() -> insert(HOUR, "A", 25.0));
    // STRICTLY greater: the existing assertions used isGreaterThanOrEqualTo, which a watermark that never moves
    // satisfies just as well.
    assertThat(ca.getWatermarkTs()).isEqualTo(HOUR);

    database.transaction(() -> insert(2 * HOUR, "A", 26.0));
    assertThat(ca.getWatermarkTs()).isEqualTo(2 * HOUR);
  }

  // #8152
  /**
   * #8156: the aggregate's WHERE is a disjunction. Every (sensor, hour) pair that satisfies it must appear once.
   * Sensor A qualifies through {@code sensor_id = 'A'} - the disjunct that used to escape the watermark - and
   * sensor B through {@code temperature > 100}, the one that stayed inside the AND and was always correct.
   */
  @Test
  void aDisjunctiveWhereDoesNotReAggregateOldBuckets() {
    createSensorType();

    database.transaction(() -> {
      insert(0L, "A", 20.0);
      insert(0L, "B", 200.0);
    });
    createHourlyAggregate("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp "
        + "FROM SensorReading WHERE temperature > 100 OR sensor_id = 'A' GROUP BY sensor_id, hour");

    for (int hour = 1; hour <= 3; hour++) {
      final long ts = hour * HOUR;
      database.transaction(() -> {
        insert(ts, "A", 20.0);
        insert(ts, "B", 200.0);
      });
    }

    // 2 sensors x 4 hours. Before the fix sensor A gained a copy of every earlier hour on every refresh.
    assertThat(countAggregateRows()).isEqualTo(8);
    assertThat(rowsFor("A")).isEqualTo(4);
    assertThat(rowsFor("B")).isEqualTo(4);
  }

  // #8152
  /**
   * A refresh that cannot read its own bucket column must FAIL rather than silently treat it as the epoch - the
   * {@code return 0} fall-through is what turned a return-type change into a data defect.
   */
  @Test
  void refreshingTwiceWithNoNewDataChangesNothing() {
    createSensorType();

    database.transaction(() -> {
      insert(0L, "A", 20.0);
      insert(HOUR, "A", 30.0);
    });
    createHourlyAggregate("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp "
        + "FROM SensorReading GROUP BY sensor_id, hour");

    final long afterCreate = countAggregateRows();
    assertThat(afterCreate).isEqualTo(2);

    final ContinuousAggregate ca = database.getSchema().getContinuousAggregate("hourly_temps");
    ca.refresh();
    ca.refresh();

    assertThat(countAggregateRows()).isEqualTo(afterCreate);
    assertThat(ca.getStatus()).isEqualTo("VALID");
  }

  // #8152
  /**
   * The watermark and its "has been set" flag both have to survive a reopen, otherwise the first refresh after a
   * restart re-appends the whole aggregate exactly as the defect did.
   */
  @Test
  void theWatermarkSurvivesAReopen() {
    createSensorType();

    database.transaction(() -> insert(0L, "A", 20.0));
    createHourlyAggregate("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp "
        + "FROM SensorReading GROUP BY sensor_id, hour");

    database.close();
    database = factory.open();

    final ContinuousAggregate ca = database.getSchema().getContinuousAggregate("hourly_temps");
    assertThat(ca.getWatermarkTs()).isEqualTo(0L);
    assertThat(ca.isWatermarkSet()).isTrue();

    ca.refresh();
    assertThat(countAggregateRows()).isEqualTo(1);
  }

  // #8152
  /**
   * Found in review: the new watermark used to be installed INSIDE the refresh transaction. A commit that then
   * failed rolled the rows back and left the watermark ahead of the data - the next refresh trusted it and skipped
   * the window that had just been lost. The commit is made to fail here by throwing at the very end of the
   * transaction scope, after every row has been written.
   */
  @Test
  void aFailedCommitLeavesTheWatermarkWhereItWas() {
    createSensorType();

    database.transaction(() -> insert(0L, "A", 20.0));
    createHourlyAggregate("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp "
        + "FROM SensorReading GROUP BY sensor_id, hour");

    final ContinuousAggregateImpl ca = (ContinuousAggregateImpl) database.getSchema()
        .getContinuousAggregate("hourly_temps");

    database.transaction(() -> insert(2 * HOUR, "A", 30.0));
    assertThat(ca.getWatermarkTs()).isEqualTo(2 * HOUR);

    final long rowsBefore = countAggregateRows();
    assertThat(rowsBefore).isEqualTo(2);

    // Wind the watermark back to the epoch bucket, so the failing refresh has a real advance to make: it will
    // recompute both buckets and want to move the watermark to hour 2.
    ca.setWatermarkTs(0);
    final long watermarkBefore = ca.getWatermarkTs();

    // A database whose transaction() runs the scope and then fails the commit. The refresh asks for a transaction of its own
    // (joinCurrentTx=false, #10688), the 2-argument form
    final Database failingCommit = (Database) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] { Database.class }, (proxy, method, args) -> {
          if ("transaction".equals(method.getName()) && args != null && args.length == 2) {
            assertThat(args[1]).isEqualTo(false);
            database.transaction(() -> {
              ((BasicDatabase.TransactionScope) args[0]).execute();
              throw new TransactionException("simulated commit failure");
            }, false);
            return true;
          }
          return method.invoke(database, args);
        });

    assertThatThrownBy(() -> ContinuousAggregateRefresher.incrementalRefresh(failingCommit, ca))
        .isInstanceOf(TransactionException.class);

    // Neither the watermark nor the rows moved: the refresh that failed left nothing behind to skip over.
    assertThat(ca.getWatermarkTs()).isEqualTo(watermarkBefore);
    assertThat(countAggregateRows()).isEqualTo(rowsBefore);
    assertThat(ca.getStatus()).isEqualTo("ERROR");
  }

  // #8152
  /**
   * The flag is also a column on {@code schema:continuousAggregates}, which is how an operator tells "never
   * refreshed" from "anchored at the epoch" - both of which report a watermark of 0.
   */
  @Test
  void theSchemaListingReportsWhetherTheWatermarkIsSet() {
    createSensorType();

    database.transaction(() -> insert(0L, "A", 20.0));
    createHourlyAggregate("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp "
        + "FROM SensorReading GROUP BY sensor_id, hour");

    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:continuousAggregates")) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("name")).isEqualTo("hourly_temps");
      assertThat(row.<Long>getProperty("watermarkTs")).isEqualTo(0L);
      assertThat(row.<Boolean>getProperty("watermarkSet")).isTrue();
    }
  }

  private void createHourlyAggregate(final String query) {
    database.getSchema().buildContinuousAggregate().withName("hourly_temps").withQuery(query).create();
  }

  private void insert(final long ts, final String sensor, final double temperature) {
    database.command("sql", "INSERT INTO SensorReading SET ts = ?, sensor_id = ?, temperature = ?",
        ts, sensor, temperature);
  }

  /**
   * {@code count(@rid)}, NOT {@code count(*)}: the latter answers from a cached per-bucket counter plus the
   * transaction delta rather than scanning (see {@code engine/CLAUDE.md}), and these assertions exist precisely to
   * catch rows that should not be there. Ground truth has to come from a real scan.
   */
  private long countAggregateRows() {
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM hourly_temps")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private long rowsFor(final String sensor) {
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM hourly_temps WHERE sensor_id = ?",
        sensor)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private long distinctBuckets() {
    final List<Object> buckets = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM hourly_temps")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final Object bucket = row.getProperty("hour");
        if (!buckets.contains(bucket))
          buckets.add(bucket);
      }
    }
    return buckets.size();
  }

  /** An hour boundary, so that the buckets of the default grid start where the test says they do. */
  private static final long H0   = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);

  private record Bucket(long start, String tag, double total) {
  }

  private void createSourceAndAggregate() {
    database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE)");
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1h', ts) AS hr, h, sum(v) AS total FROM S GROUP BY hr, h");
  }

  private TimeSeriesEngine engine() {
    return ((LocalTimeSeriesType) database.getSchema().getType("S")).getEngine();
  }

  private void append(final long ts, final double v) throws Exception {
    // without the refresh the engine API runs after an append (#10686), so the refreshes are the ones the test runs
    engine().appendSamplesWithoutRefresh(new long[] { ts }, new Object[] { "a" }, new Object[] { v });
  }

  private ContinuousAggregateImpl aggregate() {
    return (ContinuousAggregateImpl) database.getSchema().getContinuousAggregate("A");
  }

  private void refresh() {
    database.command("sql", "REFRESH CONTINUOUS AGGREGATE A").close();
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

  /**
   * Runs a refresh in another thread, holds it once it has read the source (when it writes its first row of the backing type: the
   * GROUP BY of the defining query has consumed every sample by then), runs {@code duringRefresh}, then lets the refresh end.
   */
  private void duringARefresh(final ThrowingRunnable duringRefresh) throws Exception {
    final CountDownLatch sourceRead = new CountDownLatch(1);
    final CountDownLatch resume = new CountDownLatch(1);
    final AtomicBoolean paused = new AtomicBoolean();
    final BeforeRecordCreateListener pause = record -> {
      if (paused.compareAndSet(false, true)) {
        sourceRead.countDown();
        try {
          resume.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return true;
    };
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    database.getSchema().getType("A").getEvents().registerListener(pause);
    try {
      final Thread refresher = new Thread(() -> {
        try {
          refresh();
        } catch (final Throwable t) {
          failure.set(t);
        }
      });
      refresher.start();
      assertThat(sourceRead.await(30, TimeUnit.SECONDS)).isTrue();
      try {
        duringRefresh.run();
      } finally {
        resume.countDown();
        refresher.join(30_000);
      }
      assertThat(refresher.isAlive()).isFalse();
      assertThat(failure.get()).isNull();
    } finally {
      database.getSchema().getType("A").getEvents().unregisterListener(pause);
    }
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  // Issue #10633: an append that commits while a refresh of a continuous aggregate runs judged its samples "behind the watermark"
  // against the watermark of before that refresh. A sample at or after it, which the refresh in progress had not read, was then not
  // recorded, and when the refresh installed a newer watermark past it no later refresh read it again: the sample never reached its
  // bucket.
  // <p>
  // Each test pauses a real refresh once it has read the source (it is about to write the first row of the backing type), appends a
  // sample, and lets the refresh end.
  // #10633
  /** The repro of the issue: the sample is at the old watermark, and the refresh moves the watermark one bucket past it. */
  @Test
  void aSampleAppendedDuringARefreshThatMovesTheWatermarkPastItReachesItsBucket() throws Exception {
    createSourceAndAggregate();
    append(H0 + 1_000, 10);
    append(H0 + 2 * HOUR + 1_000, 20);
    refresh();
    assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 2 * HOUR);
    // at the watermark: the next refresh reads it, and moves the watermark to its bucket
    append(H0 + 3 * HOUR + 1_000, 30);

    // not behind the watermark of before the refresh, behind the one the refresh installs
    duringARefresh(() -> append(H0 + 2 * HOUR + 2_000, 5));
    assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 3 * HOUR);
    assertThat(aggregate().getInvalidation()).isNotNull();

    refresh();
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0 + 2 * HOUR, "a", 25),
        new Bucket(H0 + 3 * HOUR, "a", 30));
    assertThat(aggregate().getInvalidation()).isNull();
  }

  // #10633
  /** The first refresh has no watermark to judge against at all, and installs one. */
  @Test
  void aSampleAppendedDuringTheFirstRefreshReachesItsBucket() throws Exception {
    createSourceAndAggregate();
    assertThat(aggregate().isWatermarkSet()).isFalse();
    append(H0 + 1_000, 10);
    append(H0 + HOUR + 1_000, 20);

    duringARefresh(() -> append(H0 + 2_000, 5));
    assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + HOUR);

    refresh();
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 15), new Bucket(H0 + HOUR, "a", 20));
    assertThat(aggregate().getInvalidation()).isNull();
  }

  // #10633
  /**
   * An {@code INSERT} in a transaction refreshes the aggregate once the transaction commits. The refresh it asks for while another one runs is not skipped any
   * more: the sample is recorded, so the refresh in progress runs again when it ends and nothing else has to be asked.
   */
  @Test
  void anInsertDuringARefreshIsAggregatedWhenThatRefreshEnds() throws Exception {
    createSourceAndAggregate();
    append(H0 + 1_000, 10);
    append(H0 + 2 * HOUR + 1_000, 20);
    refresh();
    append(H0 + 3 * HOUR + 1_000, 30);

    // in a transaction: its commit is what runs the refresh of the INSERT
    duringARefresh(() -> database.transaction(
        () -> database.command("sql", "INSERT INTO S SET ts = ?, h = ?, v = ?", H0 + 2 * HOUR + 2_000, "a", 5.0).close()));

    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0 + 2 * HOUR, "a", 25),
        new Bucket(H0 + 3 * HOUR, "a", 30));
    assertThat(aggregate().getInvalidation()).isNull();
  }

  // #10633
  /**
   * One batch during a refresh: a sample behind the old watermark, one at it, and one after the watermark the refresh installs. The
   * record spans all of them; the next refresh recomputes the buckets behind its own watermark and reads the newer one as usual.
   */
  @Test
  void aBatchAcrossSeveralBucketsDuringARefreshReachesEveryBucket() throws Exception {
    createSourceAndAggregate();
    append(H0 + 1_000, 10);
    append(H0 + 2 * HOUR + 1_000, 20);
    refresh();
    append(H0 + 3 * HOUR + 1_000, 30);

    duringARefresh(() -> engine().appendSamplesWithoutRefresh(new long[] { H0 + 2_000, H0 + 2 * HOUR + 2_000, H0 + 4 * HOUR + 2_000 },
        new Object[] { "a", "a", "a" }, new Object[] { 1.0, 2.0, 4.0 }));
    assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 3 * HOUR);

    refresh();
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 11), new Bucket(H0 + 2 * HOUR, "a", 22),
        new Bucket(H0 + 3 * HOUR, "a", 30), new Bucket(H0 + 4 * HOUR, "a", 4));
    assertThat(aggregate().getInvalidation()).isNull();
  }

  // #10633
  /** A sample behind the watermark of before the refresh is recorded during a refresh as it is outside one (#10502). */
  @Test
  void aSampleBehindTheOldWatermarkDuringARefreshReachesItsBucket() throws Exception {
    createSourceAndAggregate();
    append(H0 + 1_000, 10);
    append(H0 + 2 * HOUR + 1_000, 20);
    refresh();
    append(H0 + 3 * HOUR + 1_000, 30);

    duringARefresh(() -> append(H0 + HOUR + 2_000, 7));

    refresh();
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0 + HOUR, "a", 7), new Bucket(H0 + 2 * HOUR, "a", 20),
        new Bucket(H0 + 3 * HOUR, "a", 30));
  }

  // #10633
  /** Outside a refresh, a sample at or after the watermark is still recorded nowhere: the next refresh reads it. */
  @Test
  void aSampleAtTheWatermarkOutsideARefreshRecordsNothing() throws Exception {
    createSourceAndAggregate();
    append(H0 + 1_000, 10);
    refresh();

    append(H0 + 2_000, 5);
    assertThat(aggregate().getInvalidation()).isNull();
  }

  /**
   * Issue #10502: the incremental refresh of a continuous aggregate recomputes only the bucket open at its watermark and the newer
   * ones, so a sample written with a timestamp behind the watermark was never aggregated into its bucket.
   * <p>
   * An append now records, on every aggregate over the type whose watermark it is behind, the range of its late samples, as a delete
   * does since #10428, and the next refresh recomputes the buckets of that range. The check is in the shard append every ingest path
   * goes through, so SQL {@code INSERT}, the engine API, the async API and the line protocol gateway all record it; these tests drive
   * each of them.
   */
  @Nested
  class ContinuousAggregateLateSample {
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

    // #10502
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

    // #10502
    @Test
    void anInsertIntoABucketWithNoRowBehindTheWatermarkCreatesTheBucket() {
      createSourceAndAggregate();
      twoBucketsAndARefresh();

      insert(H0 + HOUR + 1_000, "a", 7);
      refresh();

      assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0 + HOUR, "a", 7),
          new Bucket(H0 + 2 * HOUR, "a", 20));
    }

    // #10502
    @Test
    void anInsertOlderThanEveryBucketCreatesTheBucket() {
      createSourceAndAggregate();
      twoBucketsAndARefresh();

      insert(H0 - 5 * HOUR + 1_000, "a", 3);
      refresh();

      assertThat(buckets()).containsExactly(new Bucket(H0 - 5 * HOUR, "a", 3), new Bucket(H0, "a", 10),
          new Bucket(H0 + 2 * HOUR, "a", 20));
    }

    // #10502
    @Test
    void anInsertOfANewTagBehindTheWatermarkAddsItsRowAndKeepsTheOthers() {
      createSourceAndAggregate();
      twoBucketsAndARefresh();

      insert(H0 + 3_000, "b", 4);
      refresh();

      assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0, "b", 4), new Bucket(H0 + 2 * HOUR, "a", 20));
    }

    // #10502
    @Test
    void anInsertInsideATransactionReachesItsBucketWhenTheTransactionCommits() {
      createSourceAndAggregate();
      twoBucketsAndARefresh();

      database.transaction(() -> insert(H0 + 2_000, "a", 5));

      // no REFRESH: an INSERT in a transaction refreshes the aggregates over its type when the transaction commits
      assertThat(buckets()).containsExactly(new Bucket(H0, "a", 15), new Bucket(H0 + 2 * HOUR, "a", 20));
    }

    // #10502
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

    // #10502
    // ---- the paths that are not statements: since #10686 they refresh by themselves, and that refresh consumes the record

    @Test
    void anAppendThroughTheEngineIsAggregatedByItsOwnRefresh() throws Exception {
      createSourceAndAggregate();
      twoBucketsAndARefresh();

      engine().appendSamples(new long[] { H0 + 2_000 }, new Object[] { "a" }, new Object[] { 5.0 });

      assertThat(buckets()).containsExactly(new Bucket(H0, "a", 15), new Bucket(H0 + 2 * HOUR, "a", 20));
      assertThat(aggregate().getInvalidation()).isNull();
    }

    // #10502
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

    // #10502
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

    // #10502
    @Test
    void anAsyncAppendIsAggregatedByItsOwnRefresh() {
      createSourceAndAggregate();
      twoBucketsAndARefresh();

      database.async().appendSamples("S", new long[] { H0 + 2_000 }, new Object[] { "a" }, new Object[] { 5.0 });
      database.async().waitCompletion();

      assertThat(buckets()).containsExactly(new Bucket(H0, "a", 15), new Bucket(H0 + 2 * HOUR, "a", 20));
    }

    // #10502
    @Test
    void aLineProtocolWriteIsAggregatedByItsOwnRefresh() throws Exception {
      createSourceAndAggregate();
      twoBucketsAndARefresh();

      TimeSeriesGateway.write((DatabaseInternal) database,
          new ArrayList<>(List.of(new Sample("S", Map.of("h", "a"), Map.of("v", 5.0), H0 + 2_000))));

      assertThat(buckets()).containsExactly(new Bucket(H0, "a", 15), new Bucket(H0 + 2 * HOUR, "a", 20));
    }

    // #10502
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

    // #10502
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

    // #10502
    @Test
    void anAggregateOverAnotherTypeRecordsNothing() throws Exception {
      createSourceAndAggregate();
      twoBucketsAndARefresh();
      database.command("sql", "CREATE TIMESERIES TYPE Other TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE)");

      ((LocalTimeSeriesType) database.getSchema().getType("Other")).getEngine()
          .appendSamples(new long[] { H0 }, new Object[] { "a" }, new Object[] { 1.0 });

      assertThat(aggregate().getInvalidation()).isNull();
    }

    // #10502
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

    // #10502
    @Test
    void aDeleteFromARenamedTypeIsReflectedByTheNextRefresh() {
      createSourceAndAggregate();
      twoBucketsAndARefresh();
      database.command("sql", "ALTER TYPE S NAME S2");

      database.command("sql", "DELETE FROM S2 WHERE ts = " + (H0 + 1_000)).close();
      refresh();

      assertThat(buckets()).containsExactly(new Bucket(H0 + 2 * HOUR, "a", 20));
    }

    // #10502
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

    // #10502
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
}
