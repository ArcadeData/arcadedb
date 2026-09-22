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
import com.arcadedb.exception.TransactionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.ContinuousAggregate;
import com.arcadedb.schema.ContinuousAggregateImpl;
import com.arcadedb.schema.ContinuousAggregateRefresher;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * https://github.com/ArcadeData/arcadedb/issues/8152 and
 * https://github.com/ArcadeData/arcadedb/issues/8156
 * <p>
 * A continuous aggregate's whole refresh loop rests on one number, the watermark: rows whose bucket is at or after
 * it are deleted and recomputed, rows before it are left alone. Two defects broke that, and the second was masked by
 * the first.
 * <ol>
 *   <li><b>#8152</b> - the refresher read the bucket column with a private converter that knew {@code Date},
 *   {@code Long} and {@code Number} and fell through to {@code return 0} for everything else. The bucket column is
 *   produced by {@code ts.timeBucket()}, which answers a {@code LocalDateTime} on BOTH of its evaluation paths
 *   (#7610 and #4385), so every bucket read as 0, the watermark never moved off its initial 0, the opening
 *   {@code DELETE} was skipped and the defining query ran unfiltered. Each refresh therefore APPENDED a complete
 *   second copy of the aggregate, and a refresh is scheduled after every commit into the source type: 1, 3, 6, 10
 *   rows for four buckets. The status stayed VALID and nothing was logged.</li>
 *   <li><b>#8156</b> - the watermark filter was spliced in after the caller's {@code WHERE} with no bracket, and
 *   {@code AND} binds tighter than {@code OR}. A defining query whose WHERE is a disjunction was rewritten into a
 *   different predicate whose second disjunct escapes the watermark entirely, re-aggregating buckets OLDER than it -
 *   buckets the opening DELETE does not cover, so their rows survive and the recomputed ones land beside them.</li>
 * </ol>
 * Both show up as the same silent symptom, duplicated aggregate rows, so both are asserted the same way: the
 * aggregate must hold EXACTLY one row per (tag, bucket) pair, not merely be non-empty - which is what the existing
 * assertions checked, and why neither defect was caught.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8152ContinuousAggregateWatermarkTest extends TestHelper {

  private static final long HOUR = 3_600_000L;

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

    // A database whose transaction() runs the scope and then fails the commit.
    final Database failingCommit = (Database) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] { Database.class }, (proxy, method, args) -> {
          if ("transaction".equals(method.getName()) && args != null && args.length == 1) {
            database.transaction(() -> {
              ((BasicDatabase.TransactionScope) args[0]).execute();
              throw new TransactionException("simulated commit failure");
            });
            return null;
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

  private void createSensorType() {
    database.command("sql",
        "CREATE TIMESERIES TYPE SensorReading TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE)");
  }

  private void createHourlyAggregate(final String query) {
    database.getSchema().buildContinuousAggregate().withName("hourly_temps").withQuery(query).create();
  }

  private void insert(final long ts, final String sensor, final double temperature) {
    database.command("sql", "INSERT INTO SensorReading SET ts = ?, sensor_id = ?, temperature = ?",
        ts, sensor, temperature);
  }

  private long countAggregateRows() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM hourly_temps")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private long rowsFor(final String sensor) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM hourly_temps WHERE sensor_id = ?",
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
}
