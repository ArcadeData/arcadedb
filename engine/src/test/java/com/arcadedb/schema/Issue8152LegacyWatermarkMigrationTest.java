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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * https://github.com/ArcadeData/arcadedb/issues/8152 - the upgrade path (found by CodeRabbit).
 * <p>
 * A schema written before the watermark fix carries a {@code watermarkTs} of 0 and no {@code watermarkSet}
 * property, and because the defect pinned EVERY watermark at 0 that describes every continuous aggregate that ever
 * ran on an affected version. Two things follow from that one ambiguous number:
 * <ul>
 *   <li>its backing type already holds one full copy of the aggregate per refresh that has happened, and</li>
 *   <li>reading it back as "never refreshed" - the only reading that is safe in general - makes the next refresh
 *   re-aggregate the whole source range and append it on top, adding one more copy before the repaired watermark
 *   takes hold.</li>
 * </ul>
 * So the ambiguous legacy zero is treated as what it is, a database carrying the defect's duplicates, and the next
 * refresh rebuilds the backing type from empty exactly once. That repairs the existing duplication rather than
 * merely declining to add to it. A legacy watermark that is NOT zero did advance, so its refreshes deleted before
 * recomputing and it is healthy - it must not be rebuilt.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8152LegacyWatermarkMigrationTest extends TestHelper {

  private static final long HOUR = 3_600_000L;

  @Test
  void anAmbiguousLegacyZeroWatermarkRebuildsTheBackingTypeOnce() {
    final ContinuousAggregateImpl ca = createAggregate();

    // What the defect leaves behind: extra full copies of the aggregate, one per refresh that ran.
    duplicateBackingRows();
    duplicateBackingRows();
    assertThat(countBackingRows()).isEqualTo(8);

    final ContinuousAggregateImpl legacy = reloadAsLegacySchema(ca, 0);
    assertThat(legacy.needsCleanRebuild()).isTrue();
    assertThat(legacy.isWatermarkSet()).isFalse();

    ContinuousAggregateRefresher.incrementalRefresh(database, legacy);

    // Rebuilt from empty: one row per (sensor, bucket), and the watermark is now real.
    assertThat(countBackingRows()).isEqualTo(2);
    assertThat(legacy.isWatermarkSet()).isTrue();
    assertThat(legacy.getWatermarkTs()).isEqualTo(HOUR);
    assertThat(legacy.needsCleanRebuild()).isFalse();

    // And the repair does not repeat: a second refresh is an ordinary incremental one.
    ContinuousAggregateRefresher.incrementalRefresh(database, legacy);
    assertThat(countBackingRows()).isEqualTo(2);
  }

  @Test
  void aLegacyNonZeroWatermarkIsHealthyAndIsNotRebuilt() {
    final ContinuousAggregateImpl ca = createAggregate();

    // Its watermark advanced, so its refreshes deleted before recomputing: nothing to repair.
    final ContinuousAggregateImpl legacy = reloadAsLegacySchema(ca, HOUR);
    assertThat(legacy.needsCleanRebuild()).isFalse();
    assertThat(legacy.isWatermarkSet()).isTrue();
    assertThat(legacy.getWatermarkTs()).isEqualTo(HOUR);
  }

  /**
   * A schema this version wrote carries the flag, so it is never mistaken for a legacy one - not even when its
   * watermark is a perfectly legitimate 0.
   */
  @Test
  void anAggregateAnchoredAtTheEpochIsNotMistakenForALegacyOne() {
    database.command("sql",
        "CREATE TIMESERIES TYPE SensorReading TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE)");
    database.transaction(() -> database.command("sql",
        "INSERT INTO SensorReading SET ts = 100, sensor_id = 'A', temperature = 20.0"));
    final ContinuousAggregateImpl ca = (ContinuousAggregateImpl) database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp "
            + "FROM SensorReading GROUP BY sensor_id, hour")
        .create();

    assertThat(ca.getWatermarkTs()).isEqualTo(0L);
    final JSONObject json = ca.toJSON();
    assertThat(json.getBoolean("watermarkSet", false)).isTrue();

    final ContinuousAggregateImpl reloaded = ContinuousAggregateImpl.fromJSON(database, json);
    assertThat(reloaded.needsCleanRebuild()).isFalse();
    assertThat(reloaded.isWatermarkSet()).isTrue();
  }

  private ContinuousAggregateImpl createAggregate() {
    database.command("sql",
        "CREATE TIMESERIES TYPE SensorReading TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE)");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO SensorReading SET ts = 0, sensor_id = 'A', temperature = 20.0");
      database.command("sql", "INSERT INTO SensorReading SET ts = ?, sensor_id = 'A', temperature = 25.0", HOUR);
    });
    return (ContinuousAggregateImpl) database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp "
            + "FROM SensorReading GROUP BY sensor_id, hour")
        .create();
  }

  /** The schema a version carrying the defect would have written: no flag, and the watermark it was stuck at. */
  private ContinuousAggregateImpl reloadAsLegacySchema(final ContinuousAggregateImpl ca, final long watermarkTs) {
    final JSONObject legacy = ca.toJSON();
    legacy.remove("watermarkSet");
    legacy.put("watermarkTs", watermarkTs);
    return ContinuousAggregateImpl.fromJSON(database, legacy);
  }

  /** Appends another copy of every backing row, which is exactly what a refresh used to do. */
  private void duplicateBackingRows() {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM hourly_temps")) {
        rs.stream().toList().forEach(row -> {
          final var doc = database.newDocument("hourly_temps");
          row.getPropertyNames().stream().filter(p -> !p.startsWith("@")).forEach(p -> doc.set(p, row.getProperty(p)));
          doc.save();
        });
      }
    });
  }

  private long countBackingRows() {
    // count(@rid), not count(*): the latter answers from a cached counter rather than scanning, and this test is
    // about rows that should not be there.
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM hourly_temps")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
