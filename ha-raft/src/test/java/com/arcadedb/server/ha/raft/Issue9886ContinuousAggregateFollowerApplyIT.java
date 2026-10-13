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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9886: under Raft HA, creating a continuous aggregate on a TIMESERIES type made every follower fail to apply the
 * next write to that type ("Cannot write page 1 because the file 'SensorReading_tags...tstd' is closed"), quarantine the
 * database and heal it only through a full targeted snapshot resync.
 */
class Issue9886ContinuousAggregateFollowerApplyIT extends BaseRaftHATest {

  /** The reproduction of the issue. */
  @Test
  void aWriteAfterCreatingAContinuousAggregateAppliesOnEveryFollowerWithoutAResync() throws Exception {
    runAfterDdl("CREATE CONTINUOUS AGGREGATE hourly_temps AS SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, "
        + "avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour", "hourly_temps");
  }

  /**
   * The continuous aggregate was only the trigger: a follower reloads its schema on every applied schema entry, and
   * the reload closed the TimeSeries files under the type's live engine whatever the DDL was.
   */
  @Test
  void aWriteAfterAnUnrelatedDdlAppliesOnEveryFollowerWithoutAResync() throws Exception {
    runAfterDdl("CREATE DOCUMENT TYPE Unrelated", "Unrelated");
  }

  private void runAfterDdl(final String ddl, final String createdType) throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());

    final CapturingTestLogger log = CapturingTestLogger.install();
    try {
      leader.command("sql",
          "CREATE TIMESERIES TYPE SensorReading TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE) SHARDS 1");
      leader.transaction(() -> leader.command("sql", "INSERT INTO SensorReading SET ts = 1000, sensor_id = 'A', temperature = 22.5"));
      leader.command("sql", ddl);
      leader.transaction(() -> leader.command("sql", "INSERT INTO SensorReading SET ts = 2000, sensor_id = 'B', temperature = 23.5"));

      assertClusterConsistency();

      assertThat(log.countContaining("quarantining the database"))
          .as("no follower may quarantine the database on the TimeSeries write that follows the DDL")
          .isZero();
      for (int i = 0; i < getServerCount(); i++)
        assertThat(isDatabaseQuarantined(i, getDatabaseName())).as("server %d quarantined", i).isFalse();
    } finally {
      log.uninstall();
    }

    // Every node, the followers included, holds both samples and the type the DDL created
    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServerDatabase(i, getDatabaseName());
      assertThat(db.getSchema().existsType(createdType)).as("server %d type created by the DDL", i).isTrue();
      try (final ResultSet rs = db.query("sql", "SELECT count(*) AS c FROM SensorReading")) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).as("server %d samples", i).isEqualTo(2L);
      }
      // The tag interned before the DDL and the one interned after it both resolve on every node
      final List<String> tags = new ArrayList<>();
      try (final ResultSet rs = db.query("sql", "SELECT sensor_id FROM SensorReading ORDER BY ts")) {
        while (rs.hasNext())
          tags.add(rs.next().getProperty("sensor_id"));
      }
      assertThat(tags).as("server %d tag values", i).containsExactly("A", "B");
    }
  }
}
