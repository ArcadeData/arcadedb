/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.FollowerSample;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ClusterAlerts} single-bucket type detection: the core diagnostic behind the
 * cluster write-retry alert. Runs against a plain embedded database (no Raft cluster needed).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ClusterAlertsTest {
  private static final String   DB_PATH = "./target/databases/test-cluster-alerts";
  private              Database db;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    db = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void flagsTypesWithASingleBucket() {
    db.command("sql", "CREATE VERTEX TYPE SingleBucketV BUCKETS 1");
    db.command("sql", "CREATE DOCUMENT TYPE SingleBucketD BUCKETS 1");

    final List<String> flagged = ClusterAlerts.findSingleBucketTypes(db);

    assertThat(flagged).contains("SingleBucketV", "SingleBucketD");
  }

  @Test
  void doesNotFlagTypesWithMultipleBuckets() {
    db.command("sql", "CREATE VERTEX TYPE MultiBucketV BUCKETS 8");
    db.command("sql", "CREATE EDGE TYPE MultiBucketE BUCKETS 8");

    final List<String> flagged = ClusterAlerts.findSingleBucketTypes(db);

    assertThat(flagged).doesNotContain("MultiBucketV", "MultiBucketE");
  }

  @Test
  void growingBucketCountClearsTheFlag() {
    db.command("sql", "CREATE VERTEX TYPE Account BUCKETS 1");
    assertThat(ClusterAlerts.findSingleBucketTypes(db)).contains("Account");

    // Add a second bucket to the live type: the contention diagnostic must clear.
    db.command("sql", "ALTER TYPE Account BUCKET +Account_extra1");

    assertThat(ClusterAlerts.findSingleBucketTypes(db)).doesNotContain("Account");
  }

  @Test
  void resultIsSortedForDeterministicOutput() {
    db.command("sql", "CREATE VERTEX TYPE Zebra BUCKETS 1");
    db.command("sql", "CREATE VERTEX TYPE Apple BUCKETS 1");
    db.command("sql", "CREATE VERTEX TYPE Mango BUCKETS 1");

    final List<String> flagged = ClusterAlerts.findSingleBucketTypes(db);

    assertThat(flagged).isSorted();
  }

  // ---- leader-missing-databases alert (issue #4727) ----

  @Test
  void leaderMissingAlertIsRaisedWithTheDatabaseNames() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addLeaderMissingAlert(List.of("OpenBeer", "Catalog"), alerts);

    assertThat(alerts.length()).isEqualTo(1);
    final JSONObject alert = alerts.getJSONObject(0);
    assertThat(alert.getString("id")).isEqualTo("leader-missing-databases");
    assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_WARNING);
    final JSONArray names = alert.getJSONObject("details").getJSONArray("databases");
    assertThat(names.toList()).containsExactlyInAnyOrder("OpenBeer", "Catalog");
  }

  @Test
  void leaderMissingAlertIsAbsentWhenNothingIsMissing() {
    final JSONArray empty = new JSONArray();
    ClusterAlerts.addLeaderMissingAlert(List.of(), empty);
    assertThat(empty.length()).isEqualTo(0);

    final JSONArray nullCase = new JSONArray();
    ClusterAlerts.addLeaderMissingAlert(null, nullCase);
    assertThat(nullCase.length()).isEqualTo(0);
  }

  // ---- failed-acquire-databases alert (issue #4727) ----

  @Test
  void failedAcquireAlertIsRaisedWithTheDatabaseNames() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addFailedAcquireAlert(List.of("OpenBeer"), alerts);

    assertThat(alerts.length()).isEqualTo(1);
    final JSONObject alert = alerts.getJSONObject(0);
    assertThat(alert.getString("id")).isEqualTo("failed-acquire-databases");
    assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_WARNING);
    assertThat(alert.getJSONObject("details").getJSONArray("databases").toList()).containsExactly("OpenBeer");
  }

  @Test
  void failedAcquireAlertIsAbsentWhenNothingFailed() {
    final JSONArray empty = new JSONArray();
    ClusterAlerts.addFailedAcquireAlert(List.of(), empty);
    ClusterAlerts.addFailedAcquireAlert(null, empty);
    assertThat(empty.length()).isEqualTo(0);
  }

  // -- Lagging-follower alert (issue #4812) --

  @Test
  void laggingFollowerAlertIsAbsentWhenAllHealthy() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addLaggingFollowerAlert(List.of(
        new FollowerSample("n1", 100, 101, 0, 5, "HEALTHY", 0),
        new FollowerSample("n2", 100, 101, 3, 8, "CATCHING_UP", 1200)), alerts);
    assertThat(alerts.length()).as("CATCHING_UP/HEALTHY are not alert-worthy").isZero();
  }

  @Test
  void fallingBehindFollowerRaisesWarningNamingTheNode() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addLaggingFollowerAlert(List.of(
        new FollowerSample("n1", 100, 101, 0, 5, "HEALTHY", 0),
        new FollowerSample("slow-node", 50, 51, 4200, 900, "FALLING_BEHIND", 8000)), alerts);

    assertThat(alerts.length()).isEqualTo(1);
    final JSONObject alert = alerts.getJSONObject(0);
    assertThat(alert.getString("id")).isEqualTo("lagging-followers");
    assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_WARNING);
    final JSONArray nodes = alert.getJSONObject("details").getJSONArray("nodes");
    assertThat(nodes.length()).isEqualTo(1);
    assertThat(nodes.getJSONObject(0).getString("peerId")).isEqualTo("slow-node");
    assertThat(nodes.getJSONObject(0).getLong("replicationLag")).isEqualTo(4200L);
    assertThat(nodes.getJSONObject(0).getLong("laggingForMs")).isEqualTo(8000L);
  }

  @Test
  void stalledFollowerEscalatesToCritical() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addLaggingFollowerAlert(List.of(
        new FollowerSample("stuck", 10, 11, 99999, 6000, "STALLED", 60000)), alerts);

    assertThat(alerts.length()).isEqualTo(1);
    assertThat(alerts.getJSONObject(0).getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);
    assertThat(alerts.getJSONObject(0).getString("title")).containsIgnoringCase("stalled");
  }

  @Test
  void laggingFollowerAlertIsAbsentWhenNoSamples() {
    final JSONArray empty = new JSONArray();
    ClusterAlerts.addLaggingFollowerAlert(List.of(), empty);
    ClusterAlerts.addLaggingFollowerAlert(null, empty);
    assertThat(empty.length()).isZero();
  }
}
