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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for the operator-triggered emergency resync endpoint
 * (POST /api/v1/cluster/resync/{database}) and {@link ArcadeStateMachine#resyncDatabaseFromLeader}.
 */
class RaftForceResyncIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    // 3 nodes so a majority (2) is still available while one follower is being resynced.
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // Keep the failed-install regression test fast: one retry, short backoff.
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 1);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 100L);
  }

  @Test
  void followerResyncFromLeaderKeepsDataAndResumesReplication() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("ResyncTest"))
        leaderDb.getSchema().createVertexType("ResyncTest");
      for (int i = 0; i < 25; i++)
        leaderDb.newVertex("ResyncTest").set("index", i).save();
    });

    assertClusterConsistency();
    assertThat(getServerDatabase(followerIndex, getDatabaseName()).countType("ResyncTest", true))
        .as("Follower has the initial data before resync").isEqualTo(25);

    // Trigger the emergency resync on the follower: it drops its local copy and re-downloads a full
    // snapshot from the leader.
    final JSONObject response = resync(followerIndex, getDatabaseName());
    assertThat(response.getString("result", "")).contains("resynced from leader");

    assertThat(getServerDatabase(followerIndex, getDatabaseName()).countType("ResyncTest", true))
        .as("Follower has the same data after resync").isEqualTo(25);

    // Forward replication must resume after the resync: new writes on the leader reach the follower.
    leaderDb.transaction(() -> {
      for (int i = 25; i < 40; i++)
        leaderDb.newVertex("ResyncTest").set("index", i).save();
    });

    assertClusterConsistency();
    assertThat(getServerDatabase(followerIndex, getDatabaseName()).countType("ResyncTest", true))
        .as("Follower receives writes committed after the resync").isEqualTo(40);
  }

  @Test
  void resyncOnLeaderIsRejected() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final int httpPort = getServer(leaderIndex).getHttpServer().getPort();
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + httpPort + "/api/v1/cluster/resync/" + getDatabaseName()).toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization", basicAuth());

    assertThat(conn.getResponseCode()).isEqualTo(400);
    final String body = new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
    assertThat(new JSONObject(body).getString("error", "")).contains("leader");
    conn.disconnect();
  }

  /**
   * Regression test for the production incident where a resync against an unreachable/unstable leader
   * left the follower's database closed and deregistered, surfacing as {@code DatabaseIsClosedException}
   * on every subsequent transaction. {@link SnapshotInstaller#install} now downloads the snapshot with
   * the database still open and only closes + swaps once a complete copy is on disk, so a failed
   * download must leave the database open, serving, and still replicating.
   */
  @Test
  @Timeout(120)
  void failedResyncKeepsDatabaseOpenAndServing() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final String dbName = getDatabaseName();

    final Database leaderDb = getServerDatabase(leaderIndex, dbName);
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("ResyncFail"))
        leaderDb.getSchema().createVertexType("ResyncFail");
      for (int i = 0; i < 10; i++)
        leaderDb.newVertex("ResyncFail").set("index", i).save();
    });
    assertClusterConsistency();

    final ArcadeDBServer followerServer = getServer(followerIndex);
    final String dbPath = ((DatabaseInternal) followerServer.getDatabase(dbName)).getDatabasePath();
    final String token = getRaftPlugin(followerIndex).getRaftHAServer().getClusterToken();

    // Simulate a resync whose download can never succeed (unreachable leader endpoint). The install
    // must fail without ever touching the live database.
    assertThatThrownBy(() ->
        SnapshotInstaller.install(dbName, dbPath, () -> "127.0.0.1:1", () -> null, token, followerServer))
        .isInstanceOf(IOException.class);

    // The follower database is still open and holds its data - no DatabaseIsClosedException.
    assertThat(followerServer.existsDatabase(dbName)).as("Database stays registered after a failed install").isTrue();
    assertThat(getServerDatabase(followerIndex, dbName).countType("ResyncFail", true))
        .as("Follower keeps its data after a failed install").isEqualTo(10);

    // No staging markers or directories left behind.
    assertThat(Path.of(dbPath).resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();
    assertThat(Path.of(dbPath).resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR)).doesNotExist();
    assertThat(Path.of(dbPath).resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR)).doesNotExist();

    // The follower itself accepts a write-and-commit after the failed install (forwarded to the leader
    // in Raft). This directly documents the absence of DatabaseIsClosedException: a closed/deregistered
    // database - the original outage - would throw here rather than commit.
    final Database followerDb = getServerDatabase(followerIndex, dbName);
    followerDb.transaction(() -> followerDb.newVertex("ResyncFail").set("index", 99).save());
    assertClusterConsistency();
    assertThat(getServerDatabase(followerIndex, dbName).countType("ResyncFail", true))
        .as("Follower commits a write after a failed install (no DatabaseIsClosedException)").isEqualTo(11);

    // Forward replication still works: writes committed on the leader after the failed install reach
    // the follower.
    leaderDb.transaction(() -> {
      for (int i = 10; i < 15; i++)
        leaderDb.newVertex("ResyncFail").set("index", i).save();
    });
    assertClusterConsistency();
    assertThat(getServerDatabase(followerIndex, dbName).countType("ResyncFail", true))
        .as("Follower receives writes committed after the failed install").isEqualTo(16);
  }

  private JSONObject resync(final int serverIndex, final String databaseName) throws Exception {
    final int httpPort = getServer(serverIndex).getHttpServer().getPort();
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + httpPort + "/api/v1/cluster/resync/" + databaseName).toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization", basicAuth());
    try {
      assertThat(conn.getResponseCode()).isEqualTo(200);
      return new JSONObject(new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
    } finally {
      conn.disconnect();
    }
  }

  private static String basicAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(
        ("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }
}
