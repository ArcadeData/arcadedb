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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that a drop database command submitted against a Raft replica is forwarded to the leader,
 * replicated via the DROP_DATABASE_ENTRY Raft log entry, and applied on every peer including
 * the removal of database files on disk.
 */
class RaftDropDatabase3NodesIT extends BaseRaftHATest {

  private static final String DB_NAME = "RaftDropTest";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // The default DB ("graph") is never created in this test (isCreateDatabases = false)
    // and RaftDropTest is dropped during the test, so there is nothing to compare.
  }

  @Test
  void dropDatabasePropagatesAcrossCluster() throws Exception {
    // Step A: create the database via HTTP on server 0 (will forward to leader if needed)
    postServerCommand(0, "create database " + DB_NAME);

    // Wait until all peers see the database
    Awaitility.await().atMost(15, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++)
            if (!getServer(i).existsDatabase(DB_NAME))
              return false;
          return true;
        });

    // Step B: insert a few vertices to confirm the database is usable
    commandOnDatabase(findLeaderIndex(), DB_NAME, "create vertex type RaftDropVertex");
    for (int i = 0; i < 10; i++)
      commandOnDatabase(findLeaderIndex(), DB_NAME, "create vertex RaftDropVertex content {\"idx\":" + i + "}");

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Step C: drop through a replica (not the leader) to exercise forward-to-leader
    final int leaderIndex = findLeaderIndex();
    final int replicaIndex = (leaderIndex + 1) % getServerCount();
    postServerCommand(replicaIndex, "drop database " + DB_NAME);

    // Step D: await until every peer's ArcadeDBServer no longer holds the database
    Awaitility.await().atMost(15, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++)
            if (getServer(i).existsDatabase(DB_NAME))
              return false;
          return true;
        });

    // Step E: assert the on-disk directory is gone on every peer
    for (int i = 0; i < getServerCount(); i++) {
      final String dbRoot = getServer(i).getConfiguration()
          .getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY);
      final File dir = new File(dbRoot, DB_NAME);
      assertThat(dir).as("server %d database directory should be removed", i).doesNotExist();
    }

    // Step F: sanity check - a second drop should fail with 4xx
    final int leader = findLeaderIndex();
    assertThat(leader).isGreaterThanOrEqualTo(0);
    final int status = postServerCommandExpectError(leader, "drop database " + DB_NAME);
    assertThat(status).isBetween(400, 499);
  }

  /**
   * POSTs a server command against /api/v1/server and asserts HTTP 200.
   */
  private void postServerCommand(final int serverIndex, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/server").toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(
            ("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    try {
      formatPayload(connection, new JSONObject().put("command", command));
      connection.connect();
      assertThat(connection.getResponseCode())
          .as("server command '%s' on server %d", command, serverIndex)
          .isEqualTo(200);
    } finally {
      connection.disconnect();
    }
  }

  /**
   * POSTs a SQL command against a specific database and asserts HTTP 200.
   */
  private void commandOnDatabase(final int serverIndex, final String dbName, final String sql) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + dbName).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(
            ("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    try {
      formatPayload(connection, "sql", sql, null, new HashMap<>());
      connection.connect();
      assertThat(connection.getResponseCode())
          .as("SQL command '%s' on server %d / db '%s'", sql, serverIndex, dbName)
          .isEqualTo(200);
    } finally {
      connection.disconnect();
    }
  }

  /**
   * POSTs a server command and returns the HTTP status code without asserting success.
   * Intended for use when a non-2xx response is expected.
   */
  private int postServerCommandExpectError(final int serverIndex, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/server").toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(
            ("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    try {
      formatPayload(connection, new JSONObject().put("command", command));
      connection.connect();
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }
}
