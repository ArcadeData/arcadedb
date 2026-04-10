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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that `restore database` on a Raft leader propagates restored files to every replica
 * via the INSTALL_DATABASE_ENTRY with forceSnapshot=true. Each replica closes its local copy
 * and pulls the fresh snapshot from the leader.
 */
class RaftRestoreDatabase3NodesIT extends BaseRaftHATest {

  private static final String DB_NAME = "RaftRestoreTest";

  RaftRestoreDatabase3NodesIT() {
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
    FileUtils.deleteRecursively(new File("./target/backups"));
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
  }

  @AfterEach
  @Override
  public void endTest() {
    super.endTest();
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
    FileUtils.deleteRecursively(new File("./target/backups"));
  }

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
    // The default "graph" database is never created in this test (isCreateDatabases=false),
    // and RaftRestoreTest is dropped and then re-created mid-test, so there is nothing to compare.
  }

  @Test
  void restoreDatabasePropagatesAcrossCluster() throws Exception {
    // Step A: create the DB on the leader via HTTP
    postServerCommand(0, "create database " + DB_NAME);
    Awaitility.await().atMost(15, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++)
            if (!getServer(i).existsDatabase(DB_NAME))
              return false;
          return true;
        });

    // Step B: populate with a known fixture (schema + 50 vertices)
    commandOnDatabase(findLeaderIndex(), DB_NAME, "create vertex type RaftRestoreVertex");
    for (int i = 0; i < 50; i++)
      commandOnDatabase(findLeaderIndex(), DB_NAME, "create vertex RaftRestoreVertex content {\"idx\":" + i + "}");
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Step C: take a backup via the SQL `backup database` command on the leader
    final int leader = findLeaderIndex();
    assertThat(leader).isGreaterThanOrEqualTo(0);
    final Database leaderDb = getServer(leader).getDatabase(DB_NAME);
    final String backupFileName;
    try (final ResultSet result = leaderDb.command("sql", "backup database")) {
      assertThat(result.hasNext()).isTrue();
      final Result response = result.next();
      backupFileName = response.getProperty("backupFile");
      assertThat(backupFileName).isNotNull();
    }
    final File backupFile = new File("target/backups/" + DB_NAME + "/" + backupFileName);
    assertThat(backupFile.exists()).as("backup file should exist at %s", backupFile.getAbsolutePath()).isTrue();
    final String backupUrl = "file://" + backupFile.getAbsolutePath();

    // Step D: drop the database cluster-wide via the leader
    postServerCommand(leader, "drop database " + DB_NAME);
    Awaitility.await().atMost(15, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++)
            if (getServer(i).existsDatabase(DB_NAME))
              return false;
          return true;
        });

    // Step E: restore the database from the backup via the leader
    postServerCommand(findLeaderIndex(), "restore database " + DB_NAME + " " + backupUrl);

    // Step F: wait until every peer sees the database and its data
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++)
            if (!getServer(i).existsDatabase(DB_NAME))
              return false;
          return true;
        });

    // Step G: assert the restored data is present and identical on every peer
    for (int i = 0; i < getServerCount(); i++) {
      waitForReplicationIsCompleted(i);
      final Database db = getServer(i).getDatabase(DB_NAME);
      final long count = db.countType("RaftRestoreVertex", true);
      assertThat(count).as("server %d restored vertex count", i).isEqualTo(50);
    }
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
          .as("SQL '%s' on server %d / db '%s'", sql, serverIndex, dbName)
          .isEqualTo(200);
    } finally {
      connection.disconnect();
    }
  }
}
