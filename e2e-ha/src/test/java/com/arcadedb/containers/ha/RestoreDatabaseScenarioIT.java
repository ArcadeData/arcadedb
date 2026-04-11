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
package com.arcadedb.containers.ha;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteHttpComponent;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.ServerWrapper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class RestoreDatabaseScenarioIT extends ContainersTestTemplate {

  private static final String SERVER_LIST = "arcadedb-0:2434:2480,arcadedb-1:2434:2480,arcadedb-2:2434:2480";

  @AfterEach
  @Override
  public void tearDown() {
    super.tearDown();
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.MINUTES)
  @DisplayName("Three-node Raft HA: restore database replicates to every peer via forceSnapshot")
  void restoreDatabaseReplicatedAcrossCluster() throws Exception {
    createArcadeContainer("arcadedb-0", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-1", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-2", SERVER_LIST, "majority", network);

    logger.info("Starting cluster");
    final List<ServerWrapper> servers = startCluster();

    final DatabaseWrapper db0 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    final DatabaseWrapper db1 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    final DatabaseWrapper db2 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);

    try {
      logger.info("Creating database, schema, and fixture");
      db0.createDatabase();
      db0.createSchema();
      db0.addUserAndPhotos(30, 10);

      logger.info("Waiting for initial convergence");
      Awaitility.await().atMost(120, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS)
          .until(() -> db0.countUsers() == 30 && db1.countUsers() == 30 && db2.countUsers() == 30);

      // Take a backup via SQL on the leader (node 0)
      logger.info("Taking backup on leader");
      final String backupFile;
      final RemoteDatabase leaderDb = new RemoteDatabase(servers.get(0).host(), servers.get(0).httpPort(), DATABASE, "root", PASSWORD);
      leaderDb.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
      try {
        try (final ResultSet result = leaderDb.command("sql", "backup database")) {
          assertThat(result.hasNext()).isTrue();
          final Result response = result.next();
          backupFile = response.getProperty("backupFile");
          assertThat(backupFile).isNotNull();
        }
      } finally {
        leaderDb.close();
      }
      logger.info("Backup created: {}", backupFile);
      final String backupUrl = "file:///home/arcadedb/backups/" + DATABASE + "/" + backupFile;

      // Close remote wrappers before drop
      db0.close();
      db1.close();
      db2.close();

      // Drop the database cluster-wide via HTTP on the leader
      logger.info("Dropping database cluster-wide");
      final int dropStatus = postServerCommand(servers.get(0), "drop database " + DATABASE);
      assertThat(dropStatus).as("drop database HTTP status").isEqualTo(200);

      Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
          .until(() -> !databaseExistsOnServer(servers.get(0), DATABASE)
              && !databaseExistsOnServer(servers.get(1), DATABASE)
              && !databaseExistsOnServer(servers.get(2), DATABASE));

      // Restore from the backup on the leader
      logger.info("Restoring from backup: {}", backupUrl);
      final int restoreStatus = postServerCommand(servers.get(0), "restore database " + DATABASE + " " + backupUrl);
      assertThat(restoreStatus).as("restore database HTTP status").isEqualTo(200);

      // Wait for restore to propagate to every peer
      logger.info("Waiting for restore to propagate to every peer");
      Awaitility.await().atMost(120, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
          .until(() -> databaseExistsOnServer(servers.get(0), DATABASE)
              && databaseExistsOnServer(servers.get(1), DATABASE)
              && databaseExistsOnServer(servers.get(2), DATABASE));

      // Reopen wrappers and verify restored data on every node
      final DatabaseWrapper db0b = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
      final DatabaseWrapper db1b = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
      final DatabaseWrapper db2b = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);
      try {
        Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS)
            .until(() -> db0b.countUsers() == 30 && db1b.countUsers() == 30 && db2b.countUsers() == 30);
      } finally {
        db0b.close();
        db1b.close();
        db2b.close();
      }
    } catch (final Exception e) {
      try { db0.close(); } catch (final Exception ignored) {}
      try { db1.close(); } catch (final Exception ignored) {}
      try { db2.close(); } catch (final Exception ignored) {}
      throw e;
    }
  }

  /**
   * POSTs a server command against /api/v1/server and returns the HTTP status code.
   */
  private int postServerCommand(final ServerWrapper server, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) URI.create(
        "http://" + server.host() + ":" + server.httpPort() + "/api/v1/server").toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + PASSWORD).getBytes()));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setConnectTimeout(5000);
    connection.setReadTimeout(120000);
    connection.setDoOutput(true);
    try {
      connection.getOutputStream().write(
          new JSONObject().put("command", command).toString().getBytes());
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  /**
   * Returns true if the given database name appears in the node's {@code list databases} response.
   */
  private boolean databaseExistsOnServer(final ServerWrapper server, final String dbName) {
    try {
      final HttpURLConnection connection = (HttpURLConnection) URI.create(
          "http://" + server.host() + ":" + server.httpPort() + "/api/v1/server").toURL().openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + PASSWORD).getBytes()));
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setConnectTimeout(5000);
      connection.setReadTimeout(5000);
      connection.setDoOutput(true);
      try {
        connection.getOutputStream().write(
            new JSONObject().put("command", "list databases").toString().getBytes());
        if (connection.getResponseCode() != 200)
          return false;
        final String body = new String(connection.getInputStream().readAllBytes());
        final JSONObject json = new JSONObject(body);
        final JSONArray result = json.getJSONArray("result");
        for (int i = 0; i < result.length(); i++)
          if (dbName.equals(result.getString(i)))
            return true;
        return false;
      } finally {
        connection.disconnect();
      }
    } catch (final Exception e) {
      return false;
    }
  }
}
