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

class DropDatabaseScenarioIT extends ContainersTestTemplate {

  private static final String SERVER_LIST = "arcadedb-0:2434:2480,arcadedb-1:2434:2480,arcadedb-2:2434:2480";

  @AfterEach
  @Override
  public void tearDown() {
    // Skip compareAllDatabases(): the database is intentionally dropped during the test.
    super.tearDown();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Three-node Raft HA: drop database via replica propagates removal to every peer")
  void dropDatabaseReplicatedAcrossCluster() throws Exception {
    createArcadeContainer("arcadedb-0", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-1", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-2", SERVER_LIST, "majority", network);

    logger.info("Starting all containers");
    final List<ServerWrapper> servers = startCluster();

    final DatabaseWrapper db0 = new DatabaseWrapper(servers.get(0), idSupplier, wordSupplier);
    final DatabaseWrapper db1 = new DatabaseWrapper(servers.get(1), idSupplier, wordSupplier);
    final DatabaseWrapper db2 = new DatabaseWrapper(servers.get(2), idSupplier, wordSupplier);

    try {
      logger.info("Creating database and schema on node 0");
      db0.createDatabase();
      db0.createSchema();

      logger.info("Populating 30 users + 300 photos from node 0");
      db0.addUserAndPhotos(30, 10);

      logger.info("Waiting for replication to converge across all nodes");
      Awaitility.await().atMost(120, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS)
          .until(() -> db0.countUsers() == 30 && db1.countUsers() == 30 && db2.countUsers() == 30);

      // Close the RemoteDatabase wrappers BEFORE dropping the database so we don't
      // hold open connections against a database that is about to be removed.
      db0.close();
      db1.close();
      db2.close();

      // Drop via node 2 (a replica) to exercise the forward-to-leader path
      logger.info("Dropping database via node 2 (replica, exercises forward-to-leader)");
      final int dropStatus = postServerCommand(servers.get(2), "drop database " + DATABASE);
      assertThat(dropStatus).as("drop database HTTP status from replica").isEqualTo(200);

      // Await until every node's list-databases response no longer contains the DB
      logger.info("Waiting for drop to propagate to every node");
      Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
          .until(() -> !databaseExistsOnServer(servers.get(0), DATABASE)
              && !databaseExistsOnServer(servers.get(1), DATABASE)
              && !databaseExistsOnServer(servers.get(2), DATABASE));

      // Sanity: a second drop of the same database should return 4xx
      final int secondDropStatus = postServerCommand(servers.get(0), "drop database " + DATABASE);
      assertThat(secondDropStatus).as("second drop HTTP status").isBetween(400, 499);
    } catch (final Exception e) {
      // Ensure wrappers are closed on failure too
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
    connection.setReadTimeout(30000);
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
