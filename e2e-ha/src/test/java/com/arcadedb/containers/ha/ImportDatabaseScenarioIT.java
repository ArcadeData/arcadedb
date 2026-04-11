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
import com.arcadedb.test.support.ServerWrapper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class ImportDatabaseScenarioIT extends ContainersTestTemplate {

  private static final String SERVER_LIST = "arcadedb-0:2434:2480,arcadedb-1:2434:2480,arcadedb-2:2434:2480";
  private static final String IMPORT_DB   = "RaftImportTest";

  @AfterEach
  @Override
  public void tearDown() {
    super.tearDown();
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.MINUTES)
  @DisplayName("Three-node Raft HA: import database replicates data to every peer via TX_ENTRY")
  void importDatabaseReplicatedAcrossCluster() throws Exception {
    final GenericContainer<?> leaderContainer = createArcadeContainer("arcadedb-0", SERVER_LIST, "majority", network);
    leaderContainer.withCopyToContainer(
        MountableFile.forClasspathResource("raft-import-fixture.jsonl.tgz"),
        "/home/arcadedb/import-fixture.jsonl.tgz");
    createArcadeContainer("arcadedb-1", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-2", SERVER_LIST, "majority", network);

    logger.info("Starting cluster with fixture staged on leader");
    final List<ServerWrapper> servers = startCluster();

    // Issue the import command on node 0
    logger.info("Issuing import database on node 0");
    final int importStatus = postServerCommand(servers.get(0),
        "import database " + IMPORT_DB + " file:///home/arcadedb/import-fixture.jsonl.tgz",
        180_000);
    assertThat(importStatus).as("import database HTTP status").isEqualTo(200);

    // Await until every node sees the imported database
    logger.info("Waiting for import to propagate to every node");
    Awaitility.await().atMost(180, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS)
        .until(() -> databaseExistsOnServer(servers.get(0), IMPORT_DB)
            && databaseExistsOnServer(servers.get(1), IMPORT_DB)
            && databaseExistsOnServer(servers.get(2), IMPORT_DB));

    // For each node, open a RemoteDatabase and count Person vertices
    logger.info("Verifying Person count on every node");
    final long[] counts = new long[3];
    for (int i = 0; i < 3; i++) {
      final RemoteDatabase db = new RemoteDatabase(
          servers.get(i).host(), servers.get(i).httpPort(), IMPORT_DB, "root", PASSWORD);
      db.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
      try {
        final int nodeIndex = i;
        Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS)
            .until(() -> {
              try (final ResultSet rs = db.query("sql", "select count(*) as cnt from Person")) {
                if (!rs.hasNext())
                  return false;
                final Result r = rs.next();
                final Number cnt = r.getProperty("cnt");
                return cnt != null && cnt.longValue() > 0L;
              } catch (final Exception e) {
                logger.debug("Node {} not ready yet: {}", nodeIndex, e.getMessage());
                return false;
              }
            });
        try (final ResultSet rs = db.query("sql", "select count(*) as cnt from Person")) {
          final Result r = rs.next();
          final Number cnt = r.getProperty("cnt");
          counts[i] = cnt != null ? cnt.longValue() : 0L;
        }
      } finally {
        db.close();
      }
    }

    logger.info("Person counts: node0={}, node1={}, node2={}", counts[0], counts[1], counts[2]);
    assertThat(counts[0]).as("node 0 Person count should be > 0").isGreaterThan(0L);
    assertThat(counts[1]).as("node 1 Person count matches node 0").isEqualTo(counts[0]);
    assertThat(counts[2]).as("node 2 Person count matches node 0").isEqualTo(counts[0]);
  }

  /**
   * POSTs a server command against /api/v1/server and returns the HTTP status code.
   */
  private int postServerCommand(final ServerWrapper server, final String command, final int readTimeoutMs) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) URI.create(
        "http://" + server.host() + ":" + server.httpPort() + "/api/v1/server").toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + PASSWORD).getBytes()));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setConnectTimeout(5000);
    connection.setReadTimeout(readTimeoutMs);
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
