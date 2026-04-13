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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that dropping a database on the leader is replicated to all servers in the cluster.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftDropDatabaseReplicationIT extends BaseGraphServerTest {

  private static final String EXTRA_DB = "droptest";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void dropDatabaseReplicatesToAllServers() throws Exception {
    final int leaderIndex = getLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    // Create a new database on the leader via HTTP
    final int leaderPort = 2480 + leaderIndex;
    createDatabaseViaHTTP(leaderPort, EXTRA_DB);

    // Wait for the database to appear on all servers
    Awaitility.await().atMost(15, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS).until(() -> {
      for (int i = 0; i < getServerCount(); i++)
        if (!getServer(i).existsDatabase(EXTRA_DB))
          return false;
      return true;
    });

    // Verify all servers have the database
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServer(i).existsDatabase(EXTRA_DB))
          .as("Server %d should have database '%s' before drop", i, EXTRA_DB)
          .isTrue();

    // Insert some data so the database is not empty
    getServer(leaderIndex).getDatabase(EXTRA_DB).transaction(() -> {
      final var db = getServer(leaderIndex).getDatabase(EXTRA_DB);
      db.getSchema().createVertexType("DropTest");
      for (int i = 0; i < 10; i++)
        db.newVertex("DropTest").set("value", i).save();
    });

    waitForReplicationConvergence();

    // Drop the database on the leader via HTTP
    dropDatabaseViaHTTP(leaderPort, EXTRA_DB);

    // Wait for the database to disappear from all servers
    Awaitility.await().atMost(15, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS).until(() -> {
      for (int i = 0; i < getServerCount(); i++)
        if (getServer(i).existsDatabase(EXTRA_DB))
          return false;
      return true;
    });

    // Final verification: database must be gone from every server
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServer(i).existsDatabase(EXTRA_DB))
          .as("Server %d should NOT have database '%s' after drop", i, EXTRA_DB)
          .isFalse();
  }

  private void createDatabaseViaHTTP(final int httpPort, final String dbName) throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + httpPort + "/api/v1/server").toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));

    final String body = new JSONObject().put("command", "create database " + dbName).toString();
    conn.getOutputStream().write(body.getBytes(StandardCharsets.UTF_8));

    assertThat(conn.getResponseCode()).as("Create database should succeed").isEqualTo(200);
    conn.disconnect();
  }

  private void dropDatabaseViaHTTP(final int httpPort, final String dbName) throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + httpPort + "/api/v1/server").toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));

    final String body = new JSONObject().put("command", "drop database " + dbName).toString();
    conn.getOutputStream().write(body.getBytes(StandardCharsets.UTF_8));

    assertThat(conn.getResponseCode()).as("Drop database should succeed").isEqualTo(200);
    conn.disconnect();
  }
}
