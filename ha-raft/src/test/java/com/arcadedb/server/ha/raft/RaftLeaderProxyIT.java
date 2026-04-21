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
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test verifying that write commands sent to a Raft follower's HTTP endpoint
 * are transparently proxied to the leader by {@link com.arcadedb.server.http.handler.LeaderProxy}
 * and succeed end-to-end.
 *
 * <p>Specifically:
 * <ul>
 *   <li>A DDL command (CREATE VERTEX TYPE) sent to a follower is forwarded to the leader,
 *       executed there, and the resulting schema change replicated to all nodes.</li>
 *   <li>A DML command (INSERT) sent to a follower is similarly forwarded, persisted on the
 *       leader, and replicated to all nodes.</li>
 * </ul>
 */
class RaftLeaderProxyIT extends BaseRaftHATest {

  private static final String     PROXIED_TYPE = "ProxiedType";
  private static final HttpClient HTTP_CLIENT  = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .build();

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void ddlViaFollowerIsProxiedToLeader() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected before the test").isGreaterThanOrEqualTo(0);

    final int followerIndex = firstFollowerIndex(leaderIndex);
    LogManager.instance().log(this, Level.INFO,
        "TEST: leader=%d, follower=%d - sending DDL to follower HTTP endpoint", leaderIndex, followerIndex);

    final int followerPort = 2480 + followerIndex;
    final String dbName = getDatabaseName();
    final HttpResponse<String> response = postCommand(followerPort, dbName,
        "CREATE VERTEX TYPE " + PROXIED_TYPE);

    assertThat(response.statusCode())
        .as("Follower should proxy DDL to leader and return 200")
        .isEqualTo(200);
    assertThat(response.body())
        .as("Response body should acknowledge type creation")
        .contains(PROXIED_TYPE);

    // Wait for replication on all nodes
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Verify the type was created and replicated to every node
    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServerDatabase(i, dbName);
      assertThat(db.getSchema().existsType(PROXIED_TYPE))
          .as("Server %d should have type %s after leader-proxy DDL replication", i, PROXIED_TYPE)
          .isTrue();
    }
  }

  @Test
  void dmlViaFollowerIsProxiedToLeader() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected before the test").isGreaterThanOrEqualTo(0);

    final int followerIndex = firstFollowerIndex(leaderIndex);
    LogManager.instance().log(this, Level.INFO,
        "TEST: leader=%d, follower=%d - sending DML to follower HTTP endpoint", leaderIndex, followerIndex);

    final int followerPort = 2480 + followerIndex;
    final String dbName = getDatabaseName();

    // Use the built-in V1 type that is pre-created by the base graph schema setup
    final int insertCount = 5;
    for (int i = 0; i < insertCount; i++) {
      final HttpResponse<String> response = postCommand(followerPort, dbName,
          "INSERT INTO V1 SET name = 'proxy-test-" + i + "'");
      assertThat(response.statusCode())
          .as("INSERT %d proxied via follower should return 200", i)
          .isEqualTo(200);
    }

    // Wait for replication on all nodes
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Verify record count is consistent across all nodes
    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServerDatabase(i, dbName);
      final long[] count = { 0 };
      db.transaction(() -> count[0] = db.countType("V1", false));
      assertThat(count[0])
          .as("Server %d should have at least %d V1 records after follower DML proxy replication", i, insertCount)
          .isGreaterThanOrEqualTo(insertCount);
    }
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private int firstFollowerIndex(final int leaderIndex) {
    for (int i = 0; i < getServerCount(); i++) {
      if (i != leaderIndex) {
        final RaftHAPlugin plugin = getRaftPlugin(i);
        if (plugin != null)
          return i;
      }
    }
    throw new IllegalStateException("No follower found in " + getServerCount() + "-node cluster");
  }

  /**
   * Sends a POST /api/v1/command/{dbName} request with a SQL command to the server
   * at {@code port}, using Basic authentication.
   */
  private HttpResponse<String> postCommand(final int port, final String dbName, final String sql) throws Exception {
    final String authHeader = "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    final String body = new JSONObject()
        .put("language", "sql")
        .put("command", sql)
        .toString();

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + port + "/api/v1/command/" + dbName))
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", authHeader)
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
        .build();

    return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
  }
}
