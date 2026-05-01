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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4039: HA Cluster INSERT with many new property keys per transaction
 * fails with "Error on updating dictionary".
 * <p>
 * The bug appears when a single INSERT introduces more than ~5 new property keys that don't yet
 * exist in the schema dictionary. The transaction would fail in HA mode with:
 * {@code SchemaException: Error on updating dictionary for key 'key_N'}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftDictionaryManyNewKeysIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * Reproduces issue #4039 via the Java API: a single record save with 50 new properties must
   * succeed end to end on the leader and replicate to all replicas.
   */
  @Test
  void insertSingleRecordWithManyNewProperties() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database db = getServerDatabase(leaderIndex, getDatabaseName());

    db.transaction(() -> {
      if (!db.getSchema().existsType("DictBugType"))
        db.getSchema().createVertexType("DictBugType");
    });

    final int newKeys = 50;

    db.transaction(() -> {
      final MutableVertex v = db.newVertex("DictBugType");
      for (int i = 0; i < newKeys; i++)
        v.set("apikey_" + i, "value_" + i);
      v.save();
    });

    assertClusterConsistency();

    for (int i = 0; i < getServerCount(); i++) {
      final Database nodeDb = getServerDatabase(i, getDatabaseName());
      assertThat(nodeDb.countType("DictBugType", true))
          .as("Server " + i + " should have 1 DictBugType record").isEqualTo(1);
    }
  }

  /**
   * Reproduces the original issue scenario via SQL INSERT INTO ... CONTENT where the JSON
   * contains many new keys that have never been seen by the schema dictionary.
   */
  @Test
  void sqlInsertContentWithManyNewProperties() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database db = getServerDatabase(leaderIndex, getDatabaseName());

    db.command("sql", "CREATE VERTEX TYPE DictBugSqlType IF NOT EXISTS");

    final int newKeys = 50;
    final StringBuilder content = new StringBuilder("{");
    for (int i = 0; i < newKeys; i++) {
      if (i > 0)
        content.append(", ");
      content.append("\"sqlkey_").append(i).append("\":\"v_").append(i).append("\"");
    }
    content.append("}");

    final ResultSet result = db.command("sql", "INSERT INTO DictBugSqlType CONTENT " + content);
    assertThat(result.hasNext()).isTrue();
    result.next();

    assertClusterConsistency();

    for (int i = 0; i < getServerCount(); i++) {
      final Database nodeDb = getServerDatabase(i, getDatabaseName());
      assertThat(nodeDb.countType("DictBugSqlType", true))
          .as("Server " + i + " should have 1 DictBugSqlType record").isEqualTo(1);
    }
  }

  /**
   * Reproduces the exact scenario from issue #4039: HTTP POST /api/v1/command with SQL
   * INSERT INTO ... CONTENT, iterating with progressively larger numbers of new keys.
   * This matches the user's Python repro script. The request is sent to a FOLLOWER so
   * that the request is forwarded to the leader through the HA path.
   */
  @Test
  void httpInsertContentWithProgressivelyMoreKeysOnFollower() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();

    runProgressiveInserts(followerIndex);
  }

  /**
   * Same as {@link #httpInsertContentWithProgressivelyMoreKeysOnFollower} but sends the
   * request directly to the leader so we can compare both paths.
   */
  @Test
  void httpInsertContentWithProgressivelyMoreKeysOnLeader() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    runProgressiveInserts(leaderIndex);
  }

  private void runProgressiveInserts(final int targetServerIndex) throws Exception {
    httpCommand(targetServerIndex, "CREATE VERTEX TYPE T IF NOT EXISTS");

    final int[] sizes = { 1, 2, 3, 5, 8, 10, 15, 20, 50 };
    for (final int n : sizes) {
      final StringBuilder body = new StringBuilder("{");
      for (int i = 0; i < n; i++) {
        if (i > 0)
          body.append(", ");
        body.append("\"key_").append(i).append("\":\"value_").append(i).append("\"");
      }
      body.append("}");

      final String response = httpCommand(targetServerIndex, "INSERT INTO T CONTENT " + body);
      assertThat(response).as("INSERT of %d new property keys should succeed", n).contains("@rid");
    }

    assertClusterConsistency();

    final long expected = sizes.length;
    for (int i = 0; i < getServerCount(); i++) {
      final Database nodeDb = getServerDatabase(i, getDatabaseName());
      assertThat(nodeDb.countType("T", true))
          .as("Server " + i + " should have %d T records", expected).isEqualTo(expected);
    }
  }

  private String httpCommand(final int serverIndex, final String sql) throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + getDatabaseName())
        .toURL().openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(
              ("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      conn.setDoOutput(true);
      final JSONObject payload = new JSONObject();
      payload.put("language", "sql");
      payload.put("command", sql);
      payload.put("params", new HashMap<>());
      conn.setRequestProperty("Content-Type", "application/json");
      conn.getOutputStream().write(payload.toString().getBytes("UTF-8"));
      conn.getOutputStream().close();
      conn.connect();

      final int code = conn.getResponseCode();
      if (code != 200) {
        final String err = readError(conn);
        throw new AssertionError("HTTP " + code + " for SQL: " + sql + " body=" + err);
      }
      return readResponse(conn);
    } finally {
      conn.disconnect();
    }
  }
}
