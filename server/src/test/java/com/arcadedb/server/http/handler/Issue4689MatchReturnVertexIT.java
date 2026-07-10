/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.http.handler;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Regression test for issue #4689: MATCH (u:IssueUser) RETURN u must not throw NSE via HTTP. */
class Issue4689MatchReturnVertexIT extends BaseGraphServerTest {

  @Test
  void cypherMatchReturnWholeVertexShouldNotThrow() throws Exception {
    executeCommand(0, "opencypher", "CREATE (u:IssueUser {name: 'Alice', age: 30})");
    executeCommand(0, "opencypher", "CREATE (u:IssueUser {name: 'Bob', age: 25})");

    final JSONObject response = executeCommand(0, "opencypher", "MATCH (u:IssueUser) RETURN u");

    assertThat(response).isNotNull();
    assertThat(response.has("result")).as("Response should have 'result' key - query must not throw").isTrue();
    final JSONObject result = response.getJSONObject("result");
    assertThat(result.getJSONArray("records").length()).as("Should return 2 User vertices").isEqualTo(2);
  }

  @Test
  void cypherMatchReturnVertexProjectionWorkaround() throws Exception {
    executeCommand(0, "opencypher", "CREATE (u:IssueUserProj {name: 'Charlie', age: 35})");

    final JSONObject response = executeCommand(0, "opencypher", "MATCH (u:IssueUserProj) RETURN u{.*} as user");

    assertThat(response).isNotNull();
    assertThat(response.has("result")).isTrue();
    final JSONObject result = response.getJSONObject("result");
    assertThat(result.getJSONArray("records").length()).as("Projection workaround should return 1 vertex").isEqualTo(1);
  }

  @Test
  void sqlSelectFromTypeShouldNotThrow() throws Exception {
    executeCommand(0, "sql", "CREATE VERTEX TYPE SqlSelectAll");
    executeCommand(0, "sql", "INSERT INTO SqlSelectAll SET name = 'Alice', age = 30");
    executeCommand(0, "sql", "INSERT INTO SqlSelectAll SET name = 'Bob', age = 25");

    final JSONObject response = executeCommand(0, "sql", "SELECT FROM SqlSelectAll");

    assertThat(response).isNotNull();
    assertThat(response.has("result")).as("SELECT FROM should not throw NoSuchElementException").isTrue();
    final JSONObject result = response.getJSONObject("result");
    assertThat(result.getJSONArray("records").length()).as("Should return 2 records").isEqualTo(2);
  }

  @Test
  void sqlSelectFieldsShouldWork() throws Exception {
    executeCommand(0, "sql", "CREATE VERTEX TYPE SqlSelectFields");
    executeCommand(0, "sql", "INSERT INTO SqlSelectFields SET name = 'Dave', age = 40");

    final JSONObject response = executeCommand(0, "sql", "SELECT name FROM SqlSelectFields");

    assertThat(response).isNotNull();
    assertThat(response.has("result")).isTrue();
    final JSONObject result = response.getJSONObject("result");
    assertThat(result.getJSONArray("records").length()).as("Should return 1 record").isEqualTo(1);
  }

  @Test
  void cypherMatchReturnManyVerticesShouldWork() throws Exception {
    // Create 110 vertices in a single command to test multi-batch pagination (>100 default batch)
    executeCommand(0, "opencypher",
        "UNWIND range(0, 109) AS i CREATE (:ManyUsers {idx: i, name: 'User' + toString(i)})");

    final JSONObject response = executeCommand(0, "opencypher", "MATCH (u:ManyUsers) RETURN u");

    assertThat(response).isNotNull();
    assertThat(response.has("result")).isTrue();
    final JSONObject result = response.getJSONObject("result");
    assertThat(result.getJSONArray("records").length())
        .as("Should return all 110 vertices across pagination batches")
        .isEqualTo(110);
  }

  @Test
  void cypherMatchReturnVertexWithEdgesShouldWork() throws Exception {
    executeCommand(0, "opencypher", "CREATE (u:UserEdgeTest {name: 'Src', role: 'admin'})");
    executeCommand(0, "opencypher", "CREATE (u:UserEdgeTest {name: 'Dst', role: 'user'})");
    executeCommand(0, "sql",
        "CREATE EDGE E1 FROM (SELECT FROM UserEdgeTest WHERE name = 'Src') TO (SELECT FROM UserEdgeTest WHERE name = 'Dst')");

    final JSONObject response = executeCommand(0, "opencypher", "MATCH (u:UserEdgeTest) RETURN u");

    assertThat(response).isNotNull();
    assertThat(response.has("result")).isTrue();
    final JSONObject result = response.getJSONObject("result");
    final JSONArray records = result.getJSONArray("records");
    assertThat(records.length()).as("Should return 2 vertices").isEqualTo(2);
    // Verify edge counts are populated via setMetadata() - at least one vertex must have edges
    boolean hasEdgeCounts = false;
    for (int i = 0; i < records.length(); i++) {
      final JSONObject rec = records.getJSONObject(i);
      if (rec.getInt("@out", 0) > 0 || rec.getInt("@in", 0) > 0) {
        hasEdgeCounts = true;
        break;
      }
    }
    assertThat(hasEdgeCounts).as("At least one vertex should have non-zero edge count in @out or @in").isTrue();
  }

  @Test
  void cypherMatchReturnVertexWithDefaultSerializer() throws Exception {
    executeCommand(0, "opencypher", "CREATE (u:DefaultSerTest {name: 'Alice2', age: 30})");
    executeCommand(0, "opencypher", "CREATE (u:DefaultSerTest {name: 'Bob2', age: 25})");

    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:2480/api/v1/command/" + getDatabaseName()).openConnection();
    try {
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "opencypher", "MATCH (u:DefaultSerTest) RETURN u", null, Map.of());
      connection.connect();

      assertThat(connection.getResponseCode())
          .as("MATCH RETURN u with default serializer should return HTTP 200, not 500").isEqualTo(200);

      final String responseBody = readResponse(connection);
      final JSONObject response = new JSONObject(responseBody);
      assertThat(response.has("result")).isTrue();
      final JSONArray result = response.getJSONArray("result");
      assertThat(result.length()).as("Should return 2 vertices").isEqualTo(2);
    } finally {
      connection.disconnect();
    }
  }
}
