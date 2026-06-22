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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #4689:
 * MATCH (u:User) RETURN u throws NoSuchElementException via HTTP,
 * while MATCH (u:User) RETURN u{.*} as user works.
 * Same issue affects SQL: SELECT FROM type fails, SELECT field FROM type works.
 */
class Issue4689MatchReturnVertexIT extends BaseGraphServerTest {

  @Test
  void cypherMatchReturnWholeVertexShouldNotThrow() throws Exception {
    // Set up User type with vertices
    executeCommand(0, "opencypher", "CREATE (u:IssueUser {name: 'Alice', age: 30})");
    executeCommand(0, "opencypher", "CREATE (u:IssueUser {name: 'Bob', age: 25})");

    // This is the query that the reporter says fails (issue #4689)
    final JSONObject response = executeCommand(0, "opencypher", "MATCH (u:IssueUser) RETURN u");

    assertThat(response).isNotNull();
    assertThat(response.has("result")).as("Response should have 'result' key - query must not throw").isTrue();
  }

  @Test
  void cypherMatchReturnVertexProjectionWorkaround() throws Exception {
    executeCommand(0, "opencypher", "CREATE (u:IssueUser {name: 'Charlie', age: 35})");

    // Workaround the reporter says works
    final JSONObject response = executeCommand(0, "opencypher", "MATCH (u:IssueUser) RETURN u{.*} as user");

    assertThat(response).isNotNull();
    assertThat(response.has("result")).isTrue();
  }

  @Test
  void sqlSelectFromTypeShouldNotThrow() throws Exception {
    // SQL: returning whole record should work (same issue with SQL as with Cypher)
    final JSONObject response = executeCommand(0, "sql", "SELECT FROM V1");

    assertThat(response).isNotNull();
    assertThat(response.has("result")).as("SELECT FROM should not throw NoSuchElementException").isTrue();
  }

  @Test
  void sqlSelectFieldsShouldWork() throws Exception {
    // Selecting individual fields should work (workaround for SQL)
    final JSONObject response = executeCommand(0, "sql", "SELECT name FROM V1");

    assertThat(response).isNotNull();
    assertThat(response.has("result")).isTrue();
  }

  @Test
  void cypherMatchReturnManyVerticesShouldWork() throws Exception {
    // Create more than 100 vertices to test multi-batch pagination
    for (int i = 0; i < 110; i++)
      executeCommand(0, "opencypher", "CREATE (u:ManyUsers {idx: " + i + ", name: 'User" + i + "'})");

    final JSONObject response = executeCommand(0, "opencypher", "MATCH (u:ManyUsers) RETURN u");

    assertThat(response).isNotNull();
    assertThat(response.has("result")).isTrue();
  }

  @Test
  void cypherMatchReturnVertexWithEdgesShouldWork() throws Exception {
    // Vertices connected by edges - edge counting runs in setMetadata()
    executeCommand(0, "opencypher", "CREATE (u:UserEdgeTest {name: 'Src', role: 'admin'})");
    executeCommand(0, "opencypher", "CREATE (u:UserEdgeTest {name: 'Dst', role: 'user'})");
    executeCommand(0, "sql",
        "CREATE EDGE E1 FROM (SELECT FROM UserEdgeTest WHERE name = 'Src') TO (SELECT FROM UserEdgeTest WHERE name = 'Dst')");

    final JSONObject response = executeCommand(0, "opencypher", "MATCH (u:UserEdgeTest) RETURN u");

    assertThat(response).isNotNull();
    assertThat(response.has("result")).isTrue();
  }

  @Test
  void cypherMatchReturnVertexWithDefaultSerializer() throws Exception {
    // Test using the default (non-studio) serializer via a direct HTTP call
    executeCommand(0, "opencypher", "CREATE (u:DefaultSerTest {name: 'Alice2', age: 30})");
    executeCommand(0, "opencypher", "CREATE (u:DefaultSerTest {name: 'Bob2', age: 25})");

    final java.net.HttpURLConnection connection = (java.net.HttpURLConnection) new java.net.URI(
        "http://127.0.0.1:2480/api/v1/command/graph").toURL().openConnection();
    try {
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + java.util.Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      // Sending without serializer uses the default (non-studio) code path
      formatPayload(connection, "opencypher", "MATCH (u:DefaultSerTest) RETURN u", null, java.util.Collections.emptyMap());
      connection.connect();

      assertThat(connection.getResponseCode()).as("MATCH RETURN u with default serializer should return HTTP 200, not 500").isEqualTo(200);
    } finally {
      connection.disconnect();
    }
  }
}
