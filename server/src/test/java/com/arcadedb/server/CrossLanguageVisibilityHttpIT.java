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
package com.arcadedb.server;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression IT for issue #4355: HTTP step of the test matrix. Verify that records written via
 * SQL through POST /command are visible to a subsequent Cypher query through POST /command (and
 * vice versa). Each HTTP call commits its own transaction so this exercises committed visibility
 * across protocols and languages.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/4355">Issue #4355</a>
 */
class CrossLanguageVisibilityHttpIT extends BaseGraphServerTest {

  @Override
  protected String getDatabaseName() {
    return "issue4355";
  }

  @Override
  protected void populateDatabase() {
    // Start with an empty database; each test seeds its own data via HTTP.
  }

  @Test
  void sqlInsertVisibleToCypherMatchViaHttp() throws Exception {
    postCommand("sql", "CREATE VERTEX TYPE Person", null);
    postCommand("sql", "INSERT INTO Person SET name = 'Alice'", null);

    final JSONArray result = postCommand("opencypher", "MATCH (n:Person) RETURN n.name AS name", null);
    assertThat(result.length()).isEqualTo(1);
    assertThat(result.getJSONObject(0).getString("name")).isEqualTo("Alice");
  }

  @Test
  void cypherCreateVisibleToSqlSelectViaHttp() throws Exception {
    postCommand("opencypher", "CREATE (n:Movie {title: 'Matrix'})", null);

    final JSONArray result = postCommand("sql", "SELECT title FROM Movie", null);
    assertThat(result.length()).isEqualTo(1);
    assertThat(result.getJSONObject(0).getString("title")).isEqualTo("Matrix");
  }

  @Test
  void sqlEdgeOnHierarchyVisibleToCypherViaHttp() throws Exception {
    postCommand("sql", "CREATE VERTEX TYPE Person", null);
    postCommand("sql", "CREATE EDGE TYPE E_PARENT", null);
    postCommand("sql", "CREATE EDGE TYPE E IF NOT EXISTS EXTENDS E_PARENT", null);
    postCommand("sql", "INSERT INTO Person SET name = 'Alice'", null);
    postCommand("sql", "INSERT INTO Person SET name = 'Bob'", null);
    postCommand("sql",
        "CREATE EDGE E FROM (SELECT FROM Person WHERE name = 'Alice') TO (SELECT FROM Person WHERE name = 'Bob')",
        null);

    final JSONArray result = postCommand("opencypher",
        "MATCH (a:Person)-[r:E]->(b:Person) RETURN a.name AS src, b.name AS dst", null);
    assertThat(result.length()).isEqualTo(1);
    final JSONObject row = result.getJSONObject(0);
    assertThat(row.getString("src")).isEqualTo("Alice");
    assertThat(row.getString("dst")).isEqualTo("Bob");
  }

  @Test
  void boundParameterCrossEngineIndexLookupViaHttp() throws Exception {
    postCommand("sql", "CREATE VERTEX TYPE Person", null);
    postCommand("sql", "CREATE PROPERTY Person.name STRING", null);
    postCommand("sql", "CREATE INDEX ON Person (name) UNIQUE", null);
    postCommand("opencypher", "MERGE (p:Person {name: 'Alice'})", null);
    postCommand("opencypher", "MERGE (p:Person {name: 'Bob'})", null);

    final JSONObject params = new JSONObject().put("name", "Alice");
    final JSONArray byCypher = postCommand("opencypher",
        "MATCH (p:Person {name: $name}) RETURN p.name AS name", params);
    assertThat(byCypher.length()).isEqualTo(1);
    assertThat(byCypher.getJSONObject(0).getString("name")).isEqualTo("Alice");

    final JSONObject sqlParams = new JSONObject().put("name", "Bob");
    final JSONArray bySql = postCommand("sql",
        "SELECT name FROM Person WHERE name = :name", sqlParams);
    assertThat(bySql.length()).isEqualTo(1);
    assertThat(bySql.getJSONObject(0).getString("name")).isEqualTo("Bob");
  }

  private JSONArray postCommand(final String language, final String command, final JSONObject params) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:2480/api/v1/command/" + getDatabaseName()).openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(
            ("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/json");

    final JSONObject payload = new JSONObject()
        .put("language", language)
        .put("command", command);
    if (params != null)
      payload.put("params", params);

    formatPayload(connection, payload);
    connection.connect();

    try {
      assertThat(connection.getResponseCode())
          .as("%s request failed: %s", language, command)
          .isEqualTo(200);
      final String response = readResponse(connection);
      return new JSONObject(response).getJSONArray("result");
    } finally {
      connection.disconnect();
    }
  }
}
