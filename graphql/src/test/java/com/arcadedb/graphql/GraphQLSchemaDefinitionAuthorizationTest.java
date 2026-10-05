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
package com.arcadedb.graphql;

import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ApiTokenConfiguration;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A GraphQL type definition replaces the database's shared GraphQL schema, so it is a schema change: a user holding
 * only {@code readRecord} must be refused (403) both on the read-only query endpoint and on the command endpoint,
 * while the administrator can still define it and the read-only user can still run plain GraphQL queries.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphQLSchemaDefinitionAuthorizationTest extends BaseGraphServerTest {
  private static final String SCHEMA   = "type Query { books: [Book] @sql(statement: \"SELECT name FROM Book\") } type Book { name: String }";
  private static final String REPLACED =
      "type Query { books: [Book] @sql(statement: \"SELECT 'replaced' AS name\") } type Book { name: String }";

  @Test
  void typeDefinitionIsClassifiedAsSchemaChange() {
    final QueryEngine.AnalyzedQuery analyzed = getServerDatabase(0, getDatabaseName()).getQueryEngine("graphql").analyze(SCHEMA);
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.SCHEMA);
  }

  @Test
  void readOnlyUserCannotReplaceTheGraphQLSchema() throws Exception {
    testEachServer(serverIndex -> {
      assertThat(call(serverIndex, basicAuth(), "command", "sql", "CREATE DOCUMENT TYPE Book")).isEqualTo(200);
      assertThat(call(serverIndex, basicAuth(), "command", "sql", "INSERT INTO Book SET name = 'real book'")).isEqualTo(200);
      assertThat(call(serverIndex, basicAuth(), "command", "graphql", SCHEMA)).isEqualTo(200);

      final String token = "Bearer " + createReadOnlyToken(serverIndex, "graphql-schema-token");
      try {
        assertThat(call(serverIndex, token, "query", "graphql", REPLACED)).as("query endpoint refuses a non-idempotent document").isEqualTo(400);
        assertThat(call(serverIndex, token, "command", "graphql", REPLACED)).as("command endpoint").isEqualTo(403);

        // the schema is untouched: a plain query by the read-only user and by the administrator returns the real book
        assertThat(callBody(serverIndex, token, "query", "graphql", "{ books { name } }")).contains("real book");
        assertThat(callBody(serverIndex, basicAuth(), "query", "graphql", "{ books { name } }")).contains("real book")
            .doesNotContain("replaced");
      } finally {
        deleteToken(serverIndex, "graphql-schema-token");
      }

      // positive control: the administrator can still replace it
      assertThat(call(serverIndex, basicAuth(), "command", "graphql", REPLACED)).isEqualTo(200);
      assertThat(callBody(serverIndex, basicAuth(), "query", "graphql", "{ books { name } }")).contains("replaced");
    });
  }

  private int call(final int serverIndex, final String auth, final String endpoint, final String language, final String text)
      throws Exception {
    final HttpURLConnection connection = post(serverIndex, auth, endpoint, language, text);
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private String callBody(final int serverIndex, final String auth, final String endpoint, final String language, final String text)
      throws Exception {
    final HttpURLConnection connection = post(serverIndex, auth, endpoint, language, text);
    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      return readResponse(connection);
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection post(final int serverIndex, final String auth, final String endpoint, final String language,
      final String text) throws Exception {
    final HttpURLConnection connection = open(serverIndex, "/api/v1/" + endpoint + "/" + getDatabaseName(), auth);
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream().write(new JSONObject().put("language", language).put("command", text).toString().getBytes());
    connection.connect();
    return connection;
  }

  private String createReadOnlyToken(final int serverIndex, final String name) throws Exception {
    final JSONObject permissions = new JSONObject()
        .put("types", new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))))
        .put("database", new JSONArray());

    final HttpURLConnection connection = open(serverIndex, "/api/v1/server/api-tokens", basicAuth());
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream().write(new JSONObject().put("name", name).put("database", getDatabaseName()).put("expiresAt", 0)
        .put("permissions", permissions).toString().getBytes());
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(201);
      return new JSONObject(readResponse(connection)).getJSONObject("result").getString("token");
    } finally {
      connection.disconnect();
    }
  }

  private void deleteToken(final int serverIndex, final String name) {
    final ApiTokenConfiguration tokenConfig = getServer(serverIndex).getSecurity().getApiTokenConfiguration();
    tokenConfig.listTokens().stream().filter(t -> name.equals(t.getString("name", "")))
        .forEach(t -> tokenConfig.deleteToken(t.getString("tokenHash")));
  }

  private HttpURLConnection open(final int serverIndex, final String path, final String auth) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:" + getServerHttpPort(serverIndex) + path).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", auth);
    return connection;
  }

  private String basicAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes());
  }
}
