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

import com.arcadedb.database.Database;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5812: the {@code @props} per-column type hint leaked into every HTTP JSON response, not just the
 * RemoteDatabase Java driver requests that actually need it to rebuild the exact Java type of a non-element
 * (projection/aggregate) column. A plain HTTP caller (curl, wget, a non-Java client) never asked for it and
 * should get plain JSON back.
 * <p>
 * Both reproductions from the issue are covered: a scalar projection on a lossy-typed column (SHORT), and
 * {@code SELECT FROM schema:types}, whose synthetic rows are not schema-backed elements either. Both are
 * verified absent by default and present only when the request explicitly opts in with {@code typeHints}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5812PropsTypeHintHttpIT extends BaseGraphServerTest {

  private static final String DATABASE_NAME = "Issue5812PropsTypeHintHttpIT";

  @Override
  protected String getDatabaseName() {
    return DATABASE_NAME;
  }

  @Override
  protected void populateDatabase() {
    final Database database = getDatabase(0);
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc").createProperty("num", Type.SHORT);
      database.newDocument("Doc").set("num", (short) 0).save();
    });
  }

  @Test
  void getQueryOmitsPropsHintByDefault() throws Exception {
    testEachServer(serverIndex -> {
      final String response = getQuery(serverIndex, "select num from Doc", null);
      final JSONObject row = new JSONObject(response).getJSONArray("result").getJSONObject(0);
      assertThat(row.getInt("num")).isZero();
      assertThat(row.has("@props")).isFalse();
    });
  }

  @Test
  void getQueryEmitsPropsHintWhenExplicitlyRequested() throws Exception {
    testEachServer(serverIndex -> {
      final String response = getQuery(serverIndex, "select num from Doc", "true");
      final JSONObject row = new JSONObject(response).getJSONArray("result").getJSONObject(0);
      assertThat(row.has("@props")).isTrue();
      assertThat(row.getString("@props")).isEqualTo("num:" + Type.SHORT.getId());
    });
  }

  @Test
  void schemaTypesQueryNeverLeaksPropsHintByDefault() throws Exception {
    testEachServer(serverIndex -> {
      final String response = getQuery(serverIndex, "select from schema:types", null);
      assertThat(response).doesNotContain("@props");
    });
  }

  @Test
  void postCommandOmitsPropsHintByDefaultForAggregate() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject payload = new JSONObject()
          .put("language", "sql")
          .put("command", "select count(*) as c from Doc")
          .put("serializer", "record");

      final HttpURLConnection connection = openCommandConnection(serverIndex);
      formatPayload(connection, payload);
      connection.connect();
      try {
        final JSONObject row = new JSONObject(readResponse(connection)).getJSONArray("result").getJSONObject(0);
        assertThat(row.getLong("c")).isEqualTo(1L);
        assertThat(row.has("@props")).isFalse();
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void postCommandEmitsPropsHintWhenExplicitlyRequestedForAggregate() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject payload = new JSONObject()
          .put("language", "sql")
          .put("command", "select count(*) as c from Doc")
          .put("serializer", "record")
          .put("typeHints", true);

      final HttpURLConnection connection = openCommandConnection(serverIndex);
      formatPayload(connection, payload);
      connection.connect();
      try {
        final JSONObject row = new JSONObject(readResponse(connection)).getJSONArray("result").getJSONObject(0);
        assertThat(row.has("@props")).isTrue();
        assertThat(row.getString("@props")).isEqualTo("c:" + Type.LONG.getId());
      } finally {
        connection.disconnect();
      }
    });
  }

  private String getQuery(final int serverIndex, final String sql, final String typeHints) throws IOException {
    final String encoded = java.net.URLEncoder.encode(sql, java.nio.charset.StandardCharsets.UTF_8).replace("+", "%20");
    String url = "http://localhost:248" + serverIndex + "/api/v1/query/" + DATABASE_NAME + "/sql/" + encoded;
    if (typeHints != null)
      url += "?typeHints=" + typeHints;

    final HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      return readResponse(connection);
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection openCommandConnection(final int serverIndex) throws IOException {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://localhost:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    return connection;
  }
}
