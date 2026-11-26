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
package com.arcadedb.server.security;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A user who may create the edge type and the vertices of a vertex type but not update that type cannot load edges between its
 * vertices through the batch endpoint: the load is refused before anything is written, whether the flush runs on the request
 * thread or on the async executor, and no edge record is left behind.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class BatchVertexUpdateGrantIT extends BaseGraphServerTest {

  private static final String SCOPED_USER = "vertex-update-scoped-user";
  private static final String SCOPED_PWD  = "vertexupdateuser1";
  private static final String VERTEX_TYPE = "GrantVertex";
  private static final String EDGE_TYPE   = "GrantEdge";

  @Test
  void loadWithoutTheVertexUpdateGrantLeavesNoEdgeBehind() throws Exception {
    testEachServer((serverIndex) -> {
      command(serverIndex, "CREATE VERTEX TYPE " + VERTEX_TYPE);
      command(serverIndex, "CREATE EDGE TYPE " + EDGE_TYPE);

      final String v1 = createVertex(serverIndex);
      final String v2 = createVertex(serverIndex);

      createScopedUser(serverIndex);
      try {
        final String auth = basicAuth(SCOPED_USER, SCOPED_PWD);
        final String edgeLine =
            "{\"@type\":\"edge\",\"@class\":\"" + EDGE_TYPE + "\",\"@from\":\"" + v1 + "\",\"@to\":\"" + v2 + "\"}\n";

        assertThat(postBatch(serverIndex, getDatabaseName(), auth, edgeLine, null)).as("parallel flush").isEqualTo(403);
        assertThat(postBatch(serverIndex, getDatabaseName(), auth, edgeLine, "parallelFlush=false")).as("sequential flush")
            .isEqualTo(403);

        assertThat(count(serverIndex, "SELECT count(@rid) AS c FROM " + EDGE_TYPE)).as("edge records").isZero();
        assertThat(count(serverIndex, "SELECT bothE('" + EDGE_TYPE + "').size() AS c FROM " + v1)).as("edges of the source")
            .isZero();
        assertThat(count(serverIndex, "SELECT bothE('" + EDGE_TYPE + "').size() AS c FROM " + v2)).as("edges of the destination")
            .isZero();
      } finally {
        deleteUser(serverIndex, SCOPED_USER);
      }
    });
  }

  private String createVertex(final int serverIndex) throws Exception {
    final String response = command(serverIndex, "CREATE VERTEX " + VERTEX_TYPE);
    return new JSONObject(response).getJSONArray("result").getJSONObject(0).getString("@rid");
  }

  private long count(final int serverIndex, final String query) throws Exception {
    final String response = command(serverIndex, query);
    return new JSONObject(response).getJSONArray("result").getJSONObject(0).getLong("c");
  }

  private void createScopedUser(final int serverIndex) throws Exception {
    final ServerSecurity security = getServer(serverIndex).getSecurity();

    // read, create and update on every type through the "*" default (it covers the edge type), but the vertex type is narrowed to
    // read and create: its vertices can be read and created, not updated
    security.getDatabaseGroupsConfiguration(getDatabaseName()).put("vertexUpdateScoped",
        new JSONObject().put("access", new JSONArray())
            .put("types", new JSONObject()
                .put("*", new JSONObject().put("access",
                    new JSONArray().put("readRecord").put("createRecord").put("updateRecord")))
                .put(VERTEX_TYPE, new JSONObject().put("access", new JSONArray().put("readRecord").put("createRecord")))));
    security.saveGroups();

    if (security.existsUser(SCOPED_USER))
      security.dropUser(SCOPED_USER);

    final JSONObject payload = new JSONObject()
        .put("name", SCOPED_USER)
        .put("password", SCOPED_PWD)
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("vertexUpdateScoped")));

    final HttpURLConnection connection = openPost(serverIndex, "/api/v1/server/users",
        basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS));
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream().write(payload.toString().getBytes());
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(201);
    } finally {
      connection.disconnect();
    }
  }

  private void deleteUser(final int serverIndex, final String name) throws Exception {
    final HttpURLConnection connection = openPost(serverIndex,
        "/api/v1/server/users?name=" + URLEncoder.encode(name, StandardCharsets.UTF_8),
        basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS));
    connection.setRequestMethod("DELETE");
    connection.connect();
    try {
      connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private int postBatch(final int serverIndex, final String db, final String auth, final String jsonl,
      final String extraQueryParam) throws Exception {
    final String query = extraQueryParam != null ? "?" + extraQueryParam : "";
    final HttpURLConnection connection = openPost(serverIndex, "/api/v1/batch/" + db + query, auth);
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/x-ndjson");
    connection.getOutputStream().write(jsonl.getBytes(StandardCharsets.UTF_8));
    connection.connect();
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection openPost(final int serverIndex, final String path, final String auth) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:" + getServerHttpPort(serverIndex) + path).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", auth);
    return connection;
  }

  private String basicAuth(final String user, final String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes());
  }
}
