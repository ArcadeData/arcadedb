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

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Base64;

import static com.arcadedb.server.http.HttpSessionManager.ARCADEDB_SESSION_ID;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12810: a request that carried a session id and named another database than the session was begun on attached the session's
 * transaction to that other database, and the engine refused it with an InvalidDatabaseInstanceException: a 500, and a failed command
 * that rolled the session back, so the work the client wrote before was lost. The request is refused without touching the session.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12810SessionOnAnotherDatabaseIT extends BaseGraphServerTest {
  private static final String OTHER_DATABASE = "Issue12810Other";

  private final HttpClient client = HttpClient.newHttpClient();

  @Test
  void aSessionNamedWithAnotherDatabaseIsRefusedAndKeepsItsWork() throws Exception {
    post("/api/v1/server", new JSONObject().put("command", "drop database " + OTHER_DATABASE).toString(), null);
    assertThat(post("/api/v1/server", new JSONObject().put("command", "create database " + OTHER_DATABASE).toString(), null)
        .statusCode()).isEqualTo(200);
    try {
      final String database = getDatabaseName();
      assertThat(sql(database, "CREATE DOCUMENT TYPE Issue12810 IF NOT EXISTS", null).statusCode()).isEqualTo(200);
      assertThat(sql(OTHER_DATABASE, "CREATE DOCUMENT TYPE Issue12810 IF NOT EXISTS", null).statusCode()).isEqualTo(200);

      final HttpResponse<String> begun = post("/api/v1/begin/" + database, "{}", null);
      assertThat(begun.statusCode()).isEqualTo(204);
      final String session = begun.headers().firstValue(ARCADEDB_SESSION_ID).orElseThrow();
      assertThat(sql(database, "INSERT INTO Issue12810 SET v = 1", session).statusCode()).isEqualTo(200);

      // the id of a session of one database, the URL of another
      final HttpResponse<String> wrong = sql(OTHER_DATABASE, "INSERT INTO Issue12810 SET v = 2", session);
      assertThat(wrong.statusCode()).as("body=%s", wrong.body()).isEqualTo(404);
      assertThat(wrong.body()).doesNotContain("InvalidDatabaseInstanceException");
      assertThat(get("/api/v1/query/" + OTHER_DATABASE + "/sql/select%20from%20Issue12810", session).statusCode()).isEqualTo(404);

      // the session was not touched: it still sees its own write, and it commits
      final HttpResponse<String> inside = sql(database, "SELECT count(*) AS c FROM Issue12810", session);
      assertThat(inside.statusCode()).as("body=%s", inside.body()).isEqualTo(200);
      assertThat(new JSONObject(inside.body()).getJSONArray("result").getJSONObject(0).getInt("c")).isEqualTo(1);
      assertThat(post("/api/v1/commit/" + database, "", session).statusCode()).isEqualTo(204);

      assertThat(count(database)).isEqualTo(1);
      assertThat(count(OTHER_DATABASE)).isEqualTo(0);
    } finally {
      post("/api/v1/server", new JSONObject().put("command", "drop database " + OTHER_DATABASE).toString(), null);
    }
  }

  private int count(final String database) throws Exception {
    final HttpResponse<String> response = sql(database, "SELECT count(*) AS c FROM Issue12810", null);
    assertThat(response.statusCode()).as("body=%s", response.body()).isEqualTo(200);
    return new JSONObject(response.body()).getJSONArray("result").getJSONObject(0).getInt("c");
  }

  private HttpResponse<String> sql(final String database, final String command, final String session) throws Exception {
    return post("/api/v1/command/" + database, new JSONObject().put("language", "sql").put("command", command).toString(), session);
  }

  private HttpResponse<String> get(final String path, final String session) throws Exception {
    final HttpRequest.Builder request = HttpRequest.newBuilder().uri(new URI(getServerHttpUrl(path))).GET()
        .setHeader("Authorization", authorization());
    if (session != null)
      request.setHeader(ARCADEDB_SESSION_ID, session);
    return client.send(request.build(), BodyHandlers.ofString());
  }

  private HttpResponse<String> post(final String path, final String body, final String session) throws Exception {
    final HttpRequest.Builder request = HttpRequest.newBuilder().uri(new URI(getServerHttpUrl(path)))
        .POST(HttpRequest.BodyPublishers.ofString(body)).setHeader("Authorization", authorization());
    if (session != null)
      request.setHeader(ARCADEDB_SESSION_ID, session);
    return client.send(request.build(), BodyHandlers.ofString());
  }

  private static String authorization() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes());
  }
}
