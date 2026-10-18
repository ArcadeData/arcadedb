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

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.AdmissionSlotConformance;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10235 over HTTP: a statement that gives its admission slot back before a wait finds the slot on the thread that
 * runs it, for a command, for a command inside an HTTP session and for a bulk load. See {@link AdmissionSlotConformance}.
 */
class AdmissionSlotConformanceHttpTest extends BaseGraphServerTest {
  private static final HttpClient HTTP = HttpClient.newHttpClient();
  private static final String     TYPE = "Conformance10235";

  @Test
  void aCommandGivesItsSlotBackFromTheThreadThatRunsIt() throws Exception {
    final Database database = createType();
    AdmissionSlotConformance.assertGivesItsSlotBack(database, TYPE, "HTTP POST /command", () -> {
      final HttpResponse<String> answer = send(command("INSERT INTO " + TYPE + " SET id = 1", null));
      assertThat(answer.statusCode()).as(answer.body()).isEqualTo(200);
      return null;
    });
  }

  @Test
  void aCommandInsideASessionGivesItsSlotBackFromTheThreadThatRunsIt() throws Exception {
    final Database database = createType();
    final HttpResponse<String> begin = send(emptyPost("/api/v1/begin/" + getDatabaseName(), null));
    assertThat(begin.statusCode()).as(begin.body()).isEqualTo(204);
    final String session = begin.headers().firstValue("arcadedb-session-id").orElseThrow();

    AdmissionSlotConformance.assertGivesItsSlotBack(database, TYPE, "HTTP POST /command in a session", () -> {
      final HttpResponse<String> answer = send(command("INSERT INTO " + TYPE + " SET id = 2", session));
      assertThat(answer.statusCode()).as(answer.body()).isEqualTo(200);
      return null;
    });

    final HttpResponse<String> commit = send(emptyPost("/api/v1/commit/" + getDatabaseName(), session));
    assertThat(commit.statusCode()).as(commit.body()).isEqualTo(204);
    assertThat(database.countType(TYPE, false)).isEqualTo(1);
  }

  @Test
  void aBatchLoadGivesItsSlotBackFromTheThreadThatRunsIt() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().getOrCreateVertexType(TYPE);
    AdmissionSlotConformance.assertGivesItsSlotBack(database, TYPE, "HTTP POST /batch", () -> {
      final String line = "{\"@type\":\"vertex\",\"@class\":\"" + TYPE + "\",\"name\":\"a\"}\n";
      final HttpResponse<String> answer = send(post("/api/v1/batch/" + getDatabaseName(), null)
          .header("Content-Type", "application/x-ndjson").POST(HttpRequest.BodyPublishers.ofString(line)).build());
      assertThat(answer.statusCode()).as(answer.body()).isEqualTo(200);
      return null;
    });
    assertThat(database.countType(TYPE, false)).isEqualTo(1);
  }

  private Database createType() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().getOrCreateDocumentType(TYPE);
    return database;
  }

  private HttpRequest command(final String sql, final String session) {
    return post("/api/v1/command/" + getDatabaseName(), session).header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("language", "sql").put("command", sql).toString())).build();
  }

  private HttpRequest emptyPost(final String path, final String session) {
    return post(path, session).POST(HttpRequest.BodyPublishers.noBody()).build();
  }

  private HttpRequest.Builder post(final String path, final String session) {
    final HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, path))).header("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    if (session != null)
      builder.header("arcadedb-session-id", session);
    return builder;
  }

  private static HttpResponse<String> send(final HttpRequest request) throws Exception {
    return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
