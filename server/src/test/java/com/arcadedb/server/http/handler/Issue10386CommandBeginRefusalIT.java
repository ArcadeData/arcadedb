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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10386: the SQL {@code BEGIN ISOLATION} and openCypher {@code START TRANSACTION ISOLATION} statements parse the level
 * through {@code IsolationLevels}, like the HTTP {@code /begin} and the gRPC {@code BeginTransaction}. Sent over
 * {@code POST /api/v1/command}, a level the engine cannot provide is a client error: a 4xx naming the levels it does, never a
 * server error and never a transaction begun at a weaker level.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10386CommandBeginRefusalIT extends BaseGraphServerTest {

  @Test
  void aSqlBeginOfAnUnsupportedLevelIsAClientError() throws Exception {
    final HttpResponse<String> response = command("sql", "BEGIN ISOLATION SERIALIZABLE");

    assertThat(response.statusCode()).isBetween(400, 499);
    assertThat(response.body()).contains("SERIALIZABLE").contains("READ_COMMITTED").contains("REPEATABLE_READ");
  }

  @Test
  void anOpenCypherStartTransactionOfAnUnsupportedLevelIsAClientError() throws Exception {
    final HttpResponse<String> response = command("opencypher", "START TRANSACTION ISOLATION SERIALIZABLE");

    assertThat(response.statusCode()).isBetween(400, 499);
    assertThat(response.body()).contains("SERIALIZABLE").contains("READ_COMMITTED").contains("REPEATABLE_READ");
  }

  private HttpResponse<String> command(final String language, final String command) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(getServerHttpUrl("/api/v1/command/" + getDatabaseName())))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("language", language).put("command", command).toString()))
        .build();
    try (final HttpClient client = HttpClient.newHttpClient()) {
      return client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }
  }
}
