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

import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10395: a request body that cannot be parsed was swallowed into a WARNING and handed to the handler as a null payload, which
 * every handler then had to tell from "no body was sent". {@code POST /api/v1/begin} remembered (#10337), {@code POST /api/v1/server}
 * did not and dereferenced the null: an NPE answered as a 500. The shared request pipeline now refuses an unparseable body once, with
 * a 400 naming the parse error, and the server command handler answers a missing body with a 400 of its own.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10395MalformedPayloadIT extends BaseGraphServerTest {

  private static final String[] MALFORMED_BODIES = { "{\"command\":", "{\"command\":\"list databases\"", "not json", "null", "123",
      "\"list databases\"", "[" };

  @Test
  void aMalformedBodyOnTheServerCommandRouteAnswers400NotAnNpe() throws Exception {
    for (final String body : MALFORMED_BODIES) {
      final Response response = post("/api/v1/server", body);
      assertThat(response.code).as(body).isEqualTo(400);
      assertThat(response.body).as(body).contains("Invalid JSON payload").doesNotContain("NullPointerException");
    }
  }

  @Test
  void theRefusalNamesTheParseError() throws Exception {
    final Response response = post("/api/v1/server", "{\"command\":");

    assertThat(response.code).isEqualTo(400);
    // The generic phrase alone tells the client nothing about WHICH part of its body was refused: the parser's position does
    assertThat(response.body).contains("Invalid JSON payload").contains("valid JSON object").contains("line 1 column");
  }

  @Test
  void theRefusalDoesNotEchoTheBody() throws Exception {
    // The exception the JSON classes throw repeats the whole body: a large or secret-bearing one must not come back, or be logged
    final String secret = "hunter2-" + "x".repeat(20_000);
    final Response response = post("/api/v1/server", "{\"command\":\"create user {\\\"password\\\":\\\"" + secret + "\\\"\"");

    assertThat(response.code).isEqualTo(400);
    assertThat(response.body).doesNotContain("hunter2").hasSizeLessThan(1_000);
  }

  @Test
  void theRefusalDoesNotQuoteWhatSitsAtTheFailurePosition() throws Exception {
    // The parser names a position, not a snippet: a secret-like token right where the body stops making sense must not come back
    for (final String body : new String[] { "{\"command\":\"x\",\"password\":\"hunter2\" hunter2-secret}",
        "{\"command\":\"x\" \"password\":\"hunter2\"}", "{hunter2-secret:", "[hunter2-secret,", "hunter2-secret",
        "\"hunter2 at line 5 column 3\"", "hunter2 at line 5 column 3" }) {
      final Response response = post("/api/v1/server", body);

      assertThat(response.code).as(body).isEqualTo(400);
      assertThat(response.body).as(body).contains("Invalid JSON payload").doesNotContain("hunter2");
    }
  }

  @Test
  void aMissingBodyOnTheServerCommandRouteAnswers400NotAnNpe() throws Exception {
    for (final String body : new String[] { null, "", "  ", "\n", "{}" }) {
      final Response response = post("/api/v1/server", body);
      assertThat(response.code).as("'" + body + "'").isEqualTo(400);
      assertThat(response.body).as("'" + body + "'").contains("Server command is null");
    }
  }

  @Test
  void aValidBodyOnTheServerCommandRouteStillRuns() throws Exception {
    final Response response = post("/api/v1/server", "{\"command\":\"list databases\"}");

    assertThat(response.code).isEqualTo(200);
    assertThat(response.body).contains(getDatabaseName());
  }

  @Test
  void everyRouteThatReadsABodyRefusesAMalformedOneTheSame() throws Exception {
    final String database = getDatabaseName();
    for (final String route : new String[] { "/api/v1/server", "/api/v1/begin/" + database, "/api/v1/command/" + database,
        "/api/v1/query/" + database }) {
      final Response response = post(route, "{\"language\":\"sql\",\"command\":");
      assertThat(response.code).as(route).isEqualTo(400);
      assertThat(response.body).as(route).contains("Invalid JSON payload");
    }
  }

  @Test
  void aRouteThatTakesABodyThatIsNotJsonIsNotAffected() throws Exception {
    // The line protocol of the time series write route is text, not JSON: that route says so (requiresJsonPayload) and its body
    // is never parsed as JSON, so the refusal of an unparseable one must not reach it
    final Response created = post("/api/v1/command/" + getDatabaseName(),
        "{\"language\":\"sql\",\"command\":\"CREATE TIMESERIES TYPE cpu TIMESTAMP ts TAGS (host STRING) FIELDS (usage DOUBLE)\"}");
    assertThat(created.code).isEqualTo(200);

    final Response written = post("/api/v1/ts/" + getDatabaseName() + "/write?precision=ms",
        "cpu,host=server1 usage=55.3 1000\ncpu,host=server2 usage=72.1 2000\n");

    assertThat(written.code).isEqualTo(204);
  }

  @Test
  void aRefusedBodyDoesNotLeaveTheNextRequestOnTheSameWorkerRefused() throws Exception {
    assertThat(post("/api/v1/server", "{\"command\":").code).isEqualTo(400);
    assertThat(post("/api/v1/server", "{\"command\":\"list databases\"}").code).isEqualTo(200);
  }

  @Test
  void aBlankBodyIsNoBodyAndStaysOnTheRouteThatAcceptsIt() throws Exception {
    // Whitespace only is an absent body, not a malformed one: begin opens a transaction with the database default
    for (final String body : new String[] { null, "", "  ", "\n" }) {
      final Response response = post("/api/v1/begin/" + getDatabaseName(), body);
      assertThat(response.code).as("'" + body + "'").isEqualTo(204);
    }
  }

  private Response post(final String route, final String body) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl(route)).toURL().openConnection();
    try {
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      if (body != null) {
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Content-Length", Integer.toString(bytes.length));
        connection.setDoOutput(true);
        try (final OutputStream out = connection.getOutputStream()) {
          out.write(bytes);
        }
      }
      connection.connect();
      final int code = connection.getResponseCode();
      return new Response(code, code >= 400 ? readError(connection) : readResponse(connection));
    } finally {
      connection.disconnect();
    }
  }

  private record Response(int code, String body) {
  }
}
