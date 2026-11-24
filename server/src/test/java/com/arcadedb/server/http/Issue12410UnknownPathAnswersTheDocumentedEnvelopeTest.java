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
package com.arcadedb.server.http;

import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.StaticBaseServerTest;
import com.arcadedb.server.http.handler.NotFoundRouteHandler;
import io.undertow.Undertow;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static com.arcadedb.server.StaticBaseServerTest.allocateFreePorts;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12410: a request to a path the server does not serve was answered 404 with an empty body: no {@code error}
 * field, no {@code X-Request-Id} and no {@code X-ArcadeDB-Replay-Protection}. The HTTP API reference promises that an
 * erroneous request has an {@code error} field and that every response carries those two headers.
 */
class Issue12410UnknownPathAnswersTheDocumentedEnvelopeTest extends BaseGraphServerTest {
  private Undertow undertow;

  @Test
  void anUnknownPathAnswersTheDocumentedEnvelope() throws Exception {
    final Response response = send("GET", "/api/v1/nosuchroute");

    assertThat(response.status).as(response.body).isEqualTo(404);
    assertThat(response.body).as("the answer carries the error field").contains("\"error\"");
    assertThat(response.requestId).as("the correlation id is on the answer").isNotNull();
    assertThat(response.replayProtection).as("the replay-protection header is on the answer").isNotNull();
  }

  @Test
  void anUnknownPathUnderTheAiPrefixAnswersTheDocumentedEnvelope() throws Exception {
    final Response response = send("GET", "/api/v1/ai/nosuchroute");

    assertThat(response.status).as(response.body).isEqualTo(404);
    assertThat(response.body).as("the answer carries the error field").contains("\"error\"");
    assertThat(response.requestId).as("the correlation id is on the answer").isNotNull();
  }

  @Test
  void theRequestIdOfTheRequestIsEchoed() throws Exception {
    final Response response = send("GET", "/api/v1/nosuchroute", "my-request-id-12410");

    assertThat(response.status).as(response.body).isEqualTo(404);
    assertThat(response.requestId).as("the id the client sent is echoed").isEqualTo("my-request-id-12410");
    assertThat(response.body).as("the answer names the id it was given").contains("my-request-id-12410");
  }

  /**
   * A path that matches no prefix at all is answered by the handler the path handler hands it to. The server mounts
   * the Studio on {@code /} in development and test mode, and the Studio answers an unknown path with its own
   * "Not Found", so the handler is exercised here on its own.
   */
  @Test
  void aPathThatIsNotEvenAPrefixIsAnsweredByTheNotFoundHandler() throws Exception {
    final PluginPathHandler paths = new PluginPathHandler(new NotFoundRouteHandler());
    final int port = allocateFreePorts(1)[0];
    undertow = Undertow.builder().addHttpListener(port, "127.0.0.1").setHandler(paths).build();
    undertow.start();
    try (final HttpClient client = HttpClient.newHttpClient()) {
      final HttpResponse<String> response = client.send(
          HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/nosuchpath")).GET().build(),
          HttpResponse.BodyHandlers.ofString());

      assertThat(response.statusCode()).as(response.body()).isEqualTo(404);
      assertThat(response.body()).as("the answer carries the error field").contains("\"error\"");
      assertThat(response.headers().firstValue("X-Request-Id")).as("the correlation id is on the answer").isPresent();
      assertThat(response.headers().firstValue("X-ArcadeDB-Replay-Protection")).as("the replay-protection header is on the answer")
          .isPresent();
    } finally {
      undertow.stop();
      undertow = null;
    }
  }

  @Test
  void aRegisteredRouteStillAnswersAsBefore() throws Exception {
    final Response response = send("GET", "/api/v1/databases");

    assertThat(response.status).as(response.body).isEqualTo(200);
    assertThat(response.body).as("a registered route still answers with its result").contains("\"result\"");
  }

  private Response send(final String method, final String path) throws Exception {
    return send(method, path, null);
  }

  private Response send(final String method, final String path, final String requestId) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl(path)).toURL().openConnection();
    connection.setRequestMethod(method);
    final String credentials = Base64.getEncoder().encodeToString(("root:" + StaticBaseServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    connection.setRequestProperty("Authorization", "Basic " + credentials);
    if (requestId != null)
      connection.setRequestProperty("X-Request-Id", requestId);
    try {
      connection.connect();
      final int status = connection.getResponseCode();
      final InputStream in = status >= 400 ? connection.getErrorStream() : connection.getInputStream();
      final String body = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
      return new Response(status, body, connection.getHeaderField("X-Request-Id"),
          connection.getHeaderField("X-ArcadeDB-Replay-Protection"));
    } finally {
      connection.disconnect();
    }
  }

  private record Response(int status, String body, String requestId, String replayProtection) {
  }
}
