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
package com.arcadedb.server.mcp;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Transport-envelope conformance for the Model Context Protocol version advertised by
 * {@link MCPDispatcher#MCP_PROTOCOL_VERSION} (issue #5394): JSON-RPC batches, notification
 * suppression, request-id validation, the HTTP status for an accepted notification, GET handling
 * and Origin validation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MCPTransportConformanceTest extends BaseGraphServerTest {

  private String getMcpUrl() {
    return "http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/mcp";
  }

  @BeforeEach
  void enableMCP() {
    final MCPConfiguration config = getServer(0).getMCPConfiguration();
    config.setEnabled(true);
    config.setAllowReads(true);
    config.setAllowedUsers(List.of("root"));
  }

  // ---------------------------------------------------------------- notifications

  @Test
  void notificationReturns202WithoutBody() throws Exception {
    // MCP 2025-03-26: a POST accepted with only notifications must answer 202 Accepted with no body.
    final Response response = post(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("method", "notifications/initialized")
        .put("params", new JSONObject()).toString(), null);

    assertThat(response.status).isEqualTo(202);
    assertThat(response.body).isEmpty();
  }

  @Test
  void unknownNotificationIsSilentlyAccepted() throws Exception {
    // A receiver must not send a response to ANY notification. Before the fix only
    // 'notifications/initialized' was special-cased and every other notification fell through to the
    // default branch, producing a 'Method not found' error carrying a null id.
    final Response response = post(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("method", "notifications/cancelled")
        .put("params", new JSONObject().put("requestId", 1)).toString(), null);

    assertThat(response.status).isEqualTo(202);
    assertThat(response.body).isEmpty();
  }

  // ---------------------------------------------------------------- request envelope

  @Test
  void requestWithExplicitNullIdIsRejected() throws Exception {
    // A request that expects a response must carry a string or integer id; an explicit null is invalid.
    final Response response = post("{\"jsonrpc\":\"2.0\",\"id\":null,\"method\":\"tools/list\"}", null);

    assertThat(response.status).isEqualTo(200);
    final JSONObject json = new JSONObject(response.body);
    assertThat(json.has("error")).isTrue();
    assertThat(json.getJSONObject("error").getInt("code")).isEqualTo(-32600);
  }

  @Test
  void requestWithFractionalIdIsRejected() throws Exception {
    final Response response = post("{\"jsonrpc\":\"2.0\",\"id\":1.5,\"method\":\"tools/list\"}", null);

    final JSONObject json = new JSONObject(response.body);
    assertThat(json.has("error")).isTrue();
    assertThat(json.getJSONObject("error").getInt("code")).isEqualTo(-32600);
  }

  @Test
  void requestWithStringIdIsAccepted() throws Exception {
    final Response response = post(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", "req-1")
        .put("method", "ping").toString(), null);

    final JSONObject json = new JSONObject(response.body);
    assertThat(json.has("result")).isTrue();
    assertThat(json.getString("id")).isEqualTo("req-1");
  }

  // ---------------------------------------------------------------- batches

  @Test
  void batchOfRequestsReturnsArrayOfResponses() throws Exception {
    final JSONArray batch = new JSONArray()
        .put(new JSONObject().put("jsonrpc", "2.0").put("id", 1).put("method", "ping"))
        .put(new JSONObject().put("jsonrpc", "2.0").put("id", 2).put("method", "tools/list"));

    final Response response = post(batch.toString(), null);

    assertThat(response.status).isEqualTo(200);
    final JSONArray responses = new JSONArray(response.body);
    assertThat(responses.length()).isEqualTo(2);
    assertThat(responses.getJSONObject(0).getInt("id")).isEqualTo(1);
    assertThat(responses.getJSONObject(0).has("result")).isTrue();
    assertThat(responses.getJSONObject(1).getInt("id")).isEqualTo(2);
    assertThat(responses.getJSONObject(1).getJSONObject("result").has("tools")).isTrue();
  }

  @Test
  void batchOfOnlyNotificationsReturns202WithoutBody() throws Exception {
    final JSONArray batch = new JSONArray()
        .put(new JSONObject().put("jsonrpc", "2.0").put("method", "notifications/initialized"))
        .put(new JSONObject().put("jsonrpc", "2.0").put("method", "notifications/cancelled"));

    final Response response = post(batch.toString(), null);

    assertThat(response.status).isEqualTo(202);
    assertThat(response.body).isEmpty();
  }

  @Test
  void batchMixesRequestsAndNotifications() throws Exception {
    // The notification contributes no entry: the array must hold exactly the one request response.
    final JSONArray batch = new JSONArray()
        .put(new JSONObject().put("jsonrpc", "2.0").put("method", "notifications/initialized"))
        .put(new JSONObject().put("jsonrpc", "2.0").put("id", 7).put("method", "ping"));

    final Response response = post(batch.toString(), null);

    assertThat(response.status).isEqualTo(200);
    final JSONArray responses = new JSONArray(response.body);
    assertThat(responses.length()).isEqualTo(1);
    assertThat(responses.getJSONObject(0).getInt("id")).isEqualTo(7);
  }

  @Test
  void batchWithNonObjectElementReportsInvalidRequestForThatElementOnly() throws Exception {
    final Response response = post("[1,{\"jsonrpc\":\"2.0\",\"id\":9,\"method\":\"ping\"}]", null);

    assertThat(response.status).isEqualTo(200);
    final JSONArray responses = new JSONArray(response.body);
    assertThat(responses.length()).isEqualTo(2);
    assertThat(responses.getJSONObject(0).getJSONObject("error").getInt("code")).isEqualTo(-32600);
    assertThat(responses.getJSONObject(1).getInt("id")).isEqualTo(9);
    assertThat(responses.getJSONObject(1).has("result")).isTrue();
  }

  @Test
  void emptyBatchIsRejected() throws Exception {
    final Response response = post("[]", null);

    final JSONObject json = new JSONObject(response.body);
    assertThat(json.getJSONObject("error").getInt("code")).isEqualTo(-32600);
  }

  // ---------------------------------------------------------------- HTTP method handling

  @Test
  void getReturns405WithAllowHeader() throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpUrl()).toURL().openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization", getBasicAuth());
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(405);
      assertThat(connection.getHeaderField("Allow")).contains("POST");
    } finally {
      connection.disconnect();
    }
  }

  // ---------------------------------------------------------------- Origin validation

  @Test
  void crossOriginRequestIsRejected() throws Exception {
    // DNS-rebinding mitigation: a browser page served from another origin must not drive the endpoint.
    final Response response = post(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 1)
        .put("method", "ping").toString(), "http://evil.example.com");

    assertThat(response.status).isEqualTo(403);
  }

  @Test
  void loopbackOriginIsAccepted() throws Exception {
    final Response response = post(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 1)
        .put("method", "ping").toString(), "http://localhost:" + getServer(0).getHttpServer().getPort());

    assertThat(response.status).isEqualTo(200);
    assertThat(new JSONObject(response.body).has("result")).isTrue();
  }

  @Test
  void configuredOriginIsAccepted() throws Exception {
    getServer(0).getMCPConfiguration().setAllowedOrigins(List.of("http://studio.example.com"));
    try {
      final Response response = post(new JSONObject()
          .put("jsonrpc", "2.0")
          .put("id", 1)
          .put("method", "ping").toString(), "http://studio.example.com");

      assertThat(response.status).isEqualTo(200);
    } finally {
      getServer(0).getMCPConfiguration().setAllowedOrigins(List.of());
    }
  }

  @Test
  void absentOriginIsAccepted() throws Exception {
    // Non-browser MCP clients send no Origin at all; they must keep working.
    final Response response = post(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 1)
        .put("method", "ping").toString(), null);

    assertThat(response.status).isEqualTo(200);
  }

  // ---------------------------------------------------------------- stdio transport

  @Test
  void stdioAcceptsBatchAndOmitsNotificationResponses() throws Exception {
    final String input = "[{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"},"
        + "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}]\n";

    final JSONArray responses = new JSONArray(runStdio(input).trim());
    assertThat(responses.length()).isEqualTo(1);
    assertThat(responses.getJSONObject(0).getInt("id")).isEqualTo(1);
    assertThat(responses.getJSONObject(0).has("result")).isTrue();
  }

  @Test
  void stdioWritesNothingForNotificationOnlyBatch() throws Exception {
    final String input = "[{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}]\n";
    assertThat(runStdio(input).trim()).isEmpty();
  }

  @Test
  void stdioWritesNothingForSingleNotification() throws Exception {
    final String input = "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\"}\n";
    assertThat(runStdio(input).trim()).isEmpty();
  }

  private String runStdio(final String input) throws Exception {
    final ServerSecurityUser user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (final PrintStream printStream = new PrintStream(out, true, StandardCharsets.UTF_8)) {
      new MCPStdioServer(getServer(0), getServer(0).getMCPConfiguration(), user,
          new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)), printStream).run();
    }
    return out.toString(StandardCharsets.UTF_8);
  }

  // ---------------------------------------------------------------- helpers

  private record Response(int status, String body) {
  }

  /**
   * Uses {@link HttpClient} rather than {@code HttpURLConnection}: the latter silently drops an {@code Origin}
   * request header, which is on its restricted list, so the Origin tests could never send one.
   */
  private Response post(final String payload, final String origin) throws Exception {
    final HttpRequest.Builder builder = HttpRequest.newBuilder(new URI(getMcpUrl()))
        .header("Authorization", getBasicAuth())
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8));
    if (origin != null)
      builder.header("Origin", origin);

    final HttpResponse<String> response = HttpClient.newHttpClient()
        .send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

    return new Response(response.statusCode(), response.body());
  }

  private static String getBasicAuth() {
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }
}
