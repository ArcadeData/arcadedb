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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
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
 * Regression test for <a href="https://github.com/ArcadeData/arcadedb/issues/5415">issue #5415</a>: a request
 * body that is a top-level JSON array was never parsed - {@code AbstractServerHttpHandler} only ever attempted a
 * {@code JSONObject} parse - so the handler was invoked with a null payload and no way to reach the array.
 * <p>
 * The shared pipeline now parses either shape once. A handler that accepts arrays opts in with
 * {@code acceptsArrayPayload()} and reads the parsed array with {@code getPayloadAsArray(exchange)}; every other
 * route answers an explicit HTTP 400 instead of running with a silently null payload.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class HttpJsonArrayPayloadTest extends BaseGraphServerTest {

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_PLUGINS, ArrayPayloadTestPlugin.class.getName());
  }

  @Test
  void arrayBodyReachesAHandlerThatAcceptsIt() throws Exception {
    final JSONArray batch = new JSONArray()
        .put(new JSONObject().put("id", 1))
        .put(new JSONObject().put("id", 2));

    final Response response = post("/api/v1/test/array", batch.toString());

    assertThat(response.status()).isEqualTo(200);
    final JSONObject json = new JSONObject(response.body());
    assertThat(json.getString("shape")).isEqualTo("array");
    assertThat(json.getInt("size")).isEqualTo(2);
  }

  @Test
  void leadingWhitespaceDoesNotHideTheArray() throws Exception {
    final Response response = post("/api/v1/test/array", "\n  [{\"id\":3}]");

    assertThat(response.status()).isEqualTo(200);
    final JSONObject json = new JSONObject(response.body());
    assertThat(json.getString("shape")).isEqualTo("array");
    assertThat(json.getInt("size")).isEqualTo(1);
  }

  @Test
  void objectBodyIsUnaffectedOnAHandlerThatAcceptsArrays() throws Exception {
    final Response response = post("/api/v1/test/array", new JSONObject().put("id", 4).toString());

    assertThat(response.status()).isEqualTo(200);
    final JSONObject json = new JSONObject(response.body());
    assertThat(json.getString("shape")).isEqualTo("object");
    assertThat(json.getInt("id")).isEqualTo(4);
  }

  @Test
  void aBodyThatStartsAsAnArrayButDoesNotParseReachesTheHandlerEmpty() throws Exception {
    // The pipeline decides the body kind from the first character and then parses once. When that parse fails
    // it logs and gives up: no 400 is synthesised, and the handler is called with a null payload AND a null
    // array. Recovering from there is the handler's job - it still has the raw body - which is how the MCP
    // endpoint turns this same shape into a JSON-RPC parse error instead of mistaking it for an empty request.
    final Response response = post("/api/v1/test/array", "[{\"id\":5}");

    assertThat(response.status()).isEqualTo(200);
    final JSONObject json = new JSONObject(response.body());
    assertThat(json.getString("shape")).isEqualTo("object");
    assertThat(json.getInt("id")).isEqualTo(-1);
  }

  @Test
  void arrayBodyOnAnObjectOnlyEndpointIsRejectedWith400() throws Exception {
    // Before the fix this ran the handler with a null payload, which surfaced as the misleading
    // "Command text is null" with no hint that the body shape was the problem.
    final Response response = post("/api/v1/command/graph",
        new JSONArray().put(new JSONObject().put("language", "sql").put("command", "select 1")).toString());

    assertThat(response.status()).isEqualTo(400);
    final JSONObject json = new JSONObject(response.body());
    assertThat(json.getString("error")).isEqualTo("The request payload must be a JSON object");
    assertThat(json.getString("detail")).contains("top-level JSON array");
  }

  @Test
  void emptyArrayBodyOnAnObjectOnlyEndpointIsRejectedWith400() throws Exception {
    final Response response = post("/api/v1/command/graph", "[]");

    assertThat(response.status()).isEqualTo(400);
    assertThat(new JSONObject(response.body()).getString("error")).isEqualTo("The request payload must be a JSON object");
  }

  @Test
  void objectBodyOnAnObjectOnlyEndpointStillWorks() throws Exception {
    final Response response = post("/api/v1/command/graph",
        new JSONObject().put("language", "sql").put("command", "select 1 as one").toString());

    assertThat(response.status()).isEqualTo(200);
    assertThat(new JSONObject(response.body()).getJSONArray("result").getJSONObject(0).getInt("one")).isEqualTo(1);
  }

  private Response post(final String path, final String body) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder(
            new URI("http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + path))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
        .build();

    final HttpResponse<String> response = HttpClient.newHttpClient()
        .send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

    return new Response(response.statusCode(), response.body());
  }

  private record Response(int status, String body) {
  }
}
