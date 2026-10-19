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

import com.arcadedb.serializer.json.JSONObject;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.arcadedb.server.StaticBaseServerTest.allocateFreePorts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10157: a plugin route registered on the raw {@link PathHandler} answered every HTTP method, so a {@code GET}
 * ran a state-changing cluster operation declared as {@code POST}. {@link PluginRoutes} keys each route on its method,
 * and answers any other method 405 with an {@code Allow} header WITHOUT invoking the plugin's handler. Proven over a
 * real Undertow listener, the same router the server uses, rather than by calling the handler directly.
 */
class Issue10157PluginRoutesMethodEnforcementTest {

  private final AtomicInteger stepDownRuns   = new AtomicInteger();
  private final AtomicInteger deletePeerRuns = new AtomicInteger();
  private final AtomicInteger configRuns     = new AtomicInteger();

  private Undertow   undertow;
  private String     baseUrl;
  private HttpClient client;

  @BeforeEach
  void startListener() {
    final PathHandler paths = new PathHandler();
    final HttpHandler config = counting(configRuns, "config");
    new PluginRoutes(paths)//
        .post("/api/v1/cluster/stepdown", counting(stepDownRuns, "stepdown"))//
        .prefix(Methods.DELETE, "/api/v1/cluster/peer/", counting(deletePeerRuns, "delete-peer"))//
        .get("/api/v1/plugin/config", config)//
        .post("/api/v1/plugin/config", config);

    final int port = allocateFreePorts(1)[0];
    undertow = Undertow.builder().addHttpListener(port, "127.0.0.1").setHandler(paths).build();
    undertow.start();
    baseUrl = "http://127.0.0.1:" + port;
    client = HttpClient.newHttpClient();
  }

  @AfterEach
  void stopListener() {
    if (undertow != null)
      undertow.stop();
    if (client != null)
      client.close();
  }

  @Test
  void theDeclaredMethodReachesTheHandler() throws Exception {
    final HttpResponse<String> response = send("POST", "/api/v1/cluster/stepdown");

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).isEqualTo("stepdown ran");
    assertThat(stepDownRuns.get()).isEqualTo(1);
  }

  @Test
  void everyOtherMethodIs405WithoutRunningTheHandler() throws Exception {
    for (final String method : List.of("GET", "PUT", "DELETE", "PATCH", "OPTIONS")) {
      final HttpResponse<String> response = send(method, "/api/v1/cluster/stepdown");

      assertThat(response.statusCode()).as("%s on a POST route", method).isEqualTo(405);
      assertThat(response.headers().firstValue("Allow")).as("%s on a POST route", method).contains("POST");
      assertThat(new JSONObject(response.body()).getString("error")).isEqualTo("Method Not Allowed");
    }
    // HEAD carries no body, so only the status and the header are there to check
    final HttpResponse<String> head = send("HEAD", "/api/v1/cluster/stepdown");
    assertThat(head.statusCode()).isEqualTo(405);
    assertThat(head.headers().firstValue("Allow")).contains("POST");

    assertThat(stepDownRuns.get()).as("no undeclared method may reach the plugin's handler").isZero();
  }

  @Test
  void aPrefixRouteIsMethodKeyedToo() throws Exception {
    final HttpResponse<String> get = send("GET", "/api/v1/cluster/peer/node2");
    assertThat(get.statusCode()).as("the issue's GET that evicted a peer").isEqualTo(405);
    assertThat(get.headers().firstValue("Allow")).contains("DELETE");
    assertThat(deletePeerRuns.get()).isZero();

    final HttpResponse<String> delete = send("DELETE", "/api/v1/cluster/peer/node2");
    assertThat(delete.statusCode()).isEqualTo(200);
    assertThat(deletePeerRuns.get()).isEqualTo(1);
  }

  @Test
  void aPathWithTwoMethodsAllowsBothAndNamesBoth() throws Exception {
    assertThat(send("GET", "/api/v1/plugin/config").statusCode()).isEqualTo(200);
    assertThat(send("POST", "/api/v1/plugin/config").statusCode()).isEqualTo(200);
    assertThat(configRuns.get()).isEqualTo(2);

    final HttpResponse<String> delete = send("DELETE", "/api/v1/plugin/config");
    assertThat(delete.statusCode()).isEqualTo(405);
    // HEAD follows the GET it is answered from (issue #10384)
    assertThat(delete.headers().firstValue("Allow")).contains("GET, HEAD, POST");
    assertThat(configRuns.get()).isEqualTo(2);
  }

  @Test
  void aCustomRejectionWritesTheBodyAndStillAnswers405() throws Exception {
    final PathHandler paths = new PathHandler();
    final AtomicInteger runs = new AtomicInteger();
    new PluginRoutes(paths).post("/rpc", counting(runs, "rpc")).onMethodNotAllowed("/rpc", exchange -> {
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
      exchange.getResponseSender().send("{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32600}}");
    });
    final int port = allocateFreePorts(1)[0];
    final Undertow rpc = Undertow.builder().addHttpListener(port, "127.0.0.1").setHandler(paths).build();
    rpc.start();
    try {
      final HttpResponse<String> response = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/rpc"))
          .GET().build(), HttpResponse.BodyHandlers.ofString());

      assertThat(response.statusCode()).isEqualTo(405);
      assertThat(response.headers().firstValue("Allow")).contains("POST");
      assertThat(new JSONObject(response.body()).getString("jsonrpc")).isEqualTo("2.0");
      assertThat(runs.get()).isZero();
    } finally {
      rpc.stop();
    }
  }

  @Test
  void aRejectionNeedsADeclaredPath() {
    assertThatThrownBy(() -> new PluginRoutes(new PathHandler()).onMethodNotAllowed("/nowhere", exchange -> {
    })).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void aRouteNeedsAMethodAPathAndAHandler() {
    final PluginRoutes routes = new PluginRoutes(new PathHandler());
    final HttpHandler handler = exchange -> {
    };
    assertThatThrownBy(() -> routes.exact(null, "/a", handler)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> routes.post(null, handler)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> routes.prefix(Methods.DELETE, "/b/", null)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new PluginRoutes(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void theRecordingHandlerReportsTheMethodDimension() {
    final RecordingPathHandler routes = new RecordingPathHandler();
    final HttpHandler handler = exchange -> {
    };
    new PluginRoutes(routes).post("/a", handler).get("/a", handler).prefix(Methods.DELETE, "/b/", handler);
    routes.addExactPath("/raw", handler);

    assertThat(routes.getRegisteredOperations()).containsExactlyInAnyOrder("POST /a", "GET /a", "DELETE /b/", "* /raw");
    assertThat(routes.getRegisteredHandlers().get("/a"))
        .as("the plugin's handler, not the method restriction in front of it").isSameAs(handler);
  }

  private HttpResponse<String> send(final String method, final String path) throws Exception {
    return client.send(HttpRequest.newBuilder(URI.create(baseUrl + path)).method(method, HttpRequest.BodyPublishers.noBody()).build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private static HttpHandler counting(final AtomicInteger runs, final String name) {
    return exchange -> {
      runs.incrementAndGet();
      exchange.getResponseSender().send(name + " ran");
    };
  }
}
