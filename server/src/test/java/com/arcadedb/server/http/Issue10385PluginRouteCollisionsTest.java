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

import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.util.Methods;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.function.Consumer;

import static com.arcadedb.server.StaticBaseServerTest.allocateFreePorts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10385: {@link PluginRoutes} registers a path once per instance, and {@code HttpServer} builds one instance per
 * plugin, so two plugins on one path replaced each other's entry: the first plugin's route answered 405 naming the OTHER
 * plugin's method, with no log line, because the audit skipped every handler {@code PluginRoutes} produces. A plugin could
 * also shadow a core route, or take over the whole {@code /api/v1} prefix, the same way. The registrar owns the plugin
 * namespace now: a second plugin declaring ANOTHER method on a path merges into the same entry, and a claim on a name that
 * is taken - by another plugin, or by the server - is refused when the route is declared, which fails the server start.
 * <p>
 * Wired the way {@code HttpServer.setupRoutes()} wires its handler: the server's own mounts first, then one
 * {@link PluginRoutes} per plugin.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10385PluginRouteCollisionsTest {

  private static final HttpHandler UNUSED = exchange -> {
  };

  private final PluginPathHandler paths = serverPaths();
  private       Undertow          undertow;
  private       HttpClient        client;
  private       String            baseUrl;

  @AfterEach
  void stopListener() {
    if (undertow != null)
      undertow.stop();
    if (client != null)
      client.close();
  }

  @Test
  void twoPluginsDeclaringDifferentMethodsOnOnePathBothKeepTheirRoute() throws Exception {
    plugin("PluginA", routes -> routes.get("/prometheus", say("PLUGIN-A metrics")));
    plugin("PluginB", routes -> routes.post("/prometheus", say("PLUGIN-B ingest")));
    startListener();

    final HttpResponse<String> get = send("GET", "/prometheus");
    assertThat(get.statusCode()).as("the issue's eviction: plugin A's route answered 405 'accepts POST only'").isEqualTo(200);
    assertThat(get.body()).isEqualTo("PLUGIN-A metrics");

    final HttpResponse<String> post = send("POST", "/prometheus");
    assertThat(post.statusCode()).isEqualTo(200);
    assertThat(post.body()).isEqualTo("PLUGIN-B ingest");

    assertThat(send("DELETE", "/prometheus").headers().firstValue("Allow")).hasValue("GET, HEAD, POST");
  }

  @Test
  void twoPluginsDeclaringTheSameMethodOnOnePathIsRefusedNamingBoth() throws Exception {
    plugin("PluginA", routes -> routes.get("/prometheus", say("PLUGIN-A metrics")));

    assertThatThrownBy(() -> plugin("PluginB", routes -> routes.get("/prometheus", say("PLUGIN-B metrics"))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("PluginB")
        .hasMessageContaining("PluginA")
        .hasMessageContaining("GET /prometheus");

    // the first plugin's route is untouched by the refused claim
    startListener();
    assertThat(send("GET", "/prometheus").body()).isEqualTo("PLUGIN-A metrics");
  }

  @Test
  void aTrailingSlashNamesTheSamePathAsWithoutIt() throws Exception {
    // PathHandler keys a path without its trailing slash, and RaftHAPlugin declares its prefixes with one: the table has to as well
    plugin("PluginA", routes -> routes.prefix(Methods.DELETE, "/api/v1/cluster/peer/", say("PLUGIN-A delete")));
    plugin("PluginB", routes -> routes.prefix(Methods.POST, "/api/v1/cluster/peer", say("PLUGIN-B post")));
    startListener();

    assertThat(send("DELETE", "/api/v1/cluster/peer/node2").body()).isEqualTo("PLUGIN-A delete");
    assertThat(send("POST", "/api/v1/cluster/peer/node2").body()).isEqualTo("PLUGIN-B post");
    assertThat(send("GET", "/api/v1/cluster/peer/node2").headers().firstValue("Allow")).hasValue("DELETE, POST");
  }

  @Test
  void theSameMethodOnTheSamePathWithAndWithoutATrailingSlashIsRefused() {
    plugin("PluginA", routes -> routes.get("/shared/", UNUSED));

    assertThatThrownBy(() -> plugin("PluginB", routes -> routes.get("/shared", UNUSED)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("PluginA")
        .hasMessageContaining("PluginB")
        .hasMessageContaining("GET /shared");
  }

  @Test
  void aRejectionIsFoundWhateverTheTrailingSlashOfThePathItIsAskedFor() {
    plugin("PluginA", routes -> routes.prefix(Methods.POST, "/rpc/", UNUSED));

    plugin("PluginA", routes -> routes.onMethodNotAllowed("/rpc", exchange -> exchange.getResponseSender().send("custom")));
    plugin("PluginA", routes -> routes.onMethodNotAllowed("/rpc/", exchange -> exchange.getResponseSender().send("custom")));
  }

  @Test
  void aPluginDeclaringTheSameMethodTwiceIsRefused() {
    assertThatThrownBy(() -> plugin("PluginA", routes -> routes.get("/x", UNUSED).get("/x", UNUSED)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("PluginA")
        .hasMessageContaining("GET /x");
  }

  @Test
  void aPluginCannotShadowACoreRoute() {
    // the issue's second collision: an exact path wins over the /api/v1 prefix the core RoutingHandler is mounted under
    assertThatThrownBy(() -> plugin("PluginA", routes -> routes.get("/api/v1/server", UNUSED)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("PluginA")
        .hasMessageContaining("/api/v1/server");

    // a core route with a path parameter is shadowed by any path that fills it
    assertThatThrownBy(() -> plugin("PluginA", routes -> routes.post("/api/v1/query/mydb", UNUSED)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("/api/v1/query/{database}");

    // and a prefix that covers a core route takes every request to it
    assertThatThrownBy(() -> plugin("PluginA", routes -> routes.prefix(Methods.GET, "/api/v1/query/", UNUSED)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("/api/v1/query/{database}");
    assertThatThrownBy(() -> plugin("PluginA", routes -> routes.prefix(Methods.GET, "/api/v1/server", UNUSED)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("/api/v1/server");
  }

  @Test
  void theBoundariesOfWhatShadowsACoreRoute() throws Exception {
    // an exact path names the same path with a trailing slash: it is the core route
    assertThatThrownBy(() -> plugin("PluginA", routes -> routes.get("/api/v1/server/", UNUSED)))
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("/api/v1/server");

    // one segment more or fewer than the core route is another path
    plugin("PluginA", routes -> routes.get("/api/v1/server/extra", say("deeper")));
    plugin("PluginA", routes -> routes.get("/api/v1/serve", say("shorter")));
    // a prefix longer than the core route cannot take a request the core route answers
    plugin("PluginB", routes -> routes.prefix(Methods.GET, "/api/v1/server/extra/", say("prefix beyond the route")));
    // a prefix shorter than the core mount loses to the mount, which is the more specific of the two
    plugin("PluginC", routes -> routes.prefix(Methods.GET, "/api", say("prefix above the mount")));
    startListener();

    assertThat(send("GET", "/api/v1/server/extra").body()).isEqualTo("deeper");
    assertThat(send("GET", "/api/v1/serve").body()).isEqualTo("shorter");
    assertThat(send("GET", "/api/v1/server/extra/x").body()).isEqualTo("prefix beyond the route");
    assertThat(send("GET", "/api/other").body()).isEqualTo("prefix above the mount");
    assertThat(send("GET", "/api/v1/server").body()).as("the core route is still the core's").isEqualTo("CORE GET /api/v1/server");
    assertThat(send("GET", "/api/v1/databases").body()).as("so is a core route under the same mount")
        .isEqualTo("CORE GET /api/v1/databases");
  }

  @Test
  void aParameterAtTheMountBoundaryAndTheRootAreJudgedByTheSameRule() {
    // /api/v1/query/{database} is POST-only on the core router: a path that fills the parameter is the core route, a path that
    // stops before it is not, and a prefix that stops before it still covers it
    assertThatThrownBy(() -> plugin("PluginA", routes -> routes.post("/api/v1/query/anything", UNUSED)))
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("/api/v1/query/{database}");
    assertThatThrownBy(() -> plugin("PluginA", routes -> routes.prefix(Methods.POST, "/api/v1/query", UNUSED)))
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("/api/v1/query/{database}");
    plugin("PluginA", routes -> routes.post("/api/v1/query", UNUSED));

    // the root prefix is free until the server mounts Studio on it, and then it is the server's
    plugin("PluginB", routes -> routes.prefix(Methods.GET, "/", UNUSED));
    final PluginPathHandler withStudio = serverPaths();
    withStudio.addPrefixPath("/", UNUSED);
    withStudio.setRegisteringPlugin("PluginB");
    try {
      assertThatThrownBy(() -> new PluginRoutes(withStudio).prefix(Methods.GET, "/", UNUSED))
          .isInstanceOf(IllegalStateException.class).hasMessageContaining("the server");
    } finally {
      withStudio.setRegisteringPlugin(null);
    }
  }

  @Test
  void aRegistrationThePathHandlerRefusesLeavesNoClaimBehind() {
    paths.setRegisteringPlugin("PluginA");
    try {
      // PathHandler itself refuses a route with no handler
      assertThatThrownBy(() -> paths.addExactPath("/refused", null)).isInstanceOf(RuntimeException.class);
    } finally {
      paths.setRegisteringPlugin(null);
    }

    // the path is free: it was never registered, so another plugin is not told it is taken
    plugin("PluginB", routes -> routes.get("/refused", UNUSED));
  }

  @Test
  void aPathIsKeyedTheWayPathHandlerKeysIt() {
    assertThat(PluginPathHandler.normalize("/a/b")).isEqualTo("/a/b");
    assertThat(PluginPathHandler.normalize("/a/b/")).isEqualTo("/a/b");
    assertThat(PluginPathHandler.normalize("/a/b//")).isEqualTo("/a/b");
    assertThat(PluginPathHandler.normalize("a/b")).isEqualTo("/a/b");
    assertThat(PluginPathHandler.normalize("/")).isEqualTo("/");
    assertThat(PluginPathHandler.normalize("//")).isEqualTo("/");
  }

  @Test
  void aPluginCannotTakeOverTheApiPrefixOrAnotherServerMount() {
    // the issue's third collision: a prefix equal to the core mount REPLACES the core RoutingHandler
    assertThatThrownBy(() -> plugin("PluginC", routes -> routes.prefix(Methods.GET, "/api/v1", UNUSED)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("PluginC")
        .hasMessageContaining("/api/v1")
        .hasMessageContaining("the server");
    assertThatThrownBy(() -> plugin("PluginC", routes -> routes.prefix(Methods.GET, "/api/v1/", UNUSED)))
        .as("the trailing slash names the same mount")
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> plugin("PluginC", routes -> routes.prefix(Methods.GET, "/ws", UNUSED)))
        .isInstanceOf(IllegalStateException.class);
    // an exact claim on the mount's own path answers it instead of the mount
    assertThatThrownBy(() -> plugin("PluginC", routes -> routes.get("/ws", UNUSED)))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void theRoutesThePluginsShipAreNotInTheWay() throws Exception {
    // what RaftHAPlugin, MCPPlugin and PrometheusMetricsPlugin declare: exact and prefix paths next to, never over, a core route
    plugin("RaftHAPlugin", routes -> routes//
        .get("/api/v1/cluster", say("cluster"))//
        .prefix(Methods.GET, "/api/v1/ha/snapshot/", say("snapshot"))//
        .prefix(Methods.DELETE, "/api/v1/cluster/peer/", say("peer"))//
        .post("/api/v1/cluster/stepdown", say("stepdown")));
    plugin("MCPPlugin", routes -> routes.post("/api/v1/mcp", say("mcp")).get("/api/v1/mcp/config", say("config")));
    plugin("PrometheusMetricsPlugin", routes -> routes.get("/prometheus", say("metrics")));
    startListener();

    assertThat(send("GET", "/api/v1/cluster").body()).isEqualTo("cluster");
    assertThat(send("GET", "/api/v1/mcp/config").body()).isEqualTo("config");
    assertThat(send("GET", "/prometheus").body()).isEqualTo("metrics");
    assertThat(send("GET", "/api/v1/server").body()).as("the core route is still the core's").isEqualTo("CORE GET /api/v1/server");
  }

  @Test
  void aRawRegistrationCollidingWithAPluginRoutesPathIsRefused() {
    plugin("PluginA", routes -> routes.get("/shared", UNUSED));

    paths.setRegisteringPlugin("LegacyPlugin");
    try {
      assertThatThrownBy(() -> paths.addExactPath("/shared", UNUSED))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("LegacyPlugin")
          .hasMessageContaining("PluginA")
          .hasMessageContaining("/shared");
    } finally {
      paths.setRegisteringPlugin(null);
    }
  }

  @Test
  void aPluginRoutesPathCollidingWithARawRegistrationIsRefused() {
    paths.setRegisteringPlugin("LegacyPlugin");
    paths.addExactPath("/legacy", UNUSED);
    paths.addPrefixPath("/legacy/items", UNUSED);
    paths.setRegisteringPlugin(null);

    assertThatThrownBy(() -> plugin("PluginA", routes -> routes.get("/legacy", UNUSED)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("PluginA")
        .hasMessageContaining("LegacyPlugin");
    assertThatThrownBy(() -> plugin("PluginA", routes -> routes.prefix(Methods.GET, "/legacy/items/", UNUSED)))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void aRawRegistrationCannotShadowACoreRouteEither() {
    paths.setRegisteringPlugin("LegacyPlugin");
    try {
      assertThatThrownBy(() -> paths.addExactPath("/api/v1/databases", UNUSED)).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> paths.addPrefixPath("/api/v1", UNUSED)).isInstanceOf(IllegalStateException.class);
    } finally {
      paths.setRegisteringPlugin(null);
    }
  }

  @Test
  void aPluginCanOnlyReplaceTheRejectionOfARouteItDeclared() {
    plugin("PluginA", routes -> routes.post("/rpc", UNUSED));

    assertThatThrownBy(() -> plugin("PluginB", routes -> routes.onMethodNotAllowed("/rpc", UNUSED)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("PluginA");
    plugin("PluginA", routes -> routes.onMethodNotAllowed("/rpc", exchange -> exchange.getResponseSender().send("custom")));
  }

  @Test
  void theServersOwnRegistrationsAreNeverRefused() {
    // HttpServer registers /ws, /api/v1, /api/v1/ai and / itself, outside any plugin, and may register one twice on a restart
    // of the handler it builds: only a plugin's claim is judged
    paths.addPrefixPath("/ws", UNUSED);
    paths.addPrefixPath("/", UNUSED);
  }

  private PluginPathHandler serverPaths() {
    final PluginPathHandler serverPaths = new PluginPathHandler();
    serverPaths.addPrefixPath("/ws", UNUSED);
    serverPaths.addPrefixPath("/api/v1", exchange -> exchange.getResponseSender()
        .send("CORE " + exchange.getRequestMethod() + " " + exchange.getRequestPath()));
    serverPaths.reserveServerRoutes("/api/v1", List.of(//
        new RouteRecordingRoutingHandler.RouteDescriptor("GET", "/server"),//
        new RouteRecordingRoutingHandler.RouteDescriptor("GET", "/databases"),//
        new RouteRecordingRoutingHandler.RouteDescriptor("POST", "/query/{database}")));
    return serverPaths;
  }

  /** Declares a plugin's routes the way {@code HttpServer.setupRoutes()} does: one {@link PluginRoutes} per plugin. */
  private void plugin(final String name, final Consumer<PluginRoutes> registerAPI) {
    paths.setRegisteringPlugin(name);
    try {
      registerAPI.accept(new PluginRoutes(paths));
    } finally {
      paths.setRegisteringPlugin(null);
    }
  }

  private static HttpHandler say(final String body) {
    return exchange -> exchange.getResponseSender().send(body);
  }

  private void startListener() {
    final int port = allocateFreePorts(1)[0];
    undertow = Undertow.builder().addHttpListener(port, "127.0.0.1").setHandler(paths).build();
    undertow.start();
    baseUrl = "http://127.0.0.1:" + port;
    client = HttpClient.newHttpClient();
  }

  private HttpResponse<String> send(final String method, final String path) throws Exception {
    return client.send(HttpRequest.newBuilder(URI.create(baseUrl + path)).method(method, HttpRequest.BodyPublishers.noBody()).build(),
        HttpResponse.BodyHandlers.ofString());
  }
}
