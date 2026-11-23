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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.StaticBaseServerTest;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static com.arcadedb.server.StaticBaseServerTest.allocateFreePorts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How the routes a plugin declares through {@link PluginRoutes} are dispatched and policed: method enforcement,
 * HEAD from the GET entry, the registration API, and the refusal of colliding claims between plugins and the server.
 */
class HttpPluginRoutesTest {
  // Regression: #10157 a plugin route answered every HTTP method; other methods now get 405 + Allow
  /**
   * Issue #10157: a plugin route registered on the raw {@link PathHandler} answered every HTTP method, so a {@code GET}
   * ran a state-changing cluster operation declared as {@code POST}. {@link PluginRoutes} keys each route on its method,
   * and answers any other method 405 with an {@code Allow} header WITHOUT invoking the plugin's handler. Proven over a
   * real Undertow listener, the same router the server uses, rather than by calling the handler directly.
   */
  @Nested
  class MethodEnforcement {

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

  // Regression: #10296 plugins get PluginRoutes instead of the raw PathHandler
  /**
   * Issue #10296: {@code ServerPlugin.registerAPI(HttpServer, PathHandler)} handed every plugin the server's raw
   * {@code PathHandler}, on which a route answers every HTTP method. The server now calls
   * {@link ServerPlugin#registerAPI(HttpServer, PluginRoutes)} first, so a plugin written against it can only declare
   * method-restricted routes. The {@code PathHandler} overload was removed by issue #10310.
   */
  @Nested
  class RegisterApi extends BaseGraphServerTest {
    private static final HttpClient CLIENT = HttpClient.newHttpClient();

    @Override
    protected void onServerConfiguration(final ContextConfiguration config) {
      config.setValue(GlobalConfiguration.SERVER_PLUGINS,
          PluginRoutesTestPlugin.class.getName() + "," + ArrayPayloadTestPlugin.class.getName());
    }

    @Test
    void aRouteDeclaredThroughThePluginRoutesOverloadIsServedOnItsMethod() throws Exception {
      final HttpResponse<String> response = send("POST", PluginRoutesTestPlugin.PATH, "{}");

      assertThat(response.statusCode()).isEqualTo(200);
      assertThat(response.body()).isEqualTo("{\"ok\":true}");
    }

    @Test
    void anUndeclaredMethodOnThatRouteIsAnswered405WithAllow() throws Exception {
      for (final String method : new String[] { "GET", "PUT", "DELETE" }) {
        final HttpResponse<String> response = send(method, PluginRoutesTestPlugin.PATH, method.equals("GET") ? null : "{}");

        assertThat(response.statusCode()).as(method + " " + PluginRoutesTestPlugin.PATH).isEqualTo(405);
        assertThat(response.headers().firstValue("Allow")).as(method + " " + PluginRoutesTestPlugin.PATH).hasValue("POST");
      }
    }

    @Test
    void aPluginPortedToThePluginRoutesOverloadIsServed() throws Exception {
      final HttpResponse<String> response = send("POST", "/api/v1/test/array", "[{\"id\":1}]");

      assertThat(response.statusCode()).isEqualTo(200);
      assertThat(send("GET", "/api/v1/test/array", null).statusCode()).isEqualTo(405);
    }

    private HttpResponse<String> send(final String method, final String path, final String body) throws Exception {
      final HttpRequest request = HttpRequest.newBuilder(new URI(getServerHttpUrl(path)))
          .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
          .header("Content-Type", "application/json")
          .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
          .build();
      return CLIENT.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }
  }

  // Regression: #10310 a plugin still declaring the removed PathHandler overload is named, not silently routeless
  /**
   * Issue #10310: {@code ServerPlugin.registerAPI(HttpServer, PathHandler)} is gone. Nothing calls it any more, so the server
   * names a plugin that still declares it instead of starting it without its routes in silence.
   */
  @Nested
  class RemovedRegisterApiOverload {

    @Test
    void serverPluginNoLongerDeclaresThePathHandlerOverload() {
      assertThatThrownBy(() -> ServerPlugin.class.getMethod("registerAPI", HttpServer.class, PathHandler.class))
          .isInstanceOf(NoSuchMethodException.class);
      assertThat(ServerPlugin.class.getMethods()).filteredOn(m -> m.getName().equals("registerAPI")).hasSize(1);
    }

    @Test
    void aPluginDeclaringTheRemovedOverloadIsDetected() {
      assertThat(PluginPathHandler.declaresRemovedRegisterAPI(new LegacyPlugin())).isTrue();
    }

    @Test
    void anInheritedDeclarationIsDetected() {
      assertThat(PluginPathHandler.declaresRemovedRegisterAPI(new LegacyPlugin() {
      })).isTrue();
      assertThat(PluginPathHandler.declaresRemovedRegisterAPI(new LegacyInterfacePlugin())).isTrue();
    }

    @Test
    void aPluginOnThePluginRoutesOverloadOrWithNoRoutesIsNot() {
      assertThat(PluginPathHandler.declaresRemovedRegisterAPI(new PluginRoutesTestPlugin())).isFalse();
      final ServerPlugin noRoutes = () -> {
      };
      assertThat(PluginPathHandler.declaresRemovedRegisterAPI(noRoutes)).isFalse();
    }

    private static class LegacyPlugin implements ServerPlugin {
      @Override
      public void startService() {
        // NO-OP
      }

      public void registerAPI(final HttpServer httpServer, final PathHandler routes) {
        // NO-OP
      }
    }

    private interface LegacyInterface {
      default void registerAPI(final HttpServer httpServer, final PathHandler routes) {
        // NO-OP
      }
    }

    private static final class LegacyInterfacePlugin implements ServerPlugin, LegacyInterface {
      @Override
      public void startService() {
        // NO-OP
      }
    }
  }

  // Regression: #10384 a plugin GET route answered HEAD with 405
  /**
   * Issue #10384: a route a plugin declares as {@code GET} through {@link PluginRoutes} answered {@code HEAD} with 405, and the
   * {@code Allow} header it sent named {@code GET}: self-contradictory (RFC 9110 requires a server that supports GET on a
   * resource to support HEAD on it), and a regression, since before #10157 a plugin route matched on the path alone and
   * Undertow discarded the body of the HEAD answer. A container healthcheck such as {@code wget --spider} issues HEAD, so the
   * Prometheus scrape endpoint failed the probe the core routes were fixed to pass (#8133). The method-keyed dispatcher
   * answers HEAD from the GET entry, once, for every plugin.
   * <p>
   * Proven over a real Undertow listener wired the way {@code HttpServer.setupRoutes()} wires its {@code PathHandler}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AnswerHead {

    private final AtomicInteger scrapeRuns = new AtomicInteger();
    private final AtomicInteger headRuns   = new AtomicInteger();

    private Undertow   undertow;
    private String     baseUrl;
    private HttpClient client;

    @BeforeEach
    void startListener() {
      final PathHandler paths = new PathHandler();
      new PluginRoutes(paths)//
          .get("/scrape", exchange -> {
            scrapeRuns.incrementAndGet();
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
            exchange.getResponseSender().send("PLUGIN scrape");
          })//
          .post("/rpc", exchange -> exchange.getResponseSender().send("rpc ran"))//
          .get("/custom", exchange -> exchange.getResponseSender().send("GET body"))//
          .exact(Methods.HEAD, "/custom", exchange -> {
            headRuns.incrementAndGet();
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/x-head");
          });

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
    void aGetRouteAnswersHeadWithTheStatusAndHeadersOfGetAndNoBody() throws Exception {
      final HttpResponse<String> get = send("GET", "/scrape");
      final HttpResponse<String> head = send("HEAD", "/scrape");

      assertThat(get.statusCode()).isEqualTo(200);
      assertThat(get.body()).isEqualTo("PLUGIN scrape");

      assertThat(head.statusCode()).isEqualTo(200);
      assertThat(head.body()).isEmpty();
      assertThat(head.headers().firstValue("Content-Type")).isEqualTo(get.headers().firstValue("Content-Type"));
      assertThat(head.headers().firstValue("Allow")).as("a successful answer carries no Allow header").isEmpty();
      assertThat(scrapeRuns.get()).as("the one GET handler served both").isEqualTo(2);
    }

    @Test
    void headOnAPostOnlyRouteIsStill405AndAllowDoesNotPromiseIt() throws Exception {
      final HttpResponse<String> head = send("HEAD", "/rpc");

      assertThat(head.statusCode()).isEqualTo(405);
      assertThat(head.headers().firstValue("Allow")).hasValue("POST");
    }

    @Test
    void theAllowHeaderOfAGetRouteNamesHeadToo() throws Exception {
      final HttpResponse<String> post = send("POST", "/scrape");

      assertThat(post.statusCode()).isEqualTo(405);
      assertThat(post.headers().firstValue("Allow")).as("GET implies HEAD, so a 405 must not leave it out").hasValue("GET, HEAD");
      assertThat(scrapeRuns.get()).isZero();
    }

    @Test
    void aPluginThatDeclaresHeadItselfKeepsItsOwnHandler() throws Exception {
      final HttpResponse<String> head = send("HEAD", "/custom");

      assertThat(head.statusCode()).isEqualTo(200);
      assertThat(head.headers().firstValue("Content-Type")).hasValue("text/x-head");
      assertThat(headRuns.get()).isEqualTo(1);

      // HEAD was declared, so it is not repeated in the Allow header the 405 for another method carries
      assertThat(send("DELETE", "/custom").headers().firstValue("Allow")).hasValue("GET, HEAD");
    }

    @Test
    void theDerivedHeadIsAnsweredNotDeclared() {
      final RecordingPathHandler routes = new RecordingPathHandler();
      final HttpHandler handler = exchange -> {
      };
      new PluginRoutes(routes).get("/a", handler);

      // The spec-drift checks compare what a plugin DECLARED with what its OpenAPI paths say, and HEAD is derived, not declared
      assertThat(routes.getRegisteredOperations()).containsExactly("GET /a");
      assertThat(routes.getRegisteredHandlers().get("/a")).isSameAs(handler);
      assertThat(routes.getGetRoutesNotAnsweringHead()).isEmpty();
    }

    private HttpResponse<String> send(final String method, final String path) throws Exception {
      return client.send(HttpRequest.newBuilder(URI.create(baseUrl + path)).method(method, HttpRequest.BodyPublishers.noBody()).build(),
          HttpResponse.BodyHandlers.ofString());
    }
  }

  // Regression: #10385 two plugins on one path silently replaced each other's entry
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
  @Nested
  class RouteCollisions {

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
      // a prefix longer than the core route cannot take a request the core route answers (on a path of its own: an exact path of
      // another plugin on the same one would take the prefix's root away, issue #10532)
      plugin("PluginB", routes -> routes.prefix(Methods.GET, "/api/v1/server/beyond/", say("prefix beyond the route")));
      // a prefix shorter than the core mount loses to the mount, which is the more specific of the two
      plugin("PluginC", routes -> routes.prefix(Methods.GET, "/api", say("prefix above the mount")));
      startListener();

      assertThat(send("GET", "/api/v1/server/extra").body()).isEqualTo("deeper");
      assertThat(send("GET", "/api/v1/serve").body()).isEqualTo("shorter");
      assertThat(send("GET", "/api/v1/server/beyond/x").body()).isEqualTo("prefix beyond the route");
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

  // Regression: #10385 a server holding colliding plugin routes must not start
  /**
   * Issue #10385, end to end: a server that holds two plugins claiming one route, or a plugin claiming a core route or the
   * {@code /api/v1} mount, does not start. The claim used to succeed silently, and the loser of the race answered a 405
   * naming the other plugin's method.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ServerRefusesCollidingPlugins extends StaticBaseServerTest {

    private ArcadeDBServer server;

    @AfterEach
    void stopServer() {
      CollidingRoutesTestPlugin.mode = CollidingRoutesTestPlugin.Mode.SAME_ROUTE_AS_ANOTHER_PLUGIN;
      if (server != null)
        server.stop();
    }

    @Test
    void twoPluginsDeclaringTheSameRouteFailTheStartNamingBoth() {
      CollidingRoutesTestPlugin.mode = CollidingRoutesTestPlugin.Mode.SAME_ROUTE_AS_ANOTHER_PLUGIN;

      assertThatThrownBy(this::startWithBothPlugins).isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("CollidingRoutesTestPlugin")
          .hasMessageContaining("PluginRoutesTestPlugin")
          .hasMessageContaining("POST " + PluginRoutesTestPlugin.PATH);
    }

    @Test
    void aPluginShadowingACoreRouteFailsTheStart() {
      CollidingRoutesTestPlugin.mode = CollidingRoutesTestPlugin.Mode.SHADOW_A_CORE_ROUTE;

      assertThatThrownBy(this::startWithBothPlugins).isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("CollidingRoutesTestPlugin")
          .hasMessageContaining("GET /api/v1/server");
    }

    @Test
    void aPluginTakingOverTheApiPrefixFailsTheStart() {
      CollidingRoutesTestPlugin.mode = CollidingRoutesTestPlugin.Mode.TAKE_OVER_THE_API_PREFIX;

      assertThatThrownBy(this::startWithBothPlugins).isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("CollidingRoutesTestPlugin")
          .hasMessageContaining("/api/v1");
    }

    @Test
    void anotherMethodOnAnotherPluginsPathIsServedAlongsideIt() throws Exception {
      CollidingRoutesTestPlugin.mode = CollidingRoutesTestPlugin.Mode.DISTINCT_METHOD_ON_ANOTHER_PLUGINS_PATH;

      startWithBothPlugins();

      // PluginRoutesTestPlugin declared POST, CollidingRoutesTestPlugin declared GET on the same path: both answer
      final String url = getServerHttpUrl(server, PluginRoutesTestPlugin.PATH);
      try (final HttpClient client = HttpClient.newHttpClient()) {
        final HttpResponse<String> get = client.send(HttpRequest.newBuilder(URI.create(url))
            .GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(get.statusCode()).isEqualTo(200);
        assertThat(get.body()).isEqualTo("B");

        final HttpResponse<String> put = client.send(HttpRequest.newBuilder(URI.create(url))
            .PUT(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(put.statusCode()).isEqualTo(405);
        // in the order the plugins registered, whichever it was
        assertThat(put.headers().firstValue("Allow").orElseThrow().split(", ")).containsExactlyInAnyOrder("POST", "GET", "HEAD");
      }
    }

    private void startWithBothPlugins() {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_0");
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/databases");
      config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
      config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
      config.setValue(GlobalConfiguration.SERVER_PLUGINS,
          PluginRoutesTestPlugin.class.getName() + "," + CollidingRoutesTestPlugin.class.getName());

      server = new ArcadeDBServer(config);
      server.start();
    }
  }

  // Regression: #10532 an exact claim on another plugin's prefix root was not refused
  /**
   * Issue #10532: an exact path registered on a {@link PluginPathHandler} is matched before any prefix, so an exact claim on the
   * path of ANOTHER plugin's prefix takes that prefix's root away while it keeps answering everything under it - the half-working
   * state the registrar's refusals (#10385) exist to prevent. The refusal only read the server's mounts: it looked at the prefix
   * register for the server and never at another plugin, and a prefix claim never looked at the exact register at all.
   * <p>
   * One plugin declaring BOTH an exact path and a prefix on one path is a deliberate "the root here, everything below there" pair and
   * stays allowed. The collision test enumerates every cell of (first claim, second claim, same plugin or not) instead of the cases
   * somebody thought of, so a missing cell is a missing row.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ExactAndPrefixClaims {
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

    /** The kind of a claim: an exact path, or a prefix. */
    private enum Kind {
      EXACT, PREFIX
    }

    static Stream<Arguments> matrix() {
      final List<Arguments> cells = new ArrayList<>();
      for (final Kind first : Kind.values())
        for (final Kind second : Kind.values())
          for (final boolean samePlugin : new boolean[] { true, false })
            cells.add(Arguments.of(first, second, samePlugin));
      return cells.stream();
    }

    /**
     * Different methods on every claim, so that a refusal is about the exact-against-prefix precedence and not about a method taken
     * twice: two claims of one kind merge into one entry, as in #10385. Only a pair of different kinds held by different plugins is
     * refused.
     */
    @ParameterizedTest(name = "{0} then {1}, same plugin: {2}")
    @MethodSource("matrix")
    void aClaimIsRefusedOnlyWhenItTakesAnotherPluginsRootAway(final Kind first, final Kind second, final boolean samePlugin) {
      plugin("PluginA", routes -> claim(routes, first, Methods.GET, "/p", say("first")));

      final String secondOwner = samePlugin ? "PluginA" : "PluginB";
      final Runnable declaringTheSecond = () -> plugin(secondOwner, routes -> claim(routes, second, Methods.POST, "/p", say("second")));

      if (first != second && !samePlugin)
        assertThatThrownBy(declaringTheSecond::run).isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("PluginA")
            .hasMessageContaining("PluginB")
            .hasMessageContaining("/p");
      else
        declaringTheSecond.run();
    }

    @Test
    void aRefusedClaimLeavesTheFirstPluginsRouteUntouched() throws Exception {
      plugin("PluginA", routes -> routes.prefix(Methods.GET, "/p", say("PLUGIN-A prefix")));

      assertThatThrownBy(() -> plugin("PluginB", routes -> routes.get("/p", say("PLUGIN-B exact"))))
          .isInstanceOf(IllegalStateException.class);

      startListener();
      assertThat(send("GET", "/p").body()).as("the root of the prefix is still the prefix's").isEqualTo("PLUGIN-A prefix");
      assertThat(send("GET", "/p/sub").body()).isEqualTo("PLUGIN-A prefix");
    }

    /** The shipped path of the report: {@code RaftHAPlugin} declares {@code /api/v1/cluster/peer/} as a prefix, with a trailing slash. */
    @Test
    void anExactPathOnTheRaftPeerPrefixIsRefusedWhateverTheTrailingSlash() {
      plugin("RaftHAPlugin", routes -> routes.prefix(Methods.DELETE, "/api/v1/cluster/peer/", say("RAFT delete-peer")));

      assertThatThrownBy(() -> plugin("Other", routes -> routes.exact(Methods.DELETE, "/api/v1/cluster/peer", say("OTHER delete"))))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Other")
          .hasMessageContaining("RaftHAPlugin");
      assertThatThrownBy(() -> plugin("Other", routes -> routes.exact(Methods.GET, "/api/v1/cluster/peer/", say("OTHER get"))))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("RaftHAPlugin");
    }

    @Test
    void aPrefixOnAnotherPluginsExactPathIsRefused() {
      plugin("PluginA", routes -> routes.get("/p", say("PLUGIN-A exact")));

      assertThatThrownBy(() -> plugin("PluginB", routes -> routes.prefix(Methods.GET, "/p/", say("PLUGIN-B prefix"))))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("PluginB")
          .hasMessageContaining("PluginA")
          .hasMessageContaining("/p");
    }

    /**
     * A second plugin declaring another method on a prefix joins the entry the first one created (#10385): the entry has two owners,
     * and an exact path of the first plugin would answer the root of the second one's method. The claim keeps every owner.
     */
    @Test
    void aPrefixTwoPluginsShareCannotBeGivenAnExactPathByEitherOfThem() {
      plugin("PluginA", routes -> routes.prefix(Methods.GET, "/p", say("A prefix")));
      plugin("PluginB", routes -> routes.prefix(Methods.POST, "/p", say("B prefix")));

      assertThatThrownBy(() -> plugin("PluginA", routes -> routes.put("/p", say("A exact"))))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("PluginA")
          .hasMessageContaining("PluginB");
      assertThatThrownBy(() -> plugin("PluginB", routes -> routes.put("/p", say("B exact"))))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("PluginA");
    }

    @Test
    void anExactPathTwoPluginsShareCannotBeGivenAPrefixByEitherOfThem() {
      plugin("PluginA", routes -> routes.get("/p", say("A exact")));
      plugin("PluginB", routes -> routes.post("/p", say("B exact")));

      assertThatThrownBy(() -> plugin("PluginA", routes -> routes.prefix(Methods.PUT, "/p", say("A prefix"))))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("PluginB");
      assertThatThrownBy(() -> plugin("PluginB", routes -> routes.prefix(Methods.PUT, "/p", say("B prefix"))))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("PluginA");
    }

    @Test
    void aPluginThatOwnsBothKindsAloneKeepsThemAfterAnotherJoinsNeither() throws Exception {
      plugin("PluginA", routes -> routes.get("/p", say("root")).prefix(Methods.GET, "/p", say("below")));
      // a different path of another plugin changes nothing for /p
      plugin("PluginB", routes -> routes.get("/q", say("other")));
      plugin("PluginA", routes -> routes.post("/p", say("root post")).prefix(Methods.POST, "/p", say("below post")));
      startListener();

      assertThat(send("POST", "/p").body()).isEqualTo("root post");
      assertThat(send("POST", "/p/x").body()).isEqualTo("below post");
    }

    @Test
    void aRawRegistrationIsJudgedAgainstTheOtherKindToo() {
      paths.setRegisteringPlugin("LegacyPlugin");
      try {
        paths.addExactPath("/legacy", UNUSED);
      } finally {
        paths.setRegisteringPlugin(null);
      }

      assertThatThrownBy(() -> plugin("PluginA", routes -> routes.prefix(Methods.GET, "/legacy", say("prefix"))))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("LegacyPlugin");

      // the plugin that registered the exact path may add the prefix under it
      paths.setRegisteringPlugin("LegacyPlugin");
      try {
        paths.addPrefixPath("/legacy", UNUSED);
      } finally {
        paths.setRegisteringPlugin(null);
      }
    }

    /** The server's own exact path is held like any other: a plugin prefix on it would never answer its own root. */
    @Test
    void aPluginPrefixOnAnExactPathTheServerRegisteredIsRefused() {
      paths.addExactPath("/health-page", UNUSED);

      assertThatThrownBy(() -> plugin("PluginA", routes -> routes.prefix(Methods.GET, "/health-page", say("prefix"))))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("PluginA")
          .hasMessageContaining("the server");
      // and the mirror, which was refused before: an exact path on a prefix the server mounts
      assertThatThrownBy(() -> plugin("PluginA", routes -> routes.get("/ws", say("exact"))))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("the server");
    }

    @Test
    void onePluginDeclaringTheRootAndTheSubtreeKeepsBothAnswers() throws Exception {
      plugin("PluginA", routes -> routes.get("/p", say("root")).prefix(Methods.GET, "/p", say("below")));
      startListener();

      assertThat(send("GET", "/p").body()).isEqualTo("root");
      assertThat(send("GET", "/p/sub").body()).isEqualTo("below");
    }

    private static void claim(final PluginRoutes routes, final Kind kind, final HttpString method, final String path,
        final HttpHandler handler) {
      if (kind == Kind.EXACT)
        routes.exact(method, path, handler);
      else
        routes.prefix(method, path, handler);
    }

    private PluginPathHandler serverPaths() {
      final PluginPathHandler serverPaths = new PluginPathHandler();
      serverPaths.addPrefixPath("/ws", UNUSED);
      serverPaths.addPrefixPath("/api/v1", UNUSED);
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
}
