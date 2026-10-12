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
import com.arcadedb.server.http.HttpSessionManager;
import com.arcadedb.server.http.RouteRecordingRoutingHandler.RouteDescriptor;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Issue #9849: every route of {@code arcadedb-server} is held to a verdict on whether it may block the Undertow IO thread, the
 * assertion {@code Issue7621BlockingHandlersWorkerThreadTest} has made for the {@code arcadedb-ha-raft} routes since issue #7621.
 * <p>
 * Until now the server's own routes were covered by four hand-maintained lists (issues #7684, #7722, #7785 and a by-name test in
 * the HA module) over 69 handler classes, the shape whose weakness that test names as the reason its first two sweeps missed
 * handlers. The inventory already existed - {@code HttpServer.setupRoutes()} registers through a recording router and
 * {@code OpenApiSpecGenerationIT} already holds the OpenAPI document against it - and the one thing missing was the handler
 * instance next to each route. With it, a route that carries no verdict fails the build, so adding one forces the decision.
 * <p>
 * Writing the verdicts is what found three real defects, each a handler that did blocking work on a selector shared by every
 * other connection on it: {@code POST /logout} (a synchronous fan-out to every peer, bounded by the proxy connect timeout, to
 * revoke the token cluster-wide), {@code GET /ai/config} (a call to the customer portal whenever the cached plan has expired, under
 * a {@code synchronized} that parks the next caller behind it) and the four {@code /ai/chats} verbs (the caller's chat files).
 * <p>
 * The verdict is the observable answer to three requests rather than "declares the override", so it can describe a handler whose
 * answer depends on the request: one that carries an {@code arcadedb-session-id} has to wait on the session's lock, and
 * {@code GET /query} also streams when the caller negotiates {@code application/x-ndjson}. What it reads is
 * {@code mustExecuteOnWorkerThread(exchange)}, the handler's own declaration. Two other triggers in {@code handleRequest} are
 * independent of it and are pinned elsewhere: waiting in the query admission gate (issue #9518), and Basic credentials (issue
 * #7785).
 * <p>
 * Routes a plugin registers on the outer {@code PathHandler} are not in this inventory; {@code arcadedb-ha-raft} asserts its own.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9849ServerRoutesIoThreadDispatchTest extends BaseGraphServerTest {

  /**
   * What a route may do with the IO thread, as the answer to a plain request, to one carrying a session id, and to one that
   * accepts {@code application/x-ndjson}.
   */
  private enum Dispatch {
    /** Answers from memory (or from state computed once and cached). Must NOT pay a worker hand-off on a request that is cheap. */
    NEVER(false, false, false),
    /** Every request blocks or reads its body, so every request is dispatched. */
    ALWAYS(true, true, true),
    /** Cheap on its own, but a request inside a session waits on the session's lock. */
    WITH_SESSION(false, true, false),
    /** As {@link #WITH_SESSION}, and the NDJSON encoding it negotiates writes blocking output. */
    WITH_SESSION_OR_NDJSON(false, true, true);

    private final boolean onAPlainRequest;
    private final boolean onASessionRequest;
    private final boolean onANdJsonRequest;

    Dispatch(final boolean onAPlainRequest, final boolean onASessionRequest, final boolean onANdJsonRequest) {
      this.onAPlainRequest = onAPlainRequest;
      this.onASessionRequest = onASessionRequest;
      this.onANdJsonRequest = onANdJsonRequest;
    }
  }

  /**
   * Every core route {@code HttpServer.setupRoutes()} registers, and what it may do with the IO thread. The test fails when this
   * map and the server disagree in either direction, so adding a route means classifying it.
   */
  private static final Map<String, Dispatch> EXPECTED_BY_ROUTE = new LinkedHashMap<>() {{
    // Reads the body, runs statements and commits: all of it blocks.
    put("POST /api/v1/batch/{database}", Dispatch.ALWAYS);
    put("POST /api/v1/begin/{database}", Dispatch.ALWAYS);
    put("POST /api/v1/command/{database}", Dispatch.ALWAYS);
    put("POST /api/v1/commit/{database}", Dispatch.ALWAYS);
    put("POST /api/v1/query/{database}", Dispatch.ALWAYS);
    // Ends the session: with no session id there is nothing to wait on, with one it takes the session's lock.
    put("POST /api/v1/rollback/{database}", Dispatch.WITH_SESSION);
    // Reads statements inside the session and negotiates the streaming encoding (issue #7306).
    put("GET /api/v1/query/{database}/{language}/{command}", Dispatch.WITH_SESSION_OR_NDJSON);
    put("POST /api/v1/vector/{database}/search", Dispatch.ALWAYS);
    put("POST /api/v1/vector/{database}/hybrid", Dispatch.ALWAYS);
    put("POST /api/v1/vector/{database}/fulltext", Dispatch.ALWAYS);

    // In-memory: the set of installed database names, one name looked up in it, the operations registry.
    put("GET /api/v1/databases", Dispatch.NEVER);
    put("GET /api/v1/exists/{database}", Dispatch.NEVER);
    put("GET /api/v1/progress/{database}", Dispatch.NEVER);
    // In-memory: the authentication-session map. (The PBKDF2 of a Basic credential is the separate trigger of issue #7785.)
    put("POST /api/v1/login", Dispatch.NEVER);
    put("GET /api/v1/sessions", Dispatch.NEVER);
    // Revokes the token on every peer of a cluster, a synchronous RPC bounded by the proxy connect timeout (issue #9849).
    put("POST /api/v1/logout", Dispatch.ALWAYS);
    // A peer asks whether this node holds a session it issued: a JSON body, which is only read on a worker thread.
    put("POST /api/v1/cluster/auth-session", Dispatch.ALWAYS);

    // Counters, gauges and the HA view the plugin keeps in memory.
    put("GET /api/v1/server", Dispatch.NEVER);
    // Administrative commands (create and drop a database, settings, backup, shutdown) with a JSON body.
    put("POST /api/v1/server", Dispatch.ALWAYS);

    // The probes must keep answering from the selector: a kubelet that waits for a worker is the failure they exist to detect.
    put("GET /api/v1/ready", Dispatch.NEVER);
    put("HEAD /api/v1/ready", Dispatch.NEVER);
    put("GET /api/v1/health", Dispatch.NEVER);
    put("HEAD /api/v1/health", Dispatch.NEVER);
    // The specification is generated once and cached; the documentation page is a constant.
    put("GET /api/v1/openapi.json", Dispatch.NEVER);
    put("GET /api/v1/docs", Dispatch.NEVER);

    // Reads of the loaded security configuration.
    put("GET /api/v1/server/api-tokens", Dispatch.NEVER);
    put("GET /api/v1/server/users", Dispatch.NEVER);
    put("GET /api/v1/server/groups", Dispatch.NEVER);
    // Writes go through the cluster-wide compare-and-set, a Raft submit-and-wait under HA (issues #7133, #7621).
    put("POST /api/v1/server/api-tokens", Dispatch.ALWAYS);
    put("DELETE /api/v1/server/api-tokens", Dispatch.ALWAYS);
    put("POST /api/v1/server/users", Dispatch.ALWAYS);
    put("PUT /api/v1/server/users", Dispatch.ALWAYS);
    put("DELETE /api/v1/server/users", Dispatch.ALWAYS);
    put("POST /api/v1/server/groups", Dispatch.ALWAYS);
    put("DELETE /api/v1/server/groups", Dispatch.ALWAYS);

    // Server-wide administration that spends the installation's credentials against the portal, a peer or the staging area.
    put("GET /api/v1/server/support", Dispatch.ALWAYS);
    put("POST /api/v1/server/support/register", Dispatch.ALWAYS);
    put("DELETE /api/v1/server/support/register", Dispatch.ALWAYS);
    put("POST /api/v1/server/support/installation", Dispatch.ALWAYS);
    put("POST /api/v1/server/support/connect", Dispatch.ALWAYS);
    put("GET /api/v1/server/support/connect", Dispatch.ALWAYS);
    put("DELETE /api/v1/server/support/connect", Dispatch.ALWAYS);
    put("GET /api/v1/server/support/peers", Dispatch.ALWAYS);
    put("POST /api/v1/server/support/peer-query", Dispatch.ALWAYS);
    put("POST /api/v1/server/support/preview", Dispatch.ALWAYS);
    put("POST /api/v1/server/support/bundle", Dispatch.ALWAYS);
    put("GET /api/v1/server/support/issues", Dispatch.ALWAYS);
    put("POST /api/v1/server/support/issues", Dispatch.ALWAYS);
    put("GET /api/v1/server/support/issues/{number}", Dispatch.ALWAYS);
    put("PUT /api/v1/server/support/issues/{number}", Dispatch.ALWAYS);
    put("POST /api/v1/server/support/issues/{number}/comments", Dispatch.ALWAYS);
    put("POST /api/v1/server/support/screenshots", Dispatch.ALWAYS);
    put("DELETE /api/v1/server/support/screenshots/{id}", Dispatch.ALWAYS);
    put("POST /api/v1/server/support/issues/{number}/requests/{requestId}/response", Dispatch.ALWAYS);
    put("POST /api/v1/server/support/issues/{number}/responses", Dispatch.ALWAYS);
    put("POST /api/v1/server/support/issues/{number}/attachments", Dispatch.ALWAYS);

    // Time series: writes and queries run in the database; the three reads below are cheap outside a session.
    put("POST /api/v1/ts/{database}/write", Dispatch.ALWAYS);
    put("POST /api/v1/ts/{database}/query", Dispatch.ALWAYS);
    put("GET /api/v1/ts/{database}/latest", Dispatch.WITH_SESSION);
    put("GET /api/v1/ts/{database}/grafana/health", Dispatch.WITH_SESSION);
    put("GET /api/v1/ts/{database}/grafana/metadata", Dispatch.WITH_SESSION);
    put("POST /api/v1/ts/{database}/grafana/query", Dispatch.ALWAYS);
    put("POST /api/v1/ts/{database}/prom/write", Dispatch.ALWAYS);
    put("POST /api/v1/ts/{database}/prom/read", Dispatch.ALWAYS);
    put("GET /api/v1/ts/{database}/prom/api/v1/query", Dispatch.ALWAYS);
    put("GET /api/v1/ts/{database}/prom/api/v1/query_range", Dispatch.ALWAYS);
    put("GET /api/v1/ts/{database}/prom/api/v1/labels", Dispatch.WITH_SESSION);
    put("GET /api/v1/ts/{database}/prom/api/v1/label/{name}/values", Dispatch.ALWAYS);
    put("GET /api/v1/ts/{database}/prom/api/v1/series", Dispatch.ALWAYS);

    // Whenever the cached plan has expired it asks the customer portal, over the network, under a lock (issue #9849).
    put("GET /api/v1/ai/config", Dispatch.ALWAYS);
    put("POST /api/v1/ai/activate", Dispatch.ALWAYS);
    put("POST /api/v1/ai/chat", Dispatch.ALWAYS);
    put("POST /api/v1/ai/chat/stream", Dispatch.ALWAYS);
    put("POST /api/v1/ai/analyze-profiler", Dispatch.ALWAYS);
    // The chat files of the caller: a directory listing, a read, an atomic write, a delete (issue #9849).
    put("GET /api/v1/ai/chats", Dispatch.ALWAYS);
    put("GET /api/v1/ai/chats/{id}", Dispatch.ALWAYS);
    put("PUT /api/v1/ai/chats/{id}", Dispatch.ALWAYS);
    put("DELETE /api/v1/ai/chats/{id}", Dispatch.ALWAYS);
  }};

  private static String keyOf(final RouteDescriptor route) {
    return route.method() + " " + route.path();
  }

  @Test
  void everyServerRouteIsClassifiedAndDispatchesAccordingly() {
    final Map<String, HttpHandler> registered = new LinkedHashMap<>();
    for (final Map.Entry<RouteDescriptor, HttpHandler> route : getServer(0).getHttpServer().getRegisteredRouteHandlers().entrySet())
      registered.put(keyOf(route.getKey()), route.getValue());

    assertThat(registered.keySet())
        .as("every route HttpServer.setupRoutes() installs needs a dispatch verdict here: a route that blocks on the Undertow IO "
            + "thread stalls every other connection on that selector, the kubelet probes included (issues #7133, #7621, #9849)")
        .containsExactlyInAnyOrderElementsOf(EXPECTED_BY_ROUTE.keySet());

    assertAll(registered.entrySet().stream().<Executable>map(route -> () -> {
      assertThat(route.getValue()).as("%s is not an AbstractServerHttpHandler, so its dispatch cannot be asked of it", route.getKey())
          .isInstanceOf(AbstractServerHttpHandler.class);
      final AbstractServerHttpHandler handler = (AbstractServerHttpHandler) route.getValue();
      final Dispatch expected = EXPECTED_BY_ROUTE.get(route.getKey());

      assertThat(handler.mustExecuteOnWorkerThread(plainRequest()))
          .as("%s (%s) on a plain request", route.getKey(), handler.getClass().getSimpleName()).isEqualTo(expected.onAPlainRequest);
      assertThat(handler.mustExecuteOnWorkerThread(sessionRequest()))
          .as("%s (%s) on a request inside a session", route.getKey(), handler.getClass().getSimpleName())
          .isEqualTo(expected.onASessionRequest);
      assertThat(handler.mustExecuteOnWorkerThread(ndJsonRequest()))
          .as("%s (%s) on a request accepting NDJSON", route.getKey(), handler.getClass().getSimpleName())
          .isEqualTo(expected.onANdJsonRequest);
    }).toArray(Executable[]::new));
  }

  /**
   * Keeps the sweep honest: if every route were classified one way the assertion above would pass while saying nothing, and the
   * cheap routes are the ones a careless "dispatch everything" would silently tax (the probes most of all).
   */
  @Test
  void theVerdictsAreNotAllTheSame() {
    assertThat(EXPECTED_BY_ROUTE.values()).contains(Dispatch.NEVER, Dispatch.ALWAYS, Dispatch.WITH_SESSION,
        Dispatch.WITH_SESSION_OR_NDJSON);
    assertThat(EXPECTED_BY_ROUTE.get("GET /api/v1/ready")).isEqualTo(Dispatch.NEVER);
    assertThat(EXPECTED_BY_ROUTE.get("GET /api/v1/health")).isEqualTo(Dispatch.NEVER);
  }

  private static HttpServerExchange plainRequest() {
    return new HttpServerExchange(null);
  }

  private static HttpServerExchange sessionRequest() {
    final HttpServerExchange exchange = new HttpServerExchange(null);
    exchange.getRequestHeaders().put(HttpString.tryFromString(HttpSessionManager.ARCADEDB_SESSION_ID), "AS-9849");
    return exchange;
  }

  private static HttpServerExchange ndJsonRequest() {
    final HttpServerExchange exchange = new HttpServerExchange(null);
    exchange.getRequestHeaders().put(Headers.ACCEPT, "application/x-ndjson");
    return exchange;
  }
}
