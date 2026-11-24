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
import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.http.HttpAuthSessionManager;
import com.arcadedb.server.http.RouteRecordingRoutingHandler.RouteDescriptor;
import com.arcadedb.server.http.ScriptedAuthSessionPeer;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9864: a request carrying a session token another node of the cluster issued
 * ({@code Authorization: Bearer AU-<issuer>-<uuid>}) made the Undertow IO thread wait on a network call to that node.
 * {@code handleRequest} authenticates on the IO thread for every handler that does not dispatch of its own accord -
 * the probes, {@code GET /databases}, {@code GET /server} - and its only dispatch for the sake of authentication
 * looked at the header alone and recognised Basic credentials (issue #7785). A session token this node has never
 * seen, or holds a copy of whose lease is due, is confirmed with its issuer (issue #7424): one synchronous HTTP call
 * bounded only by the peer connect timeout, during which the selector serves no other connection. A made-up
 * {@code AU-<peer>-...} token is enough to cause it.
 * <p>
 * The assertion is the THREAD the issuer is asked on, through a running server and a route that never dispatches,
 * not a latency (the reasoning of issue #7785's test). The counter-cases keep it meaningful: everything this node can
 * answer alone still stays on the IO thread, so the fix is not "dispatch every Bearer request".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9864ClusterSessionTokenOffIoThreadTest extends BaseGraphServerTest {
  private static final String     PEER = "issue9864-peer";
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  private final ScriptedAuthSessionPeer peer = new ScriptedAuthSessionPeer();

  @BeforeEach
  void installHA() {
    getServer(0).setHA(peer);
  }

  @AfterEach
  void removeHA() {
    getServer(0).setHA(null);
  }

  @Test
  void aTokenThisNodeHasNeverSeenIsConfirmedWithItsIssuerOffTheIoThread() throws Exception {
    final String token = tokenIssuedBy(PEER);
    peer.vouchesFor("root");
    try {
      assertThat(theRouteThatNeverDispatches().sessionTokenNeedsWorkerThread(requestWith("Bearer " + token))).isTrue();

      assertThat(getDatabases(token).statusCode()).isEqualTo(200);

      assertThat(peer.lookups()).as("the issuer the token names was asked, once").hasSize(1);
      assertThat(peer.lookups().getFirst().issuer()).isEqualTo(PEER);
      assertThat(peer.lookups().getFirst().onIoThread())
          .as("the call to the issuing node ran on %s: on an IO thread it stalls every connection of that selector",
              peer.lookups().getFirst().thread())
          .isFalse();

      // The copy is now held here and inside its lease: the next request is a hash-map lookup again.
      assertThat(theRouteThatNeverDispatches().sessionTokenNeedsWorkerThread(requestWith("Bearer " + token))).isFalse();
      assertThat(getDatabases(token).statusCode()).isEqualTo(200);
      assertThat(peer.lookups()).hasSize(1);
    } finally {
      sessions().removeSession(token);
    }
  }

  /**
   * The unauthenticated shape: nobody issued the token. It is refused, off the IO thread, and remembered, so repeating
   * it neither dials again nor costs a hand-off.
   */
  @Test
  void aMadeUpTokenIsRefusedOffTheIoThreadAndThenAnsweredFromTheRefusalCache() throws Exception {
    final String token = tokenIssuedBy(PEER);
    peer.refuses();

    assertThat(getDatabases(token).statusCode()).isEqualTo(401);

    assertThat(peer.lookups()).hasSize(1);
    assertThat(peer.lookups().getFirst().onIoThread())
        .as("the call to the issuing node ran on %s", peer.lookups().getFirst().thread()).isFalse();

    assertThat(theRouteThatNeverDispatches().sessionTokenNeedsWorkerThread(requestWith("Bearer " + token)))
        .as("a refusal a peer gave a moment ago is answered from memory").isFalse();
    assertThat(getDatabases(token).statusCode()).isEqualTo(401);
    assertThat(peer.lookups()).as("the repeat did not reach the peer").hasSize(1);
  }

  @Test
  void everythingThisNodeAnswersAloneStaysOnTheIoThread() {
    final AbstractServerHttpHandler handler = theRouteThatNeverDispatches();
    final HttpAuthSession local = sessions().createSession(getServer(0).getSecurity().getUser("root"));
    try {
      assertThat(handler.sessionTokenNeedsWorkerThread(new HttpServerExchange(null)))
          .as("no credentials at all: the probes").isFalse();
      assertThat(handler.sessionTokenNeedsWorkerThread(requestWith("Basic " + Base64.getEncoder()
          .encodeToString("root:whatever".getBytes(StandardCharsets.UTF_8)))))
          .as("Basic credentials are the trigger of issue #7785, not this one").isFalse();
      assertThat(handler.sessionTokenNeedsWorkerThread(requestWith("Bearer at-0123456789abcdef")))
          .as("an API token is validated here, even one a peer's name could be read into").isFalse();
      assertThat(handler.sessionTokenNeedsWorkerThread(requestWith("Bearer at-" + PEER + "-" + UUID.randomUUID())))
          .as("an API token is never taken for a session token").isFalse();
      assertThat(handler.sessionTokenNeedsWorkerThread(requestWith("Bearer " + local.token)))
          .as("a session this node issued and holds").isFalse();
      assertThat(handler.sessionTokenNeedsWorkerThread(requestWith("Bearer " + tokenIssuedBy(sessions().getIssuerName()))))
          .as("a token naming this node that it does not hold has expired: nobody to ask").isFalse();
      assertThat(handler.sessionTokenNeedsWorkerThread(requestWith("Bearer AU-" + UUID.randomUUID())))
          .as("a legacy token names no issuer").isFalse();

      assertThat(handler.sessionTokenNeedsWorkerThread(requestWith("Bearer " + tokenIssuedBy(PEER))))
          .as("the control: the same handler does hand off a token a peer has to confirm").isTrue();

      getServer(0).setHA(null);
      assertThat(handler.sessionTokenNeedsWorkerThread(requestWith("Bearer " + tokenIssuedBy(PEER))))
          .as("a server that is not clustered has no peer to ask").isFalse();

      assertThat(peer.lookups()).as("deciding never dials").isEmpty();
    } finally {
      sessions().removeSession(local.token);
    }
  }

  private HttpAuthSessionManager sessions() {
    return getServer(0).getHttpServer().getAuthSessionManager();
  }

  /**
   * {@code GET /api/v1/databases}, checked to still be a route that does not dispatch on its own account: if it did,
   * the issuer would be asked off the IO thread whatever the authentication decided, and this test would say nothing.
   */
  private AbstractServerHttpHandler theRouteThatNeverDispatches() {
    for (final Map.Entry<RouteDescriptor, HttpHandler> route : getServer(0).getHttpServer().getRegisteredRouteHandlers().entrySet())
      if ("GET".equals(route.getKey().method()) && "/api/v1/databases".equals(route.getKey().path())) {
        final AbstractServerHttpHandler handler = (AbstractServerHttpHandler) route.getValue();
        assertThat(handler.mustExecuteOnWorkerThread(new HttpServerExchange(null)) || handler.mayWaitForAdmission())
            .as("GET /api/v1/databases now dispatches by itself: pick another route that authenticates on the IO thread")
            .isFalse();
        return handler;
      }
    throw new AssertionError("GET /api/v1/databases is no longer registered: pick another route that never dispatches");
  }

  private HttpResponse<String> getDatabases(final String token) throws Exception {
    return HTTP.send(HttpRequest.newBuilder().uri(URI.create(getServerHttpUrl("/api/v1/databases")))
        .header("Authorization", "Bearer " + token).GET().build(), HttpResponse.BodyHandlers.ofString());
  }

  private static String tokenIssuedBy(final String issuer) {
    return "AU-" + issuer + "-" + UUID.randomUUID();
  }

  private static HttpServerExchange requestWith(final String authorization) {
    final HttpServerExchange exchange = new HttpServerExchange(null);
    exchange.getRequestHeaders().put(Headers.AUTHORIZATION, authorization);
    return exchange;
  }
}
