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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.PostBatchHandler.CountingInputStream;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression for issue #7508: a forward to the cluster leader used to build {@code "http://" + leaderAddress + path}
 * unconditionally, while every other peer-to-peer dial in the cluster already prefers the peer's HTTPS endpoint when
 * SSL is enabled. On an SSL cluster that put the relayed credentials - the client's own {@code Authorization} header,
 * or the cluster token - on the wire in cleartext, and on a cluster that declares {@code https} ports but no
 * {@code http} ones it made the forward refuse itself, because the derived HTTP address is then this node's own.
 * <p>
 * The dial decision is {@link LeaderDial}; these tests drive it directly and then through the two entry points in
 * this module that make it - {@link LeaderCommandForwarder} (the four {@code /api/v1/server*} routes) and
 * {@link PostBatchHandler#forwardBatchToLeader} ({@code POST /api/v1/batch/{database}}).
 */
class Issue7508LeaderForwardSchemeTest {

  private static final String LEADER_HTTP  = "leader.example.com:2480";
  private static final String LEADER_HTTPS = "leader.example.com:2490";

  // ---------------------------------------------------------------------------------------------------------------
  // The decision itself
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void theHttpsEndpointWinsWhenTheClusterHasOneForTheLeader() {
    final HttpClient plain = HttpClient.newHttpClient();
    final RecordingHttpClient tls = new RecordingHttpClient(200, "{}");

    final LeaderDial dial = LeaderDial.resolve(new StubHA(LEADER_HTTP, LEADER_HTTPS, tls), plain);

    assertThat(dial).isNotNull();
    assertThat(dial.https()).isTrue();
    assertThat(dial.address()).isEqualTo(LEADER_HTTPS);
    assertThat(dial.client()).isSameAs(tls);
    assertThat(dial.url("/api/v1/server")).isEqualTo("https://" + LEADER_HTTPS + "/api/v1/server");
  }

  @Test
  void theDialStaysOnPlainHttpWhenNoHttpsEndpointResolvesForTheLeader() {
    final HttpClient plain = HttpClient.newHttpClient();

    // What every non-SSL cluster answers, and what the HAServerPlugin interface defaults to.
    final LeaderDial dial = LeaderDial.resolve(new StubHA(LEADER_HTTP, null, null), plain);

    assertThat(dial).isNotNull();
    assertThat(dial.https()).isFalse();
    assertThat(dial.address()).isEqualTo(LEADER_HTTP);
    assertThat(dial.client()).isSameAs(plain);
    assertThat(dial.url("/api/v1/server/users?name=bob"))
        .isEqualTo("http://" + LEADER_HTTP + "/api/v1/server/users?name=bob");
  }

  @Test
  void theDialIsRefusedWhenTheClusterNamesAnHttpsEndpointButNoClientCanDialIt() {
    final HttpClient plain = HttpClient.newHttpClient();

    final LeaderDial dial = LeaderDial.resolve(new StubHA(LEADER_HTTP, LEADER_HTTPS, null), plain);

    // Fail closed, not open. A cluster that named an HTTPS endpoint for the leader has said where this forward
    // belongs; sending it to the plain listener instead would put the relayed Authorization header, the cluster
    // token and the body on the wire in clear, which is the very failure this class exists to end.
    assertThat(dial.refused()).isTrue();
    assertThat(dial.refusal()).contains(LEADER_HTTPS).contains("refused rather than forwarded in cleartext");
    assertThat(dial.address()).isNull();
    assertThat(dial.client()).isNull();
  }

  @Test
  void theDialIsRefusedWhenTheTrustMaterialCannotBeRead() {
    final HttpClient plain = HttpClient.newHttpClient();

    final LeaderDial dial = LeaderDial.resolve(new StubHA(LEADER_HTTP, LEADER_HTTPS, null) {
      @Override
      public HttpClient getPeerHttpsClient() throws IOException {
        throw new IOException("truststore.jks (No such file or directory)");
      }
    }, plain);

    // Not a reason to downgrade either: buildSSLContext already falls back to the JVM default truststore when none
    // is configured, so a failure here means the trust material itself could not be loaded.
    assertThat(dial.refused()).isTrue();
    assertThat(dial.refusal()).contains("truststore.jks");
  }

  @Test
  void aClusterThatNeverDeclaredItsHttpsPortsIsNotRefused() {
    // The other half of the rule: no HTTPS endpoint resolves at all, which is what every SSL cluster that left the
    // optional 5th field of arcadedb.ha.serverList out answers. Refusing there would break a cluster that works.
    final HttpClient plain = HttpClient.newHttpClient();

    final LeaderDial dial = LeaderDial.resolve(new StubHA(LEADER_HTTP, null, null), plain);

    assertThat(dial.refused()).isFalse();
    assertThat(dial.https()).isFalse();
    assertThat(dial.address()).isEqualTo(LEADER_HTTP);
  }

  @Test
  void thereIsNoDialWhenNoLeaderAddressIsKnownAtAll() {
    assertThat(LeaderDial.resolve(new StubHA(null, null, null), HttpClient.newHttpClient())).isNull();
    assertThat(LeaderDial.resolve(new StubHA("  ", null, null), HttpClient.newHttpClient())).isNull();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Entry point 1: LeaderCommandForwarder - POST /api/v1/server and POST/PUT/DELETE /api/v1/server/users
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void theServerCommandForwardDialsTheLeaderOverHttps() throws Exception {
    final RecordingHttpClient tls = new RecordingHttpClient(200, "{\"result\":\"ok\"}");
    final StubHA ha = new StubHA(LEADER_HTTP, LEADER_HTTPS, tls);

    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha));
    final HttpServerExchange exchange = exchangeFor("/api/v1/server/users", "name=bob");

    final ExecutionResponse response = forwarder.forwardIfReplica(exchange, user("root"),
        LeaderCommandForwarder.currentPathWithQuery(exchange), "{\"name\":\"bob\"}");

    assertThat(response).isNotNull();
    assertThat(response.getCode()).isEqualTo(200);
    assertThat(tls.lastUri.get())
        .as("the forward has to reach the leader's HTTPS listener, not its plaintext one")
        .hasToString("https://" + LEADER_HTTPS + "/api/v1/server/users?name=bob");
  }

  @Test
  void theServerCommandForwardKeepsDialingPlainHttpWhenTheClusterHasNoHttpsEndpoint() {
    // No HTTPS endpoint and the resolved HTTP address is this node's own: the self-address refusal (#6191) is
    // what proves the plain-HTTP branch was taken, without this test opening a socket.
    final StubHA ha = new StubHA(LEADER_HTTP, null, null);
    ha.ownHttpAddress = LEADER_HTTP;

    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha));
    final HttpServerExchange exchange = exchangeFor("/api/v1/server", null);

    assertThatThrownBy(() -> forwarder.forwardIfReplica(exchange, user("root"),
        LeaderCommandForwarder.currentPathWithQuery(exchange), "{}"))
        .isInstanceOf(ServerIsNotTheLeaderException.class)
        .hasMessageContaining("is this node's own");
  }

  @Test
  void theHttpSelfAddressCheckNoLongerRefusesAForwardThatTravelsOverHttps() throws Exception {
    // The second failure mode of #7508: a cluster that declares its 'https' ports and not its 'http' ones derives
    // every peer's HTTP address as THIS node's, so isOwnHttpAddress answers true for the leader and the forward
    // used to refuse itself - with a perfectly good HTTPS endpoint sitting unused.
    final RecordingHttpClient tls = new RecordingHttpClient(200, "{}");
    final StubHA ha = new StubHA(LEADER_HTTP, LEADER_HTTPS, tls);
    ha.ownHttpAddress = LEADER_HTTP;

    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha));
    final HttpServerExchange exchange = exchangeFor("/api/v1/server", null);

    final ExecutionResponse response = forwarder.forwardIfReplica(exchange, user("root"),
        LeaderCommandForwarder.currentPathWithQuery(exchange), "{}");

    assertThat(response).isNotNull();
    assertThat(response.getCode()).isEqualTo(200);
    assertThat(tls.lastUri.get()).hasToString("https://" + LEADER_HTTPS + "/api/v1/server");
  }

  @Test
  void theServerCommandForwardIsRefusedRatherThanDowngradedWhenTheHttpsEndpointCannotBeReached() {
    // The cluster named an HTTPS endpoint and no client can dial it. The forward relays the caller's own
    // Authorization header, so the plain listener is not an acceptable second choice.
    final StubHA ha = new StubHA(LEADER_HTTP, LEADER_HTTPS, null);

    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha));
    final HttpServerExchange exchange = exchangeFor("/api/v1/server", null);

    assertThatThrownBy(() -> forwarder.forwardIfReplica(exchange, user("root"),
        LeaderCommandForwarder.currentPathWithQuery(exchange), "{}"))
        .isInstanceOf(ServerIsNotTheLeaderException.class)
        .hasMessageContaining("refused rather than forwarded in cleartext");
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Entry point 2: PostBatchHandler - POST /api/v1/batch/{database}
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void theBatchForwardDialsTheLeaderOverHttps() throws Exception {
    final RecordingHttpClient tls = new RecordingHttpClient(200, "{\"records\":1}");
    final StubHA ha = new StubHA(LEADER_HTTP, LEADER_HTTPS, tls);

    final PostBatchHandler handler = new PostBatchHandler(httpServerWith(ha));
    final HttpServerExchange exchange = exchangeFor("/api/v1/batch/graph", "async=false");

    final ExecutionResponse response = handler.forwardBatchToLeader(exchange, ha, "graph", user("root"),
        "application/json", body("{\"@type\":\"vertex\"}\n"), false);

    assertThat(response.getCode()).isEqualTo(200);
    assertThat(tls.lastUri.get())
        .hasToString("https://" + LEADER_HTTPS + "/api/v1/batch/graph?async=false");
  }

  @Test
  void theBatchForwardKeepsDialingPlainHttpWhenTheClusterHasNoHttpsEndpoint() throws Exception {
    final StubHA ha = new StubHA(LEADER_HTTP, null, null);
    ha.ownHttpAddress = LEADER_HTTP;

    final PostBatchHandler handler = new PostBatchHandler(httpServerWith(ha));
    final HttpServerExchange exchange = exchangeFor("/api/v1/batch/graph", null);

    final ExecutionResponse response = handler.forwardBatchToLeader(exchange, ha, "graph", user("root"),
        "application/json", body("{\"@type\":\"vertex\"}\n"), false);

    // Same evidence as above: the plain branch is the only one the HTTP self-address check can refuse.
    assertThat(response.getCode()).isEqualTo(400);
    assertThat(response.getResponse()).contains("is this node's own");
  }

  @Test
  void theBatchForwardIsRefusedRatherThanDowngradedWhenTheHttpsEndpointCannotBeReached() throws Exception {
    final StubHA ha = new StubHA(LEADER_HTTP, LEADER_HTTPS, null);

    final PostBatchHandler handler = new PostBatchHandler(httpServerWith(ha));
    final HttpServerExchange exchange = exchangeFor("/api/v1/batch/graph", null);

    final ExecutionResponse response = handler.forwardBatchToLeader(exchange, ha, "graph", user("root"),
        "application/json", body("{\"@type\":\"vertex\"}\n"), false);

    assertThat(response.getCode()).isEqualTo(503);
    assertThat(response.getResponse()).contains("refused rather than forwarded in cleartext");
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Entry point 3: LeaderProxy - unreachable in production today (nothing constructs it, issue #7547), so only the
  // branch that needs no request body is driven here. The HTTPS dial itself reads the body through
  // exchange.startBlocking(), which a detached HttpServerExchange cannot serve.
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void theProxyStandsDownRatherThanRelayingInClearWhenTheHttpsEndpointCannotBeReached() {
    final StubHA ha = new StubHA(LEADER_HTTP, LEADER_HTTPS, null);

    final LeaderProxy proxy = new LeaderProxy(httpServerWith(ha));

    // Returning false hands the request back to the caller's own error path, which is what the proxy already did
    // for an unusable leader address - and it happens before the body is read, so no upload is buffered for a
    // request that was never going to be relayed.
    assertThat(proxy.tryProxy(new HttpServerExchange(null), LEADER_HTTP, user("root"))).isFalse();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------------------------------------------

  private static HttpServer httpServerWith(final HAServerPlugin ha) {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getHA()).thenReturn(ha);
    when(server.getConfiguration()).thenReturn(new ContextConfiguration());
    final HttpServer httpServer = mock(HttpServer.class);
    when(httpServer.getServer()).thenReturn(server);
    return httpServer;
  }

  private static HttpServerExchange exchangeFor(final String path, final String query) {
    final HttpServerExchange exchange = new HttpServerExchange(null);
    exchange.setRequestPath(path);
    exchange.setRequestURI(path);
    exchange.setRequestMethod(Methods.POST);
    if (query != null)
      exchange.setQueryString(query);
    exchange.getRequestHeaders().put(new HttpString("Authorization"), "Basic cm9vdDpwbGF5d2l0aGRhdGE=");
    return exchange;
  }

  private static CountingInputStream body(final String payload) {
    return new CountingInputStream(new HttpServerExchange(null),
        new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8)));
  }

  private static ServerSecurityUser user(final String name) {
    final ServerSecurityUser user = mock(ServerSecurityUser.class);
    when(user.getName()).thenReturn(name);
    return user;
  }

  /** An {@link HAServerPlugin} that answers only what the forward asks it. */
  private static class StubHA implements HAServerPlugin {
    private final String     httpAddress;
    private final String     httpsAddress;
    private final HttpClient httpsClient;
    private       String     ownHttpAddress;

    private StubHA(final String httpAddress, final String httpsAddress, final HttpClient httpsClient) {
      this.httpAddress = httpAddress;
      this.httpsAddress = httpsAddress;
      this.httpsClient = httpsClient;
    }

    @Override
    public String getLeaderAddress() {
      return httpAddress;
    }

    @Override
    public String getLeaderHttpsAddress() {
      return httpsAddress;
    }

    @Override
    public HttpClient getPeerHttpsClient() throws IOException {
      return httpsClient;
    }

    @Override
    public boolean isOwnHttpAddress(final String address) {
      return ownHttpAddress != null && ownHttpAddress.equals(address);
    }

    @Override
    public String getClusterToken() {
      return "cluster-token";
    }

    @Override
    public boolean isLeader() {
      return false;
    }

    @Override
    public ELECTION_STATUS getElectionStatus() {
      return ELECTION_STATUS.DONE;
    }

    @Override
    public String getLeaderName() {
      return "leader";
    }

    @Override
    public String getClusterName() {
      return "test";
    }

    @Override
    public Map<String, Object> getStats() {
      return Collections.emptyMap();
    }

    @Override
    public int getConfiguredServers() {
      return 3;
    }

    @Override
    public String getReplicaAddresses() {
      return "";
    }

    @Override
    public void shutdownRemoteServer(final String serverName) {
    }

    @Override
    public void disconnectCluster() {
    }

    @Override
    public void startService() {
    }
  }

  /** Records the URI the forward dialled and answers a canned response, so no socket is opened. */
  private static final class RecordingHttpClient extends HttpClient {
    private final AtomicReference<URI> lastUri = new AtomicReference<>();
    private final int                  status;
    private final String               payload;

    private RecordingHttpClient(final int status, final String payload) {
      this.status = status;
      this.payload = payload;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> HttpResponse<T> send(final HttpRequest request, final HttpResponse.BodyHandler<T> responseBodyHandler) {
      lastUri.set(request.uri());
      return (HttpResponse<T>) new CannedResponse(request, status, payload);
    }

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(final HttpRequest request,
        final HttpResponse.BodyHandler<T> responseBodyHandler) {
      return CompletableFuture.completedFuture(send(request, responseBodyHandler));
    }

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(final HttpRequest request,
        final HttpResponse.BodyHandler<T> responseBodyHandler, final HttpResponse.PushPromiseHandler<T> pushHandler) {
      return sendAsync(request, responseBodyHandler);
    }

    @Override
    public Optional<CookieHandler> cookieHandler() {
      return Optional.empty();
    }

    @Override
    public Optional<Duration> connectTimeout() {
      return Optional.empty();
    }

    @Override
    public Redirect followRedirects() {
      return Redirect.NEVER;
    }

    @Override
    public Optional<ProxySelector> proxy() {
      return Optional.empty();
    }

    @Override
    public SSLContext sslContext() {
      try {
        return SSLContext.getDefault();
      } catch (final Exception e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public SSLParameters sslParameters() {
      return new SSLParameters();
    }

    @Override
    public Optional<Authenticator> authenticator() {
      return Optional.empty();
    }

    @Override
    public Version version() {
      return Version.HTTP_1_1;
    }

    @Override
    public Optional<Executor> executor() {
      return Optional.empty();
    }
  }

  /** The smallest {@link HttpResponse} the forward paths read: a status code and a body. */
  private record CannedResponse(HttpRequest request, int status, String payload) implements HttpResponse<Object> {
    private static final BiPredicate<String, String> ALL = (name, value) -> true;

    @Override
    public int statusCode() {
      return status;
    }

    @Override
    public HttpRequest request() {
      return request;
    }

    @Override
    public Optional<HttpResponse<Object>> previousResponse() {
      return Optional.empty();
    }

    @Override
    public HttpHeaders headers() {
      return HttpHeaders.of(Map.of("Content-Type", List.of("application/json")), ALL);
    }

    @Override
    public Object body() {
      return payload;
    }

    @Override
    public Optional<SSLSession> sslSession() {
      return Optional.empty();
    }

    @Override
    public URI uri() {
      return request.uri();
    }

    @Override
    public HttpClient.Version version() {
      return HttpClient.Version.HTTP_1_1;
    }
  }
}
