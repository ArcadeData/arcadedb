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
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ForwardedRequestIdContext;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.LeaderForwardContext;
import com.arcadedb.server.PeerRequest;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.UnstartedHttpServers;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.IdempotencyCache;
import com.arcadedb.server.http.handler.PostBatchHandler.CountingInputStream;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.StallAwareStopwatch;
import com.sun.net.httpserver.HttpExchange;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Where and how a follower forwards to the leader: the scheme, what crosses the hop, which routes forward whole, the peer credential, and the ports a node advertises.
 * <p>
 * One {@code @Nested} class per regression, each keeping the fixture and the Javadoc of the issue it pins.
 */
class LeaderForwardRoutingTest {

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
  @Nested
  class LeaderForwardScheme {
    private static final String LEADER_HTTP  = "leader.example.com:2480";
    private static final String LEADER_HTTPS = "leader.example.com:2490";

    @RegisterExtension
    static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

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
    // Entry point 3 used to be LeaderProxy, and it never ran: nothing constructed the class, so the branch driven
    // here described no request any server had served. It was deleted with issue #7528, and with it the third arm of
    // this test. The two entry points above are the whole of the follower-to-leader HTTP relay; the SQL write forward
    // in RaftReplicatedDatabase is the third and is covered by PeerEndpointIdentityTest.LeaderHttpsEndpoint in ha-raft.
    // ---------------------------------------------------------------------------------------------------------------

    // ---------------------------------------------------------------------------------------------------------------
    // Fixtures
    // ---------------------------------------------------------------------------------------------------------------

    private static HttpServer httpServerWith(final HAServerPlugin ha) {
      final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, new ContextConfiguration());
      server.setHA(ha);
      final HttpServer httpServer = HTTP_SERVERS.of(server);
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
      return TestServerHelper.securityUser(name);
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

  /**
   * Issue #7528 (with #7547 and #7551, which report the same thing): {@code LeaderProxy} was a complete
   * follower-to-leader HTTP proxy - body capping, hop-by-hop header stripping, cluster-token injection, response
   * relay - that <b>nothing ever constructed</b>. Its Javadoc said it was "invoked from
   * {@code AbstractServerHttpHandler}"; no {@code new LeaderProxy} and no {@code tryProxy} call existed anywhere
   * under {@code src/main}. Task 9 of the Ratis port, which was to wire it in, never landed after Task 8 built it.
   * <p>
   * It was deleted rather than revived. Everything it was for already runs, in code that has the properties it
   * lacked: {@link LeaderCommandForwarder} relays the administrative routes, resolving the cluster token through
   * {@code HAServerPlugin.effectiveClusterToken} and setting the one-hop marker, while
   * {@code RaftReplicatedDatabase.forwardCommandToLeaderViaRaft} carries SQL and DDL writes. {@code LeaderProxy}
   * read the RAW {@code arcadedb.ha.clusterToken} setting, which is blank on every cluster that did not declare one
   * explicitly, so had it ever been wired in it would have refused every request on a default-configured cluster.
   * <p>
   * What this test pins is the removal, because the failure it prevents is one a green suite cannot show: a class
   * nothing calls has no failing test, and a fix applied to it - #7508 fixed its {@code http://} scheme - is a
   * no-op that reads as done.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LeaderProxyRemoved {

    @Test
    void theUnwiredProxyIsNotOnTheClasspath() {
      assertThatCode(() -> Class.forName("com.arcadedb.server.http.handler.LeaderProxy"))
          .as("reviving LeaderProxy means revisiting issue #7528, not restoring the class as it was")
          .isInstanceOf(ClassNotFoundException.class);
    }

    /**
     * {@code arcadedb.ha.proxyMaxBodySize} went with it: the proxy's constructor was its only reader, so the
     * setting was documented, tunable and inert. Its two siblings stay, because they DO have live readers -
     * {@link LeaderCommandForwarder}, {@code RaftReplicatedDatabase}, {@code PostBatchHandler} and
     * {@code TrustedHttpClientCache}.
     */
    @Test
    void theSettingThatOnlyTheProxyReadIsGoneAndItsLiveSiblingsAreNot() {
      assertThat(GlobalConfiguration.findByKey("arcadedb.ha.proxyMaxBodySize")).isNull();

      assertThat(GlobalConfiguration.findByKey("arcadedb.ha.proxyReadTimeout"))
          .isEqualTo(GlobalConfiguration.HA_PROXY_READ_TIMEOUT);
      assertThat(GlobalConfiguration.findByKey("arcadedb.ha.proxyConnectTimeout"))
          .isEqualTo(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT);
    }

    /**
     * Removing a declared setting must not break a server whose configuration file still names it. An unknown key
     * is dropped with a warning ({@code ContextConfiguration.fromJSON}), so an operator upgrading with the old line
     * in place starts normally instead of failing at parse time.
     */
    @Test
    void aConfigurationFileStillNamingTheRemovedSettingLoads() {
      final ContextConfiguration configuration = new ContextConfiguration();

      assertThatCode(() -> configuration.fromJSON(
          "{\"configuration\":{\"ha.proxyMaxBodySize\":1048576,\"ha.proxyReadTimeout\":12345}}"))
          .doesNotThrowAnyException();

      assertThat(configuration.getValueAsLong(GlobalConfiguration.HA_PROXY_READ_TIMEOUT))
          .as("the surviving settings in the same file are still applied").isEqualTo(12345L);
    }

    /**
     * The Javadoc of {@link com.arcadedb.server.LeaderForwardContext} used to name the proxy as one of the places
     * the one-hop rule is enforced, which is the sentence a reader of that design consults. The marker itself is
     * what the surviving paths set, so assert it is still the contract they share.
     */
    @Test
    void theOneHopMarkerIsUnchanged() {
      assertThat(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER).isEqualTo("X-ArcadeDB-Forwarded-To-Leader");
    }
  }

  /**
   * Issue #7603: three gaps in {@link LeaderCommandForwarder}, all on what a forward carries across the hop and what
   * it tells the caller when the hop does not work out.
   * <ol>
   *   <li>{@code X-Request-Id} was not relayed, so the leader ran a forwarded command outside its idempotency cache
   *   while the follower promised the client it was inside one;</li>
   *   <li>{@code Accept} was not relayed and the answer was read with {@code BodyHandlers.ofString()}, so a restore's
   *   progress stream reached a client that asked a follower for it as one buffered object at the end;</li>
   *   <li>the one-hop refusal answered a routine leadership change with the non-retryable 400 meant for a
   *   misconfigured {@code arcadedb.ha.serverList}, and used up the warning latch that misconfiguration gets.</li>
   * </ol>
   * The leader is a real HTTP listener on the loopback interface, so the headers asserted are the ones that reached a
   * socket, and the stream is observed arriving event by event.
   */
  @Nested
  class LeaderForwardHop {
    private static final String LOCAL_PEER  = "peer-follower";
    private static final String LEADER_PEER = "peer-leader";

    @RegisterExtension
    static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

    private StubLeader leader;

    @BeforeEach
    void startLeader() throws IOException {
      leader = new StubLeader();
    }

    @AfterEach
    void stopLeader() {
      leader.close();
      LeaderForwardContext.clear();
      ForwardedRequestIdContext.clear();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // 1. X-Request-Id
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void theRequestIdTravelsToTheLeaderSoItsIdempotencyCacheSeesTheCommand() throws Exception {
      leader.answerJson(200, "{\"result\":\"ok\"}");
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));
      final HttpServerExchange exchange = exchange("/api/v1/server");
      exchange.getRequestHeaders().put(new HttpString(IdempotencyCache.HEADER_REQUEST_ID), "issue7603-request-abc");

      final ExecutionResponse response = forwarder.forwardIfReplica(exchange, user("root"), "/api/v1/server",
          "{\"command\":\"create database Foo\"}", false);

      assertThat(response.getCode()).isEqualTo(200);
      assertThat(leader.header(IdempotencyCache.HEADER_REQUEST_ID))
          .as("the leader's idempotency gate keys on this header: without it the command runs there uncached")
          .isEqualTo("issue7603-request-abc");
    }

    @Test
    void aForwardWithNoRequestIdSendsNone() throws Exception {
      leader.answerJson(200, "{\"result\":\"ok\"}");
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));

      forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"), "/api/v1/server", "{}", false);

      assertThat(leader.header(IdempotencyCache.HEADER_REQUEST_ID)).isNull();
    }

    /**
     * Issue #8347: the body relayed is the payload re-serialized, not the client's bytes, so the leader keys the forward
     * differently from a retry the client sends it directly. The key the client's request has on this node travels too,
     * and the leader claims it beside the forward's own.
     */
    @Test
    void theClientsKeyTravelsToTheLeaderSoADirectRetryFindsTheForward() throws Exception {
      leader.answerJson(200, "{\"result\":\"ok\"}");
      final String key = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));
      final HttpServerExchange exchange = exchange("/api/v1/server");
      exchange.getRequestHeaders().put(new HttpString(IdempotencyCache.HEADER_REQUEST_ID), "issue8347-request");
      ForwardedRequestIdContext.set("issue8347-request", key, false);

      forwarder.forwardIfReplica(exchange, user("root"), "/api/v1/server", "{\"command\":\"create database Foo\"}", false);

      assertThat(leader.header(ForwardedRequestIdContext.CLIENT_KEY_HEADER)).isEqualTo(key);
    }

    /** A request this node did not treat as idempotent published no key, and none is invented. */
    @Test
    void aForwardOfARequestThatPublishedNoKeySendsNone() throws Exception {
      leader.answerJson(200, "{\"result\":\"ok\"}");
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));
      final HttpServerExchange exchange = exchange("/api/v1/server");
      exchange.getRequestHeaders().put(new HttpString(IdempotencyCache.HEADER_REQUEST_ID), "issue8347-request");

      forwarder.forwardIfReplica(exchange, user("root"), "/api/v1/server", "{}", false);

      assertThat(leader.header(ForwardedRequestIdContext.CLIENT_KEY_HEADER)).isNull();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // 2. Accept, and the stream relayed as it arrives
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void theAcceptHeaderTravelsToTheLeader() throws Exception {
      leader.answerJson(200, "{\"result\":\"ok\"}");
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));
      final HttpServerExchange exchange = exchange("/api/v1/server");
      exchange.getRequestHeaders().put(new HttpString("Accept"), "text/event-stream");

      forwarder.forwardIfReplica(exchange, user("root"), "/api/v1/server", "{}", true, false);

      assertThat(leader.header("Accept")).isEqualTo("text/event-stream");
    }

    /**
     * The reported defect, observed: the leader writes one progress event and then waits. With the old
     * {@code BodyHandlers.ofString()} relay the client saw nothing until the leader finished, so the first event
     * could never reach it while the leader was still holding the second one back.
     */
    @Test
    void aProgressStreamReachesTheClientEventByEventWhileTheLeaderIsStillRunning() throws Exception {
      final CountDownLatch releaseSecondEvent = new CountDownLatch(1);
      leader.answerStream(releaseSecondEvent, true);
      final RecordingTarget target = new RecordingTarget();
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));
      forwarder.streamTargetFactory = exchange -> target;
      final HttpServerExchange exchange = exchange("/api/v1/server");
      exchange.getRequestHeaders().put(new HttpString("Accept"), "text/event-stream");

      final ExecutorService caller = Executors.newSingleThreadExecutor();
      try {
        final Future<ExecutionResponse> forward = caller.submit(() ->
            forwarder.forwardIfReplica(exchange, user("root"), "/api/v1/server", "{\"command\":\"restore database x\"}",
                true, true));

        await().atMost(20, TimeUnit.SECONDS).until(() -> target.flushed().contains("\"first\""));
        assertThat(target.flushed())
            .as("the second event is held back by the leader, so only a relay that streams can have shown the first")
            .doesNotContain("\"completed\"");
        assertThat(target.contentType).isEqualTo("text/event-stream");

        releaseSecondEvent.countDown();
        assertThat(forward.get(20, TimeUnit.SECONDS))
            .as("the stream is already on the wire, so the caller must answer nothing more")
            .isSameAs(LeaderCommandForwarder.STREAMED);
        assertThat(target.flushed()).contains("\"first\"").contains("\"completed\"");
        assertThat(target.closed).isTrue();
      } finally {
        releaseSecondEvent.countDown();
        caller.shutdownNow();
      }
    }

    /**
     * The bound issue #7507 put on every forward survives the move to a streaming relay: a leader that starts the
     * stream and then stops writing is given up on at the deadline, and - the status line being long gone - the client
     * is told so in an SSE error frame naming the setting.
     */
    @Test
    void aStreamThatStallsMidwayIsGivenUpOnAtTheDeadlineAndEndsWithAnErrorFrame() throws Exception {
      final CountDownLatch neverReleased = new CountDownLatch(1);
      leader.answerStream(neverReleased, true);
      final RecordingTarget target = new RecordingTarget();
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(1_000L)));
      forwarder.streamTargetFactory = exchange -> target;
      final HttpServerExchange exchange = exchange("/api/v1/server");
      exchange.getRequestHeaders().put(new HttpString("Accept"), "text/event-stream");

      try {
        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        final ExecutionResponse response = forwarder.forwardIfReplica(exchange, user("root"), "/api/v1/server",
            "{\"command\":\"restore database x\"}", true, true);
        watch.assertGaveUpWithin(30_000L, "a 1s forward deadline from a relay parked on a stream the leader stopped writing");

        assertThat(response).isSameAs(LeaderCommandForwarder.STREAMED);
        assertThat(target.flushed()).contains("\"first\"");
        assertThat(target.flushed()).contains("\"status\":\"error\"")
            .contains(GlobalConfiguration.HA_PROXY_LONG_COMMAND_TIMEOUT.getKey());
        assertThat(target.closed).isTrue();
      } finally {
        neverReleased.countDown();
      }
    }

    /** A refusal issued before the operation started is an ordinary status-carrying answer, and stays one. */
    @Test
    void aLeaderThatAnswersAStreamRequestWithoutAStreamIsRelayedAsItsBufferedAnswer() throws Exception {
      leader.answerJson(400, "{\"error\":\"Database 'x' already exists\"}");
      final RecordingTarget target = new RecordingTarget();
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));
      forwarder.streamTargetFactory = exchange -> target;
      final HttpServerExchange exchange = exchange("/api/v1/server");
      exchange.getRequestHeaders().put(new HttpString("Accept"), "text/event-stream");

      final ExecutionResponse response = forwarder.forwardIfReplica(exchange, user("root"), "/api/v1/server",
          "{\"command\":\"restore database x\"}", true, true);

      assertThat(response.getCode()).isEqualTo(400);
      assertThat(response.getResponse()).contains("already exists");
      assertThat(target.contentType).as("no stream may be started for an answer that is not one").isNull();
    }

    /**
     * A leader whose buffered answer to a stream request breaks part-way - it declares a length and closes the
     * connection short of it - fails the forward as an I/O error, exactly as the {@code ofString()} relay did, rather
     * than relaying a truncated body as if it were whole.
     */
    @Test
    void aBufferedAnswerThatBreaksPartWayFailsTheForwardInsteadOfRelayingATruncatedBody() {
      leader.answerTruncatedJson();
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));
      forwarder.streamTargetFactory = exchange -> contentType -> {
        throw new AssertionError("a JSON answer must not start a stream");
      };
      final HttpServerExchange exchange = exchange("/api/v1/server");
      exchange.getRequestHeaders().put(new HttpString("Accept"), "text/event-stream");

      assertThatThrownBy(() -> forwarder.forwardIfReplica(exchange, user("root"), "/api/v1/server",
          "{\"command\":\"restore database x\"}", true, true))
          .isInstanceOf(IOException.class);
    }

    /**
     * A route that cannot stream - the {@code /server/users} ones - never gets {@link LeaderCommandForwarder#STREAMED}
     * back, whatever its client's Accept header says: its handler would have nothing to return it as.
     */
    @Test
    void aRouteThatDidNotOptInIsNeverHandedTheStreamedSentinel() throws Exception {
      leader.answerStream(new CountDownLatch(0), true);
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));
      forwarder.streamTargetFactory = exchange -> {
        throw new AssertionError("a route that did not opt in must not start a stream");
      };
      final HttpServerExchange exchange = exchange("/api/v1/server/users");
      exchange.getRequestHeaders().put(new HttpString("Accept"), "text/event-stream");

      final ExecutionResponse response = forwarder.forwardIfReplica(exchange, user("root"), "/api/v1/server/users",
          "{}");

      assertThat(response).isNotSameAs(LeaderCommandForwarder.STREAMED);
      assertThat(response.getCode()).isEqualTo(200);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // 3. The one-hop refusal: leadership moved (503) vs an address that names the wrong node (400)
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void theForwardNamesTheLeaderItMeansToReach() throws Exception {
      leader.answerJson(200, "{}");
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));

      forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"), "/api/v1/server", "{}", false);

      assertThat(leader.header(LeaderForwardContext.FORWARDED_LEADER_ID_HEADER)).isEqualTo(LEADER_PEER);
      assertThat(leader.header(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER)).isEqualTo("true");
    }

    @Test
    void aForwardThatSawLeadershipChangeWhileResolvingNamesNoLeader() throws Exception {
      leader.answerJson(200, "{}");
      final HAServerPlugin ha = ha();
      // Read once before the address is resolved and once after: a change in between means the id may not be the
      // node the address names, so the only honest thing to send is nothing.
      when(ha.getLeaderPeerId()).thenReturn(LEADER_PEER, "peer-new-leader");
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha, config(30_000L)));

      forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"), "/api/v1/server", "{}", false);

      assertThat(leader.header(LeaderForwardContext.FORWARDED_LEADER_ID_HEADER)).isNull();
    }

    /**
     * The routine case the issue is about: the peer dialled this very node as the leader, and it stopped being the
     * leader while the request travelled. Retryable - the refusal names no leader, which the HTTP layer answers 503 -
     * and it must not use up the one warning the genuine misconfiguration gets.
     */
    @Test
    void aRefusalCausedByLeadershipMovingIsRetryableAndLeavesTheWarningForTheMisconfiguration() {
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));
      LeaderForwardContext.markAlreadyForwarded(LOCAL_PEER);

      final ServerIsNotTheLeaderException refusal = catchThrowableOfType(ServerIsNotTheLeaderException.class,
          () -> forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"), "/api/v1/server", "{}", false));

      assertThat(refusal.getLeaderAddress())
          .as("a named leader is what AbstractServerHttpHandler answers 400; an unnamed one is the retryable 503")
          .isNull();
      assertThat(refusal.getMessage()).contains("leadership moved").contains("retry");
      assertThat(forwarder.misconfigurationWarned()).isFalse();
      assertThat(leader.requests()).as("a refused hop is never relayed on").isZero();
    }

    @Test
    void aRefusalCausedByAnAddressThatNamesTheWrongNodeIsTheConfigurationError() {
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));
      LeaderForwardContext.markAlreadyForwarded(LEADER_PEER);

      final ServerIsNotTheLeaderException refusal = catchThrowableOfType(ServerIsNotTheLeaderException.class,
          () -> forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"), "/api/v1/server", "{}", false));

      assertThat(refusal.getLeaderAddress()).isEqualTo("leader");
      assertThat(refusal.getMessage()).contains("already forwarded").contains("does not identify")
          .contains(GlobalConfiguration.HA_SERVER_LIST.getKey()).doesNotContain("retry");
      assertThat(forwarder.misconfigurationWarned()).isTrue();
    }

    /**
     * A peer that says nothing - an older node mid rolling upgrade, or one that saw leadership change while it resolved
     * the address - proves nothing about the address, and is answered as the election it usually is (issue #8393).
     */
    @Test
    void aRefusalWithNoIntendedLeaderIsRetryable() {
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(30_000L)));
      LeaderForwardContext.markAlreadyForwarded();

      assertThatThrownBy(() -> forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"), "/api/v1/server",
          "{}", false))
          .isInstanceOf(ServerIsNotTheLeaderException.class)
          .hasMessageContaining("retry")
          .satisfies(e -> assertThat(((ServerIsNotTheLeaderException) e).getLeaderAddress()).isNull());
      assertThat(forwarder.misconfigurationWarned()).isFalse();
    }

    /** The same split on the batch relay, the other follower-to-leader forward in this module. */
    @Test
    void theBatchRelayAnswersALeadershipChangeWith503AndAWrongAddressWith400() throws Exception {
      final PostBatchHandler handler = new PostBatchHandler(httpServerWith(ha(), config(30_000L)));

      LeaderForwardContext.markAlreadyForwarded(LOCAL_PEER);
      final ExecutionResponse moved = handler.forwardBatchToLeader(exchange("/api/v1/batch/graph"), ha(), "graph",
          user("root"), "application/json", body("{\"@type\":\"vertex\"}\n"), false);
      assertThat(moved.getCode()).isEqualTo(503);
      assertThat(moved.getResponse()).contains("leadership moved").contains("retry");

      LeaderForwardContext.markAlreadyForwarded(LEADER_PEER);
      final ExecutionResponse misidentified = handler.forwardBatchToLeader(exchange("/api/v1/batch/graph"), ha(), "graph",
          user("root"), "application/json", body("{\"@type\":\"vertex\"}\n"), false);
      assertThat(misidentified.getCode()).isEqualTo(400);
      assertThat(misidentified.getResponse()).contains("does not identify");
      assertThat(leader.requests()).isZero();
    }

    @Test
    void theBatchRelayNamesTheLeaderItMeansToReach() {
      final HttpRequest request = PostBatchHandler.buildForwardRequest("http://leader:2480/api/v1/batch/graph",
          "application/json", "token", "root", 3, new ByteArrayInputStream(new byte[3]), null, Duration.ofSeconds(5),
          LEADER_PEER);

      assertThat(request.headers().firstValue(LeaderForwardContext.FORWARDED_LEADER_ID_HEADER)).contains(LEADER_PEER);
      assertThat(PostBatchHandler.buildForwardRequest("http://leader:2480/api/v1/batch/graph", "application/json",
          "token", "root", 3, new ByteArrayInputStream(new byte[3]), null, Duration.ofSeconds(5)).headers()
          .firstValue(LeaderForwardContext.FORWARDED_LEADER_ID_HEADER)).isEmpty();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Fixtures
    // ---------------------------------------------------------------------------------------------------------------

    private HAServerPlugin ha() {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.isLeader()).thenReturn(false);
      when(ha.getLeaderAddress()).thenReturn(leader.address());
      when(ha.getLeaderName()).thenReturn("leader");
      when(ha.getLeaderPeerId()).thenReturn(LEADER_PEER);
      when(ha.getLocalPeerId()).thenReturn(LOCAL_PEER);
      when(ha.getClusterToken()).thenReturn("issue7603-cluster-token");
      return ha;
    }

    private static ContextConfiguration config(final long longCommandTimeoutMs) {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_PROXY_READ_TIMEOUT, 30_000L);
      cfg.setValue(GlobalConfiguration.HA_PROXY_LONG_COMMAND_TIMEOUT, longCommandTimeoutMs);
      cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 5_000L);
      cfg.setValue(GlobalConfiguration.HA_PROXY_BATCH_READ_TIMEOUT, 30_000L);
      return cfg;
    }

    private static HttpServer httpServerWith(final HAServerPlugin ha, final ContextConfiguration configuration) {
      final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, configuration);
      server.setHA(ha);
      final HttpServer httpServer = HTTP_SERVERS.of(server);
      return httpServer;
    }

    private static HttpServerExchange exchange(final String path) {
      final HttpServerExchange exchange = new HttpServerExchange(null);
      exchange.setRequestPath(path);
      exchange.setRequestURI(path);
      exchange.setRequestMethod(Methods.POST);
      exchange.getRequestHeaders().put(new HttpString("Authorization"), "Basic cm9vdDpwbGF5d2l0aGRhdGE=");
      return exchange;
    }

    private static CountingInputStream body(final String payload) {
      return new CountingInputStream(new HttpServerExchange(null),
          new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8)));
    }

    private static ServerSecurityUser user(final String name) {
      return TestServerHelper.securityUser(name);
    }

    /** Records what the relay wrote and whether it flushed it, the way a client socket would see it. */
    private static final class RecordingTarget implements LeaderCommandForwarder.StreamTarget {
      private final    ByteArrayOutputStream written = new ByteArrayOutputStream();
      private volatile String                flushed = "";
      private volatile String                contentType;
      private volatile boolean               closed;

      @Override
      public OutputStream open(final String contentType) {
        this.contentType = contentType;
        return new OutputStream() {
          @Override
          public void write(final int b) {
            synchronized (written) {
              written.write(b);
            }
          }

          @Override
          public void write(final byte[] b, final int off, final int len) {
            synchronized (written) {
              written.write(b, off, len);
            }
          }

          @Override
          public void flush() {
            synchronized (written) {
              flushed = written.toString(StandardCharsets.UTF_8);
            }
          }

          @Override
          public void close() {
            flush();
            closed = true;
          }
        };
      }

      String flushed() {
        return flushed;
      }
    }

    /** The leader: a loopback HTTP listener that records the request headers and answers as a test tells it to. */
    private static final class StubLeader implements AutoCloseable {
      private final com.sun.net.httpserver.HttpServer server;
      private final Map<String, String>              headers  = new ConcurrentHashMap<>();
      private final ExecutorService                  executor = Executors.newCachedThreadPool();
      private volatile int                           requests;
      private volatile Answer                        answer;

      private interface Answer {
        void write(HttpExchange exchange) throws Exception;
      }

      StubLeader() throws IOException {
        server = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 16);
        server.setExecutor(executor);
        server.createContext("/", exchange -> {
          requests++;
          exchange.getRequestHeaders().forEach((name, values) -> headers.put(name.toLowerCase(), values.getFirst()));
          exchange.getRequestBody().readAllBytes();
          try {
            answer.write(exchange);
          } catch (final Exception e) {
            // the forwarder gave up and closed the connection: nothing left to answer
          } finally {
            exchange.close();
          }
        });
        server.start();
      }

      void answerJson(final int status, final String json) {
        answer = exchange -> {
          final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(status, bytes.length);
          exchange.getResponseBody().write(bytes);
        };
      }

      /** Declares a hundred bytes of JSON, sends five, and closes the connection. */
      void answerTruncatedJson() {
        answer = exchange -> {
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, 100);
          exchange.getResponseBody().write("{\"res".getBytes(StandardCharsets.UTF_8));
          exchange.getResponseBody().flush();
          exchange.getHttpContext().getServer().stop(0);
        };
      }

      /** One progress event, then - once {@code release} opens - a completion event. */
      void answerStream(final CountDownLatch release, final boolean eventStream) {
        answer = exchange -> {
          exchange.getResponseHeaders().add("Content-Type", eventStream ? "text/event-stream" : "application/json");
          exchange.sendResponseHeaders(200, 0);
          final OutputStream out = exchange.getResponseBody();
          out.write(("data: " + new JSONObject().put("status", "progress").put("message", "first") + "\n\n")
              .getBytes(StandardCharsets.UTF_8));
          out.flush();
          release.await(60, TimeUnit.SECONDS);
          out.write(("data: " + new JSONObject().put("status", "completed").put("message", "done") + "\n\n")
              .getBytes(StandardCharsets.UTF_8));
          out.flush();
        };
      }

      String address() {
        return "127.0.0.1:" + server.getAddress().getPort();
      }

      String header(final String name) {
        return headers.get(name.toLowerCase());
      }

      int requests() {
        return requests;
      }

      @Override
      public void close() {
        server.stop(0);
        executor.shutdownNow();
      }
    }
  }

  /**
   * Issue #8359: only a route that runs its body as one command can have that command forwarded whole, with the client's
   * own body. {@code POST /query} shares {@link PostCommandHandler#execute} but runs its statement on the node that serves
   * it, so a write it issues from inside - through a function it calls - is a part of the request, never the whole of it.
   */
  @Nested
  class WholeRequestRoute {

    @Test
    void theCommandRouteCanBeForwardedWhole() {
      assertThat(new PostCommandHandler(null).commandForwardIsWholeRequest()).isTrue();
    }

    @Test
    void theQueryRouteIsNeverForwardedWhole() {
      assertThat(new PostQueryHandler(null).commandForwardIsWholeRequest()).isFalse();
    }
  }

  /**
   * Issue #8578: {@code GET /api/v1/server?mode=cluster} publishes the client-facing ports of the active
   * plugins. A plugin that throws must not fail the route, a port that is null or not positive is dropped, and a service name
   * two plugins both advertise keeps its first owner.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AdvertisedPorts {

    @Test
    void publishesTheBoundPortOfEachService() {
      final JSONObject ports = GetServerHandler.buildAdvertisedPorts(List.of(plugin(Map.of("gremlin", 18182)), plugin(Map.of("mcp", 9000))));

      assertThat(ports.keySet()).containsExactlyInAnyOrder("gremlin", "mcp");
      assertThat(ports.getInt("gremlin")).isEqualTo(18182);
    }

    @Test
    void dropsPortsThatAreNullOrNotPositive() {
      final Map<String, Integer> raw = new HashMap<>();
      raw.put("unbound", 0);
      raw.put("negative", -1);
      raw.put("missing", null);
      raw.put("ok", 7);

      assertThat(GetServerHandler.buildAdvertisedPorts(List.of(plugin(raw))).keySet()).containsExactly("ok");
    }

    @Test
    void aPluginThatThrowsDoesNotFailTheRoute() {
      final ServerPlugin broken = new ServerPlugin() {
        @Override
        public void startService() {
        }

        @Override
        public Map<String, Integer> getAdvertisedPorts() {
          throw new IllegalStateException("boom");
        }
      };

      final JSONObject ports = GetServerHandler.buildAdvertisedPorts(List.of(broken, plugin(Map.of("gremlin", 1234))));

      assertThat(ports.keySet()).containsExactly("gremlin");
    }

    @Test
    void aServiceNameKeepsItsFirstOwner() {
      final JSONObject ports = GetServerHandler.buildAdvertisedPorts(List.of(plugin(Map.of("gremlin", 1111)), plugin(Map.of("gremlin", 2222))));

      assertThat(ports.getInt("gremlin")).isEqualTo(1111);
    }

    private static ServerPlugin plugin(final Map<String, Integer> ports) {
      return new ServerPlugin() {
        @Override
        public void startService() {
        }

        @Override
        public Map<String, Integer> getAdvertisedPorts() {
          return ports;
        }
      };
    }
  }

  /**
   * Issue #10068: the batch forward builds its request through {@link PeerRequest}, so it presents the credential the way every
   * other peer dial does and owes the operator the same notice when it goes out in clear text on an SSL cluster. It spelled the pair
   * by hand, and was silent about the fallback.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PeerDialCredential {

    private static final String PLAIN_URL     = "http://leader:2480/api/v1/batch/db";
    private static final String ENCRYPTED_URL = "https://leader:2490/api/v1/batch/db";

    @BeforeEach
    @AfterEach
    void rearmTheNotice() {
      PeerRequest.rearmForTests();
    }

    private static HttpRequest batchForward(final String url, final String token, final boolean sslEnabled) {
      return PostBatchHandler.buildForwardRequest(url, "application/json", token, "reader", 0L, new ByteArrayInputStream(new byte[0]),
          null, null, "peer-1", sslEnabled);
    }

    @Test
    void theBatchForwardPresentsTheTokenWithThePrincipalItActsFor() {
      final HttpRequest request = batchForward(PLAIN_URL, "s3cr3t", false);

      assertThat(request.headers().firstValue(PeerRequest.CLUSTER_TOKEN_HEADER)).hasValue("s3cr3t");
      assertThat(request.headers().firstValue(PeerRequest.FORWARDED_USER_HEADER)).hasValue("reader");
      assertThat(request.headers().firstValue("Content-Type")).hasValue("application/json");
    }

    @Test
    void aPlainBatchForwardOnAnSslClusterSaysTheNotice() {
      batchForward(PLAIN_URL, "s3cr3t", true);

      assertThat(PeerRequest.saidForTests()).isTrue();
    }

    @Test
    void anEncryptedOrNonSslBatchForwardSaysNothing() {
      batchForward(ENCRYPTED_URL, "s3cr3t", true);
      batchForward(PLAIN_URL, "s3cr3t", false);

      assertThat(PeerRequest.saidForTests()).isFalse();
    }
  }
}
