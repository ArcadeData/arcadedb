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
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.CommandTimeoutOverride;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.LeaderForwardContext;
import com.arcadedb.server.PeerRequest;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.UnstartedHttpServers;
import com.arcadedb.server.http.FakeLeader;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.StallAwareStopwatch;
import io.undertow.Undertow;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.BlockingHandler;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Bounds on a follower-to-leader forward: the request timeout, the body deadline, and the command budget the leader enforces.
 * <p>
 * One {@code @Nested} class per regression, each keeping the fixture and the Javadoc of the issue it pins.
 */
class LeaderForwardTimeoutTest {

  /**
   * Issue #7507: {@link LeaderCommandForwarder} dialled the leader with a bare
   * {@code HttpClient.newHttpClient()} and built the request without {@code .timeout(...)}, so a leader that
   * accepted the connection and then never answered held the calling Undertow worker thread until the OS tore
   * the socket down.
   * <p>
   * The invariant these tests pin: every forward carries a finite response deadline and the client carries a
   * finite connect timeout, and a blown deadline is answered 504 rather than parking the caller.
   */
  @Nested
  class CommandForwardTimeout {

    /**
     * The tripwire between "the deadline fired" and "the call is unbounded". The deadline under test is 1 s, so
     * anything under this bound proves the former; a stalled leader without the fix sits here until the OS gives
     * up on the socket, which is minutes.
     */
    private static final long GAVE_UP_BOUND_MS = 30_000L;

    private static ContextConfiguration configuration(final long readTimeoutMs, final long longCommandTimeoutMs,
        final long connectTimeoutMs) {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_PROXY_READ_TIMEOUT, readTimeoutMs);
      cfg.setValue(GlobalConfiguration.HA_PROXY_LONG_COMMAND_TIMEOUT, longCommandTimeoutMs);
      cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, connectTimeoutMs);
      return cfg;
    }

    /**
     * The reported failure, end to end over a real socket: a leader that accepts the connection and never
     * answers. Before the fix this parked the caller; now the deadline fires and the follower answers 504.
     */
    @Test
    void aLeaderThatAcceptsAndNeverAnswersIsGivenUpOnAndAnswered504() throws Exception {
      try (final FakeLeader leader = FakeLeader.silent()) {
        final LeaderCommandForwarder.Transport transport =
            new LeaderCommandForwarder.Transport(configuration(1_000L, 60_000L, 5_000L));

        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        final ExecutionResponse response = transport.send(transport.client(),
            transport.newRequest(plainRequest("http://" + leader.address() + "/api/v1/server"), "POST", "{}", false).build(),
            leader.address(), false);
        watch.assertGaveUpWithin(GAVE_UP_BOUND_MS,
            "a 1s forward deadline from the unbounded wait a leader that never answers used to produce");

        assertThat(response.getCode()).isEqualTo(504);
        final JSONObject body = new JSONObject(response.getResponse());
        assertThat(body.getString("error")).contains(leader.address());
        assertThat(body.getString("error")).contains(GlobalConfiguration.HA_PROXY_READ_TIMEOUT.getKey());
        assertThat(leader.acceptedConnections()).isGreaterThanOrEqualTo(1);
        assertThat(leader.firstConnectionClosedByClientWithin(10_000L))
            .as("giving up must tear the connection down, not just release this thread: a leader that wedges "
                + "repeatedly would otherwise trade a parked worker for a leaked socket")
            .isTrue();
      }
    }

    /**
     * The half of the wait a {@code HttpRequest.timeout} does not cover: the leader answers the headers - status
     * line, {@code Content-Length}, blank line - and then stops writing part-way through the body. The JDK's
     * request timeout has already been satisfied by the headers at that point, so {@code HttpClient.send} would
     * block on the stalled body indefinitely; the forwarder bounds the whole exchange instead.
     */
    @Test
    void aLeaderThatAnswersTheHeadersAndThenStallsMidBodyIsAlsoGivenUpOn() throws Exception {
      try (final FakeLeader leader = FakeLeader.scripted(CommandForwardTimeout::answerHeadersThenStall)) {
        final LeaderCommandForwarder.Transport transport =
            new LeaderCommandForwarder.Transport(configuration(1_000L, 60_000L, 5_000L));

        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        final ExecutionResponse response = transport.send(transport.client(),
            transport.newRequest(plainRequest("http://" + leader.address() + "/api/v1/server"), "POST", "{}", false).build(),
            leader.address(), false);
        watch.assertGaveUpWithin(GAVE_UP_BOUND_MS,
            "a 1s forward deadline from the unbounded wait a body that stalls after the headers used to produce");

        assertThat(response.getCode()).isEqualTo(504);
        assertThat(new JSONObject(response.getResponse()).getString("error")).contains(leader.address());
      }
    }

    /**
     * Every request the forwarder can build carries a deadline: {@code newRequest} is the only way
     * {@code forwardIfReplica} constructs one, so no call site can opt out of it.
     */
    @Test
    void everyForwardedRequestCarriesTheDefaultDeadline() {
      final LeaderCommandForwarder.Transport transport =
          new LeaderCommandForwarder.Transport(configuration(7_000L, 60_000L, 5_000L));

      assertThat(transport.newRequest(plainRequest("http://leader:2480/api/v1/server/users?name=bob"), "DELETE", null, false)
          .build().timeout()).contains(Duration.ofMillis(7_000L));
      assertThat(transport.newRequest(plainRequest("http://leader:2480/api/v1/server/users"), "POST", "{\"name\":\"bob\"}", false)
          .build().timeout()).contains(Duration.ofMillis(7_000L));
      assertThat(transport.newRequest(plainRequest("http://leader:2480/api/v1/server/users"), "PUT", "{\"name\":\"bob\"}", false)
          .build().timeout()).contains(Duration.ofMillis(7_000L));
    }

    /**
     * {@code restore backup}, {@code restore database} and {@code import database} legitimately run for minutes,
     * so they get their own, larger deadline instead of being aborted by the 30 s one.
     */
    @Test
    void longRunningCommandsGetTheLongerDeadline() {
      final LeaderCommandForwarder.Transport transport =
          new LeaderCommandForwarder.Transport(configuration(7_000L, 600_000L, 5_000L));

      assertThat(transport.newRequest(plainRequest("http://leader:2480/api/v1/server"), "POST", "{}", true)
          .build().timeout()).contains(Duration.ofMillis(600_000L));
      assertThat(transport.responseTimeout(true)).isGreaterThan(transport.responseTimeout(false));
    }

    /**
     * Which commands claim the longer deadline. The classification lives in {@code PostServerCommandHandler}
     * because that is where the command grammar lives, and it has to match the set of commands that forward at
     * all, otherwise a long restore silently gets the 30 s budget.
     */
    @Test
    void onlyRestoreImportAndBackupAreClassifiedAsLongRunning() {
      assertThat(PostServerCommandHandler.isLongRunningForwardedCommand("restore backup mybackup.zip")).isTrue();
      assertThat(PostServerCommandHandler.isLongRunningForwardedCommand("restore database mydb")).isTrue();
      assertThat(PostServerCommandHandler.isLongRunningForwardedCommand("import database file:///data.csv")).isTrue();
      // Issue #12417: the leader answers a forwarded backup once the archive is written
      assertThat(PostServerCommandHandler.isLongRunningForwardedCommand("trigger backup mydb")).isTrue();

      assertThat(PostServerCommandHandler.isLongRunningForwardedCommand("create database mydb")).isFalse();
      assertThat(PostServerCommandHandler.isLongRunningForwardedCommand("drop database mydb")).isFalse();
      assertThat(PostServerCommandHandler.isLongRunningForwardedCommand("create user { \"name\": \"bob\" }")).isFalse();
      assertThat(PostServerCommandHandler.isLongRunningForwardedCommand("drop user bob")).isFalse();
      assertThat(PostServerCommandHandler.isLongRunningForwardedCommand("list backups mydb")).isFalse();
      assertThat(PostServerCommandHandler.isLongRunningForwardedCommand("delete backup mydb mybackup.zip")).isFalse();
    }

    // Regression: #12417 the whole backup family is forwarded to the leader, node-local settings are not
    /**
     * Issue #12417: the long deadline is only ever read for a command that forwards, so each long-running command
     * must be in the forwarded set too, and the whole backup family has to be: {@code restore backup} reads the
     * leader's backup directory, so the commands that write, list and delete archives act on the same one.
     */
    @Test
    void theBackupFamilyIsForwardedToTheLeaderAsOne() {
      for (final String command : new String[] { "trigger backup mydb", "list backups mydb", "delete backup mydb mybackup.zip",
          "restore backup mydb mybackup.zip", "restore database mydb", "import database file:///data.csv", "create database mydb",
          "drop database mydb", "create user { \"name\": \"bob\" }", "drop user bob" })
        assertThat(PostServerCommandHandler.isForwardedToLeader(command)).as(command).isTrue();

      // Each node keeps its own backup configuration and settings: those are answered where they are asked
      for (final String command : new String[] { "get backup config", "set backup config", "get server events",
          "set server setting arcadedb.x y", "close database mydb", "open database mydb" })
        assertThat(PostServerCommandHandler.isForwardedToLeader(command)).as(command).isFalse();
    }

    /**
     * The connect timeout bounds the other half of the failure: a leader whose host black-holes the SYN. It is
     * immutable once the client is built, so it is asserted on the client rather than over a socket - an address
     * that reliably black-holes a connect is not something a unit test can conjure portably.
     */
    @Test
    void theClientCarriesTheConfiguredConnectTimeout() {
      final LeaderCommandForwarder.Transport transport =
          new LeaderCommandForwarder.Transport(configuration(7_000L, 60_000L, 2_500L));

      assertThat(transport.client().connectTimeout()).contains(Duration.ofMillis(2_500L));
    }

    /**
     * A leader this node cannot even reach is a different failure from one that will not answer, and it points at
     * a different setting. It gets its own 504, saying the command certainly did not run - which, unlike the
     * response-deadline case, is something this node can promise.
     */
    @Test
    void aLeaderThatCannotBeConnectedToIsAnsweredWithItsOwnGatewayTimeout() {
      final LeaderCommandForwarder.Transport transport =
          new LeaderCommandForwarder.Transport(configuration(7_000L, 60_000L, 2_500L));

      final ExecutionResponse response = transport.couldNotConnect(transport.client(), "leader.example:2480");

      assertThat(response.getCode()).isEqualTo(504);
      final String error = new JSONObject(response.getResponse()).getString("error");
      assertThat(error).contains("leader.example:2480");
      assertThat(error).contains(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT.getKey());
      assertThat(error).contains("2500 ms");
      assertThat(error).doesNotContain(GlobalConfiguration.HA_PROXY_READ_TIMEOUT.getKey());
    }

    /**
     * The deadlines are re-read per forward, so {@code SET SERVER SETTING arcadedb.ha.proxyReadTimeout} takes
     * effect without a restart. (The connect timeout cannot: an {@link java.net.http.HttpClient}'s is fixed at
     * build time.)
     */
    @Test
    void responseDeadlineIsReReadOnEveryForward() {
      final ContextConfiguration cfg = configuration(7_000L, 60_000L, 5_000L);
      final LeaderCommandForwarder.Transport transport = new LeaderCommandForwarder.Transport(cfg);

      assertThat(transport.responseTimeout(false)).isEqualTo(Duration.ofMillis(7_000L));
      cfg.setValue(GlobalConfiguration.HA_PROXY_READ_TIMEOUT, 11_000L);
      assertThat(transport.responseTimeout(false)).isEqualTo(Duration.ofMillis(11_000L));
    }

    /**
     * Zero and negative are not a back door to the unbounded behaviour this issue removed: they clamp to the
     * smallest deadline the JDK accepts rather than disabling the bound.
     */
    @Test
    void zeroOrNegativeTimeoutClampsInsteadOfDisablingTheBound() {
      final LeaderCommandForwarder.Transport transport =
          new LeaderCommandForwarder.Transport(configuration(0L, -1L, 0L));

      assertThat(transport.responseTimeout(false)).isPositive();
      assertThat(transport.responseTimeout(true)).isPositive();
      assertThat(transport.client().connectTimeout()).isPresent();
      assertThat(transport.client().connectTimeout().get()).isPositive();
      // and a request can actually be built from it - Duration.ZERO would throw
      assertThat(transport.newRequest(plainRequest("http://leader:2480/api/v1/server"), "POST", "{}", false)
          .build().timeout()).isPresent();
    }

    /** The request {@code LeaderCommandForwarder} hands the transport: the leader's URL, no credential (issue #10068). */
    private static HttpRequest.Builder plainRequest(final String url) {
      return PeerRequest.newRequest(url, false, CommandForwardTimeout.class, "testing", null);
    }

    /**
     * A complete, well-formed response head that promises a hundred bytes, then five of them and never another: the
     * case an {@code HttpRequest.timeout} alone does not catch. The connection stays open until the leader is closed.
     */
    private static void answerHeadersThenStall(final OutputStream out) throws IOException {
      out.write(("HTTP/1.1 200 OK\r\n"
          + "Content-Type: application/json\r\n"
          + "Content-Length: 100\r\n"
          + "\r\n").getBytes(StandardCharsets.US_ASCII));
      out.write("{\"res\"".getBytes(StandardCharsets.US_ASCII));
    }
  }

  /**
   * Regression test for issues #7526/#7542: {@link PostBatchHandler}'s {@code forwardBatchToLeader} dialled the
   * leader with a bare {@code HttpClient.newHttpClient()} and built the request with no {@code .timeout(...)},
   * so a leader that accepted the connection and never answered parked the Undertow worker thread serving
   * {@code POST /api/v1/batch} until the OS tore the socket down.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BatchForwardTimeout {
    /** The tripwire between "the deadline fired" and "the call is unbounded" (minutes, without the fix). */
    private static final long GAVE_UP_BOUND_MS = 30_000L;

    @RegisterExtension
    static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

    private static PostBatchHandler handlerWith(final ContextConfiguration cfg) {
      final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, cfg);
      final HttpServer httpServer = HTTP_SERVERS.of(server);
      return new PostBatchHandler(httpServer);
    }

    private static HAServerPlugin haPointingAt(final String leaderAddress) {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getLeaderAddress()).thenReturn(leaderAddress);
      when(ha.getClusterToken()).thenReturn("test-token");
      return ha;
    }

    private static ServerSecurityUser rootUser() {
      return TestServerHelper.securityUser("root");
    }

    private static PostBatchHandler.CountingInputStream emptyBody() {
      return new PostBatchHandler.CountingInputStream(new HttpServerExchange(null),
          new ByteArrayInputStream(new byte[0]));
    }

    @Test
    void aLeaderThatAcceptsAndNeverAnswersIsGivenUp504() throws Exception {
      try (final FakeLeader leader = FakeLeader.silent()) {
        final ContextConfiguration cfg = new ContextConfiguration();
        cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 5_000L);
        cfg.setValue(GlobalConfiguration.HA_PROXY_BATCH_READ_TIMEOUT, 1_000L);

        final PostBatchHandler handler = handlerWith(cfg);
        final HttpServerExchange exchange = new HttpServerExchange(null);

        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        final ExecutionResponse response = handler.forwardBatchToLeader(exchange, haPointingAt(leader.address()),
            "mydb", rootUser(), "application/x-ndjson", emptyBody(), false);
        watch.assertGaveUpWithin(GAVE_UP_BOUND_MS,
            "a 1s forward deadline from the unbounded wait a leader that never answers used to produce");

        assertThat(response.getCode()).isEqualTo(504);
        assertThat(new JSONObject(response.getResponse()).getString("error")).contains(leader.address());
        assertThat(leader.acceptedConnections()).isGreaterThanOrEqualTo(1);
      }
    }

    @Test
    void aLeaderThatCannotBeConnectedToIsGivenUp504() throws Exception {
      final String unreachable;
      try (final ServerSocket probe = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
        unreachable = probe.getInetAddress().getHostAddress() + ":" + probe.getLocalPort();
      }

      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 2_000L);
      cfg.setValue(GlobalConfiguration.HA_PROXY_BATCH_READ_TIMEOUT, 60_000L);

      final PostBatchHandler handler = handlerWith(cfg);
      final HttpServerExchange exchange = new HttpServerExchange(null);

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      final ExecutionResponse response = handler.forwardBatchToLeader(exchange, haPointingAt(unreachable),
          "mydb", rootUser(), "application/x-ndjson", emptyBody(), false);
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "the configured connect timeout, not an OS-level connect refusal delay");

      assertThat(response.getCode()).isEqualTo(504);
      assertThat(new JSONObject(response.getResponse()).getString("error")).contains(unreachable);
    }

    @Test
    void theClientCarriesTheConfiguredConnectTimeout() throws Exception {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 2_500L);

      final PostBatchHandler handler = handlerWith(cfg);

      final Field field = PostBatchHandler.class.getDeclaredField("httpClient");
      field.setAccessible(true);
      final HttpClient client = (HttpClient) field.get(handler);

      assertThat(client.connectTimeout()).contains(Duration.ofMillis(2_500L));
    }

    /**
     * {@link PostBatchHandler#buildForwardRequest} carries the deadline all the way to the built request when
     * one is given, and none when it is not - the shape the production forward and the pre-existing request-shape
     * tests respectively rely on.
     */
    @Test
    void buildForwardRequestCarriesTheGivenTimeout() {
      final HttpRequest withTimeout = PostBatchHandler.buildForwardRequest(
          "http://leader:2480/api/v1/batch/db", "application/x-ndjson", "token", "root", -1,
          new ByteArrayInputStream("x".getBytes(StandardCharsets.UTF_8)), null, Duration.ofMillis(1234L));
      assertThat(withTimeout.timeout()).contains(Duration.ofMillis(1234L));

      final HttpRequest withoutTimeout = PostBatchHandler.buildForwardRequest(
          "http://leader:2480/api/v1/batch/db", "application/x-ndjson", "token", "root", -1,
          new ByteArrayInputStream("x".getBytes(StandardCharsets.UTF_8)));
      assertThat(withoutTimeout.timeout()).isEmpty();
    }
  }

  /**
   * Issue #8313, the leader's half: a follower's forward carries the {@code arcadedb.command.timeout} budget it waits for
   * in {@link LeaderForwardContext#FORWARDED_COMMAND_TIMEOUT_HEADER}, and the node that receives it enforces that budget
   * in place of its own database setting - under a valid cluster token only. From a client the header would let a
   * request lift the budget its database imposes, so without the token it is ignored.
   * <p>
   * The statement is a self-join over {@link #NODES} vertices, far more than the 1 ms budget; the database here has no
   * budget of its own, so a request that is not bounded by the header runs it to completion.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ForwardedCommandTimeoutBudget extends BaseGraphServerTest {
    private static final String     CLUSTER_TOKEN = "issue8313-cluster-secret-token";
    private static final int        NODES         = 600;
    private static final String     QUERY         =
        "MATCH {type: Issue8313Node, as: a}, {type: Issue8313Node, as: b, where: (v + $matched.a.v = -1)} RETURN count(*) AS c";
    private static final HttpClient HTTP          = HttpClient.newHttpClient();

    @BeforeEach
    void setUpData() {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, CLUSTER_TOKEN);
      final Database database = getServerDatabase(0, getDatabaseName());
      if (!database.getSchema().existsType("Issue8313Node")) {
        database.getSchema().createVertexType("Issue8313Node");
        database.transaction(() -> {
          for (int i = 0; i < NODES; i++)
            database.newVertex("Issue8313Node").set("v", i).save();
        });
      }
    }

    @AfterEach
    void clearClusterToken() {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, "");
    }

    @Test
    void aForwardedBudgetIsEnforcedUnderTheClusterToken() throws Exception {
      final HttpResponse<String> response = command(b -> b
          .header("X-ArcadeDB-Cluster-Token", CLUSTER_TOKEN)
          .header("X-ArcadeDB-Forwarded-User", "root")
          .header(LeaderForwardContext.FORWARDED_COMMAND_TIMEOUT_HEADER, "1"));

      assertThat(response.statusCode()).as("body: %s", response.body()).isNotEqualTo(200);
      assertThat(response.body()).contains(GlobalConfiguration.COMMAND_TIMEOUT.getKey() + " of 1ms");
      assertThat(CommandTimeoutOverride.get()).as("the budget is scoped to the request, never left on a thread").isEqualTo(-1L);
    }

    @Test
    void aClientCannotSetTheBudgetWithoutTheClusterToken() throws Exception {
      final HttpResponse<String> response = command(b -> b
          .header("Authorization", basic("root", DEFAULT_PASSWORD_FOR_TESTS))
          .header(LeaderForwardContext.FORWARDED_COMMAND_TIMEOUT_HEADER, "1"));

      assertThat(response.statusCode()).as("body: %s", response.body()).isEqualTo(200);
      assertThat(response.body()).doesNotContain(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    }

    private HttpResponse<String> command(final UnaryOperator<HttpRequest.Builder> decorate) throws Exception {
      HttpRequest.Builder builder = HttpRequest.newBuilder()
          .uri(URI.create("http://localhost:" + getServer(0).getHttpServer().getPort()
              + "/api/v1/command/" + getDatabaseName()))
          .header("Content-Type", "application/json");
      builder = decorate.apply(builder);
      final String body = "{\"language\":\"sql\",\"command\":\"" + QUERY.replace("\"", "\\\"") + "\"}";
      return HTTP.send(builder.POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build(),
          HttpResponse.BodyHandlers.ofString());
    }

    private static String basic(final String user, final String password) {
      return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
    }
  }

  /**
   * Regression test for issue #8325: the follower-to-leader forwards bounded the leader with the JDK request timeout
   * ({@code HttpRequest.Builder.timeout}) alone, and what that timeout covers depends on the JDK:
   * <ul>
   * <li>on JDK 21-25 it stops at the response HEADERS, so a leader that answers {@code 200} with a
   * {@code Content-Length} and then stalls inside its body parks a forward that reads it with
   * {@code BodyHandlers.ofString()} with no bound at all;</li>
   * <li>on JDK 26+ it covers the whole body, so on the streamed {@code /api/v1/batch} relay it turned
   * {@code arcadedb.ha.proxyBatchReadTimeout} - meant as a bound on the leader's SILENCE (#7738) - into a cap on the
   * total length of a load that is working.</li>
   * </ul>
   * Both halves are driven here on whichever JDK runs the suite: the first fails on JDK 21-25 without the fix, the
   * second on JDK 26+.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class ForwardBodyDeadline {
    private static final long   BUDGET_MS        = 1_000L;
    /** The tripwire between "the deadline fired" and "the forward is unbounded" (forever, without the fix). */
    private static final long   GAVE_UP_BOUND_MS = 15_000L;
    /** A hang detector, not a latency bound: how long the test waits before calling the forward unbounded. */
    private static final long   HANG_DETECT_MS   = 60_000L;
    private static final int    CLIENT_READ_MS   = 30_000;
    /** Headers promising 100 bytes, then five of them, then silence. */
    private static final String STALLED_BODY     =
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\"res";

    @RegisterExtension
    static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

    private Undertow follower;

    @AfterEach
    void stopFollower() {
      if (follower != null)
        follower.stop();
    }

    // ------------------------------------------------------------------------------------------------------------
    // The shared bound
    // ------------------------------------------------------------------------------------------------------------

    /** The defect at its root: a buffered read of a body the leader stops writing half-way. */
    @Test
    void sendBoundedGivesUpOnALeaderThatStallsInsideItsBody() throws Exception {
      try (final FakeLeader leader = FakeLeader.scripted(out -> write(out, STALLED_BODY));
          final HttpClient client = HttpClient.newHttpClient()) {
        // No request timeout at all: the bound under test is sendBounded's own, on every JDK.
        final HttpRequest request = HttpRequest.newBuilder(URI.create("http://" + leader.address() + "/")).GET().build();

        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        assertThatThrownBy(() -> callWithin(
            () -> LeaderDial.sendBounded(client, request, HttpResponse.BodyHandlers.ofString(), BUDGET_MS), leader))
            .isInstanceOf(HttpTimeoutException.class)
            .hasMessageContaining(leader.address());
        watch.assertGaveUpWithin(GAVE_UP_BOUND_MS,
            "a 1s deadline over the whole exchange from the unbounded body read the JDK 21-25 request timeout leaves");

        assertThat(leader.awaitConnectionClosedByClient(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS))
            .as("the exchange is cancelled, not only abandoned by the calling thread").isTrue();
      }
    }

    @Test
    void sendBoundedReturnsAWholeAnswer() throws Exception {
      final String body = "{\"result\":[1,2,3]}";
      try (final FakeLeader leader = FakeLeader.scripted(out -> write(out,
          "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + body.length() + "\r\n\r\n" + body));
          final HttpClient client = HttpClient.newHttpClient()) {
        final HttpRequest request = HttpRequest.newBuilder(URI.create("http://" + leader.address() + "/")).GET().build();

        final HttpResponse<String> response = LeaderDial.sendBounded(client, request, HttpResponse.BodyHandlers.ofString(),
            BUDGET_MS * 10);

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo(body);
      }
    }

    /** A connect failure keeps its own type, so the callers' "cannot connect" arms still see it. */
    @Test
    void sendBoundedRethrowsAConnectFailureAsItself() throws Exception {
      final String unreachable;
      try (final ServerSocket probe = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
        unreachable = probe.getInetAddress().getHostAddress() + ":" + probe.getLocalPort();
      }
      try (final HttpClient client = HttpClient.newHttpClient()) {
        final HttpRequest request = HttpRequest.newBuilder(URI.create("http://" + unreachable + "/")).GET().build();

        assertThatThrownBy(() -> LeaderDial.sendBounded(client, request, HttpResponse.BodyHandlers.ofString(), BUDGET_MS * 10))
            .isInstanceOf(ConnectException.class);
      }
    }

    /** An interrupt while waiting reaches the caller as itself, and the exchange it abandons is cancelled. */
    @Test
    void sendBoundedInterruptedWhileWaitingThrowsTheInterruptAndCancelsTheExchange() throws Exception {
      try (final FakeLeader leader = FakeLeader.scripted(out -> write(out, STALLED_BODY));
          final HttpClient client = HttpClient.newHttpClient()) {
        final HttpRequest request = HttpRequest.newBuilder(URI.create("http://" + leader.address() + "/")).GET().build();
        final AtomicReference<Throwable> thrown = new AtomicReference<>();
        final CountDownLatch done = new CountDownLatch(1);
        final Thread caller = new Thread(() -> {
          try {
            LeaderDial.sendBounded(client, request, HttpResponse.BodyHandlers.ofString(), HANG_DETECT_MS);
          } catch (final Throwable t) {
            thrown.set(t);
          } finally {
            done.countDown();
          }
        }, "issue8325-interrupted-forward");
        caller.setDaemon(true);
        caller.start();

        assertThat(leader.awaitRequestReceived(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS)).isTrue();
        caller.interrupt();

        assertThat(done.await(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS)).as("the interrupt releases the caller").isTrue();
        assertThat(thrown.get()).isInstanceOf(InterruptedException.class);
        assertThat(leader.awaitConnectionClosedByClient(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS)).isTrue();
      }
    }

    /**
     * The interrupt path's clean-up, handed the one future a live race produces too rarely to test: one that already
     * failed. {@code getNow} would rethrow that failure in place of the interrupt the caller is about to rethrow.
     */
    @Test
    void closingTheBodyOfAFailedExchangeThrowsNothing() {
      LeaderDial.closeBodyOf(CompletableFuture.failedFuture(new IOException("connection reset")));
      LeaderDial.closeBodyOf(new CompletableFuture<>());
    }

    // ------------------------------------------------------------------------------------------------------------
    // PostBatchHandler.forwardBatchToLeader
    // ------------------------------------------------------------------------------------------------------------

    /** Part (1) of the issue on the /batch forward: the non-streaming branch reads the leader's answer buffered. */
    @Test
    void aBufferedBatchForwardWhoseLeaderStallsInsideItsBodyIsAnswered504() throws Exception {
      try (final FakeLeader leader = FakeLeader.scripted(out -> write(out, STALLED_BODY))) {
        final PostBatchHandler handler = handlerWith(config());

        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        final ExecutionResponse response = callWithin(() -> handler.forwardBatchToLeader(new HttpServerExchange(null),
            haPointingAt(leader.address()), "mydb", rootUser(), "application/x-ndjson", emptyBody(), false), leader);
        watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s deadline from the unbounded ofString() body read");

        assertThat(response.getCode()).isEqualTo(504);
        assertThat(new JSONObject(response.getResponse()).getString("error")).contains(leader.address());
        assertThat(leader.awaitConnectionClosedByClient(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS)).isTrue();
      }
    }

    /**
     * With the request timeout taken off the streamed forward, the wait for the leader's first line must still be
     * bounded: this is the #7526 guarantee, on the encoding it had not been driven through before.
     */
    @Test
    void aStreamingBatchForwardWhoseLeaderNeverAnswersIsStillAnswered504() throws Exception {
      try (final FakeLeader leader = FakeLeader.scripted(out -> {
        // headers never come
      })) {
        final PostBatchHandler handler = handlerWith(config());

        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        final ExecutionResponse response = callWithin(() -> handler.forwardBatchToLeader(new HttpServerExchange(null),
            haPointingAt(leader.address()), "mydb", rootUser(), "application/x-ndjson", emptyBody(), true), leader);
        watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s bound on the wait for the leader's response headers");

        assertThat(response.getCode()).isEqualTo(504);
        assertThat(new JSONObject(response.getResponse()).getString("error")).contains(leader.address());
      }
    }

    /**
     * Part (2) of the issue: a streamed load that takes several budgets in total but is never silent for one is a load
     * that is working, and is relayed whole. On JDK 26+ the request timeout covered the whole streamed body and cut
     * this off after one budget.
     */
    @Test
    void aStreamedAnswerLongerThanTheBudgetButNeverSilentIsRelayedWhole() throws Exception {
      final List<String> sent = new ArrayList<>();
      for (int i = 0; i < 12; i++)
        sent.add("{\"type\":\"progress\",\"verticesCreated\":" + i + "}");
      sent.add("{\"type\":\"summary\"}");

      try (final FakeLeader leader = FakeLeader.scripted(out -> {
        write(out, "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nTransfer-Encoding: chunked\r\n\r\n");
        for (final String line : sent) {
          writeChunk(out, line + "\n");
          sleep(BUDGET_MS / 4);
        }
        write(out, "0\r\n\r\n");
      })) {
        startFollowerRelayingTo(leader.address());

        assertThat(readFollowerStream()).as("about 3 budgets in total, never one of silence").isEqualTo(sent);
      }
    }

    // ------------------------------------------------------------------------------------------------------------

    private static ContextConfiguration config() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 5_000L);
      cfg.setValue(GlobalConfiguration.HA_PROXY_BATCH_READ_TIMEOUT, BUDGET_MS);
      return cfg;
    }

    private static PostBatchHandler handlerWith(final ContextConfiguration cfg) {
      final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, cfg);
      final HttpServer httpServer = HTTP_SERVERS.of(server);
      return new PostBatchHandler(httpServer);
    }

    private static HAServerPlugin haPointingAt(final String leaderAddress) {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getLeaderAddress()).thenReturn(leaderAddress);
      when(ha.getClusterToken()).thenReturn("test-token");
      return ha;
    }

    private static ServerSecurityUser rootUser() {
      return TestServerHelper.securityUser("root");
    }

    private static PostBatchHandler.CountingInputStream emptyBody() {
      return new PostBatchHandler.CountingInputStream(new HttpServerExchange(null), new ByteArrayInputStream(new byte[0]));
    }

    /**
     * Runs {@code call} on its own thread and fails - rather than hanging the suite - when it has not returned within
     * the hang detector. Closing the leader is what releases a forward that never gave up.
     */
    private static <T> T callWithin(final Callable<T> call, final FakeLeader leader) throws Exception {
      final FutureTask<T> task = new FutureTask<>(call);
      final Thread thread = new Thread(task, "issue8325-forward");
      thread.setDaemon(true);
      thread.start();
      try {
        return task.get(HANG_DETECT_MS, TimeUnit.MILLISECONDS);
      } catch (final TimeoutException e) {
        leader.close();
        throw new AssertionError("The forward was still waiting on the stalled leader after " + HANG_DETECT_MS
            + " ms: nothing bounds the read of the leader's body", e);
      } catch (final ExecutionException e) {
        if (e.getCause() instanceof Exception cause)
          throw cause;
        throw e;
      }
    }

    private void startFollowerRelayingTo(final String leaderAddress) {
      final PostBatchHandler handler = handlerWith(config());
      final HAServerPlugin ha = haPointingAt(leaderAddress);
      final ServerSecurityUser user = rootUser();

      follower = Undertow.builder()
          .addHttpListener(0, "127.0.0.1")
          .setHandler(new BlockingHandler(exchange -> {
            final ExecutionResponse response = handler.forwardBatchToLeader(exchange, ha, "mydb", user,
                "application/x-ndjson", new PostBatchHandler.CountingInputStream(exchange, exchange.getInputStream()), true);
            if (response != null) {
              exchange.setStatusCode(response.getCode());
              exchange.getOutputStream().write(response.getResponse().getBytes(StandardCharsets.UTF_8));
            }
          }))
          .build();
      follower.start();
    }

    private List<String> readFollowerStream() throws IOException {
      final InetSocketAddress address = (InetSocketAddress) follower.getListenerInfo().get(0).getAddress();
      final HttpURLConnection conn = (HttpURLConnection) URI.create(
          "http://127.0.0.1:" + address.getPort() + "/api/v1/batch/mydb").toURL().openConnection();
      conn.setRequestProperty("Accept", "application/x-ndjson");
      conn.setReadTimeout(CLIENT_READ_MS);
      // An upload with a declared length, as a real load has: the forward then relays it with that length too
      // (issue #5618), which is the request shape the JDK 26 response timer cuts the streamed answer off on.
      final byte[] upload = "{\"@type\":\"vertex\",\"type\":\"V\"}\n".getBytes(StandardCharsets.UTF_8);
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setFixedLengthStreamingMode(upload.length);
      try (final OutputStream out = conn.getOutputStream()) {
        out.write(upload);
      }
      assertThat(conn.getResponseCode()).isEqualTo(200);

      final List<String> lines = new ArrayList<>();
      try (final BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
        for (String line = in.readLine(); line != null; line = in.readLine())
          lines.add(line);
      } catch (final SocketTimeoutException e) {
        throw new AssertionError("The follower relayed " + lines + " and then held the stream open", e);
      } catch (final IOException e) {
        // A stream cut short is an ending too; the assertion on what arrived says whether it was cut.
      }
      return lines;
    }

    private static void write(final OutputStream out, final String data) throws IOException {
      out.write(data.getBytes(StandardCharsets.UTF_8));
      out.flush();
    }

    private static void writeChunk(final OutputStream out, final String data) throws IOException {
      final byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
      out.write((Integer.toHexString(bytes.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
      out.write(bytes);
      out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
      out.flush();
    }

    private static void sleep(final long ms) {
      try {
        Thread.sleep(ms);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
