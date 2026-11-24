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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TransactionOutcomeUnknownException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ClusterCapabilityNotReadyException;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.LeaderForwardContext;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.UnstartedHttpServers;
import com.arcadedb.server.http.FakeLeader;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.RetryLaterException;
import com.arcadedb.server.http.handler.PostBatchHandler.CountingInputStream;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.Undertow;
import io.undertow.io.Sender;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.BlockingHandler;
import io.undertow.util.HeaderMap;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLHandshakeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * What a forward answers when the leader, or the node that receives it, refuses it or cannot be reached.
 * <p>
 * One {@code @Nested} class per regression, each keeping the fixture and the Javadoc of the issue it pins.
 */
class LeaderForwardRefusalTest {

  /**
   * Issue #7511 on the HTTP transport: a group or API-token change refused because a peer of the cluster cannot
   * decode the Raft entry it would be replicated as answers {@code 409 Conflict}.
   * <p>
   * The status is the contract, not a cosmetic detail. Falling through to the generic {@code 500} would tell every
   * HTTP client, driver and load balancer that the SERVER broke - inviting a blind retry against another node,
   * where the request is refused identically - about a condition in which nothing was submitted, nothing changed,
   * and the remedy is to finish the rolling upgrade. It pairs with the {@code FAILED_PRECONDITION} the same refusal
   * gets on gRPC, which {@code GrpcRefusalStatusMappingTest} pins in the {@code grpcw} module.
   * <p>
   * The exception is raised inside the {@code ha-raft} module, which the server module cannot depend on, so - as
   * {@code HttpErrorStatusMappingTest.CommittedRemotely} does for the same reason - the real
   * {@code AbstractServerHttpHandler} catch chain is driven with a handler whose {@code execute()} throws it.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class ClusterNotReadyHttpStatus {
    @RegisterExtension
    static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

    private static final String CAPABILITY = "security-groups-entry";
    private static final String LAGGING    = "arcadedb2";
    private static final String REFUSAL    = "Refusing to replicate the group document: peer(s) [" + LAGGING
        + "] have not advertised the '" + CAPABILITY + "' capability. Peer '" + LAGGING
        + "': the capability route answered HTTP 404.";

    private static ClusterCapabilityNotReadyException refusal() {
      return new ClusterCapabilityNotReadyException(REFUSAL, CAPABILITY, List.of(LAGGING));
    }

    @Test
    void aClusterNotReadyRefusalMapsTo409RatherThanAGeneric500() {
      final HandledResponse response = handle(refusal());

      assertThat(response.statusCode)
          .as("a refusal in which nothing was submitted is a conflict with the cluster's state, not a server fault "
              + "(body=%s)", response.body)
          .isEqualTo(409);

      final JSONObject json = new JSONObject(response.body);
      assertThat(json.getString("error")).isEqualTo("Cluster is not ready for this operation");
      assertThat(json.getString("exception")).isEqualTo(ClusterCapabilityNotReadyException.class.getName());
      assertThat(json.getString("detail"))
          .as("the peer holding the cluster back is the only thing an operator can act on")
          .contains("arcadedb2");
    }

    /**
     * Guards the catch ORDER. The refusal extends {@code OperationNotAvailableException}, which extends
     * {@code CommandExecutionException} - so an arm placed after the generic ones would be unreachable and this
     * would answer 500. The two are not the same answer: "this server cannot do this at all" is a permanent
     * property of the deployment, while this clears itself when the last node is upgraded.
     */
    @Test
    void aPlainOperationNotAvailableRefusalIsNotSweptIntoTheSame409() {
      final HandledResponse response = handle(
          new ServerControlPlane.OperationNotAvailableException("High availability is not enabled on this server"));

      assertThat(response.statusCode).isNotEqualTo(409);
    }

    /** The 409 must survive the wrapping a command planner or the auto-commit wrapper puts around it. */
    @Test
    void aWrappedClusterNotReadyRefusalKeepsThe409() {
      final HandledResponse response = handle(new CommandExecutionException("Error on command execution",
          refusal()));

      assertThat(response.statusCode).isEqualTo(409);
      assertThat(new JSONObject(response.body).getString("exception"))
          .isEqualTo(ClusterCapabilityNotReadyException.class.getName());
    }

    /**
     * In {@code production} mode {@code buildErrorBody} conceals {@code detail}, where the refusal's prose lives. A
     * 409 that reaches an operator as a bare "Cluster is not ready for this operation" has told them nothing they
     * can act on, which is the silence #7511 exists to end - so the lagging peer and the capability ride
     * {@code exceptionArgs}, which every mode emits (PR #7555 review).
     * <p>
     * The per-peer REASON is deliberately absent from that field and is asserted absent: it is free-form text built
     * from a probe failure and can carry a host, a port or a JDK exception message, which is the class of content
     * {@code detail} is concealed for in the first place.
     */
    @Test
    void inProductionModeThePeerAndTheCapabilitySurviveInExceptionArgs() {
      final HandledResponse response = handle(refusal(), "production");

      assertThat(response.statusCode).isEqualTo(409);

      final JSONObject json = new JSONObject(response.body);
      assertThat(json.has("detail"))
          .as("production conceals the free-form cause chain, as it does for every other error")
          .isFalse();
      assertThat(json.getString("exceptionArgs"))
          .as("and what is left must still name the node holding the cluster back")
          .contains(LAGGING)
          .contains(CAPABILITY);
      assertThat(json.getString("exceptionArgs"))
          .as("but not the probe-failure text, which is exactly what production mode conceals detail for")
          .doesNotContain("404");
    }

    /** The bounded rendering stays bounded: a large cluster cannot turn one error body into a peer dump. */
    @Test
    void theExceptionArgsAreBoundedOnALargeCluster() {
      final List<String> manyPeers = List.of("p1", "p2", "p3", "p4", "p5", "p6", "p7");
      final String args = new ClusterCapabilityNotReadyException(REFUSAL, CAPABILITY, manyPeers).toExceptionArgs();

      assertThat(args).startsWith(CAPABILITY + "|p1,p2,p3,p4,p5");
      assertThat(args).endsWith(",+2 more");
      assertThat(args).doesNotContain("p6").doesNotContain("p7");
    }

    private record HandledResponse(int statusCode, String body) {
    }

    private HandledResponse handle(final RuntimeException toThrow) {
      return handle(toThrow, "development");
    }

    private HandledResponse handle(final RuntimeException toThrow, final String serverMode) {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.SERVER_MODE, serverMode);

      final ArcadeDBServer server = TestServerHelper.unstartedServer("test", configuration);

      final HttpServer httpServer = HTTP_SERVERS.of(server);

      final Sender sender = mock(Sender.class);
      final HttpServerExchange exchange = mock(HttpServerExchange.class);
      final int[] statusCode = { 200 };
      when(exchange.setStatusCode(anyInt())).thenAnswer(invocation -> {
        statusCode[0] = invocation.getArgument(0);
        return exchange;
      });
      when(exchange.getStatusCode()).thenAnswer(invocation -> statusCode[0]);
      when(exchange.getRequestHeaders()).thenReturn(new HeaderMap());
      when(exchange.getResponseHeaders()).thenReturn(new HeaderMap());
      when(exchange.getRequestMethod()).thenReturn(Methods.POST);
      when(exchange.getRelativePath()).thenReturn("/server/groups");
      when(exchange.getResponseSender()).thenReturn(sender);

      new ThrowingHandler(httpServer, toThrow).handleRequest(exchange);

      final ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
      verify(sender).send(body.capture());
      return new HandledResponse(statusCode[0], body.getValue());
    }

    /** Handler whose execute() throws, standing in for a group save refused by the cluster-capability interlock. */
    private static final class ThrowingHandler extends AbstractServerHttpHandler {
      private final RuntimeException toThrow;

      private ThrowingHandler(final HttpServer httpServer, final RuntimeException toThrow) {
        super(httpServer);
        this.toThrow = toThrow;
      }

      @Override
      protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
          final JSONObject payload) {
        throw toThrow;
      }

      @Override
      public boolean isRequireAuthentication() {
        // Skip the Authorization machinery: this test targets the error-mapping catch chain only.
        return false;
      }
    }
  }

  /**
   * Issue #8355: the refusal a node installing a snapshot answers every request with, before any handler runs, names its
   * exception and carries its back-off in {@code exceptionArgs}. That is what lets a follower that forwarded a SQL write
   * to it tell this refusal - nothing ran - apart from an anonymous 503 that something between the two nodes may have
   * produced after the write ran, and answer its own client 503 + {@code Retry-After} only for the former.
   */
  @Nested
  class SnapshotInstallRefusalBody {
    @RegisterExtension
    static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

    @Test
    void theSnapshotInstallRefusalIsATypedServiceUnavailableWithItsRetryAfter() {
      final Sender sender = mock(Sender.class);
      final HttpServerExchange exchange = mock(HttpServerExchange.class);
      final int[] statusCode = { 200 };
      when(exchange.setStatusCode(anyInt())).thenAnswer(invocation -> {
        statusCode[0] = invocation.getArgument(0);
        return exchange;
      });
      when(exchange.getRequestHeaders()).thenReturn(new HeaderMap());
      final HeaderMap responseHeaders = new HeaderMap();
      when(exchange.getResponseHeaders()).thenReturn(responseHeaders);
      when(exchange.getRequestMethod()).thenReturn(Methods.POST);
      when(exchange.getRelativePath()).thenReturn("/command/graph");
      when(exchange.getResponseSender()).thenReturn(sender);

      final ArcadeDBServer server = TestServerHelper.unstartedServer("test", new ContextConfiguration());
      server.setSnapshotInstallInProgress(true);
      final HttpServer httpServer = HTTP_SERVERS.of(server);

      final boolean[] executed = { false };
      new AbstractServerHttpHandler(httpServer) {
        @Override
        protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
            final JSONObject payload) {
          executed[0] = true;
          return new ExecutionResponse(200, "{}");
        }

        @Override
        public boolean isRequireAuthentication() {
          return false;
        }
      }.handleRequest(exchange);

      final ArgumentCaptor<String> sent = ArgumentCaptor.forClass(String.class);
      verify(sender).send(sent.capture());
      final JSONObject body = new JSONObject(sent.getValue());

      assertThat(executed[0]).as("the request is refused before any handler runs").isFalse();
      assertThat(statusCode[0]).isEqualTo(503);
      assertThat(responseHeaders.getFirst("Retry-After"))
          .isEqualTo(String.valueOf(RetryLaterException.SNAPSHOT_INSTALL_RETRY_AFTER_SECONDS));
      assertThat(body.getString("error")).isEqualTo(RetryLaterException.SNAPSHOT_INSTALL_REFUSAL);
      assertThat(body.getString("exception")).isEqualTo(RetryLaterException.class.getName());
      assertThat(body.getString("exceptionArgs"))
          .isEqualTo(String.valueOf(RetryLaterException.SNAPSHOT_INSTALL_RETRY_AFTER_SECONDS));
      assertThat(body.getString("detail"))
          .as("a client reading the typed body's detail - RemoteHttpComponent does - keeps the reason")
          .isEqualTo(RetryLaterException.SNAPSHOT_INSTALL_REFUSAL);
    }
  }

  /**
   * Issue #8393: a one-hop refusal the receiving node cannot classify - the forwarding peer sent no intended leader id,
   * because leadership changed while it resolved the leader's address or because it runs a build that predates the
   * header (a rolling upgrade), or this node cannot name itself - is almost always an election. It used to be answered
   * with the non-retryable 400 meant for a misconfigured {@code arcadedb.ha.serverList}, and it used up the one warning
   * that misconfiguration gets. It is now retryable (503, no leader address) and leaves the latch alone; only the
   * refusal that PROVES the address named the wrong node keeps the 400 and the warning.
   * <p>
   * The third refusal site, the SQL forward in {@code RaftReplicatedDatabase}, lives in the ha-raft module and is driven
   * end to end by {@code HaForwardedWriteIdempotencyIT}.
   */
  @Nested
  class UndeterminedForwardRefusal {
    private static final String LOCAL_PEER  = "peer-follower";
    private static final String LEADER_PEER = "peer-leader";

    @RegisterExtension
    static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

    @AfterEach
    void clear() {
      LeaderForwardContext.clear();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // POST /api/v1/server and the /server/* routes: LeaderCommandForwarder
    // ---------------------------------------------------------------------------------------------------------------

    /** Path 2 of the issue: a peer on an older build sends no leader id at all. */
    @Test
    void aServerCommandFromAPeerThatNamedNoLeaderIsRetryable() {
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(LOCAL_PEER)));
      LeaderForwardContext.markAlreadyForwarded();

      final ServerIsNotTheLeaderException refusal = serverCommandRefusal(forwarder);

      assertThat(refusal.getLeaderAddress())
          .as("a named leader is what AbstractServerHttpHandler answers 400; an unnamed one is the retryable 503")
          .isNull();
      assertThat(refusal.getMessage()).contains("already forwarded").contains("retry");
      assertThat(forwarder.misconfigurationWarned())
          .as("an unclassifiable refusal is usually an election and must not spend the misconfiguration's notice")
          .isFalse();
    }

    /** This node cannot name itself: nothing proves the address was wrong either. */
    @Test
    void aServerCommandRefusedByANodeThatCannotNameItselfIsRetryable() {
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(null)));
      LeaderForwardContext.markAlreadyForwarded(LEADER_PEER);

      final ServerIsNotTheLeaderException refusal = serverCommandRefusal(forwarder);

      assertThat(refusal.getLeaderAddress()).isNull();
      assertThat(forwarder.misconfigurationWarned()).isFalse();
    }

    /**
     * The latch is kept for the fault it was written for: a run of unclassifiable refusals first, then the one that
     * proves the address wrong, which must still be logged and still be the configuration error.
     */
    @Test
    void theMisconfigurationStillGetsItsNoticeAfterUndeterminedRefusals() {
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(LOCAL_PEER)));
      for (int i = 0; i < 3; i++) {
        LeaderForwardContext.markAlreadyForwarded();
        assertThat(serverCommandRefusal(forwarder).getLeaderAddress()).isNull();
      }
      assertThat(forwarder.misconfigurationWarned()).isFalse();

      LeaderForwardContext.markAlreadyForwarded(LEADER_PEER);
      final ServerIsNotTheLeaderException misidentified = serverCommandRefusal(forwarder);

      assertThat(misidentified.getLeaderAddress()).isEqualTo("leader");
      assertThat(misidentified.getMessage()).contains("does not identify")
          .contains(GlobalConfiguration.HA_SERVER_LIST.getKey());
      assertThat(forwarder.misconfigurationWarned()).isTrue();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // POST /api/v1/batch: PostBatchHandler
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void aBatchFromAPeerThatNamedNoLeaderIsRetryable() throws Exception {
      final HAServerPlugin ha = ha(LOCAL_PEER);
      final PostBatchHandler handler = new PostBatchHandler(httpServerWith(ha));
      LeaderForwardContext.markAlreadyForwarded();

      final ExecutionResponse refusal = batchRefusal(handler, ha);

      assertThat(refusal.getCode()).isEqualTo(503);
      final JSONObject body = new JSONObject(refusal.getResponse());
      assertThat(body.getString("error")).contains("already forwarded").contains("retry");
      assertThat(body.getString("exception", null))
          .as("typed like the other unnamed refusals, so a relaying follower recognizes it")
          .isEqualTo(ServerIsNotTheLeaderException.class.getName());
      assertThat(LeaderCommandForwarder.isUnnamedNotTheLeaderRefusal(refusal)).isTrue();
      assertThat(handler.misconfigurationWarned()).isFalse();
    }

    @Test
    void aBatchRefusalThatProvesTheAddressWrongIsStillTheConfigurationError() throws Exception {
      final HAServerPlugin ha = ha(LOCAL_PEER);
      final PostBatchHandler handler = new PostBatchHandler(httpServerWith(ha));
      LeaderForwardContext.markAlreadyForwarded();
      assertThat(batchRefusal(handler, ha).getCode()).isEqualTo(503);
      assertThat(handler.misconfigurationWarned()).isFalse();

      LeaderForwardContext.markAlreadyForwarded(LEADER_PEER);
      final ExecutionResponse misidentified = batchRefusal(handler, ha);

      assertThat(misidentified.getCode()).isEqualTo(400);
      assertThat(misidentified.getResponse()).contains("does not identify");
      assertThat(handler.misconfigurationWarned()).isTrue();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Fixtures
    // ---------------------------------------------------------------------------------------------------------------

    private static ServerIsNotTheLeaderException serverCommandRefusal(final LeaderCommandForwarder forwarder) {
      final ServerIsNotTheLeaderException refusal = catchThrowableOfType(ServerIsNotTheLeaderException.class,
          () -> forwarder.forwardIfReplica(exchange("/api/v1/server"), user(), "/api/v1/server", "{}", false));
      assertThat(refusal).as("an already-forwarded request on a non-leader must be refused in one hop").isNotNull();
      return refusal;
    }

    private static ExecutionResponse batchRefusal(final PostBatchHandler handler, final HAServerPlugin ha)
        throws Exception {
      return handler.forwardBatchToLeader(exchange("/api/v1/batch/graph"), ha, "graph", user(), "application/json",
          new CountingInputStream(new HttpServerExchange(null),
              new ByteArrayInputStream("{\"@type\":\"vertex\"}\n".getBytes(StandardCharsets.UTF_8))), false);
    }

    private static HAServerPlugin ha(final String localPeerId) {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.isLeader()).thenReturn(false);
      // Never dialled: every request in this class is refused before an address is resolved.
      when(ha.getLeaderAddress()).thenReturn("127.0.0.1:1");
      when(ha.getLeaderName()).thenReturn("leader");
      when(ha.getLeaderPeerId()).thenReturn(LEADER_PEER);
      when(ha.getLocalPeerId()).thenReturn(localPeerId);
      when(ha.getClusterToken()).thenReturn("issue8393-cluster-token");
      return ha;
    }

    private static HttpServer httpServerWith(final HAServerPlugin ha) {
      final ContextConfiguration cfg = new ContextConfiguration();
      final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, cfg);
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

    private static ServerSecurityUser user() {
      return TestServerHelper.securityUser("root");
    }
  }

  /**
   * Issue #8486: the two follower-to-leader forwarders of the {@code server} module - {@link LeaderCommandForwarder} for
   * server commands and the security REST routes, and {@link PostBatchHandler} for bulk loads - relayed the answer of a
   * leader that had just stepped down, "not the leader, and I cannot name one" (503), to the client at once. This node
   * still named that ex-leader, so a client retrying without back-off dialled it again through the same stale view, over
   * and over for a whole election timeout. {@code RaftReplicatedDatabase}'s forward of a SQL write holds the same refusal
   * back until its view moves (issue #8480); these two forwarders now do the same.
   * <p>
   * The leader is a real loopback HTTP listener. The follower's leader view is a counter-driven probe: it keeps naming
   * the ex-leader for a fixed number of reads after the refusal was answered, then names another node. Whether the
   * forwarder waited is read off that counter, never off the clock.
   */
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  @Nested
  class ForwardersHoldUnnamedLeaderRefusal {
    private static final String EX_LEADER  = "peer-leader";
    private static final String NEW_LEADER = "peer-new-leader";

    /** What the ex-leader's error pipeline answers for a {@code ServerIsNotTheLeaderException} with no leader address. */
    private static final String UNNAMED_REFUSAL = new JSONObject()
        .put("error", "Cannot execute command")
        .put("exception", ServerIsNotTheLeaderException.class.getName())
        .toString();

    @RegisterExtension
    static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

    private StubLeader leader;
    private LeaderView view;

    @BeforeEach
    void setUp() throws IOException {
      leader = new StubLeader();
      view = new LeaderView(leader);
    }

    @AfterEach
    void tearDown() {
      leader.close();
      LeaderForwardContext.clear();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // LeaderCommandForwarder
    // ---------------------------------------------------------------------------------------------------------------

    /** The reported path: a server command forwarded to an ex-leader that could name no successor. */
    @Test
    void aForwardedServerCommandHoldsTheExLeadersUnnamedRefusalUntilTheViewMoves() throws Exception {
      leader.answer(503, UNNAMED_REFUSAL);
      view.moveAfterReads(3);
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(20_000L)));

      final ExecutionResponse response = forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"),
          "/api/v1/server", "{\"command\":\"create database x\"}");

      assertThat(response.getCode()).as("the refusal is still relayed - the client's retry is what gets past it")
          .isEqualTo(503);
      assertThat(view.moved()).as("the refusal was held until this node stopped naming the ex-leader").isTrue();
      assertThat(view.readsAfterRefusal()).isGreaterThanOrEqualTo(4);
    }

    /** The security REST routes share the forwarder, so a user creation forwarded to an ex-leader waits the same way. */
    @Test
    void aForwardedSecurityRouteHoldsTheRefusalToo() throws Exception {
      leader.answer(503, UNNAMED_REFUSAL);
      view.moveAfterReads(2);
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(20_000L)));

      final ExecutionResponse response = forwarder.forwardIfReplica(exchange("/api/v1/server/users"), user("root"),
          "/api/v1/server/users", "{\"name\":\"bob\"}");

      assertThat(response.getCode()).isEqualTo(503);
      assertThat(view.moved()).isTrue();
    }

    /**
     * A client that asked for a progress stream and got the refusal instead: the non-stream branch of the streaming
     * relay reads the answer whole, and holds it exactly as the buffered forward does.
     */
    @Test
    void theRefusalAnsweredToAStreamRequestIsHeldToo() throws Exception {
      leader.answer(503, UNNAMED_REFUSAL);
      view.moveAfterReads(2);
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(20_000L)));
      forwarder.streamTargetFactory = exchange -> contentType -> {
        throw new AssertionError("no stream may be started for an answer that is not one");
      };
      final HttpServerExchange exchange = exchange("/api/v1/server");
      exchange.getRequestHeaders().put(new HttpString("Accept"), "text/event-stream");

      final ExecutionResponse response = forwarder.forwardIfReplica(exchange, user("root"), "/api/v1/server",
          "{\"command\":\"restore database x\"}", true, true);

      assertThat(response.getCode()).isEqualTo(503);
      assertThat(view.moved()).isTrue();
    }

    /** Bounded: a view that never moves releases the refusal after {@code arcadedb.ha.forwardLeaderWaitTimeoutMs}. */
    @Test
    void aViewThatNeverMovesReleasesTheRefusalAfterTheConfiguredWait() throws Exception {
      leader.answer(503, UNNAMED_REFUSAL);
      view.neverMove();
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(300L)));

      final ExecutionResponse response = forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"),
          "/api/v1/server", "{}");

      assertThat(response.getCode()).isEqualTo(503);
      assertThat(view.moved()).isFalse();
      assertThat(view.readsAfterRefusal()).as("it did poll the view before giving up").isGreaterThanOrEqualTo(2);
    }

    /** The setting at 0 restores the fail-fast relay, as it does for the leaderless wait it bounds. */
    @Test
    void aZeroWaitRelaysTheRefusalAtOnce() throws Exception {
      leader.answer(503, UNNAMED_REFUSAL);
      view.neverMove();
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(0L)));

      final ExecutionResponse response = forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"),
          "/api/v1/server", "{}");

      assertThat(response.getCode()).isEqualTo(503);
      assertThat(view.readsAfterRefusal()).isZero();
    }

    /** A refusal that names the leader is 400 and tells the client where to go: nothing to wait for. */
    @Test
    void aRefusalThatNamesTheLeaderIsNotHeld() throws Exception {
      leader.answer(400, new JSONObject().put("error", "Cannot execute command")
          .put("exception", ServerIsNotTheLeaderException.class.getName()).put("exceptionArgs", "10.0.0.3:2480").toString());
      view.neverMove();
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(20_000L)));

      final ExecutionResponse response = forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"),
          "/api/v1/server", "{}");

      assertThat(response.getCode()).isEqualTo(400);
      assertThat(view.readsAfterRefusal()).isZero();
    }

    /** Every other 503 - here the snapshot-install refusal - says nothing about who leads, and is relayed at once. */
    @Test
    void anotherServiceUnavailableIsNotHeld() throws Exception {
      leader.answer(503, "{\"error\":\"Server is installing a snapshot, please retry\"}");
      view.neverMove();
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(20_000L)));

      final ExecutionResponse response = forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"),
          "/api/v1/server", "{}");

      assertThat(response.getCode()).isEqualTo(503);
      assertThat(view.readsAfterRefusal()).isZero();
    }

    /** Without a stable id for the node dialled, there is no "view still names it" to wait out. */
    @Test
    void aForwardWithoutAStableIntendedLeaderIsNotHeld() throws Exception {
      leader.answer(503, UNNAMED_REFUSAL);
      view.noLeaderId();
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(20_000L)));

      final ExecutionResponse response = forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"),
          "/api/v1/server", "{}");

      assertThat(response.getCode()).isEqualTo(503);
      assertThat(view.readsAfterRefusal()).isZero();
    }

    /**
     * Issue #8709: leadership blinked while the leader's address was being resolved, so the two reads of the leader id do
     * not agree and no stable id is named. The node dialled is still the one that refused, and this node's view names it
     * again afterwards: the refusal is held on the id read before the dial, not relayed at once.
     */
    @Test
    void aForwardWhoseLeaderIdBlinkedDuringTheDialIsStillHeld() throws Exception {
      leader.answer(503, UNNAMED_REFUSAL);
      view.blinkDuringTheDial();
      view.moveAfterReads(3);
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config(20_000L)));

      final ExecutionResponse response = forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"),
          "/api/v1/server", "{}");

      assertThat(response.getCode()).isEqualTo(503);
      assertThat(view.moved()).isTrue();
      assertThat(view.readsAfterRefusal()).isGreaterThanOrEqualTo(4);
    }

    @Test
    void theHoldKeyFallsBackToTheIdReadBeforeThenAfterTheDial() {
      assertThat(LeaderForwardContext.holdLeaderId("stable", "before", "after")).isEqualTo("stable");
      assertThat(LeaderForwardContext.holdLeaderId(null, "before", "after")).isEqualTo("before");
      // Issue #8925: no leader named before the dial, one named after it - the after id is the only one the forward has.
      assertThat(LeaderForwardContext.holdLeaderId(LeaderForwardContext.stableLeaderId(null, "n2"), null, "n2")).isEqualTo("n2");
      assertThat(LeaderForwardContext.holdLeaderId(null, " ", "n2")).isEqualTo("n2");
      assertThat(LeaderForwardContext.holdLeaderId(null, null, " ")).isNull();
      assertThat(LeaderForwardContext.holdLeaderId(null, null, null)).isNull();
      assertThat(LeaderForwardContext.holdLeaderId(null, " ", null)).isNull();
      // A to B during the dial: keyed on A, so a view that already names B is not waited on
      assertThat(LeaderForwardContext.awaitLeaderViewMovedFrom(() -> NEW_LEADER,
          LeaderForwardContext.holdLeaderId(null, EX_LEADER, NEW_LEADER),
          20_000L, 1L)).isTrue();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // PostBatchHandler
    // ---------------------------------------------------------------------------------------------------------------

    /** The other reported path: a buffered batch forwarded to an ex-leader. */
    @Test
    void aForwardedBatchHoldsTheExLeadersUnnamedRefusalUntilTheViewMoves() throws Exception {
      leader.answer(503, UNNAMED_REFUSAL);
      view.moveAfterReads(3);
      final HAServerPlugin ha = ha();
      final PostBatchHandler handler = new PostBatchHandler(httpServerWith(ha, config(20_000L)));

      final ExecutionResponse response = handler.forwardBatchToLeader(exchange("/api/v1/batch/graph"), ha, "graph",
          user("root"), "application/json", body("{\"@type\":\"vertex\"}\n"), false);

      assertThat(response.getCode()).isEqualTo(503);
      assertThat(view.moved()).isTrue();
      assertThat(view.readsAfterRefusal()).isGreaterThanOrEqualTo(4);
    }

    /**
     * A client that negotiated the NDJSON stream and got the refusal: the stream relay's buffered fallback. That relay
     * arms its read deadline on the exchange's IO thread, so the follower is a real Undertow listener here.
     */
    @Test
    void aStreamingBatchHoldsTheRefusalToo() throws Exception {
      leader.answer(503, UNNAMED_REFUSAL);
      view.moveAfterReads(2);
      final HAServerPlugin ha = ha();
      final PostBatchHandler handler = new PostBatchHandler(httpServerWith(ha, config(20_000L)));
      final ServerSecurityUser user = user("root");
      final AtomicInteger relayedStatus = new AtomicInteger();

      final Undertow follower = Undertow.builder()
          .addHttpListener(0, "127.0.0.1")
          .setHandler(new BlockingHandler(exchange -> {
            final ExecutionResponse response = handler.forwardBatchToLeader(exchange, ha, "graph", user,
                "application/json", new CountingInputStream(exchange, exchange.getInputStream()), true);
            relayedStatus.set(response != null ? response.getCode() : 200);
            if (response != null) {
              exchange.setStatusCode(response.getCode());
              exchange.getOutputStream().write(response.getResponse().getBytes(StandardCharsets.UTF_8));
            }
          }))
          .build();
      follower.start();
      try {
        final int port = ((InetSocketAddress) follower.getListenerInfo().get(0).getAddress()).getPort();
        final HttpResponse<String> answer = HttpClient.newHttpClient().send(HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + "/api/v1/batch/graph"))
                .header("Accept", "application/x-ndjson")
                .POST(HttpRequest.BodyPublishers.ofString("{\"@type\":\"vertex\"}\n")).build(),
            HttpResponse.BodyHandlers.ofString());

        assertThat(answer.statusCode()).isEqualTo(503);
        assertThat(relayedStatus.get()).isEqualTo(503);
        assertThat(view.moved()).isTrue();
      } finally {
        follower.stop();
      }
    }

    @Test
    void aBatchRefusalThatNamesTheLeaderIsNotHeld() throws Exception {
      leader.answer(400, new JSONObject().put("error", "Cannot execute command")
          .put("exception", ServerIsNotTheLeaderException.class.getName()).put("exceptionArgs", "10.0.0.3:2480").toString());
      view.neverMove();
      final HAServerPlugin ha = ha();
      final PostBatchHandler handler = new PostBatchHandler(httpServerWith(ha, config(20_000L)));

      final ExecutionResponse response = handler.forwardBatchToLeader(exchange("/api/v1/batch/graph"), ha, "graph",
          user("root"), "application/json", body("{\"@type\":\"vertex\"}\n"), false);

      assertThat(response.getCode()).isEqualTo(400);
      assertThat(view.readsAfterRefusal()).isZero();
    }

    /**
     * What an ex-leader answers a batch a peer forwarded to it while leadership moved (issue #7603's LEADERSHIP_MOVED)
     * used to be a bare 503 with no exception class, which no forwarder could tell from any other 503. It now carries
     * the same {@code ServerIsNotTheLeaderException}, with no leader address, that the server-command route and the SQL
     * forward answer, so the follower that relayed it recognizes the refusal and holds it.
     */
    @Test
    void theExLeadersOwnRefusalOfAForwardedBatchIsTheRecognizableUnnamedRefusal() throws Exception {
      final HAServerPlugin exLeader = mock(HAServerPlugin.class);
      when(exLeader.isLeader()).thenReturn(false);
      when(exLeader.getLocalPeerId()).thenReturn(EX_LEADER);
      when(exLeader.getLeaderName()).thenReturn(null);
      final PostBatchHandler handler = new PostBatchHandler(httpServerWith(exLeader, config(20_000L)));
      LeaderForwardContext.markAlreadyForwarded(EX_LEADER);

      final ExecutionResponse refusal = handler.forwardBatchToLeader(exchange("/api/v1/batch/graph"), exLeader, "graph",
          user("root"), "application/json", body("{\"@type\":\"vertex\"}\n"), false);

      assertThat(refusal.getCode()).isEqualTo(503);
      final JSONObject json = new JSONObject(refusal.getResponse());
      assertThat(json.getString("exception", null)).isEqualTo(ServerIsNotTheLeaderException.class.getName());
      assertThat(json.has("exceptionArgs")).isFalse();
      assertThat(LeaderCommandForwarder.isUnnamedNotTheLeaderRefusal(refusal)).isTrue();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // The recognizer and the wait on their own
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void onlyAnUnnamedNotTheLeaderServiceUnavailableIsRecognized() {
      assertThat(LeaderCommandForwarder.isUnnamedNotTheLeaderRefusal(new ExecutionResponse(503, UNNAMED_REFUSAL))).isTrue();
      assertThat(LeaderCommandForwarder.isUnnamedNotTheLeaderRefusal(new ExecutionResponse(503, new JSONObject()
          .put("exception", ServerIsNotTheLeaderException.class.getName()).put("exceptionArgs", "").toString()))).isTrue();

      assertThat(LeaderCommandForwarder.isUnnamedNotTheLeaderRefusal(new ExecutionResponse(503, new JSONObject()
          .put("exception", ServerIsNotTheLeaderException.class.getName()).put("exceptionArgs", "h:2480").toString())))
          .as("a named refusal").isFalse();
      assertThat(LeaderCommandForwarder.isUnnamedNotTheLeaderRefusal(new ExecutionResponse(400, UNNAMED_REFUSAL)))
          .as("not a 503").isFalse();
      assertThat(LeaderCommandForwarder.isUnnamedNotTheLeaderRefusal(new ExecutionResponse(503, new JSONObject()
          .put("exception", "com.arcadedb.exception.NeedRetryException").toString()))).as("another exception").isFalse();
      assertThat(LeaderCommandForwarder.isUnnamedNotTheLeaderRefusal(new ExecutionResponse(503, "not json "
          + ServerIsNotTheLeaderException.class.getName()))).as("an unparsable body").isFalse();
      assertThat(LeaderCommandForwarder.isUnnamedNotTheLeaderRefusal(new ExecutionResponse(503, (String) null))).isFalse();
      assertThat(LeaderCommandForwarder.isUnnamedNotTheLeaderRefusal(null)).isFalse();
    }

    @Test
    void theWaitReturnsAsSoonAsTheViewNamesAnotherNodeOrNone() {
      final AtomicInteger reads = new AtomicInteger();
      assertThat(LeaderForwardContext.awaitLeaderViewMovedFrom(() -> reads.incrementAndGet() < 3 ? EX_LEADER : null,
          EX_LEADER, 20_000L, 1L)).as("an election in progress is a moved view").isTrue();
      assertThat(reads.get()).isEqualTo(3);

      reads.set(0);
      assertThat(LeaderForwardContext.awaitLeaderViewMovedFrom(() -> reads.incrementAndGet() < 3 ? EX_LEADER : NEW_LEADER,
          EX_LEADER, 20_000L, 1L)).isTrue();

      assertThat(LeaderForwardContext.awaitLeaderViewMovedFrom(() -> EX_LEADER, EX_LEADER, 50L, 1L)).isFalse();
      assertThat(LeaderForwardContext.awaitLeaderViewMovedFrom(() -> EX_LEADER, EX_LEADER, 0L, 1L)).isFalse();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Fixtures
    // ---------------------------------------------------------------------------------------------------------------

    private HAServerPlugin ha() {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.isLeader()).thenReturn(false);
      when(ha.getLeaderAddress()).thenReturn(leader.address());
      when(ha.getLeaderName()).thenReturn("leader");
      when(ha.getLeaderPeerId()).thenAnswer(invocation -> view.read());
      when(ha.getLocalPeerId()).thenReturn("peer-follower");
      when(ha.getClusterToken()).thenReturn("issue8486-cluster-token");
      return ha;
    }

    private static ContextConfiguration config(final long forwardLeaderWaitMs) {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_PROXY_READ_TIMEOUT, 30_000L);
      cfg.setValue(GlobalConfiguration.HA_PROXY_LONG_COMMAND_TIMEOUT, 30_000L);
      cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 5_000L);
      cfg.setValue(GlobalConfiguration.HA_PROXY_BATCH_READ_TIMEOUT, 30_000L);
      cfg.setValue(GlobalConfiguration.HA_FORWARD_LEADER_WAIT_TIMEOUT_MS, forwardLeaderWaitMs);
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

    /**
     * The follower's leader view. It names the ex-leader until the stub leader has answered, then keeps naming it for a
     * fixed number of further reads before naming another node - so "did the forwarder wait for the view to move" is
     * a count of reads, not a measurement of time.
     */
    private static final class LeaderView {
      private final    StubLeader    leader;
      private final    AtomicInteger readsAfterRefusal = new AtomicInteger();
      private volatile int           movesAfterReads   = Integer.MAX_VALUE;
      private volatile boolean       noLeaderId;
      private volatile boolean       blinkDuringTheDial;
      private final    AtomicInteger reads             = new AtomicInteger();
      private volatile boolean       moved;

      LeaderView(final StubLeader leader) {
        this.leader = leader;
      }

      void moveAfterReads(final int reads) {
        movesAfterReads = reads;
      }

      void neverMove() {
        movesAfterReads = Integer.MAX_VALUE;
      }

      void noLeaderId() {
        noLeaderId = true;
      }

      /** The second read - the one after the address was resolved - finds an election in progress. */
      void blinkDuringTheDial() {
        blinkDuringTheDial = true;
      }

      String read() {
        if (noLeaderId)
          return null;
        if (blinkDuringTheDial && reads.incrementAndGet() == 2)
          return null;
        if (leader.answered() == 0)
          return EX_LEADER;
        if (readsAfterRefusal.incrementAndGet() > movesAfterReads) {
          moved = true;
          return NEW_LEADER;
        }
        return EX_LEADER;
      }

      boolean moved() {
        return moved;
      }

      int readsAfterRefusal() {
        return readsAfterRefusal.get();
      }
    }

    /** The ex-leader: a loopback HTTP listener that answers every request with the status and body set last. */
    private static final class StubLeader implements AutoCloseable {
      private final    com.sun.net.httpserver.HttpServer server;
      private final    ExecutorService                   executor = Executors.newCachedThreadPool();
      private final    AtomicInteger                     answered = new AtomicInteger();
      private volatile int                               status   = 200;
      private volatile String                            body     = "{}";

      StubLeader() throws IOException {
        server = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 16);
        server.setExecutor(executor);
        server.createContext("/", exchange -> {
          try {
            exchange.getRequestBody().readAllBytes();
            final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            // Counted before the answer leaves, so every read of the view that follows it sees the refusal as sent.
            answered.incrementAndGet();
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
          } finally {
            exchange.close();
          }
        });
        server.start();
      }

      void answer(final int status, final String body) {
        this.status = status;
        this.body = body;
      }

      int answered() {
        return answered.get();
      }

      String address() {
        return "127.0.0.1:" + server.getAddress().getPort();
      }

      @Override
      public void close() {
        server.stop(0);
        executor.shutdownNow();
      }
    }
  }

  /**
   * Issue #11899: a load a follower forwards to the leader over {@code POST /api/v1/batch}, whose connection to the leader is
   * lost after the load went out, may have been applied by the leader. It was answered with an untyped 503, which tells a
   * client the load was refused before it ran; measured with a killed leader, 6 of 2,792 batches answered so were stored
   * whole. It is the outcome-unknown 504 with {@link TransactionOutcomeUnknownException} now, as a forwarded SQL write is
   * answered (issue #11887), and so is a leader that does not answer within the deadline.
   */
  @Nested
  class BatchForwardOutcomeUnknown {
    @RegisterExtension
    static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

    private static final String LOAD = "{\"@type\":\"vertex\",\"@class\":\"B\",\"k\":\"a\"}\n";

    @Test
    void aLeaderConnectionLostAfterTheLoadWasSentIsAnOutcomeUnknown() throws Exception {
      assertOutcomeUnknownWhenTheLeaderDropsTheLoad(false);
    }

    @Test
    void theStreamingEncodingAnswersTheSameBeforeAnyLineWasRelayed() throws Exception {
      assertOutcomeUnknownWhenTheLeaderDropsTheLoad(true);
    }

    @Test
    void aLeaderThatDoesNotAnswerInTimeIsAnOutcomeUnknown() throws Exception {
      try (final FakeLeader leader = FakeLeader.draining()) {
        final ExecutionResponse response = forward(leader, 1_000L, false);

        assertThat(response.getCode()).isEqualTo(504);
        final JSONObject body = new JSONObject(response.getResponse());
        assertThat(body.getString("exception")).isEqualTo(TransactionOutcomeUnknownException.class.getName());
        assertThat(body.getString("error")).contains(leader.address()).contains("may have been applied");
      }
    }

    @Test
    void aTlsHandshakeFailureStaysTheRetryable503() throws Exception {
      // No load byte left the node when the handshake failed, so this must not become the outcome-unknown 504
      final HttpClient tls = mock(HttpClient.class);
      when(tls.send(any(), any())).thenThrow(new SSLHandshakeException("handshake failed"));
      when(tls.sendAsync(any(), any())).thenReturn(CompletableFuture.failedFuture(new SSLHandshakeException("handshake failed")));

      final ContextConfiguration cfg = new ContextConfiguration();
      final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, cfg);
      final PostBatchHandler handler = new PostBatchHandler(HTTP_SERVERS.of(server));

      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getLeaderAddress()).thenReturn("leader.example.com:2480");
      when(ha.getLeaderHttpsAddress()).thenReturn("leader.example.com:2490");
      when(ha.getPeerHttpsClient()).thenReturn(tls);
      when(ha.getClusterToken()).thenReturn("test-token");

      final HttpServerExchange exchange = new HttpServerExchange(null);
      final PostBatchHandler.CountingInputStream body = new PostBatchHandler.CountingInputStream(exchange,
          new ByteArrayInputStream(LOAD.getBytes(StandardCharsets.UTF_8)));
      final ExecutionResponse response = handler.forwardBatchToLeader(exchange, ha, "mydb", TestServerHelper.securityUser("root"),
          "application/x-ndjson", body, false);

      assertThat(response.getCode()).as("body=%s", response.getResponse()).isEqualTo(503);
      assertThat(response.getResponse()).contains("handshake failed").doesNotContain(TransactionOutcomeUnknownException.class.getName());
    }

    private static void assertOutcomeUnknownWhenTheLeaderDropsTheLoad(final boolean streaming) throws Exception {
      // Reads the whole request head, then closes the connection without a byte of answer: a leader killed mid-load
      try (final FakeLeader leader = FakeLeader.scripted(out -> out.close())) {
        final ExecutionResponse response = forward(leader, 30_000L, streaming);

        assertThat(leader.awaitRequestReceived(5, TimeUnit.SECONDS)).as("the leader read the forwarded load").isTrue();
        assertThat(response.getCode()).as("body=%s", response.getResponse()).isEqualTo(504);
        final JSONObject body = new JSONObject(response.getResponse());
        assertThat(body.getString("exception")).isEqualTo(TransactionOutcomeUnknownException.class.getName());
        assertThat(body.getString("error")).contains(leader.address()).contains("may have been applied");
      }
    }

    private static ExecutionResponse forward(final FakeLeader leader, final long batchReadTimeoutMs, final boolean streaming)
        throws Exception {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 5_000L);
      cfg.setValue(GlobalConfiguration.HA_PROXY_BATCH_READ_TIMEOUT, batchReadTimeoutMs);
      final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, cfg);
      final PostBatchHandler handler = new PostBatchHandler(HTTP_SERVERS.of(server));

      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getLeaderAddress()).thenReturn(leader.address());
      when(ha.getClusterToken()).thenReturn("test-token");

      final HttpServerExchange exchange = new HttpServerExchange(null);
      final PostBatchHandler.CountingInputStream body = new PostBatchHandler.CountingInputStream(exchange,
          new ByteArrayInputStream(LOAD.getBytes(StandardCharsets.UTF_8)));
      return handler.forwardBatchToLeader(exchange, ha, "mydb", TestServerHelper.securityUser("root"), "application/x-ndjson",
          body, streaming);
    }
  }
}
