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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.timeseries.TimeSeriesWalkCoarsenedException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.exception.QueryAdmissionException;
import com.arcadedb.exception.QueryHeapBudgetExceededException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ForwardedRequestIdContext;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.LeaderForwardContext;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.UnstartedHttpServers;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.IdempotencyCache;
import com.arcadedb.server.http.RetryLaterException;
import com.arcadedb.server.http.handler.PostBatchHandler.CountingInputStream;
import com.arcadedb.server.http.handler.openapi.CoreApiSpec;
import com.arcadedb.server.security.ServerSecurityUser;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.undertow.server.HttpServerExchange;
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
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static com.arcadedb.server.http.HttpSessionManager.ARCADEDB_SESSION_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Retries and replay protection of HTTP requests: the auto-commit retry, idempotency keys of forwarded writes, and the {@code Retry-After} a retryable refusal carries.
 * <p>
 * One {@code @Nested} class per regression, each keeping the fixture and the Javadoc of the issue it pins.
 */
class ForwardRetryIdempotencyTest {

  /**
   * Regression test for the first half of issue #6201: the {@code retries} the auto-commit wrapper passes to the
   * engine had no effect on anything raised while the command ran.
   * <p>
   * {@code DatabaseAbstractHandler} used to convert <b>every</b> exception the handler raised into a
   * {@code TransactionException} from inside the transaction block. {@code LocalDatabase.transaction(...)} decides
   * what to retry by type - {@code catch (NeedRetryException | DuplicatedKeyException)} - so a conflict raised
   * during {@code execute()} arrived there already wrapped, matched neither arm, fell into the generic
   * {@code catch (Throwable)} and propagated on the first attempt. Only a conflict detected by the wrapper's own
   * {@code commit()} was ever retried.
   * <p>
   * The wrapper now rethrows a {@link RuntimeException} unchanged and wraps only the checked exceptions
   * {@code TransactionScope} cannot declare, which is what these tests pin.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AutoCommitRetry {
    private static final String DATABASE_PATH = "./target/databases/ForwardRetryIdempotencyTest-AutoCommitRetry";

    private DatabaseInternal database;

    @BeforeEach
    void createDatabase() {
      final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
      if (factory.exists())
        factory.open().drop();
      database = (DatabaseInternal) factory.create();
    }

    @AfterEach
    void dropDatabase() {
      if (database != null && database.isOpen())
        database.drop();
    }

    @Test
    void aConflictRaisedWhileTheCommandRunsIsRetried() {
      final AtomicInteger attempts = new AtomicInteger();
      final CountingHandler handler = new CountingHandler(attempts,
          attempt -> attempt < 3
              ? new ConcurrentModificationException("Record #1:1 modified by another transaction")
              : null);

      final AtomicReference<ExecutionResponse> response = new AtomicReference<>();
      handler.executeInTransaction(null, null, database, null, response, 5);

      assertThat(attempts.get())
          .as("the engine retries a NeedRetryException raised inside the transaction block, so the command runs "
              + "again until it succeeds")
          .isEqualTo(3);
      assertThat(response.get()).isNotNull();
      assertThat(response.get().getCode()).isEqualTo(200);
    }

    /**
     * A duplicated key is deterministic and fails identically on every attempt, so the engine allows it exactly one
     * retry (#4959) - the retry only exists to disambiguate a concurrency-induced duplicate. What matters here is
     * that it is classified at all: before the fix it was wrapped out of the classification and never retried once.
     */
    @Test
    void aDuplicatedKeyRaisedWhileTheCommandRunsIsRetriedOnce() {
      final AtomicInteger attempts = new AtomicInteger();
      final CountingHandler handler = new CountingHandler(attempts,
          attempt -> new DuplicatedKeyException("Idx", "[1]", new RID(1, 1)));

      assertThatThrownBy(
          () -> handler.executeInTransaction(null, null, database, null, new AtomicReference<>(), 5))
          .isInstanceOf(DuplicatedKeyException.class);

      assertThat(attempts.get()).isEqualTo(2);
    }

    /**
     * The other half of #6201: the exception reaching the HTTP error mapping must still be the typed one, so the
     * mapping answers 503 rather than the opaque 500 the wrapper produced. Exhausting the retries must not put the
     * wrapper back on.
     */
    @Test
    void anExhaustedConflictPropagatesAsItselfAndNotWrapped() {
      final AtomicInteger attempts = new AtomicInteger();
      final CountingHandler handler = new CountingHandler(attempts,
          attempt -> new ConcurrentModificationException("Record #1:1 modified by another transaction"));

      assertThatThrownBy(
          () -> handler.executeInTransaction(null, null, database, null, new AtomicReference<>(), 3))
          .isInstanceOf(ConcurrentModificationException.class);

      assertThat(attempts.get()).isEqualTo(3);
    }

    /**
     * A CHECKED exception is the one case that still needs the wrapper: {@code TransactionScope.execute()} declares
     * none, so it cannot travel out of the block as itself.
     */
    @Test
    void aCheckedExceptionIsStillWrapped() {
      final CountingHandler handler = new CountingHandler(new AtomicInteger(), attempt -> null) {
        @Override
        protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
            final Database database, final JSONObject payload) throws IOException {
          throw new IOException("simulated I/O failure");
        }
      };

      assertThatThrownBy(
          () -> handler.executeInTransaction(null, null, database, null, new AtomicReference<>(), 1))
          .isInstanceOf(TransactionException.class)
          .hasMessage("Error on executing command")
          .hasCauseInstanceOf(IOException.class);
    }

    /** Decides what the n-th (1-based) attempt throws; {@code null} means "succeed". */
    @FunctionalInterface
    private interface AttemptOutcome {
      RuntimeException failureFor(int attempt);
    }

    /**
     * A handler whose {@code execute()} counts its invocations and fails according to {@code outcome}, standing in
     * for a command losing an MVCC race on a record it saves.
     */
    private static class CountingHandler extends DatabaseAbstractHandler {
      private final AtomicInteger  attempts;
      private final AttemptOutcome outcome;

      private CountingHandler(final AtomicInteger attempts, final AttemptOutcome outcome) {
        super(null);
        this.attempts = attempts;
        this.outcome = outcome;
      }

      @Override
      protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
          final Database database, final JSONObject payload) throws Exception {
        final RuntimeException failure = outcome.failureFor(attempts.incrementAndGet());
        if (failure != null)
          throw failure;
        return new ExecutionResponse(200, "{}");
      }
    }
  }

  /**
   * Issue #8323, the auto-commit retry: {@code DatabaseAbstractHandler.executeInTransaction} runs {@code execute()}
   * again after a conflict, and every forward to the leader that attempt takes is a retry of a forward the previous
   * attempt took. It must reuse the ordinal that forward had, not draw the next one - otherwise the retried write gets
   * a fresh key on the leader and is never deduplicated against the first attempt.
   */
  @Nested
  class AutoCommitRetryRequestId {
    private static final String DATABASE_PATH = "./target/databases/ForwardRetryIdempotencyTest-AutoCommitRetryRequestId";

    private DatabaseInternal database;

    @BeforeEach
    void createDatabase() {
      final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
      if (factory.exists())
        factory.open().drop();
      database = (DatabaseInternal) factory.create();
    }

    @AfterEach
    void dropDatabase() {
      ForwardedRequestIdContext.clear();
      if (database != null && database.isOpen())
        database.drop();
    }

    @Test
    void aRetriedAttemptForwardsUnderTheSameOrdinalsAsTheFirst() {
      ForwardedRequestIdContext.set("client-8323");
      final List<List<Integer>> ordinalsPerAttempt = new ArrayList<>();
      final AtomicInteger attempts = new AtomicInteger();

      final DatabaseAbstractHandler handler = new DatabaseAbstractHandler(null) {
        @Override
        protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
            final Database database, final JSONObject payload) {
          // Two forwards per attempt, standing in for RaftReplicatedDatabase.forwardCommandToLeaderViaRaft.
          ordinalsPerAttempt.add(List.of(ForwardedRequestIdContext.nextForwardOrdinal(),
              ForwardedRequestIdContext.nextForwardOrdinal()));
          if (attempts.incrementAndGet() == 1)
            throw new ConcurrentModificationException("Record #1:1 modified by another transaction");
          return new ExecutionResponse(200, "{}");
        }
      };

      handler.executeInTransaction(null, null, database, null, new AtomicReference<>(), 3);

      assertThat(attempts.get()).isEqualTo(2);
      assertThat(ordinalsPerAttempt).containsExactly(List.of(1, 2), List.of(1, 2));
    }
  }

  /**
   * Issue #8323, the receiving side: the leader folds the ordinal of a follower's second, third, ... SQL write forward
   * within one client request into its idempotency key - and only when the request carries a valid cluster token. A
   * client can send any {@code X-Request-Id} it likes, so the ordinal must not be something a client can reproduce: an
   * ordinal encoded in the id itself ({@code order#2}) would let a request with that id share a key with another
   * request's second forward of the same statement, and one of the two writes would be answered from the other's cache.
   */
  @Nested
  class ForwardOrdinalKey extends BaseGraphServerTest {

    private static final String      CLUSTER_TOKEN = "issue8323-cluster-token";
    private static final String      TYPE          = "Issue8323OrdinalDoc";
    private static final HttpClient  HTTP          = HttpClient.newHttpClient();

    @BeforeEach
    void setClusterToken() {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, CLUSTER_TOKEN);
      final Database db = getServerDatabase(0, getDatabaseName());
      db.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS");
    }

    @AfterEach
    void clearClusterToken() {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, "");
    }

    @Test
    void theOrdinalIsPartOfTheKeyAndNoClientIdCanReproduceIt() {
      final String first = AbstractServerHttpHandler.buildIdempotencyKey("order", "POST", "/api/v1/command/db", "db",
          "{\"command\":\"x\"}", null, 0);
      final String second = AbstractServerHttpHandler.buildIdempotencyKey("order", "POST", "/api/v1/command/db", "db",
          "{\"command\":\"x\"}", null, 2);
      final String clientSuffixed = AbstractServerHttpHandler.buildIdempotencyKey("order#2", "POST", "/api/v1/command/db",
          "db", "{\"command\":\"x\"}", null, 0);

      assertThat(second).isNotEqualTo(first);
      assertThat(second).isNotEqualTo(clientSuffixed);
      // Ordinal 0 leaves the key exactly as it was before the ordinal existed.
      assertThat(first).isEqualTo(AbstractServerHttpHandler.buildIdempotencyKey("order", "POST", "/api/v1/command/db", "db",
          "{\"command\":\"x\"}", null));
    }

    /**
     * The bodies are digested with no length prefix, so a client can spell the tagged ordinal section inside its own
     * body. For an ordinal whose low byte is zero (256, 512, ...) that spelling used to reproduce the forward's key input
     * byte for byte, because the section then ended in the same zero byte every untagged key ends in.
     */
    @Test
    void aBodyThatSpellsTheOrdinalSectionDoesNotReproduceTheForwardsKey() {
      final String body = "{\"command\":\"x\"}";
      for (final int ordinal : new int[] { 2, 256, 512, 65_536 }) {
        final String forward = AbstractServerHttpHandler.buildIdempotencyKey("order", "POST", "/api/v1/command/db", "db",
            body, null, ordinal);
        // The untagged input is <...body> 0x00; the tagged one is <...body> 0x00 0x00 "forward-ordinal" b3 b2 b1 b0 ...
        final String crafted = body + '\0' + '\0' + "forward-ordinal" + (char) ((ordinal >>> 24) & 0xFF)
            + (char) ((ordinal >>> 16) & 0xFF) + (char) ((ordinal >>> 8) & 0xFF);
        final String client = AbstractServerHttpHandler.buildIdempotencyKey("order", "POST", "/api/v1/command/db", "db",
            crafted, null, 0);

        assertThat(client).as("ordinal %d", ordinal).isNotEqualTo(forward);
      }
    }

    @Test
    void aSecondForwardUnderTheTokenExecutesAndIsItselfDeduplicated() throws Exception {
      final String body = insert("forward");

      assertThat(send(body, "issue8323-fwd", true, null).statusCode()).isEqualTo(200);
      assertThat(send(body, "issue8323-fwd", true, "2").statusCode()).isEqualTo(200);
      assertThat(countTagged("forward")).as("the second forward is a separate write").isEqualTo(2L);

      // A retry of that second forward is answered from the cache.
      assertThat(send(body, "issue8323-fwd", true, "2").statusCode()).isEqualTo(200);
      assertThat(countTagged("forward")).isEqualTo(2L);
    }

    /** From a client the header means nothing: the request keys exactly as it would without it. */
    @Test
    void anOrdinalWithoutTheClusterTokenIsIgnored() throws Exception {
      final String body = insert("client");

      assertThat(send(body, "issue8323-client", false, null).statusCode()).isEqualTo(200);
      assertThat(send(body, "issue8323-client", false, "2").statusCode()).isEqualTo(200);

      assertThat(countTagged("client")).as("the second request is a retry of the first, whatever header it adds").isEqualTo(1L);
    }

    /** The collision an ordinal folded into the id would have had: a client id that spells the suffix is its own key. */
    @Test
    void aClientIdThatSpellsAnOrdinalDoesNotReplayASecondForward() throws Exception {
      final String body = insert("suffix");

      assertThat(send(body, "issue8323-order", true, null).statusCode()).isEqualTo(200);
      assertThat(send(body, "issue8323-order", true, "2").statusCode()).isEqualTo(200);
      assertThat(send(body, "issue8323-order#2", false, null).statusCode()).isEqualTo(200);

      assertThat(countTagged("suffix")).isEqualTo(3L);
    }

    // ---------------------------------------------------------------------------------------------

    private static String insert(final String tag) {
      return new JSONObject().put("language", "sql").put("command", "INSERT INTO " + TYPE + " SET tag = '" + tag + "'")
          .toString();
    }

    private long countTagged(final String tag) {
      return ((Number) getServerDatabase(0, getDatabaseName())
          .query("sql", "SELECT count(*) AS cnt FROM " + TYPE + " WHERE tag = ?", tag).next().getProperty("cnt")).longValue();
    }

    private HttpResponse<String> send(final String body, final String requestId, final boolean asPeer, final String ordinal)
        throws Exception {
      final HttpRequest.Builder builder = HttpRequest.newBuilder()
          .uri(URI.create(getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName())))
          .header("Content-Type", "application/json")
          .header(IdempotencyCache.HEADER_REQUEST_ID, requestId);
      if (asPeer)
        builder.header("X-ArcadeDB-Cluster-Token", CLUSTER_TOKEN).header("X-ArcadeDB-Forwarded-User", "root");
      else
        builder.header("Authorization", "Basic " + Base64.getEncoder()
            .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      if (ordinal != null)
        builder.header(ForwardedRequestIdContext.FORWARD_ORDINAL_HEADER, ordinal);
      return HTTP.send(builder.POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
  }

  /**
   * Issue #8343: a follower that forwarded a request to the leader relayed only the leader's status and body, so the
   * {@code Retry-After} of the leader's {@code 409} (a retry whose {@code X-Request-Id} twin is still executing, issue
   * #8324) and of its {@code 503} (a snapshot install) never reached a client that talked to a follower.
   * <p>
   * The leader is a real loopback HTTP listener, so the headers asserted are the ones that crossed a socket.
   */
  @Nested
  class RetryAfterRelay {
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
    }

    /** The reported path: the leader's in-flight 409 relayed by the buffered forward of an admin command. */
    @Test
    void theLeadersInFlightConflictReachesTheClientWithItsRetryAfter() throws Exception {
      leader.answer(409, "{\"error\":\"A request with the same X-Request-Id is still executing\"}",
          Map.of("Retry-After", "5"));
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config()));

      final ExecutionResponse response = forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"),
          "/api/v1/server", "{\"command\":\"restore database x\"}", true);

      assertThat(response.getCode()).isEqualTo(409);
      assertThat(response.getHeader("Retry-After"))
          .as("the machine-readable back-off a client talking to the leader gets")
          .isEqualTo("5");
    }

    /** The leader's snapshot-install 503, the other answer that carries a Retry-After. */
    @Test
    void theLeadersServiceUnavailableReachesTheClientWithItsRetryAfter() throws Exception {
      leader.answer(503, "{\"error\":\"Server is installing a snapshot, please retry\"}", Map.of("Retry-After", "5"));
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config()));

      final ExecutionResponse response = forwarder.forwardIfReplica(exchange("/api/v1/server/users"), user("root"),
          "/api/v1/server/users", "{\"name\":\"bob\"}");

      assertThat(response.getCode()).isEqualTo(503);
      assertThat(response.getHeader("Retry-After")).isEqualTo("5");
    }

    /**
     * A client that asked for a progress stream and got a refusal instead: the non-stream branch of the streaming relay
     * reads the answer whole, and must relay its Retry-After exactly as the buffered forward does.
     */
    @Test
    void aRefusalAnsweredToAStreamRequestAlsoKeepsItsRetryAfter() throws Exception {
      leader.answer(409, "{\"error\":\"A request with the same X-Request-Id is still executing\"}",
          Map.of("Retry-After", "7"));
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config()));
      forwarder.streamTargetFactory = exchange -> contentType -> {
        throw new AssertionError("no stream may be started for an answer that is not one");
      };
      final HttpServerExchange exchange = exchange("/api/v1/server");
      exchange.getRequestHeaders().put(new HttpString("Accept"), "text/event-stream");

      final ExecutionResponse response = forwarder.forwardIfReplica(exchange, user("root"), "/api/v1/server",
          "{\"command\":\"restore database x\"}", true, true);

      assertThat(response.getCode()).isEqualTo(409);
      assertThat(response.getHeader("Retry-After")).isEqualTo("7");
    }

    /**
     * An allow-list, not a copy: the leader's other response headers describe the leader's own exchange and must not be
     * passed off as this node's.
     */
    @Test
    void onlyTheAllowListedHeadersCrossTheHop() throws Exception {
      final Map<String, String> headers = new LinkedHashMap<>();
      headers.put("Set-Cookie", "leader-session=abc");
      headers.put("X-Request-Id", "the-leaders-own-correlation-id");
      headers.put("X-Custom", "leader-only");
      leader.answer(200, "{\"result\":\"ok\"}", headers);
      final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(), config()));

      final ExecutionResponse response = forwarder.forwardIfReplica(exchange("/api/v1/server"), user("root"),
          "/api/v1/server", "{}");

      assertThat(response.getCode()).isEqualTo(200);
      assertThat(response.getHeader("Retry-After")).isNull();
      assertThat(response.getHeader("Set-Cookie")).isNull();
      assertThat(response.getHeader("X-Request-Id")).isNull();
      assertThat(response.getHeader("X-Custom")).isNull();
    }

    /** The batch relay, the other follower-to-leader forward in this module, shares the same allow-list. */
    @Test
    void theBatchRelayKeepsTheLeadersRetryAfterToo() throws Exception {
      leader.answer(503, "{\"error\":\"Server is installing a snapshot, please retry\"}", Map.of("Retry-After", "5"));
      final PostBatchHandler handler = new PostBatchHandler(httpServerWith(ha(), config()));

      final ExecutionResponse response = handler.forwardBatchToLeader(exchange("/api/v1/batch/graph"), ha(), "graph",
          user("root"), "application/json", body("{\"@type\":\"vertex\"}\n"), false);

      assertThat(response.getCode()).isEqualTo(503);
      assertThat(response.getHeader("Retry-After")).isEqualTo("5");
    }

    /** What makes the relayed header reach the client: {@code send} writes it onto the exchange's response headers. */
    @Test
    void theHeadersOfAResponseAreWrittenOntoTheExchange() {
      final ExecutionResponse response = new ExecutionResponse(409, "{}").setHeader("Retry-After", "5");
      final HeaderMap target = new HeaderMap();

      response.applyHeaders(target);

      assertThat(target.getFirst("Retry-After")).isEqualTo("5");
    }

    @Test
    void settingAHeaderTwiceKeepsTheLastValueAndAResponseWithoutHeadersWritesNone() {
      final ExecutionResponse response = new ExecutionResponse(409, "{}").setHeader("Retry-After", "5")
          .setHeader("retry-after", "9");
      assertThat(response.getHeader("RETRY-AFTER")).isEqualTo("9");

      final HeaderMap target = new HeaderMap();
      new ExecutionResponse(200, "{}").applyHeaders(target);
      assertThat(target.size()).isZero();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Fixtures
    // ---------------------------------------------------------------------------------------------------------------

    private HAServerPlugin ha() {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.isLeader()).thenReturn(false);
      when(ha.getLeaderAddress()).thenReturn(leader.address());
      when(ha.getLeaderName()).thenReturn("leader");
      when(ha.getLeaderPeerId()).thenReturn("peer-leader");
      when(ha.getLocalPeerId()).thenReturn("peer-follower");
      when(ha.getClusterToken()).thenReturn("issue8343-cluster-token");
      return ha;
    }

    private static ContextConfiguration config() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_PROXY_READ_TIMEOUT, 30_000L);
      cfg.setValue(GlobalConfiguration.HA_PROXY_LONG_COMMAND_TIMEOUT, 30_000L);
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

    /** The leader: a loopback HTTP listener that answers every request with the status, body and headers set last. */
    private static final class StubLeader implements AutoCloseable {
      private final    com.sun.net.httpserver.HttpServer server;
      private final    ExecutorService                   executor = Executors.newCachedThreadPool();
      private volatile int                               status   = 200;
      private volatile String                            body     = "{}";
      private volatile Map<String, String>               headers  = Map.of();

      StubLeader() throws IOException {
        server = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 16);
        server.setExecutor(executor);
        server.createContext("/", exchange -> {
          try {
            exchange.getRequestBody().readAllBytes();
            final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            headers.forEach((name, value) -> exchange.getResponseHeaders().add(name, value));
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
          } finally {
            exchange.close();
          }
        });
        server.start();
      }

      void answer(final int status, final String body, final Map<String, String> headers) {
        this.status = status;
        this.body = body;
        this.headers = headers;
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
   * Issue #8344: the replay protection {@code X-Request-Id} gives every POST route (issue #5023), and the {@code 409} +
   * {@code Retry-After} a retry receives while the first request is still executing (issue #8324), were not in the
   * OpenAPI document, so a client generated from it knew of neither.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RequestReplayIsSpecified {
    private static final String REQUEST_ID_PARAM_REF = "#/components/parameters/RequestIdParam";

    private final OpenAPI openAPI = new OpenApiSpecGenerator(null).generateSpec();

    @Test
    void everyReplayedPostRouteDeclaresTheRequestIdAndTheInFlightConflict() {
      openAPI.getPaths().forEach((path, item) -> {
        final Operation post = item.getPost();
        if (post == null || path.equals("/api/v1/batch/{database}"))
          return;
        assertThat(refs(post.getParameters())).as("POST %s", path).contains(REQUEST_ID_PARAM_REF);
        final ApiResponse conflict = post.getResponses().get("409");
        assertThat(conflict).as("POST %s", path).isNotNull();
        assertThat(conflict.getDescription()).as("POST %s", path).contains("still executing");
        assertThat(conflict.getHeaders()).as("POST %s", path).containsKeys("Retry-After", IdempotencyCache.HEADER_REQUEST_ID);
      });
    }

    @Test
    void theCommandRouteIsCovered() {
      // Guards the loop above against passing vacuously on a document with no matching route
      final Operation command = openAPI.getPaths().get("/api/v1/command/{database}").getPost();
      assertThat(refs(command.getParameters())).contains(REQUEST_ID_PARAM_REF);
      assertThat(command.getResponses()).containsKey("409");
    }

    @Test
    void theBulkLoadRouteIsNotPromisedAReplayItDoesNotGive() {
      // Its body is never buffered, so it cannot be part of the replay key (issue #7381)
      final Operation batch = openAPI.getPaths().get("/api/v1/batch/{database}").getPost();
      assertThat(refs(batch.getParameters())).doesNotContain(REQUEST_ID_PARAM_REF);
    }

    @Test
    void aReadIsNotPromisedAReplay() {
      openAPI.getPaths().forEach((path, item) -> {
        if (item.getGet() != null)
          assertThat(refs(item.getGet().getParameters())).as("GET %s", path).doesNotContain(REQUEST_ID_PARAM_REF);
      });
    }

    @Test
    void theParameterDescribesTheReplaySemantics() {
      final Parameter param = openAPI.getComponents().getParameters().get("RequestIdParam");
      assertThat(param.getName()).isEqualTo(IdempotencyCache.HEADER_REQUEST_ID);
      assertThat(param.getIn()).isEqualTo("header");
      assertThat(param.getRequired()).isFalse();
      assertThat(param.getDescription())
          .contains(GlobalConfiguration.HA_IDEMPOTENCY_CACHE_TTL_MS.getKey())
          .contains(GlobalConfiguration.HA_IDEMPOTENCY_CACHE_MAX_BODY_BYTES.getKey())
          .contains("2xx")
          .contains("409");
    }

    private static List<String> refs(final List<Parameter> parameters) {
      return parameters == null ? List.of() : parameters.stream().map(Parameter::get$ref).filter(r -> r != null).toList();
    }
  }

  /**
   * Issue #8347, the receiving side: a follower forwards a client's SQL write to the leader with a body it rebuilt from
   * the statement, so the leader keyed the forward differently from the retry the client may send it directly - with its
   * own body, carrying 'serializer', 'limit' or simply other whitespace - and ran that retry as a second write. The
   * forward now names the key the client's own request has, and the leader claims it beside the forward's own key.
   * <p>
   * The server here plays the leader. A request that carries the cluster token plays the follower's forward; one that
   * carries Basic credentials plays the client. The type has no unique index, so a second execution is a second record.
   */
  @Nested
  class DirectRetryClientKey extends BaseGraphServerTest {

    private static final String     CLUSTER_TOKEN = "issue8347-cluster-token";
    private static final String     TYPE          = "Issue8347Doc";
    private static final HttpClient HTTP          = HttpClient.newHttpClient();

    @BeforeEach
    void setClusterToken() {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, CLUSTER_TOKEN);
      getServerDatabase(0, getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS");
    }

    @AfterEach
    void clearClusterToken() {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, "");
    }

    /** The entry point of the issue: the answer to a follower-forwarded write was lost, and the client asks the leader. */
    @Test
    void aRetrySentStraightToTheLeaderIsReplayedFromTheForward() throws Exception {
      final String requestId = "issue8347-direct-after-forward";
      final String clientBody = clientBody("direct-after-forward");

      final HttpResponse<String> forward = send(forwardBody("direct-after-forward"), requestId, true,
          clientKey(requestId, clientBody));
      assertThat(forward.statusCode()).as("forward, body: %s", forward.body()).isEqualTo(200);

      final HttpResponse<String> retry = send(clientBody, requestId, false, null);
      assertThat(retry.statusCode()).as("retry, body: %s", retry.body()).isEqualTo(200);

      assertThat(countTagged("direct-after-forward")).as("the retry must be replayed, not executed a second time")
          .isEqualTo(1L);
      assertThat(retry.body()).as("the replay is the forward's answer").isEqualTo(forward.body());
    }

    /**
     * The other order: the client's first attempt went straight to the leader and its answer was lost, and the retry
     * reached a follower, which forwards it with the rebuilt body.
     */
    @Test
    void aForwardOfARetryIsReplayedFromTheDirectAttempt() throws Exception {
      final String requestId = "issue8347-forward-after-direct";
      final String clientBody = clientBody("forward-after-direct");

      final HttpResponse<String> direct = send(clientBody, requestId, false, null);
      assertThat(direct.statusCode()).as("direct, body: %s", direct.body()).isEqualTo(200);

      final HttpResponse<String> forward = send(forwardBody("forward-after-direct"), requestId, true,
          clientKey(requestId, clientBody));
      assertThat(forward.statusCode()).as("forward, body: %s", forward.body()).isEqualTo(200);

      assertThat(countTagged("forward-after-direct")).isEqualTo(1L);
      assertThat(forward.body()).isEqualTo(direct.body());
    }

    /** The control: without the key the two bodies really are two keys, which is the defect the issue reports. */
    @Test
    void withoutTheClientKeyTheTwoBodiesAreTwoWrites() throws Exception {
      final String requestId = "issue8347-control";

      assertThat(send(forwardBody("control"), requestId, true, null).statusCode()).isEqualTo(200);
      assertThat(send(clientBody("control"), requestId, false, null).statusCode()).isEqualTo(200);

      assertThat(countTagged("control")).isEqualTo(2L);
    }

    /**
     * From a client the header means nothing: honored, it would let a request settle - and be answered from - the key of
     * a different request with the same id.
     */
    @Test
    void aClientKeySentWithoutTheClusterTokenIsIgnored() throws Exception {
      final String requestId = "issue8347-client-sent";
      final String firstBody = clientBody("client-sent-1");

      assertThat(send(firstBody, requestId, false, null).statusCode()).isEqualTo(200);
      assertThat(send(clientBody("client-sent-2"), requestId, false, clientKey(requestId, firstBody)).statusCode())
          .isEqualTo(200);

      assertThat(countTagged("client-sent-2")).as("a different request, whatever key it names").isEqualTo(1L);
    }

    /** A value that is not a key a peer could have computed is dropped, and the forward keys as it did before. */
    @Test
    void aMalformedClientKeyFromAPeerIsIgnored() throws Exception {
      final String requestId = "issue8347-malformed";

      assertThat(send(forwardBody("malformed"), requestId, true, "not-a-key").statusCode()).isEqualTo(200);
      assertThat(send(clientBody("malformed"), requestId, false, null).statusCode()).isEqualTo(200);

      assertThat(countTagged("malformed")).isEqualTo(2L);
    }

    // ---------------------------------------------------------------------------------------------

    private static String insert(final String tag) {
      return "INSERT INTO " + TYPE + " SET tag = '" + tag + "'";
    }

    /** What the follower's forward carries: the statement alone, rebuilt by RaftReplicatedDatabase. */
    private static String forwardBody(final String tag) {
      return new JSONObject().put("language", "sql").put("command", insert(tag)).toString();
    }

    /** What a client sends: its own fields, in its own order and spacing. */
    private static String clientBody(final String tag) {
      return "{ \"serializer\": \"record\", \"limit\": 20, \"language\": \"sql\", \"command\": \"" + insert(tag) + "\" }";
    }

    /** The key the follower computed for the client's request, exactly as this server computes it for a direct one. */
    private String clientKey(final String requestId, final String clientBody) {
      return AbstractServerHttpHandler.buildIdempotencyKey(requestId, "POST", "/command/" + getDatabaseName(),
          getDatabaseName(), clientBody, null, 0);
    }

    private long countTagged(final String tag) {
      final Database db = getServerDatabase(0, getDatabaseName());
      return ((Number) db.query("sql", "SELECT count(*) AS cnt FROM " + TYPE + " WHERE tag = ?", tag).next()
          .getProperty("cnt")).longValue();
    }

    private HttpResponse<String> send(final String body, final String requestId, final boolean asPeer,
        final String clientKey) throws Exception {
      final HttpRequest.Builder builder = HttpRequest.newBuilder()
          .uri(URI.create(getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName())))
          .header("Content-Type", "application/json")
          .header(IdempotencyCache.HEADER_REQUEST_ID, requestId);
      if (asPeer)
        builder.header("X-ArcadeDB-Cluster-Token", CLUSTER_TOKEN).header("X-ArcadeDB-Forwarded-User", "root");
      else
        builder.header("Authorization", "Basic " + Base64.getEncoder()
            .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      if (clientKey != null)
        builder.header(ForwardedRequestIdContext.CLIENT_KEY_HEADER, clientKey);
      return HTTP.send(builder.POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
  }

  /**
   * Issues #12440 and #12441: a commit that loses a race with a concurrent commit is answered 503 with the retryable
   * {@link ConcurrentModificationException}, and the answer carried no {@code Retry-After}, unlike the other retryable refusals that
   * say how long to wait. The status stays 503, the one every client reads as "retry the request as it is" (the Java client never
   * retries a 409), and every retryable 503 now carries a {@code Retry-After}: 0 for a refusal that clears by itself, a conflict
   * whose competing commit has already landed, and 1 for a server too busy to start the request.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CommitConflictRetryAfter extends BaseGraphServerTest {
    private final HttpClient client = HttpClient.newHttpClient();

    /** The reproduction of the reports: the transaction that read the record first commits second, at REPEATABLE_READ. */
    @Test
    void aCommitThatLosesARaceIs503WithRetryAfter() throws Exception {
      command(null, "CREATE DOCUMENT TYPE T");
      command(null, "INSERT INTO T SET id = 1, v = 0");

      final String first = begin("{\"isolationLevel\":\"REPEATABLE_READ\"}");
      final String second = begin(null);

      assertThat(command(first, "SELECT v FROM T WHERE id = 1").statusCode()).isEqualTo(200);
      assertThat(command(second, "UPDATE T SET v = 42 WHERE id = 1").statusCode()).isEqualTo(200);
      assertThat(commit(second).statusCode()).isEqualTo(204);
      assertThat(command(first, "UPDATE T SET v = 5 WHERE id = 1").statusCode()).isEqualTo(200);

      final HttpResponse<String> lost = commit(first);
      assertThat(lost.statusCode()).isEqualTo(503);
      assertThat(lost.headers().firstValue("Retry-After")).as("the back-off a client reads from the status line's headers").hasValue("0");
      final JSONObject body = new JSONObject(lost.body());
      assertThat(body.getString("exception")).isEqualTo(ConcurrentModificationException.class.getName());

      // the conflict cleared by itself: the losing write, re-issued at once in a new transaction, commits
      final String retry = begin(null);
      assertThat(command(retry, "UPDATE T SET v = 5 WHERE id = 1").statusCode()).isEqualTo(200);
      assertThat(commit(retry).statusCode()).isEqualTo(204);
      final HttpResponse<String> read = command(null, "SELECT v FROM T WHERE id = 1");
      assertThat(new JSONObject(read.body()).getJSONArray("result").getJSONObject(0).getInt("v")).isEqualTo(5);
    }

    /** Every retryable 503 says how long to wait, and a RetryLaterException keeps the wait it names. */
    @Test
    void everyRetryable503CarriesRetryAfter() {
      final ClassifyingHandler handler = new ClassifyingHandler(getServer(0).getHttpServer());

      assertRetryable(handler.classifyError(new ConcurrentModificationException("conflict")), "0");
      // the same conflict as the engine raises it at commit time, under the transaction wrapper
      assertRetryable(handler.classifyError(new TransactionException("Error on commit", new ConcurrentModificationException("x"))), "0");
      assertRetryable(handler.classifyError(new LockTimeoutException("lock")), "0");
      assertRetryable(handler.classifyError(new TimeSeriesWalkCoarsenedException("coarsened")), "0");
      // a server too busy to start the request: back off
      assertRetryable(handler.classifyError(new QueryHeapBudgetExceededException("heap")), "1");
      assertRetryable(handler.classifyError(new QueryAdmissionException("queue")), "1");
      assertRetryable(handler.classifyError(new RetryLaterException("snapshot", 7)), "7");
    }

    /** The published contract of the commit route lists the answer (issue #12440), with the header it carries. */
    @Test
    void theCommitRouteDocumentsTheConflict() {
      final OpenAPI openAPI = new OpenAPI();
      openAPI.setPaths(new Paths());
      openAPI.setComponents(new Components());
      new CoreApiSpec().contribute(openAPI);

      final ApiResponse conflict = openAPI.getPaths().get("/api/v1/commit/{database}").getPost().getResponses().get("503");
      assertThat(conflict).isNotNull();
      assertThat(conflict.getDescription()).contains("ConcurrentModificationException");
      assertThat(conflict.getHeaders()).containsKey("Retry-After");
    }

    private static void assertRetryable(final AbstractServerHttpHandler.ErrorClassification classification, final String retryAfter) {
      assertThat(classification.status()).as(classification.reported().getClass().getSimpleName()).isEqualTo(503);
      assertThat(classification.retryAfter()).as(classification.reported().getClass().getSimpleName()).isEqualTo(retryAfter);
    }

    private String begin(final String body) throws Exception {
      final HttpResponse<String> response = send("/api/v1/begin/" + getDatabaseName(), null, body);
      assertThat(response.statusCode()).isEqualTo(204);
      return response.headers().firstValue(ARCADEDB_SESSION_ID).orElseThrow().trim();
    }

    private HttpResponse<String> commit(final String session) throws Exception {
      return send("/api/v1/commit/" + getDatabaseName(), session, null);
    }

    private HttpResponse<String> command(final String session, final String sql) throws Exception {
      return send("/api/v1/command/" + getDatabaseName(), session,
          new JSONObject().put("language", "sql").put("command", sql).toString());
    }

    private HttpResponse<String> send(final String path, final String session, final String body) throws Exception {
      final HttpRequest.Builder request = HttpRequest.newBuilder(new URI(getServerHttpUrl(path)))
          .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(
              StandardCharsets.UTF_8)))
          .POST(body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
      if (body != null)
        request.header("Content-Type", "application/json");
      if (session != null)
        request.header(ARCADEDB_SESSION_ID, session);
      return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static final class ClassifyingHandler extends AbstractServerHttpHandler {
      private ClassifyingHandler(final HttpServer httpServer) {
        super(httpServer);
      }

      @Override
      protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
        throw new UnsupportedOperationException();
      }
    }
  }
}
