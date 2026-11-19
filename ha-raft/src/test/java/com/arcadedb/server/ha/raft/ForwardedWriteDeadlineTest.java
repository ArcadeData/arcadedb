/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.http.FakeLeader;
import com.arcadedb.utility.StallAwareStopwatch;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.http.HttpClient;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static com.arcadedb.utility.SubclassMocks.mock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/**
 * The deadlines of a forward to the leader: a leader that never answers, or stalls inside its body, is given up on within a bound.
 */
class ForwardedWriteDeadlineTest {
  // ---- #7527 ----
  // Regression test for issues #7527/#7543: {@code RaftReplicatedDatabase}'s SQL write forward to the leader
  // (the private {@code forwardCommandToLeaderViaRaft}) used a bare {@code HttpClient.newHttpClient()} with no
  // connect timeout and built the request with no {@code .timeout(...)}, so a leader that accepted the
  // connection and never answered parked the calling thread until the OS tore the socket down.
  //
  // These tests drive the private method directly - reaching it through a live cluster would need one, and the
  // behaviour under test is entirely local to the forward itself - the same reflection approach
  // {@code StepDownFailureHandlingTest} in this package already uses for a private method of this
  // same class.

  /** The tripwire between "the deadline fired" and "the call is unbounded" (minutes, without the fix). */
  private static final long GAVE_UP_BOUND_MS = 30_000L;

  private static Method forwardMethod() throws NoSuchMethodException {
    final Method m = RaftReplicatedDatabase.class.getDeclaredMethod("forwardCommandToLeaderViaRaft",
        String.class, String.class, Map.class, Object[].class);
    m.setAccessible(true);
    return m;
  }

  private static RaftReplicatedDatabase databaseWith(final ArcadeDBServer server, final RaftHAServer raft) {
    // The command budget is the DATABASE's arcadedb.command.timeout (issue #8313): the database here reads the same
    // configuration the server does, as one opened with no settings of its own would.
    final ContextConfiguration databaseConfiguration = server.getConfiguration();
    final LocalDatabase local = mock(LocalDatabase.class);
    when(local.getConfiguration()).thenReturn(databaseConfiguration);
    return new RaftReplicatedDatabase(server, local, raft);
  }

  private static ArcadeDBServer serverWith(final ContextConfiguration cfg) {
    final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, cfg);
    return server;
  }

  private static RaftHAServer raftPointingAt(final String leaderHttpAddress) {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leaderHttpAddress(leaderHttpAddress);
    raft.clusterToken("test-token");
    return raft;
  }

  private static <T extends ArcadeDBException> T invokeAndUnwrap(final RaftReplicatedDatabase db,
      final ContextConfiguration cfg, final Class<T> expectedType) throws Exception {
    try {
      forwardMethod().invoke(db, "sql", "insert into V set a = 1", null, new Object[0]);
      throw new AssertionError("expected the forward to throw");
    } catch (final InvocationTargetException e) {
      assertThat(e.getCause()).isInstanceOf(expectedType);
      return expectedType.cast(e.getCause());
    }
  }

  // Regression for #7527: a leader that accepts and never answers is given up on with A non retryable exception
  @Test
  void aLeaderThatAcceptsAndNeverAnswersIsGivenUpOnWithANonRetryableException() throws Exception {
    try (final FakeLeader leader = FakeLeader.silent()) {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 5_000L);
      cfg.setValue(GlobalConfiguration.HA_PROXY_COMMAND_TIMEOUT, 1_000L);

      final RaftReplicatedDatabase db = databaseWith(serverWith(cfg), raftPointingAt(leader.address()));

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      // TransactionException, NOT NeedRetryException (review finding on PR #7650): the leader accepted the
      // connection, so it may already have applied this non-idempotent write before the response was lost -
      // the outcome is unknown, and NeedRetryException here would let RemoteDatabase.transaction's automatic
      // retry double-apply an already-committed write. A connect failure (the other tests below) has no such
      // ambiguity: the command provably never left this node.
      final TransactionException e = invokeAndUnwrap(db, cfg, TransactionException.class);
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS,
          "a 1s forward deadline from the unbounded wait a leader that never answers used to produce");

      assertThat(e).isNotInstanceOf(NeedRetryException.class);
      assertThat(e.getMessage()).contains(leader.address());
      assertThat(leader.acceptedConnections()).isGreaterThanOrEqualTo(1);
    }
  }

  // Regression for #7527: a leader that cannot be connected to is given up on with need retry exception
  @Test
  void aLeaderThatCannotBeConnectedToIsGivenUpOnWithNeedRetryException() throws Exception {
    // A closed server socket's address refuses the connection outright rather than accepting and stalling.
    final String unreachable;
    try (final ServerSocket probe = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      unreachable = probe.getInetAddress().getHostAddress() + ":" + probe.getLocalPort();
    }

    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 2_000L);
    cfg.setValue(GlobalConfiguration.HA_PROXY_COMMAND_TIMEOUT, 60_000L);

    final RaftReplicatedDatabase db = databaseWith(serverWith(cfg), raftPointingAt(unreachable));

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    // NeedRetryException is correct here: a refused/unreachable connection means the command never left this
    // node, so retrying is provably safe - unlike the response-timeout case above.
    final NeedRetryException e = invokeAndUnwrap(db, cfg, NeedRetryException.class);
    watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "the configured connect timeout, not an OS-level connect refusal delay");

    assertThat(e.getMessage()).contains(unreachable);
  }

  // Regression for #7527: the commands own timeout takes precedence over the fallback
  /**
   * The command's own {@code arcadedb.command.timeout} wins over the fallback
   * {@code arcadedb.ha.proxyCommandTimeout} when both are set: a short per-command budget must not be
   * overridden by a longer server-wide fallback.
   */
  @Test
  void theCommandsOwnTimeoutTakesPrecedenceOverTheFallback() throws Exception {
    try (final FakeLeader leader = FakeLeader.silent()) {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 5_000L);
      cfg.setValue(GlobalConfiguration.COMMAND_TIMEOUT, 1_000L);
      // The command budget is extended by the quorum and connect budgets (issue #7737); kept short so the
      // deadline stays far below the fallback.
      cfg.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 1_000L);
      cfg.setValue(GlobalConfiguration.HA_PROXY_COMMAND_TIMEOUT, 600_000L); // would time this test out if used

      final RaftReplicatedDatabase db = databaseWith(serverWith(cfg), raftPointingAt(leader.address()));

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      invokeAndUnwrap(db, cfg, TransactionException.class);
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS,
          "the short arcadedb.command.timeout, proving it was used instead of the much longer fallback");
    }
  }

  // Regression for #7527: the client carries the configured connect timeout
  @Test
  void theClientCarriesTheConfiguredConnectTimeout() throws Exception {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 2_500L);

    final RaftReplicatedDatabase db = databaseWith(serverWith(cfg), FakeRaftHAServer.detached());

    final Field field = RaftReplicatedDatabase.class.getDeclaredField("httpClient");
    field.setAccessible(true);
    final HttpClient client = (HttpClient) field.get(db);

    assertThat(client.connectTimeout()).contains(Duration.ofMillis(2_500L));
  }
  // ---- #7737 ----
  // Regression test for issue #7737: the follower's SQL write forward used the command's own
  // {@code arcadedb.command.timeout} as its HTTP response deadline, unchanged. The leader enforces that same number
  // against the same command, but its clock starts later (after connect, transit, parse and dispatch) and does not
  // count the Raft quorum commit or the response transit that follow execution. So the follower always gave up first:
  // <ul>
  // <li>a write the leader committed inside its budget was reported as an unknown, do-not-retry failure;</li>
  // <li>the leader's own accurate {@code TimeoutException} naming {@code arcadedb.command.timeout} could never reach
  // the client, which saw the ambiguous "did not answer within" message instead.</li>
  // </ul>
  // The leader here is a plain HTTP server that answers {@link #LEADER_LATENESS_MS} after the command budget has
  // elapsed on the follower's clock - the shape of a command that finishes (or is aborted by the leader) right at its
  // budget, then spends a little longer on commit and transit. The forward is driven through the private method the
  // same way the tests above do.

  private static final long COMMAND_TIMEOUT_MS  = 1_000L;
  /** How late, past the command budget, the fake leader answers: well inside the headroom the fix grants. */
  private static final long LEADER_LATENESS_MS  = 700L;
  private static final long CONNECT_TIMEOUT_MS  = 2_000L;
  private static final long QUORUM_TIMEOUT_MS   = 2_000L;

  // Regression for #7737: a write the leader commits just after the command budget is reported as its result not as an unknown outcome
  @Test
  void aWriteTheLeaderCommitsJustAfterTheCommandBudgetIsReportedAsItsResultNotAsAnUnknownOutcome() throws Exception {
    final JSONObject success = new JSONObject().put("result", new JSONArray().put(new JSONObject().put("count", 1)));
    try (final SlowLeader leader = new SlowLeader(COMMAND_TIMEOUT_MS + LEADER_LATENESS_MS, 200, success.toString())) {
      final ContextConfiguration cfg = config();
      final RaftReplicatedDatabase db = databaseWith(serverWith(cfg), raftPointingAt(leader.address()));

      final Object result = invoke(db, cfg);

      assertThat(result).isInstanceOf(ResultSet.class);
      final ResultSet rs = (ResultSet) result;
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("count")).isEqualTo(1);
    }
  }

  // Regression for #7737: the leaders own command timeout error reaches the client
  @Test
  void theLeadersOwnCommandTimeoutErrorReachesTheClient() throws Exception {
    final String leaderMessage = "Command timed out: " + GlobalConfiguration.COMMAND_TIMEOUT.getKey() + " of "
        + COMMAND_TIMEOUT_MS + "ms";
    final JSONObject error = new JSONObject()
        .put("error", "Error on command execution")
        .put("detail", leaderMessage)
        .put("exception", TimeoutException.class.getName());
    try (final SlowLeader leader = new SlowLeader(COMMAND_TIMEOUT_MS + LEADER_LATENESS_MS, 500, error.toString())) {
      final ContextConfiguration cfg = config();
      final RaftReplicatedDatabase db = databaseWith(serverWith(cfg), raftPointingAt(leader.address()));

      final Object thrown = invokeExpectingFailure(db, cfg);

      assertThat(thrown).isInstanceOf(TimeoutException.class);
      assertThat(((Throwable) thrown).getMessage()).contains(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
      assertThat(((Throwable) thrown).getMessage()).doesNotContain("did not answer within");
    }
  }

  // Regression for #7737: a leader that never answers is still given up on after the headroom
  /**
   * The headroom is bounded too: a leader that never answers is still given up on, at the command budget plus the
   * quorum and connect budgets, and the message states that deadline.
   */
  @Test
  void aLeaderThatNeverAnswersIsStillGivenUpOnAfterTheHeadroom() throws Exception {
    try (final SlowLeader leader = new SlowLeader(GAVE_UP_BOUND_MS * 4, 200, "{}")) {
      final ContextConfiguration cfg = config();
      final RaftReplicatedDatabase db = databaseWith(serverWith(cfg), raftPointingAt(leader.address()));

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      final Object thrown = invokeExpectingFailure(db, cfg);
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "the command budget plus a bounded headroom, from a leader that never answers");

      assertThat(thrown).isInstanceOf(TransactionException.class);
      final long expectedDeadline = COMMAND_TIMEOUT_MS + QUORUM_TIMEOUT_MS + CONNECT_TIMEOUT_MS;
      assertThat(((Throwable) thrown).getMessage()).contains("did not answer within " + expectedDeadline + "ms");
    }
  }

  // Regression for #7737: the headroom saturates instead of overflowing
  @Test
  void theHeadroomSaturatesInsteadOfOverflowing() {
    assertThat(RaftReplicatedDatabase.commandTimeoutWithHeadroom(1_000L, 10_000L, 5_000L)).isEqualTo(16_000L);
    assertThat(RaftReplicatedDatabase.commandTimeoutWithHeadroom(1_000L, -1L, 0L)).isEqualTo(1_000L);
    assertThat(RaftReplicatedDatabase.commandTimeoutWithHeadroom(Long.MAX_VALUE - 10, 10_000L, 5_000L))
        .isEqualTo(Long.MAX_VALUE - 10);
    // The headroom's own sum overflowing must saturate too, not wrap into a shorter deadline.
    assertThat(RaftReplicatedDatabase.commandTimeoutWithHeadroom(1_000L, Long.MAX_VALUE / 2 + 1, Long.MAX_VALUE / 2 + 1))
        .isEqualTo(1_000L);
  }

  private static ContextConfiguration config() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.COMMAND_TIMEOUT, COMMAND_TIMEOUT_MS);
    cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, CONNECT_TIMEOUT_MS);
    cfg.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, QUORUM_TIMEOUT_MS);
    cfg.setValue(GlobalConfiguration.HA_PROXY_COMMAND_TIMEOUT, 600_000L); // must not be used: the command has a budget
    return cfg;
  }

  private static Object invoke(final RaftReplicatedDatabase db, final ContextConfiguration cfg) throws Exception {
    return forwardMethod().invoke(db, "sql", "update V set a = 1", null, new Object[0]);
  }

  private static Object invokeExpectingFailure(final RaftReplicatedDatabase db, final ContextConfiguration cfg)
      throws Exception {
    try {
      invoke(db, cfg);
      throw new AssertionError("expected the forward to throw");
    } catch (final InvocationTargetException e) {
      return e.getCause();
    }
  }

  /** A leader that answers every command with a fixed status and body, after a fixed delay. */
  private static final class SlowLeader implements AutoCloseable {
    private final HttpServer     server;
    private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
      final Thread t = new Thread(r, "issue7737-slow-leader");
      t.setDaemon(true);
      return t;
    });
    /** Released on close, so a handler still waiting out its delay does not outlive the test. */
    private final CountDownLatch closed   = new CountDownLatch(1);

    SlowLeader(final long delayMs, final int status, final String body) throws IOException {
      server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 16);
      server.createContext("/", exchange -> {
        try {
          if (closed.await(delayMs, TimeUnit.MILLISECONDS)) {
            exchange.close();
            return;
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          exchange.close();
          return;
        }
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        try {
          exchange.sendResponseHeaders(status, bytes.length);
          try (final OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
          }
        } catch (final IOException ignored) {
          // the follower gave up and closed the connection
        }
      });
      server.setExecutor(executor);
      server.start();
    }

    String address() {
      return server.getAddress().getAddress().getHostAddress() + ":" + server.getAddress().getPort();
    }

    @Override
    public void close() {
      closed.countDown();
      server.stop(0);
      executor.shutdownNow();
    }
  }
  // ---- #8325 ----
  // Regression test for issue #8325 on the ha-raft forwards to the leader: each one bounded the leader with the JDK
  // request timeout alone and read its answer with {@code BodyHandlers.ofString()}. On JDK 21-25 that timeout stops at
  // the response headers, so a leader that sent {@code 200} with a {@code Content-Length} and then stalled inside its
  // body parked the calling thread with no bound at all. Each entry point is driven through its own code here, against a
  // leader that does exactly that; without the fix every one of them waits on it until the hang detector gives up.

  /** The tripwire between "the deadline fired" and "the call is unbounded" (forever, without the fix). */
  private static final long GAVE_UP_BOUND_MS8325 = 15_000L;

  // Regression for #8325: a write forward whose leader stalls inside its body is given up on as unknown outcome
  /** {@code RaftReplicatedDatabase.forwardCommandToLeaderViaRaft}: the write forward. */
  @Test
  void aWriteForwardWhoseLeaderStallsInsideItsBodyIsGivenUpOnAsUnknownOutcome() throws Exception {
    try (final StallingBodyPeer leader = new StallingBodyPeer()) {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 5_000L);
      cfg.setValue(GlobalConfiguration.HA_PROXY_COMMAND_TIMEOUT, 1_000L);

      final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, cfg);
      final FakeRaftHAServer raft = FakeRaftHAServer.detached();
      raft.leaderHttpAddress(leader.address());
      raft.clusterToken("test-token");
      final LocalDatabase local = mock(LocalDatabase.class);
      when(local.getConfiguration()).thenReturn(cfg);
      final RaftReplicatedDatabase db = new RaftReplicatedDatabase(server, local, raft);

      final Method forward = RaftReplicatedDatabase.class.getDeclaredMethod("forwardCommandToLeaderViaRaft",
          String.class, String.class, Map.class, Object[].class);
      forward.setAccessible(true);

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      final Throwable thrown = leader.callWithin(() -> {
        try {
          forward.invoke(db, "sql", "insert into V set a = 1", null, new Object[0]);
          return null;
        } catch (final InvocationTargetException e) {
          return e.getCause();
        }
      });
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS8325, "a 1s deadline from the unbounded ofString() body read");

      // The leader answered the headers, so the write may have run there: an unknown outcome, never a retry.
      assertThat(thrown).isInstanceOf(TransactionException.class).isNotInstanceOf(NeedRetryException.class);
      assertThat(thrown.getMessage()).contains(leader.address()).contains("did not answer within");
      assertThat(leader.connectionClosedByCaller.await(GAVE_UP_BOUND_MS8325, TimeUnit.MILLISECONDS)).isTrue();
    }
  }

  // Regression for #8325: a bootstrap state query whose peer stalls inside its body is given up on
  /** {@code LeaderDatabaseQuery.fetch}: the bootstrap-state query. */
  @Test
  void aBootstrapStateQueryWhosePeerStallsInsideItsBodyIsGivenUpOn() throws Exception {
    try (final StallingBodyPeer leader = new StallingBodyPeer()) {
      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> leader.callWithin(
          () -> LeaderDatabaseQuery.fetch(null, leader.address(), null, "test-token", 1_000L, null)))
          .isInstanceOf(HttpTimeoutException.class);
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS8325, "a 1s deadline from the unbounded ofString() body read");

      assertThat(leader.connectionClosedByCaller.await(GAVE_UP_BOUND_MS8325, TimeUnit.MILLISECONDS)).isTrue();
    }
  }

  // Regression for #8325: a security seed request whose leader stalls inside its body is given up on
  /**
   * {@code ClusterSecuritySeedQuery}: the security seed requested from the leader. Slow, because its deadline is the
   * seed's retry budget plus a fixed 30 s margin that no setting lowers.
   */
  @Test
  @Tag("slow")
  void aSecuritySeedRequestWhoseLeaderStallsInsideItsBodyIsGivenUpOn() throws Exception {
    try (final StallingBodyPeer leader = new StallingBodyPeer()) {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_SECURITY_SEED_RETRY_TIMEOUT, 0L);
      final long deadlineMs = ClusterSecuritySeedQuery.reportTimeoutMs(cfg);

      final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, cfg);
      final FakeRaftHAServer raft = FakeRaftHAServer.detached();
      raft.clusterToken("test-token");
      raft.leader(false).leaderHttpAddress(leader.address());
      final RaftHAPlugin plugin = raft.plugin();

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      // The first attempt meets the stall; the leader then stops listening, so the retries fail at once and the
      // seed reports the failure instead of an outcome it never received.
      assertThatThrownBy(() -> leader.callWithin(
          () -> ClusterSecuritySeedQuery.seedForAdmission(server, plugin, "peer-1"),
          deadlineMs + StallingBodyPeer.HANG_DETECT_MS))
          .isInstanceOf(IOException.class);
      watch.assertGaveUpWithin(deadlineMs + GAVE_UP_BOUND_MS8325,
          "the seed's own deadline from the unbounded ofString() body read");

      assertThat(leader.connectionClosedByCaller.await(GAVE_UP_BOUND_MS8325, TimeUnit.MILLISECONDS)).isTrue();
    }
  }
}
