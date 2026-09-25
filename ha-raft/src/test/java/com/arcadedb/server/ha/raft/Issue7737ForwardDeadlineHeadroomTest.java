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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.StallAwareStopwatch;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #7737: the follower's SQL write forward used the command's own
 * {@code arcadedb.command.timeout} as its HTTP response deadline, unchanged. The leader enforces that same number
 * against the same command, but its clock starts later (after connect, transit, parse and dispatch) and does not
 * count the Raft quorum commit or the response transit that follow execution. So the follower always gave up first:
 * <ul>
 *   <li>a write the leader committed inside its budget was reported as an unknown, do-not-retry failure;</li>
 *   <li>the leader's own accurate {@code TimeoutException} naming {@code arcadedb.command.timeout} could never reach
 *   the client, which saw the ambiguous "did not answer within" message instead.</li>
 * </ul>
 * The leader here is a plain HTTP server that answers {@link #LEADER_LATENESS_MS} after the command budget has
 * elapsed on the follower's clock - the shape of a command that finishes (or is aborted by the leader) right at its
 * budget, then spends a little longer on commit and transit. The forward is driven through the private method the
 * same way {@code Issue7527RaftReplicatedDatabaseForwardTimeoutTest} does.
 */
class Issue7737ForwardDeadlineHeadroomTest {

  private static final long COMMAND_TIMEOUT_MS  = 1_000L;
  /** How late, past the command budget, the fake leader answers: well inside the headroom the fix grants. */
  private static final long LEADER_LATENESS_MS  = 700L;
  private static final long CONNECT_TIMEOUT_MS  = 2_000L;
  private static final long QUORUM_TIMEOUT_MS   = 2_000L;
  /** Tripwire between the bounded forward and a wait that ignores the headroom's own bound. */
  private static final long GAVE_UP_BOUND_MS    = 30_000L;

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
    return forwardMethod().invoke(db, "sql", "update V set a = 1", null, new Object[0], cfg);
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

  private static Method forwardMethod() throws NoSuchMethodException {
    final Method m = RaftReplicatedDatabase.class.getDeclaredMethod("forwardCommandToLeaderViaRaft",
        String.class, String.class, Map.class, Object[].class, ContextConfiguration.class);
    m.setAccessible(true);
    return m;
  }

  private static RaftReplicatedDatabase databaseWith(final ArcadeDBServer server, final RaftHAServer raft) {
    return new RaftReplicatedDatabase(server, mock(LocalDatabase.class), raft);
  }

  private static ArcadeDBServer serverWith(final ContextConfiguration cfg) {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(cfg);
    when(server.getHA()).thenReturn(null); // plain HTTP forward, no HTTPS dial to resolve
    return server;
  }

  private static RaftHAServer raftPointingAt(final String leaderHttpAddress) {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.getLeaderHttpAddress()).thenReturn(leaderHttpAddress);
    when(raft.getClusterToken()).thenReturn("test-token");
    return raft;
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
}
