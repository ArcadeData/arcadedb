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
import com.arcadedb.exception.TransactionException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issues #7527/#7543: {@code RaftReplicatedDatabase}'s SQL write forward to the leader
 * (the private {@code forwardCommandToLeaderViaRaft}) used a bare {@code HttpClient.newHttpClient()} with no
 * connect timeout and built the request with no {@code .timeout(...)}, so a leader that accepted the
 * connection and never answered parked the calling thread until the OS tore the socket down.
 * <p>
 * These tests drive the private method directly - reaching it through a live cluster would need one, and the
 * behaviour under test is entirely local to the forward itself - the same reflection approach
 * {@code Issue7134StepDownRetryStopsOnRefusalTest} in this package already uses for a private method of this
 * same class.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7527RaftReplicatedDatabaseForwardTimeoutTest {

  /** The tripwire between "the deadline fired" and "the call is unbounded" (minutes, without the fix). */
  private static final long GAVE_UP_BOUND_MS = 30_000L;

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

  private static <T extends ArcadeDBException> T invokeAndUnwrap(final RaftReplicatedDatabase db,
      final ContextConfiguration cfg, final Class<T> expectedType) throws Exception {
    try {
      forwardMethod().invoke(db, "sql", "insert into V set a = 1", null, new Object[0], cfg);
      throw new AssertionError("expected the forward to throw");
    } catch (final InvocationTargetException e) {
      assertThat(e.getCause()).isInstanceOf(expectedType);
      return expectedType.cast(e.getCause());
    }
  }

  @Test
  void aLeaderThatAcceptsAndNeverAnswersIsGivenUpOnWithANonRetryableException() throws Exception {
    try (final StalledLeader leader = new StalledLeader()) {
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

  /**
   * The command's own {@code arcadedb.command.timeout} wins over the fallback
   * {@code arcadedb.ha.proxyCommandTimeout} when both are set: a short per-command budget must not be
   * overridden by a longer server-wide fallback.
   */
  @Test
  void theCommandsOwnTimeoutTakesPrecedenceOverTheFallback() throws Exception {
    try (final StalledLeader leader = new StalledLeader()) {
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

  @Test
  void theClientCarriesTheConfiguredConnectTimeout() throws Exception {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 2_500L);

    final RaftReplicatedDatabase db = databaseWith(serverWith(cfg), mock(RaftHAServer.class));

    final Field field = RaftReplicatedDatabase.class.getDeclaredField("httpClient");
    field.setAccessible(true);
    final HttpClient client = (HttpClient) field.get(db);

    assertThat(client.connectTimeout()).contains(Duration.ofMillis(2_500L));
  }

  /** A server socket that accepts connections and answers nothing at all. */
  private static final class StalledLeader implements AutoCloseable {
    private final ServerSocket serverSocket;
    private final Thread       acceptor;
    private final CountDownLatch started = new CountDownLatch(1);
    private volatile int       accepted  = 0;

    StalledLeader() throws IOException, InterruptedException {
      serverSocket = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
      acceptor = new Thread(() -> {
        started.countDown();
        while (!serverSocket.isClosed()) {
          try {
            final Socket socket = serverSocket.accept();
            accepted++;
            // never answers: the socket stays open until close() tears it down
          } catch (final IOException e) {
            return;
          }
        }
      }, "issue7527-stalled-leader");
      acceptor.setDaemon(true);
      acceptor.start();
      started.await(10, TimeUnit.SECONDS);
    }

    String address() {
      return serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getLocalPort();
    }

    int acceptedConnections() {
      return accepted;
    }

    @Override
    public void close() {
      try {
        serverSocket.close();
      } catch (final IOException ignored) {
        // best effort
      }
    }
  }
}
