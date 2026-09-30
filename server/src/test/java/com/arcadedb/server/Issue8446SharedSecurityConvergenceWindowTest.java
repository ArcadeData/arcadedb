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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #8446: the security-convergence readiness window (#7532, per join since #8414) lived in the instance fields
 * of {@link ServerControlPlane}, which is not a singleton - the HTTP {@code /api/v1/ready} handler and the gRPC admin
 * service each construct one. So the two readiness surfaces of one process kept independent windows: the gRPC one
 * opened at the first gRPC probe and could hold the node long after HTTP had released it, and each logged its own
 * "once per window" give-up line. The window now belongs to the {@link ArcadeDBServer}.
 * <p>
 * Every control plane below stands for one surface: {@code http} for {@code GetReadyHandler}'s, {@code grpc} for
 * {@code ArcadeDbGrpcAdminService}'s. Both are built on the same server, exactly as the two production call sites
 * build them.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8446SharedSecurityConvergenceWindowTest extends StaticBaseServerTest {
  private ArcadeDBServer realServer;

  @AfterEach
  @Override
  public void endTest() {
    if (realServer != null && realServer.isStarted())
      realServer.stop();
    realServer = null;
    super.endTest();
  }

  /**
   * The issue's first symptom: HTTP opens the window and it expires, then the first gRPC probe arrives. With a
   * window per control plane the gRPC probe opened a fresh one and held the node for a whole timeout more.
   */
  @Test
  void aSecondSurfaceSeesTheWindowTheFirstOneOpened() {
    final ArcadeDBServer server = heldServer(1L);
    final ServerControlPlane http = new ServerControlPlane(server);
    final ServerControlPlane grpc = new ServerControlPlane(server);

    assertThat(http.notReadyReason()).as("the HTTP probe opens the window").isNotNull();
    await(2L);
    assertThat(grpc.notReadyReason()).as("the gRPC probe finds the same, already expired, window").isNull();
    assertThat(http.notReadyReason()).as("and both surfaces agree").isNull();
  }

  /** The converse: while the shared window is open, a surface that has never probed is held by it too. */
  @Test
  void bothSurfacesAreHeldByTheSameOpenWindow() {
    final ArcadeDBServer server = heldServer(60_000L);
    final ServerControlPlane http = new ServerControlPlane(server);
    final ServerControlPlane grpc = new ServerControlPlane(server);

    assertThat(http.notReadyReason()).isNotNull();
    assertThat(grpc.notReadyReason()).isNotNull();
    assertThat(http.gate()).isSameAs(grpc.gate());
  }

  /** The issue's second symptom: the SEVERE give-up line is once per window, not once per readiness surface. */
  @Test
  void theGiveUpIsLoggedOnceAcrossBothSurfaces() {
    final ArcadeDBServer server = heldServer(1L);
    final ServerControlPlane http = new ServerControlPlane(server);
    final ServerControlPlane grpc = new ServerControlPlane(server);

    final AtomicInteger giveUpLines = new AtomicInteger();
    final Logger original = installGiveUpCountingLogger(giveUpLines);
    try {
      http.notReadyReason();  // opens the window
      await(2L);
      http.notReadyReason();  // expires it: the decision
      assertThat(giveUpLines.get()).isEqualTo(1);

      grpc.notReadyReason();
      await(2L);
      grpc.notReadyReason();
      http.notReadyReason();
      assertThat(giveUpLines.get()).as("the gRPC surface reports no second give-up for the same window").isEqualTo(1);
    } finally {
      LogManager.instance().setLogger(original);
    }
  }

  /**
   * The two surfaces are served by different threads, so they can pass the expiry at the same instant: the give-up
   * decision is claimed with a compare-and-set, and exactly one of them logs it. Repeated over many fresh windows,
   * because a single race is not guaranteed to interleave.
   */
  @Test
  void surfacesRacingPastTheExpiryLogTheGiveUpOnce() throws Exception {
    final int rounds = 200;
    final AtomicInteger giveUpLines = new AtomicInteger();
    final Logger original = installGiveUpCountingLogger(giveUpLines);
    final ExecutorService probes = Executors.newFixedThreadPool(2);
    try {
      for (int round = 0; round < rounds; round++) {
        final ArcadeDBServer server = heldServer(1L);
        final ServerControlPlane http = new ServerControlPlane(server);
        final ServerControlPlane grpc = new ServerControlPlane(server);

        assertThat(http.notReadyReason()).isNotNull();  // opens the window
        await(2L);

        final CountDownLatch start = new CountDownLatch(1);
        final Future<String> fromHttp = probes.submit(() -> {
          start.await();
          return http.notReadyReason();
        });
        final Future<String> fromGrpc = probes.submit(() -> {
          start.await();
          return grpc.notReadyReason();
        });
        start.countDown();
        assertThat(fromHttp.get(10, TimeUnit.SECONDS)).isNull();
        assertThat(fromGrpc.get(10, TimeUnit.SECONDS)).isNull();
      }
      assertThat(giveUpLines.get()).as("one give-up per window, however the two surfaces interleave").isEqualTo(rounds);
    } finally {
      probes.shutdownNow();
      LogManager.instance().setLogger(original);
    }
  }

  /**
   * A forward join move seen by both surfaces at once (issue #8414's fresh window, now shared): exactly one of them
   * clears the spent window, both are held by the fresh one, and when it expires it is reported once.
   */
  @Test
  void surfacesRacingOverAJoinMoveShareOneFreshWindow() throws Exception {
    final int rounds = 200;
    final AtomicInteger giveUpLines = new AtomicInteger();
    final Logger original = installGiveUpCountingLogger(giveUpLines);
    final ExecutorService probes = Executors.newFixedThreadPool(2);
    try {
      for (int round = 0; round < rounds; round++) {
        final AtomicLong joinIndex = new AtomicLong(10L);
        final ArcadeDBServer server = heldServer(1L, joinIndex);
        final ServerControlPlane http = new ServerControlPlane(server);
        final ServerControlPlane grpc = new ServerControlPlane(server);

        http.notReadyReason();  // the first join opens its window
        await(2L);
        http.notReadyReason();  // and gives up on it
        assertThat(giveUpLines.get()).isEqualTo(2 * round + 1);

        // The window is read at every probe: long while the surfaces race, so neither can find it expired merely
        // because its thread was scheduled late, and short again to watch it expire.
        final ContextConfiguration configuration = server.getConfiguration();
        configuration.setValue(GlobalConfiguration.HA_SECURITY_CONVERGENCE_READINESS_TIMEOUT, 60_000L);
        joinIndex.set(20L);     // the operator re-adds the node
        final CountDownLatch start = new CountDownLatch(1);
        final Future<String> fromHttp = probes.submit(() -> {
          start.await();
          return http.notReadyReason();
        });
        final Future<String> fromGrpc = probes.submit(() -> {
          start.await();
          return grpc.notReadyReason();
        });
        start.countDown();
        assertThat(fromHttp.get(10, TimeUnit.SECONDS)).as("the re-add holds the HTTP surface").isNotNull();
        assertThat(fromGrpc.get(10, TimeUnit.SECONDS)).as("and the gRPC one").isNotNull();

        configuration.setValue(GlobalConfiguration.HA_SECURITY_CONVERGENCE_READINESS_TIMEOUT, 1L);
        await(2L);
        assertThat(grpc.notReadyReason()).isNull();
        assertThat(http.notReadyReason()).isNull();
        assertThat(giveUpLines.get()).as("the re-add's window is reported once").isEqualTo(2 * round + 2);
      }
    } finally {
      probes.shutdownNow();
      LogManager.instance().setLogger(original);
    }
  }

  /**
   * Reachability: a real server hands every control plane built on it - the HTTP handler's and the gRPC service's
   * alike - its one window, and a start forgets whatever a previous run of the same instance left in it, as a start
   * did when the handlers holding the window were rebuilt by every start.
   */
  @Test
  void aRealServerSharesOneWindowAndClearsItOnStart() {
    realServer = new ArcadeDBServer(serverConfiguration());

    final SecurityConvergenceGate window = realServer.getSecurityConvergenceGate();
    assertThat(window).isNotNull();
    assertThat(new ServerControlPlane(realServer).gate()).isSameAs(window);
    assertThat(new ServerControlPlane(realServer).gate()).isSameAs(window);

    // What an expired window of an earlier run of this instance leaves behind.
    window.joinIndex = 42L;
    window.windowOpenedAt = 1L;
    window.giveUpLogged = true;
    window.leaderLoggedFor = 42L;

    realServer.start();

    assertThat(realServer.getSecurityConvergenceGate()).isSameAs(window);
    assertThat(window.joinIndex).isEqualTo(-1L);
    assertThat(window.windowOpenedAt).isZero();
    assertThat(window.giveUpLogged).isFalse();
    assertThat(window.leaderLoggedFor).isEqualTo(-1L);
  }

  // -----------------------------------------------------------------------------------------------------------

  /** A server held by the gate: an armed, caught-up runtime joiner missing every security document. */
  private static ArcadeDBServer heldServer(final long windowMs) {
    return heldServer(windowMs, new AtomicLong(10L));
  }

  /** As above, with the join index read from {@code joinIndex} at every probe. */
  private static ArcadeDBServer heldServer(final long windowMs, final AtomicLong joinIndex) {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
    when(ha.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);
    when(ha.getConfiguredServers()).thenReturn(3);
    when(ha.hasJoinedClusterAtRuntime()).thenReturn(true);
    when(ha.getRuntimeJoinIndex()).thenAnswer(invocation -> joinIndex.get());
    when(ha.securityDocumentsNotInstalledSinceRuntimeJoin()).thenReturn(List.of("users"));

    final ServerSecurity security = mock(ServerSecurity.class);
    when(security.unconvergedClusterSecurityDocuments()).thenReturn(List.of("users"));

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_ENABLED, true);
    configuration.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);
    configuration.setValue(GlobalConfiguration.HA_SECURITY_CONVERGENCE_READINESS_TIMEOUT, windowMs);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getStatus()).thenReturn(ArcadeDBServer.STATUS.ONLINE);
    when(server.getConfiguration()).thenReturn(configuration);
    when(server.getHA()).thenReturn(ha);
    when(server.getSecurity()).thenReturn(security);
    when(server.getSecurityConvergenceGate()).thenReturn(new SecurityConvergenceGate());
    return server;
  }

  private ContextConfiguration serverConfiguration() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_NAME, "convergence_window_8446");
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/databases0");
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    configuration.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, allocateFreePorts(1)[0]);
    configuration.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    configuration.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);
    return configuration;
  }

  private static void await(final long ms) {
    final long until = System.currentTimeMillis() + ms;
    while (System.currentTimeMillis() < until)
      Thread.onSpinWait();
  }

  /** Counts the SEVERE give-up lines; returns the logger to restore (the pattern of Issue8414's test). */
  private static Logger installGiveUpCountingLogger(final AtomicInteger counter) {
    final Logger counting = new Logger() {
      private void record(final Level level, final String message) {
        if (level == Level.SEVERE && message != null && message.startsWith("Reporting READY after waiting"))
          counter.incrementAndGet();
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable throwable,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
          final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
          final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
          final Object arg15, final Object arg16, final Object arg17) {
        record(level, message);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable throwable,
          final String context, final Object... args) {
        record(level, message);
      }

      @Override
      public void flush() {
      }
    };
    final Logger previous = new DefaultLogger();
    LogManager.instance().setLogger(counting);
    return previous;
  }
}
