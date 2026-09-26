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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #8414 (#8382, #8388): the security-convergence readiness window was reset by convergence alone, and that
 * reset was decided before the runtime-join check.
 * <ul>
 * <li>#8382: a joiner whose window expired without convergence kept the expired window and the latched give-up
 * flag for the rest of the process, so a later re-add - exactly what the give-up line tells the operator to do -
 * was not gated for a single probe and logged nothing. The window is now per join: a join index that moves forward
 * opens a fresh one.</li>
 * <li>#8388: on a re-added node (a fingerprint for every document from its previous membership) a disarmed reading
 * - {@code hasJoinedClusterAtRuntime()} is false while the HA plugin has no Raft server - produced an empty union and
 * took the reset branch, restarting the bound. A disarmed reading now changes nothing.</li>
 * </ul>
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8414SecurityConvergenceWindowResetTest {

  private static final long LONG_WINDOW_MS = 60_000L;

  /**
   * #8382's repro: the first join's window expires and gives up; the operator re-adds the node, which moves the
   * join index forward, and the re-added node must be held again rather than answering READY on the first probe.
   */
  @Test
  void aReAddAfterTheWindowExpiredIsGatedAgain() {
    final HAServerPlugin ha = armedHa(new long[] { 10L, 10L, 20L }, "users", "groups", "API tokens");
    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(ha, fingerprintsMissing(), configurationWith(1L)));

    assertThat(controlPlane.notReadyReason()).as("the first join opens the window").isNotNull();
    await(2L);
    assertThat(controlPlane.notReadyReason()).as("the first join's window expires").isNull();

    // The probe that opens a window compares it against the same instant it opened it at, so it is held whatever
    // the window's length: a 1 ms window cannot flake here, and it cannot be held by the spent window either.
    final String reason = controlPlane.notReadyReason();
    assertThat(reason).as("the re-add opens a fresh window instead of inheriting the spent one").isNotNull();
    assertThat(reason).contains("users", "groups", "API tokens");
  }

  /** The re-added node's own window, when it expires too, is a fresh decision the operator must be told about. */
  @Test
  void aReAddWhoseWindowExpiresLogsItsOwnGiveUp() {
    final HAServerPlugin ha = armedHa(new long[] { 10L, 10L, 20L, 20L }, "users");
    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(ha, fingerprintsMissing(), configurationWith(1L)));

    final AtomicInteger giveUpLines = new AtomicInteger();
    final Logger original = installGiveUpCountingLogger(giveUpLines);
    try {
      controlPlane.notReadyReason();  // first join: opens the window
      await(2L);
      controlPlane.notReadyReason();  // it expires: first decision
      assertThat(giveUpLines.get()).isEqualTo(1);

      controlPlane.notReadyReason();  // re-add at index 20: a fresh window opens
      await(2L);
      controlPlane.notReadyReason();  // it expires too

      assertThat(giveUpLines.get()).as("the re-add's window reports its own give-up").isEqualTo(2);
    } finally {
      LogManager.instance().setLogger(original);
    }
  }

  /**
   * Only a join index that moves FORWARD opens a fresh window. A plugin without a Raft server reports none
   * ({@code -1}), and a reading of it must not be taken for a join, or a flapping HA layer would restart the bound
   * on every blip - twice, once going to {@code -1} and once coming back.
   */
  @Test
  void aJoinIndexThatDoesNotMoveForwardDoesNotRestartTheBound() {
    final HAServerPlugin ha = armedHa(new long[] { 10L, -1L, 10L }, "users");
    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(ha, fingerprintsMissing(), configurationWith(1L)));

    assertThat(controlPlane.notReadyReason()).as("opens the window").isNotNull();
    await(2L);
    assertThat(controlPlane.notReadyReason()).as("an unreadable join index is no join").isNull();
    assertThat(controlPlane.notReadyReason()).as("nor is the same join index read again").isNull();
  }

  /**
   * #8388's repro: a re-added node holds a fingerprint for every document, so a disarmed reading yields an EMPTY
   * union. That reading must not take the convergence branch and clear the deadline: the next armed probe must
   * still find the window it opened first, already expired.
   */
  @Test
  void aDisarmedReadingOnAReAddedNodeDoesNotResetTheWindow() {
    final HAServerPlugin ha = armedHa(new long[] { 10L }, "users", "groups", "API tokens");
    when(ha.hasJoinedClusterAtRuntime()).thenReturn(true, false, true);
    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(ha, fingerprintsMissing(), configurationWith(1L)));

    final AtomicInteger giveUpLines = new AtomicInteger();
    final Logger original = installGiveUpCountingLogger(giveUpLines);
    try {
      assertThat(controlPlane.notReadyReason()).as("opens the window on an armed reading").isNotNull();
      await(2L);
      assertThat(controlPlane.notReadyReason()).as("a disarmed tick is not gated").isNull();
      assertThat(giveUpLines.get()).as("and decides nothing").isZero();

      assertThat(controlPlane.notReadyReason())
          .as("the deadline opened on the first reading has passed and must still count")
          .isNull();
      assertThat(giveUpLines.get()).as("the expiry is reported, not silently restarted").isEqualTo(1);
    } finally {
      LogManager.instance().setLogger(original);
    }
  }

  /** Convergence on an ARMED reading still resets the window: the case the reset was reasoned about. */
  @Test
  void convergenceOnAnArmedReadingStillClearsTheWindow() {
    final HAServerPlugin ha = armedHa(new long[] { 10L });
    when(ha.securityDocumentsNotInstalledSinceRuntimeJoin())
        .thenReturn(List.of("users"), List.of(), List.of("users"));
    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(ha, fingerprintsMissing(), configurationWith(LONG_WINDOW_MS)));

    assertThat(controlPlane.notReadyReason()).as("window opens").isNotNull();
    assertThat(controlPlane.notReadyReason()).as("converged").isNull();
    assertThat(controlPlane.notReadyReason()).as("a fresh window").isNotNull();
  }

  /** An HA implementation that predates the signal keeps the interface default: no join index known. */
  @Test
  void theInterfaceDefaultReportsNoJoinIndex() {
    final HAServerPlugin legacy = mock(HAServerPlugin.class,
        invocation -> invocation.getMethod().isDefault() ? invocation.callRealMethod() : null);
    assertThat(legacy.getRuntimeJoinIndex()).isEqualTo(-1L);
  }

  // -----------------------------------------------------------------------------------------------------------

  /** An armed, caught-up HA plugin answering the given join indexes in turn (the last one repeats). */
  private static HAServerPlugin armedHa(final long[] joinIndexes, final String... notInstalledSinceJoin) {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
    when(ha.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);
    when(ha.getConfiguredServers()).thenReturn(3);
    when(ha.hasJoinedClusterAtRuntime()).thenReturn(true);
    when(ha.securityDocumentsNotInstalledSinceRuntimeJoin()).thenReturn(List.of(notInstalledSinceJoin));
    final Long[] rest = new Long[joinIndexes.length - 1];
    for (int i = 1; i < joinIndexes.length; i++)
      rest[i - 1] = joinIndexes[i];
    when(ha.getRuntimeJoinIndex()).thenReturn(joinIndexes[0], rest);
    return ha;
  }

  /** The node re-added with its config volume retained: a fingerprint for every document unless named here. */
  private static ServerSecurity fingerprintsMissing(final String... documents) {
    final ServerSecurity security = mock(ServerSecurity.class);
    when(security.unconvergedClusterSecurityDocuments()).thenReturn(List.of(documents));
    return security;
  }

  private static ContextConfiguration configurationWith(final long windowMs) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_ENABLED, true);
    configuration.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);
    configuration.setValue(GlobalConfiguration.HA_SECURITY_CONVERGENCE_READINESS_TIMEOUT, windowMs);
    return configuration;
  }

  private static ArcadeDBServer onlineServerWith(final HAServerPlugin ha, final ServerSecurity security,
      final ContextConfiguration configuration) {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getStatus()).thenReturn(ArcadeDBServer.STATUS.ONLINE);
    when(server.getConfiguration()).thenReturn(configuration);
    when(server.getHA()).thenReturn(ha);
    when(server.getSecurity()).thenReturn(security);
    return server;
  }

  private static void await(final long ms) {
    final long until = System.currentTimeMillis() + ms;
    while (System.currentTimeMillis() < until)
      Thread.onSpinWait();
  }

  /** Counts the SEVERE give-up lines; returns the logger to restore (the pattern of Issue7532's test). */
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
