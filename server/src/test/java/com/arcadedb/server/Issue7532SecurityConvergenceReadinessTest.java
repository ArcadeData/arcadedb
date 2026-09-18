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
 * Issue #7532: a peer whose security documents have not converged used to report READY.
 * <p>
 * {@code getReadinessSignal} gates on a known leader, membership in the current configuration and - for a
 * follower - applied-index lag. None of that says anything about {@code server-users.jsonl},
 * {@code server-groups.json} or {@code server-api-tokens.json}, which live outside the database directory and
 * which no snapshot install carries. They reach a new peer only through the seed the admission verbs run AFTER
 * the membership change commits, so there is a window - opened before the seed's first attempt, which is why
 * issue #7521's bounded retry could not close it - in which the peer is a member, is caught up, answers READY,
 * and serves requests against the credentials in its own config directory.
 * <p>
 * The gate added here consults the ONE form of that question a node can answer by itself: has an
 * {@code applyReplicated*} ever installed this document from the replicated log? It is bounded, because the
 * answer is also "no" on every node of a cluster that has simply never replicated a security document - nobody
 * has run a {@code create user}, or the node self-joined through {@code KubernetesAutoJoin}, which seeds nothing
 * (issue #7531). Unbounded, those nodes would stall a rolling restart forever; hence the window, hence the
 * {@code 0} default, and hence the single SEVERE line when the window expires, which is the explicit decision
 * the issue asked for rather than a silent deadlock or a silent pass.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7532SecurityConvergenceReadinessTest {

  private static final long WINDOW_MS = 60_000L;

  /**
   * The defect: a member of a multi-node cluster holding none of the cluster's security documents answered
   * READY. It is the joined-but-unseeded peer, and every gate that existed before this one passes it.
   */
  @Test
  void aMemberHoldingNoneOfTheClustersSecurityDocumentsIsNotReady() {
    final String reason = new ServerControlPlane(
        onlineServerWith(caughtUpHaWith(3), securityMissing("users", "groups", "API tokens"),
            configurationWith(true, WINDOW_MS))).notReadyReason();

    assertThat(reason).isNotNull();
    assertThat(reason).as("the operator has to know WHICH documents").contains("users", "groups", "API tokens");
  }

  /** One document short is still short: a partially failed seed leaves exactly this state. */
  @Test
  void aSinglyUnconvergedDocumentIsEnoughToHoldReadiness() {
    final String reason = new ServerControlPlane(
        onlineServerWith(caughtUpHaWith(3), securityMissing("API tokens"), configurationWith(true, WINDOW_MS)))
        .notReadyReason();

    assertThat(reason).isNotNull();
    assertThat(reason).contains("API tokens");
    assertThat(reason).doesNotContain("groups");
  }

  /** Once every document has been installed from the replicated log the gate is silent. */
  @Test
  void aConvergedMemberIsReady() {
    assertThat(new ServerControlPlane(
        onlineServerWith(caughtUpHaWith(3), securityMissing(), configurationWith(true, WINDOW_MS)))
        .notReadyReason()).isNull();
  }

  /**
   * The deadlock answer. A node nobody is ever going to seed must not stall a rolling restart, so the wait is
   * bounded: past the window the same unconverged node reports READY. The SEVERE line the production code emits
   * alongside is what makes that a decision rather than a silent pass.
   */
  @Test
  void theWaitIsBoundedSoANeverSeededNodeStillBecomesReady() {
    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(caughtUpHaWith(3), securityMissing("users"), configurationWith(true, 1L)));

    // First probe opens the window; by the time the second runs, a 1 ms window has certainly closed. Nothing
    // here measures elapsed time as an assertion - the bound is the smallest the setting allows, and a stall
    // can only make it more expired, never less.
    controlPlane.notReadyReason();
    await(2L);

    assertThat(controlPlane.notReadyReason())
        .as("a bounded window must expire, or a node that is never seeded is never ready again")
        .isNull();
  }

  /**
   * A window that has not been asked for does not exist. The default is {@code 0} because "has never installed a
   * replicated document" is also true of every node of a statically configured cluster that has never replicated
   * one, and holding all of those off the load balancer at every start would be a regression rather than a fix.
   */
  @Test
  void theGateIsOffByDefault() {
    // -1 leaves the setting untouched, so the value read is GlobalConfiguration's own default.
    assertThat(new ServerControlPlane(
        onlineServerWith(caughtUpHaWith(3), securityMissing("users", "groups", "API tokens"),
            configurationWith(true, -1L))).notReadyReason()).isNull();
  }

  /**
   * The gate rides inside {@code readinessRequiresHA} like the lag gate it extends, and unlike the wedged-log
   * gate of issue #7118 that sits outside it. It has to: it can fire on a cluster that is not faulty at all,
   * whereas a failed log writer never can.
   */
  @Test
  void theGateRespectsReadinessRequiresHA() {
    assertThat(new ServerControlPlane(
        onlineServerWith(caughtUpHaWith(3), securityMissing("users"), configurationWith(false, WINDOW_MS)))
        .notReadyReason()).isNull();
  }

  /** A single-node cluster has no peer for the documents to have come from, so there is nothing to wait for. */
  @Test
  void aSingleNodeClusterIsNeverGated() {
    assertThat(new ServerControlPlane(
        onlineServerWith(caughtUpHaWith(1), securityMissing("users", "groups", "API tokens"),
            configurationWith(true, WINDOW_MS))).notReadyReason()).isNull();
  }

  /**
   * A bound that can be restarted is not a bound. {@code getConfiguredServers()} answers {@code 1} whenever the
   * Raft server is not readable this tick - {@code RaftHAPlugin} returns it literally when its
   * {@code raftHAServer} is null - so a node whose HA layer is flapping would, if that reading reset the
   * window, start the wait again after every blip and never reach the give-up branch. Only convergence resets
   * it.
   */
  @Test
  void aTransientSingleNodeReadingDoesNotRestartTheBound() {
    final HAServerPlugin flapping = mock(HAServerPlugin.class);
    when(flapping.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
    when(flapping.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);
    when(flapping.getConfiguredServers()).thenReturn(3, 1, 3);

    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(flapping, securityMissing("users"), configurationWith(true, 1L)));

    controlPlane.notReadyReason();  // opens the window on a 3-peer reading
    await(2L);
    controlPlane.notReadyReason();  // an unreadable tick: must not forget the deadline

    assertThat(controlPlane.notReadyReason())
        .as("the deadline opened on the first reading has passed and must still count")
        .isNull();
  }

  /** Convergence is the one thing that clears the window, so a later join is measured from itself. */
  @Test
  void convergenceClearsTheWindow() {
    final ServerSecurity security = mock(ServerSecurity.class);
    when(security.unconvergedClusterSecurityDocuments())
        .thenReturn(List.of("users"), List.of(), List.of("users"));

    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(caughtUpHaWith(3), security, configurationWith(true, WINDOW_MS)));

    assertThat(controlPlane.notReadyReason()).as("window opens").isNotNull();
    assertThat(controlPlane.notReadyReason()).as("converged").isNull();
    assertThat(controlPlane.notReadyReason())
        .as("a fresh unconverged reading starts a fresh window rather than inheriting the spent one")
        .isNotNull();
  }

  /**
   * The give-up line is once per window, not once per process. The window is cleared when the node converges,
   * so a node that converges and then opens a FRESH window (a later membership change against a cluster whose
   * documents have not reached it) has a second decision to report, and an operator who never sees it is told
   * nothing about the second one. Unreachable today - a fingerprint is only ever recorded, never cleared - but
   * the flag has to mean what the window means, or the next change to either makes this silent.
   */
  @Test
  void aFreshWindowReportsItsOwnGiveUpDecision() {
    final ServerSecurity security = mock(ServerSecurity.class);
    when(security.unconvergedClusterSecurityDocuments())
        .thenReturn(List.of("users"), List.of("users"), List.of(), List.of("users"), List.of("users"));

    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(caughtUpHaWith(3), security, configurationWith(true, 1L)));

    final AtomicInteger giveUpLines = new AtomicInteger();
    final Logger original = installGiveUpCountingLogger(giveUpLines);
    try {
      controlPlane.notReadyReason();  // opens the first window
      await(2L);
      controlPlane.notReadyReason();  // it expires: first decision logged
      assertThat(giveUpLines.get()).as("the expired window reports its decision").isEqualTo(1);

      controlPlane.notReadyReason();  // converged: the window, and the decision with it, are forgotten
      controlPlane.notReadyReason();  // unconverged again: a fresh window opens
      await(2L);
      controlPlane.notReadyReason();  // it expires too

      assertThat(giveUpLines.get())
          .as("a fresh window that expires is a fresh decision the operator has to be told about")
          .isEqualTo(2);
    } finally {
      LogManager.instance().setLogger(original);
    }
  }

  /**
   * Order matters: a node that has not caught up reports THAT, not the security documents it also does not
   * hold. The consensus answer is the one an operator acts on first, and it is the one that clears itself.
   */
  @Test
  void aLaggingNodeReportsItsLagRatherThanTheSecurityDocuments() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
    when(ha.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.NOT_READY);

    assertThat(new ServerControlPlane(
        onlineServerWith(ha, securityMissing("users"), configurationWith(true, WINDOW_MS))).notReadyReason())
        .isEqualTo("Node is not yet in the Raft configuration or has not caught up");
  }

  /**
   * A probe answers; it does not propagate. A signal that cannot be read leaves the node READY rather than
   * turning {@code /api/v1/ready} into a 500 the orchestrator reads as "unknown".
   */
  @Test
  void aSignalThatCannotBeReadDoesNotFailTheProbe() {
    final ServerSecurity security = mock(ServerSecurity.class);
    when(security.unconvergedClusterSecurityDocuments()).thenThrow(new IllegalStateException("not installed"));

    assertThat(new ServerControlPlane(
        onlineServerWith(caughtUpHaWith(3), security, configurationWith(true, WINDOW_MS))).notReadyReason()).isNull();
  }

  // -----------------------------------------------------------------------------------------------------------

  /**
   * Installs a {@link Logger} counting the SEVERE give-up lines, returning the previous logger for restore
   * (the sanctioned test pattern, see {@code DefaultLogger}).
   */
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

  private static void await(final long ms) {
    final long until = System.currentTimeMillis() + ms;
    while (System.currentTimeMillis() < until)
      Thread.onSpinWait();
  }

  private static HAServerPlugin caughtUpHaWith(final int configuredServers) {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
    when(ha.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);
    when(ha.getConfiguredServers()).thenReturn(configuredServers);
    return ha;
  }

  private static ServerSecurity securityMissing(final String... documents) {
    final ServerSecurity security = mock(ServerSecurity.class);
    when(security.unconvergedClusterSecurityDocuments()).thenReturn(List.of(documents));
    return security;
  }

  private static ContextConfiguration configurationWith(final boolean readinessRequiresHA, final long windowMs) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_ENABLED, true);
    configuration.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, readinessRequiresHA);
    if (windowMs >= 0)
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
}
