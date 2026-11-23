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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
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
 * The security-convergence readiness gate: a peer whose replicated security documents have not converged is not READY,
 * which joins arm it, how its window resets and is shared, and how {@code GET /api/v1/cluster} publishes it.
 */
class SecurityConvergenceGateTest {
  /*
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
   * {@code 0} default it shipped with, and hence the single SEVERE line when the window expires, which is the
   * explicit decision the issue asked for rather than a silent deadlock or a silent pass.
   * <p>
   * Issue #7819 armed the gate on a runtime join and moved the default off zero; every test here drives a node
   * that joined at runtime, and {@code SecurityConvergenceGateTest.RuntimeJoinArmsGate} covers the ones that did not.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  private static final long WINDOW_MS = 60_000L;

  // Regression: #7532 a peer whose security documents have not converged used to report READY
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
  // Regression: #7532 a peer whose security documents have not converged used to report READY
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
  // Regression: #7532 a peer whose security documents have not converged used to report READY
  @Test
  void aConvergedMemberIsReady() {
    assertThat(new ServerControlPlane(
        onlineServerWith(caughtUpHaWith(3), securityMissing(), configurationWith(true, WINDOW_MS)))
        .notReadyReason()).isNull();
  }

  // Regression: #7532 a peer whose security documents have not converged used to report READY
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

  // Regression: #7532 a peer whose security documents have not converged used to report READY
  /**
   * {@code 0} still switches the gate off, for an operator who wants readiness exactly as it was before issue
   * #7532. Issue #7819 moved the DEFAULT off zero, not the meaning of zero.
   */
  @Test
  void aZeroWindowDisablesTheGate() {
    assertThat(new ServerControlPlane(
        onlineServerWith(caughtUpHaWith(3), securityMissing("users", "groups", "API tokens"),
            configurationWith(true, 0L))).notReadyReason()).isNull();
  }

  // Regression: #7532 a peer whose security documents have not converged used to report READY
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

  // Regression: #7532 a peer whose security documents have not converged used to report READY
  /**
   * A bound that can be restarted is not a bound. {@code hasJoinedClusterAtRuntime()} answers {@code false}
   * whenever the Raft server is not readable this tick - {@code RaftHAPlugin} returns it literally when its
   * {@code raftHAServer} is null - so a node whose HA layer is flapping would, if that reading reset the window,
   * start the wait again after every blip and never reach the give-up branch. Only convergence resets it.
   */
  @Test
  void aTransientDisarmedReadingDoesNotRestartTheBound() {
    final HAServerPlugin flapping = mock(HAServerPlugin.class);
    when(flapping.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
    when(flapping.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);
    when(flapping.getConfiguredServers()).thenReturn(3);
    when(flapping.hasJoinedClusterAtRuntime()).thenReturn(true, false, true);

    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(flapping, securityMissing("users"), configurationWith(true, 1L)));

    assertThat(controlPlane.notReadyReason()).as("opens the window on an armed reading").isNotNull();
    await(2L);
    assertThat(controlPlane.notReadyReason()).as("an unreadable tick is not gated").isNull();

    assertThat(controlPlane.notReadyReason())
        .as("the deadline opened on the first reading has passed and must still count")
        .isNull();
  }

  /** Convergence is the one thing that clears the window, so a later join is measured from itself. */
  // Regression: #7532 a peer whose security documents have not converged used to report READY
  @Test
  void convergenceClearsTheWindow() {
    final FakeServerSecurity security = FakeServerSecurity.create();
    security.on("unconvergedClusterSecurityDocuments", CallLog.inOrder(List.of("users"), List.of(), List.of("users")));

    final ServerControlPlane controlPlane = new ServerControlPlane(
        onlineServerWith(caughtUpHaWith(3), security, configurationWith(true, WINDOW_MS)));

    assertThat(controlPlane.notReadyReason()).as("window opens").isNotNull();
    assertThat(controlPlane.notReadyReason()).as("converged").isNull();
    assertThat(controlPlane.notReadyReason())
        .as("a fresh unconverged reading starts a fresh window rather than inheriting the spent one")
        .isNotNull();
  }

  // Regression: #7532 a peer whose security documents have not converged used to report READY
  /**
   * The give-up line is once per window, not once per process. The window is cleared when the node converges,
   * so a node that converges and then opens a FRESH window (a later membership change against a cluster whose
   * documents have not reached it) has a second decision to report, and an operator who never sees it is told
   * nothing about the second one. Unreachable today - a fingerprint is only ever recorded, never cleared - but
   * the flag has to mean what the window means, or the next change to either makes this silent.
   */
  @Test
  void aFreshWindowReportsItsOwnGiveUpDecision() {
    final FakeServerSecurity security = FakeServerSecurity.create();
    security.on("unconvergedClusterSecurityDocuments", CallLog.inOrder(List.of("users"), List.of("users"), List.of(), List.of("users"), List.of("users")));

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

  // Regression: #7532 a peer whose security documents have not converged used to report READY
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

  // Regression: #7532 a peer whose security documents have not converged used to report READY
  /**
   * A probe answers; it does not propagate. A signal that cannot be read leaves the node READY rather than
   * turning {@code /api/v1/ready} into a 500 the orchestrator reads as "unknown".
   */
  @Test
  void aSignalThatCannotBeReadDoesNotFailTheProbe() {
    final FakeServerSecurity security = FakeServerSecurity.create();
    security.fails("unconvergedClusterSecurityDocuments", new IllegalStateException("not installed"));

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
    // Issue #7819: the gate is armed only on a node that joined the cluster at runtime, which is the node every
    // test in this class is about.
    when(ha.hasJoinedClusterAtRuntime()).thenReturn(true);
    return ha;
  }

  private static ServerSecurity securityMissing(final String... documents) {
    final FakeServerSecurity security = FakeServerSecurity.create();
    security.returns("unconvergedClusterSecurityDocuments", List.of(documents));
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

  private static FakeArcadeDBServer onlineServerWith(final HAServerPlugin ha, final ServerSecurity security,
      final ContextConfiguration configuration) {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.status(ArcadeDBServer.STATUS.ONLINE);
    server.returns("getConfiguration", configuration);
    server.setHA(ha);
    server.security(security);
    return server;
  }

  // Regression: #7819 a runtime join arms the security-convergence gate
  /**
   * Issue #7819: the security-convergence readiness gate of issue #7532 could not default on, because "this node has
   * never installed a replicated security document" is true both of a peer admitted at runtime whose seed has not
   * landed and of every node of a cluster that has simply never replicated one. The gate is now armed only on the
   * first - a node that {@link HAServerPlugin#hasJoinedClusterAtRuntime() joined the cluster at runtime} - and the
   * window defaults on.
   * <p>
   * Every test here runs on the DEFAULT window, because the default is what changed: a test that set the window
   * explicitly would pass on the code before the fix too.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class RuntimeJoinArmsGate {
    /**
     * The case #7532 is about, now held without an operator having to know to switch anything on: a peer added at
     * runtime that holds none of the cluster's documents is NOT READY on the default configuration.
     */
    @Test
    void aRuntimeJoinerIsHeldOnTheDefaultConfiguration() {
      final String reason = new ServerControlPlane(
          onlineServerWith(caughtUpHa(true), securityMissing("users", "groups", "API tokens"))).notReadyReason();

      assertThat(reason).isNotNull();
      assertThat(reason).contains("users", "groups", "API tokens");
    }

    /**
     * The regression the default-off existed to avoid: a statically configured cluster where nobody has run a
     * {@code create user} since it was built has no node with a replicated document. Its nodes did not join at
     * runtime, so they are READY at every start, with the window on.
     */
    @Test
    void aStaticMemberOfAClusterThatNeverReplicatedASecurityDocumentIsReady() {
      assertThat(new ServerControlPlane(
          onlineServerWith(caughtUpHa(false), securityMissing("users", "groups", "API tokens"))).notReadyReason())
          .isNull();
    }

    /**
     * An HA implementation that cannot tell - the interface default, which every non-Raft implementation keeps -
     * leaves the gate disarmed rather than holding every node of it.
     */
    @Test
    void anImplementationThatCannotTellLeavesTheGateDisarmed() {
      final HAServerPlugin legacy = mock(HAServerPlugin.class,
          invocation -> invocation.getMethod().isDefault() ? invocation.callRealMethod() : null);
      when(legacy.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
      when(legacy.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);
      when(legacy.getConfiguredServers()).thenReturn(3);

      assertThat(legacy.hasJoinedClusterAtRuntime()).isFalse();
      assertThat(new ServerControlPlane(onlineServerWith(legacy, securityMissing("users"))).notReadyReason())
          .isNull();
    }

    /**
     * Replay is inert. A runtime joiner that restarts replays the configuration entry that added it and is armed
     * again, but it converged the first time round and the fingerprint recording that is on disk: armed and
     * converged is READY, so a converged node is never held by the re-arm.
     */
    @Test
    void anArmedNodeThatHasConvergedIsReady() {
      assertThat(new ServerControlPlane(onlineServerWith(caughtUpHa(true), securityMissing())).notReadyReason())
          .isNull();
    }

    /**
     * The joiner is armed on the evidence that it was added to a configuration that did not contain it, which
     * proves a peer to converge WITH. The static peer count is the node's own declared server list, not the live
     * configuration, and must not disarm it: a joiner's declared list is whatever its operator wrote.
     */
    @Test
    void aRuntimeJoinerIsHeldWhateverItsOwnDeclaredServerListSays() {
      final HAServerPlugin ha = caughtUpHa(true);
      when(ha.getConfiguredServers()).thenReturn(1);

      assertThat(new ServerControlPlane(onlineServerWith(ha, securityMissing("users"))).notReadyReason())
          .isNotNull();
    }

    /** The default is the one thing this issue moved, so pin it: on, and bounded. */
    @Test
    void theDefaultWindowIsOnAndBounded() {
      final long defaultWindow = new ContextConfiguration().getValueAsLong(
          GlobalConfiguration.HA_SECURITY_CONVERGENCE_READINESS_TIMEOUT);
      assertThat(defaultWindow).isPositive();
      assertThat(defaultWindow).isLessThanOrEqualTo(120_000L);
    }

    // -----------------------------------------------------------------------------------------------------------

    private static HAServerPlugin caughtUpHa(final boolean joinedAtRuntime) {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
      when(ha.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);
      when(ha.getConfiguredServers()).thenReturn(3);
      when(ha.hasJoinedClusterAtRuntime()).thenReturn(joinedAtRuntime);
      return ha;
    }

    private static ServerSecurity securityMissing(final String... documents) {
      return TestServerHelper.securityConvergedExcept(documents);
    }

    /** The window is left at its default on purpose: see the class javadoc. */
    private static ArcadeDBServer onlineServerWith(final HAServerPlugin ha, final ServerSecurity security) {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.HA_ENABLED, true);
      configuration.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);

      final FakeArcadeDBServer server = FakeArcadeDBServer.create((String) null, configuration);
      server.online();
      server.setHA(ha);
      server.security(security);
      return server;
    }
  }

  // Regression: #8317 a node re-added with its config volume retained is gated until its security converges
  /**
   * Issue #8317: a node re-added to the cluster with its config volume retained holds a recorded replicated
   * fingerprint for every security document from its previous membership, so
   * {@code ServerSecurity.unconvergedClusterSecurityDocuments()} is empty and the security-convergence gate released
   * it on the first probe, while it could still enforce a user dropped, a group narrowed or a token revoked while it
   * was out. On a runtime joiner the gate now also waits for
   * {@link HAServerPlugin#securityDocumentsNotInstalledSinceRuntimeJoin()}.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class ReAddedNode {
    /** The issue's entry point: armed, fingerprints on disk from before, nothing installed since the re-add. */
    @Test
    void aReAddedNodeWithFingerprintsFromItsPreviousMembershipIsHeld() {
      final String reason = new ServerControlPlane(onlineServerWith(
          reAddedHa(true, "users", "groups", "API tokens"), fingerprintsMissing())).notReadyReason();

      assertThat(reason).isNotNull();
      assertThat(reason).contains("users", "groups", "API tokens");
    }

    /** The seed lands: every document installed after the re-add, and the node is READY. */
    @Test
    void aReAddedNodeIsReadyOnceEveryDocumentWasInstalledSinceTheReAdd() {
      assertThat(new ServerControlPlane(onlineServerWith(reAddedHa(true), fingerprintsMissing())).notReadyReason())
          .isNull();
    }

    /** Only the documents not installed since the re-add are named, in the usual order. */
    @Test
    void onlyTheDocumentsStillAwaitedAreNamed() {
      final String reason = new ServerControlPlane(onlineServerWith(
          reAddedHa(true, "groups", "API tokens"), fingerprintsMissing())).notReadyReason();

      assertThat(reason).contains("groups, API tokens");
      assertThat(reason).doesNotContain("users");
    }

    /** The two signals are a union: a document never installed at all is still awaited. */
    @Test
    void theFingerprintSignalStillCounts() {
      final String reason = new ServerControlPlane(onlineServerWith(
          reAddedHa(true, "API tokens"), fingerprintsMissing("users"))).notReadyReason();

      assertThat(reason).contains("users, API tokens");
    }

    /**
     * A static member is not armed, so the join-relative signal is never consulted on it, whatever it would answer:
     * #7819's regression guard for a cluster that never replicated a security document is unchanged.
     */
    @Test
    void aStaticMemberIsNotHeldByTheJoinRelativeSignal() {
      assertThat(new ServerControlPlane(onlineServerWith(
          reAddedHa(false, "users", "groups", "API tokens"), fingerprintsMissing())).notReadyReason()).isNull();
    }

    /**
     * An HA implementation that predates the signal keeps the interface default, which awaits nothing, leaving the
     * gate on the fingerprints alone - exactly as before this issue.
     */
    @Test
    void theInterfaceDefaultAwaitsNothing() {
      final HAServerPlugin legacy = mock(HAServerPlugin.class,
          invocation -> invocation.getMethod().isDefault() ? invocation.callRealMethod() : null);
      assertThat(legacy.securityDocumentsNotInstalledSinceRuntimeJoin()).isEmpty();
    }

    // -----------------------------------------------------------------------------------------------------------

    private static HAServerPlugin reAddedHa(final boolean joinedAtRuntime, final String... notInstalledSinceJoin) {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
      when(ha.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);
      when(ha.getConfiguredServers()).thenReturn(3);
      when(ha.hasJoinedClusterAtRuntime()).thenReturn(joinedAtRuntime);
      when(ha.securityDocumentsNotInstalledSinceRuntimeJoin()).thenReturn(List.of(notInstalledSinceJoin));
      return ha;
    }

    private static ServerSecurity fingerprintsMissing(final String... documents) {
      return TestServerHelper.securityConvergedExcept(documents);
    }

    /** The window is left at its default, which is on (issue #7819). */
    private static ArcadeDBServer onlineServerWith(final HAServerPlugin ha, final ServerSecurity security) {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.HA_ENABLED, true);
      configuration.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);

      final FakeArcadeDBServer server = FakeArcadeDBServer.create((String) null, configuration);
      server.online();
      server.setHA(ha);
      server.security(security);
      return server;
    }
  }

  // Regression: #8414 the convergence window resets per join, not on convergence alone
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
  @Nested
  class WindowReset {
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
      return TestServerHelper.securityConvergedExcept(documents);
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
      final FakeArcadeDBServer server = FakeArcadeDBServer.create((String) null, configuration);
      server.online();
      server.setHA(ha);
      server.security(security);
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

  // Regression: #8432 a static member caught up by a snapshot install is gated
  /**
   * Regression test for issue #8432: a STATICALLY configured member - never armed as a runtime joiner (#7819) - that
   * was removed while down, re-added with its config volume retained and caught up by a leader-driven snapshot install
   * past the leader's compaction point was never gated for security convergence. The readiness gate returned early on
   * {@code !hasJoinedClusterAtRuntime()} and the node reported READY while it could still hold a user dropped, a group
   * narrowed or a token revoked while it was out.
   * <p>
   * The gate now holds such a node, transiently, until the documents are confirmed after the install - the
   * leader-confirmed match or a seed applied from the log - or until the same bounded window expires. It does not arm
   * the node, so the fingerprint half of the gate (which a cluster that never replicated a security document fails on
   * every node) is not consulted.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class StaticMemberSnapshotInstall {
    private static final long LONG_WINDOW_MS = 60_000L;
    private static final long INSTALL_INDEX  = 5_000L;

    /** The issue's scenario: unarmed, a snapshot install this process, the documents not yet confirmed after it. */
    @Test
    void aStaticMemberCaughtUpBySnapshotIsHeldUntilItsDocumentsAreConfirmed() {
      final HAServerPlugin ha = staticMemberHa(INSTALL_INDEX, "users", "groups", "API tokens");
      final ServerControlPlane controlPlane = new ServerControlPlane(
          onlineServerWith(ha, fingerprintsMissing(), configurationWith(LONG_WINDOW_MS)));

      final String reason = controlPlane.notReadyReason();
      assertThat(reason).as("the defect: an unarmed node reported READY right after the install").isNotNull();
      assertThat(reason).contains("users", "groups", "API tokens");
    }

    /** Released as soon as the catch-up confirms every document. */
    @Test
    void theConfirmationReleasesIt() {
      final HAServerPlugin ha = staticMemberHa(INSTALL_INDEX);
      when(ha.securityDocumentsNotConfirmedSinceSnapshotInstall())
          .thenReturn(List.of("groups"), List.of());
      final ServerControlPlane controlPlane = new ServerControlPlane(
          onlineServerWith(ha, fingerprintsMissing(), configurationWith(LONG_WINDOW_MS)));

      assertThat(controlPlane.notReadyReason()).contains("groups");
      assertThat(controlPlane.notReadyReason()).isNull();
    }

    /**
     * The fingerprint half of the gate is NOT consulted on an unarmed node: a cluster that never replicated a security
     * document has none on any node, and #7819 exists so that such a cluster is not held. Once the install is
     * confirmed, the node is ready whatever the fingerprints say.
     */
    @Test
    void theFingerprintsAreNotConsultedOnAnUnarmedNode() {
      final HAServerPlugin ha = staticMemberHa(INSTALL_INDEX);
      final ServerControlPlane controlPlane = new ServerControlPlane(
          onlineServerWith(ha, fingerprintsMissing("users", "groups", "API tokens"), configurationWith(LONG_WINDOW_MS)));

      assertThat(controlPlane.notReadyReason()).isNull();
    }

    /** A static member that never caught up by snapshot install in this process is not gated, as before (#7819). */
    @Test
    void noInstallNoHold() {
      final HAServerPlugin ha = staticMemberHa(-1L, "users", "groups", "API tokens");
      final ServerControlPlane controlPlane = new ServerControlPlane(
          onlineServerWith(ha, fingerprintsMissing("users"), configurationWith(LONG_WINDOW_MS)));

      assertThat(controlPlane.notReadyReason()).isNull();
    }

    /** Bounded by the same window: a static member the leader never answers is not held forever. */
    @Test
    void theHoldIsBoundedByTheSameWindow() {
      final HAServerPlugin ha = staticMemberHa(INSTALL_INDEX, "users");
      final ServerControlPlane controlPlane = new ServerControlPlane(
          onlineServerWith(ha, fingerprintsMissing(), configurationWith(1L)));

      assertThat(controlPlane.notReadyReason()).as("opens the window").isNotNull();
      await(2L);
      assertThat(controlPlane.notReadyReason()).as("the window expired").isNull();
    }

    /** A later snapshot install is a fresh window, the way a re-add is on an armed node (#8414). */
    @Test
    void aLaterInstallOpensAFreshWindow() {
      final HAServerPlugin ha = staticMemberHa(INSTALL_INDEX, "users");
      when(ha.getLastSnapshotInstallIndex()).thenReturn(INSTALL_INDEX, INSTALL_INDEX, INSTALL_INDEX + 1_000L);
      final ServerControlPlane controlPlane = new ServerControlPlane(
          onlineServerWith(ha, fingerprintsMissing(), configurationWith(1L)));

      assertThat(controlPlane.notReadyReason()).isNotNull();
      await(2L);
      assertThat(controlPlane.notReadyReason()).as("the first install's window expired").isNull();
      assertThat(controlPlane.notReadyReason()).as("the second install is held again").isNotNull();
    }

    /** An HA implementation that predates the signal keeps the interface defaults: no install known, nothing held. */
    @Test
    void theInterfaceDefaultsReportNoInstall() {
      final HAServerPlugin legacy = mock(HAServerPlugin.class,
          invocation -> invocation.getMethod().isDefault() ? invocation.callRealMethod() : null);
      assertThat(legacy.getLastSnapshotInstallIndex()).isEqualTo(-1L);
      assertThat(legacy.securityDocumentsNotConfirmedSinceSnapshotInstall()).isEmpty();
    }

    // -----------------------------------------------------------------------------------------------------------

    /** An unarmed, caught-up HA plugin that installed a snapshot at {@code installIndex} ({@code -1}: none). */
    private static HAServerPlugin staticMemberHa(final long installIndex, final String... unconfirmed) {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
      when(ha.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);
      when(ha.getConfiguredServers()).thenReturn(3);
      when(ha.hasJoinedClusterAtRuntime()).thenReturn(false);
      when(ha.getLastSnapshotInstallIndex()).thenReturn(installIndex);
      when(ha.securityDocumentsNotConfirmedSinceSnapshotInstall()).thenReturn(List.of(unconfirmed));
      return ha;
    }

    private static ServerSecurity fingerprintsMissing(final String... documents) {
      return TestServerHelper.securityConvergedExcept(documents);
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
      final FakeArcadeDBServer server = FakeArcadeDBServer.create((String) null, configuration);
      server.online();
      server.setHA(ha);
      server.security(security);
      return server;
    }

    private static void await(final long ms) {
      final long until = System.currentTimeMillis() + ms;
      while (System.currentTimeMillis() < until)
        Thread.onSpinWait();
    }
  }

  // Regression: #8446 the convergence window is shared by every ServerControlPlane of a server
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
  @Nested
  class SharedWindow extends StaticBaseServerTest {
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

      final ServerSecurity security = TestServerHelper.securityConvergedExcept("users");

      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.HA_ENABLED, true);
      configuration.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);
      configuration.setValue(GlobalConfiguration.HA_SECURITY_CONVERGENCE_READINESS_TIMEOUT, windowMs);

      // A real server owns its own, fresh security convergence gate
      final FakeArcadeDBServer server = FakeArcadeDBServer.create(configuration).online().security(security);
      server.setHA(ha);
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

  // Regression: #8555 GET /api/v1/cluster publishes the security-convergence gate
  /**
   * Regression test for issue #8555: {@code ServerControlPlane.notReadyReason()} has five sources of a 503, and the
   * security-convergence gate was the one {@code GET /api/v1/cluster} did not publish, so a node held by it read fully
   * green in the status document. {@link ServerControlPlane#getSecurityConvergenceStatus()} is what the document now
   * renders; it must agree with the readiness probe on every state of the gate, and on one shared window: the HTTP probe,
   * the gRPC probe and the status handler each build their own control plane.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ClusterStatus {
    private static final long LONG_WINDOW_MS = 60_000L;
    private static final long INSTALL_INDEX  = 5_000L;

    @Test
    void aHeldNodeIsPublishedWithTheDocumentsTheIndexAndTheWindow() {
      final HAServerPlugin ha = staticMemberHa("users", "groups");
      final ServerControlPlane controlPlane = new ServerControlPlane(onlineServerWith(ha, configurationWith(LONG_WINDOW_MS)));

      final String reason = controlPlane.notReadyReason();
      final SecurityConvergenceStatus status = controlPlane.getSecurityConvergenceStatus();

      assertThat(reason).as("the 503 the document must explain").contains("users, groups");
      assertThat(status.held()).isTrue();
      assertThat(status.reason()).isEqualTo(reason);
      assertThat(status.unconvergedDocuments()).containsExactly("users", "groups");
      assertThat(status.armed()).as("a static member held on a snapshot install").isFalse();
      assertThat(status.sinceIndex()).isEqualTo(INSTALL_INDEX);
      assertThat(status.windowOpenedAt()).isPositive();
      assertThat(status.gaveUp()).isFalse();
      assertThat(status.skippedBecauseLeading()).isFalse();
    }

    @Test
    void anArmedJoinerIsPublishedAsArmedWithItsJoinIndex() {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
      when(ha.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);
      when(ha.hasJoinedClusterAtRuntime()).thenReturn(true);
      when(ha.getRuntimeJoinIndex()).thenReturn(INSTALL_INDEX);
      when(ha.securityDocumentsNotInstalledSinceRuntimeJoin()).thenReturn(List.of("API tokens"));
      final ServerControlPlane controlPlane = new ServerControlPlane(onlineServerWith(ha, configurationWith(LONG_WINDOW_MS)));

      final SecurityConvergenceStatus status = controlPlane.getSecurityConvergenceStatus();

      assertThat(status.held()).isTrue();
      assertThat(status.armed()).isTrue();
      assertThat(status.sinceIndex()).isEqualTo(INSTALL_INDEX);
      assertThat(status.unconvergedDocuments()).containsExactly("API tokens");
    }

    /**
     * The state most worth publishing: past its window the node is READY and enforcing its own copies, and nothing but one
     * SEVERE line, emitted once, ever said so.
     */
    @Test
    void aNodeThatGaveUpIsPublishedAsGaveUpAndNotHeld() throws InterruptedException {
      final HAServerPlugin ha = staticMemberHa("users");
      final ServerControlPlane controlPlane = new ServerControlPlane(onlineServerWith(ha, configurationWith(1L)));

      assertThat(controlPlane.getSecurityConvergenceStatus().held()).as("the window opens").isTrue();
      Thread.sleep(5L);

      final SecurityConvergenceStatus status = controlPlane.getSecurityConvergenceStatus();
      assertThat(controlPlane.notReadyReason()).as("READY").isNull();
      assertThat(status.held()).isFalse();
      assertThat(status.gaveUp()).isTrue();
      assertThat(status.unconvergedDocuments()).as("still unconfirmed, which is the point").containsExactly("users");
      assertThat(status.reason()).isNull();
    }

    @Test
    void aLeaderIsPublishedAsSkippedBecauseLeading() {
      final HAServerPlugin ha = staticMemberHa("groups");
      when(ha.isLeader()).thenReturn(true);
      final ServerControlPlane controlPlane = new ServerControlPlane(onlineServerWith(ha, configurationWith(LONG_WINDOW_MS)));

      final SecurityConvergenceStatus status = controlPlane.getSecurityConvergenceStatus();

      assertThat(status.held()).isFalse();
      assertThat(status.skippedBecauseLeading()).isTrue();
      assertThat(status.unconvergedDocuments()).containsExactly("groups");
      assertThat(status.gaveUp()).isFalse();
    }

    @Test
    void aConvergedNodeAndAGateThatDoesNotApplyPublishNothingToWaitFor() {
      final ServerControlPlane converged = new ServerControlPlane(
          onlineServerWith(staticMemberHa(), configurationWith(LONG_WINDOW_MS)));
      final SecurityConvergenceStatus status = converged.getSecurityConvergenceStatus();
      assertThat(status.held()).isFalse();
      assertThat(status.unconvergedDocuments()).isEmpty();
      assertThat(status.windowOpenedAt()).isZero();

      final ContextConfiguration notRequiringHa = configurationWith(LONG_WINDOW_MS);
      notRequiringHa.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, false);
      assertThat(new ServerControlPlane(onlineServerWith(staticMemberHa("users"), notRequiringHa)).getSecurityConvergenceStatus())
          .isEqualTo(SecurityConvergenceStatus.NOT_CONVERGING);

      assertThat(new ServerControlPlane(onlineServerWith(null, configurationWith(LONG_WINDOW_MS))).getSecurityConvergenceStatus())
          .as("no HA layer").isEqualTo(SecurityConvergenceStatus.NOT_CONVERGING);

      assertThat(new ServerControlPlane(onlineServerWith(staticMemberHa("users"), configurationWith(0L)))
          .getSecurityConvergenceStatus()).as("timeout 0 disables the gate").isEqualTo(SecurityConvergenceStatus.NOT_CONVERGING);
    }

    /**
     * The HTTP probe, the gRPC probe and the status handler each build their own control plane: the window they consult has
     * to be the server's, or the document could say "held" while the probe said READY, or the reverse.
     */
    @Test
    void everyControlPlaneOfAServerSeesTheSameWindow() throws InterruptedException {
      final ArcadeDBServer server = onlineServerWith(staticMemberHa("users"), configurationWith(1L));
      final ServerControlPlane readinessProbe = new ServerControlPlane(server);
      final ServerControlPlane statusHandler = new ServerControlPlane(server);

      assertThat(readinessProbe.notReadyReason()).as("the probe opens the window").isNotNull();
      final long opened = statusHandler.getSecurityConvergenceStatus().windowOpenedAt();
      assertThat(opened).as("the status handler reads that window, it does not open its own").isPositive();

      Thread.sleep(5L);
      assertThat(statusHandler.getSecurityConvergenceStatus().gaveUp()).as("expired for the handler").isTrue();
      assertThat(readinessProbe.notReadyReason()).as("and for the probe").isNull();
      assertThat(statusHandler.getSecurityConvergenceStatus().windowOpenedAt()).isEqualTo(opened);
    }

    /**
     * The gate opens its window on first sight, and the probe only reaches it once every earlier
     * check passed. A status poll during catch-up must not start the clock, or the window is already spent when the
     * probe first evaluates the gate and the node goes straight to give-up.
     */
    @Test
    void aStatusPollBeforeTheNodeIsConsensusReadyDoesNotOpenTheWindow() {
      final HAServerPlugin ha = staticMemberHa("users");
      when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.VOTING_FOR_ME);
      final SecurityConvergenceGate gate = new SecurityConvergenceGate();
      final ArcadeDBServer server = onlineServerWith(ha, configurationWith(LONG_WINDOW_MS)).securityConvergenceGate(gate);
      final ServerControlPlane controlPlane = new ServerControlPlane(server);

      assertThat(controlPlane.getSecurityConvergenceStatus()).isEqualTo(SecurityConvergenceStatus.NOT_CONVERGING);
      assertThat(controlPlane.notReadyReason()).contains("Raft group");
      assertThat(gate.windowOpenedAt).as("still catching up: the window is not opened").isZero();

      when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
      assertThat(controlPlane.getSecurityConvergenceStatus().held()).isTrue();
      assertThat(gate.windowOpenedAt).as("opened once the probe would reach the gate").isPositive();
    }

    /**
     * A node that gave up keeps being reported as given up while it is briefly held for another reason, and reading it does
     * not evaluate the gate: a join index that moved forward (a re-add) must not reset the window or open a new one.
     */
    @Test
    void aGaveUpNodeStaysReportedWhileHeldForAnotherReasonWithoutStartingAClock() throws InterruptedException {
      final HAServerPlugin ha = staticMemberHa("users");
      final SecurityConvergenceGate gate = new SecurityConvergenceGate();
      final ArcadeDBServer server = onlineServerWith(ha, configurationWith(1L)).securityConvergenceGate(gate);
      final ServerControlPlane controlPlane = new ServerControlPlane(server);

      assertThat(controlPlane.notReadyReason()).isNotNull();
      Thread.sleep(5L);
      assertThat(controlPlane.getSecurityConvergenceStatus().gaveUp()).isTrue();
      final long opened = gate.windowOpenedAt;

      when(ha.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.NOT_READY);
      when(ha.getLastSnapshotInstallIndex()).thenReturn(INSTALL_INDEX + 1000L);

      final SecurityConvergenceStatus status = controlPlane.getSecurityConvergenceStatus();
      assertThat(controlPlane.notReadyReason()).as("held for an earlier reason").contains("caught up");
      assertThat(status.gaveUp()).as("the critical alert does not flap").isTrue();
      assertThat(status.unconvergedDocuments()).containsExactly("users");
      assertThat(gate.windowOpenedAt).as("no clock started or reset by the poll").isEqualTo(opened);
      assertThat(gate.giveUpLogged).isTrue();
    }

    // -----------------------------------------------------------------------------------------------------------

    /** An unarmed, caught-up HA plugin held after a snapshot install at {@link #INSTALL_INDEX}. */
    private static HAServerPlugin staticMemberHa(final String... unconfirmed) {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
      when(ha.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);
      when(ha.getConfiguredServers()).thenReturn(3);
      when(ha.hasJoinedClusterAtRuntime()).thenReturn(false);
      when(ha.getLastSnapshotInstallIndex()).thenReturn(INSTALL_INDEX);
      when(ha.securityDocumentsNotConfirmedSinceSnapshotInstall()).thenReturn(List.of(unconfirmed));
      return ha;
    }

    private static ContextConfiguration configurationWith(final long windowMs) {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.HA_ENABLED, true);
      configuration.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);
      configuration.setValue(GlobalConfiguration.HA_SECURITY_CONVERGENCE_READINESS_TIMEOUT, windowMs);
      return configuration;
    }

    private static FakeArcadeDBServer onlineServerWith(final HAServerPlugin ha, final ContextConfiguration configuration) {
      final ServerSecurity security = TestServerHelper.securityConvergedExcept();
      final FakeArcadeDBServer server = FakeArcadeDBServer.create((String) null, configuration);
      server.online();
      server.setHA(ha);
      server.security(security);
      return server;
    }
  }
}
