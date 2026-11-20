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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The readiness and liveness probes of {@code ServerControlPlane}: a failed Raft log, the bootstrap window, an escalated
 * crash loop and a critical halt.
 */
class ServerReadinessTest {
  /*
   * Regression test for issue #7118, the follow-up to #7037.
   * <p>
   * #7037 gave a Raft log-writer failure a detection signal and a bounded in-place restart driven by the health
   * monitor. Nothing wired that signal to readiness, so a follower whose log writer was wedged kept answering 200
   * on {@code /api/v1/ready}: Kubernetes left the pod in the Service and kept routing reads to a replica frozen at
   * the moment the writer failed - stale reads, no error anywhere on the request path, and on a 3-node cluster
   * roughly a third of the reads.
   * <p>
   * {@code ServerControlPlane.notReadyReason()} now consults {@link HAServerPlugin#getRaftLogFailure()}. The gate is
   * OUTSIDE {@code arcadedb.server.readinessRequiresHA} on purpose, and the last test here pins that: that switch is
   * opt-in because it gates a node that is merely BEHIND, which a deployment can reasonably serve from, whereas a
   * node whose log writer has failed cannot catch up at all. Putting this behind the switch would leave every
   * default deployment - the one the issue describes - exactly as it was.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static ArcadeDBServer onlineServerWith(final HAServerPlugin ha, final ContextConfiguration configuration) {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create(configuration);
    server.online();
    server.setHA(ha);
    return server;
  }

  private static ContextConfiguration haConfiguration(final boolean readinessRequiresHA) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_ENABLED, true);
    configuration.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, readinessRequiresHA);
    return configuration;
  }

  // Regression: #7118 a node whose Raft log failed must not report READY
  // Regression: #7519 the bootstrap window does not gate a server without HA
  /** No HA layer at all: nothing to gate, and {@code getHA()} is null rather than a plugin answering null. */
  @Test
  void readyWhenThereIsNoHaLayerAtAll() {
    assertThat(new ServerControlPlane(onlineServerWith(null, new ContextConfiguration())).notReadyReason()).isNull();
  }

  // Regression: #7118 a node whose Raft log failed must not report READY
  @Test
  void readyWhileTheLogWriterIsHealthy() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getRaftLogFailure()).thenReturn(null);
    when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
    when(ha.getReadinessSignal(org.mockito.ArgumentMatchers.anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);

    assertThat(new ServerControlPlane(onlineServerWith(ha, haConfiguration(true))).notReadyReason()).isNull();
  }

  // Regression: #7118 a node whose Raft log failed must not report READY
  @Test
  void notReadyOnceTheLogWriterHasFailed() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getRaftLogFailure()).thenReturn("at index 4711: java.io.IOException: No space left on device");

    final String reason = new ServerControlPlane(onlineServerWith(ha, haConfiguration(true))).notReadyReason();

    assertThat(reason).isNotNull();
    assertThat(reason).contains("No space left on device");
  }

  // Regression: #7118 a node whose Raft log failed must not report READY
  /**
   * The point of the issue: the gate must not be behind {@code readinessRequiresHA}, which is off by default.
   * With the gate inside it, this exact configuration - a wedged follower on a cluster that never opted in - kept
   * answering Ready, which is the deployment the report describes.
   */
  @Test
  void notReadyOnceTheLogWriterHasFailedEvenWithoutReadinessRequiresHA() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getRaftLogFailure()).thenReturn("on a log segment: java.io.IOException: disk full");

    assertThat(new ServerControlPlane(onlineServerWith(ha, haConfiguration(false))).notReadyReason())
        .as("a wedged log writer removes the node from the Service on a default-configured cluster too")
        .isNotNull();
  }

  // Regression: #7118 a node whose Raft log failed must not report READY
  /**
   * A probe must answer, not 500. A plugin that throws while being asked is treated as having no signal, leaving
   * the remaining gates to decide.
   */
  @Test
  void aSignalThatCannotBeReadDoesNotFailTheProbe() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getRaftLogFailure()).thenThrow(new IllegalStateException("plugin not initialized"));

    assertThat(new ServerControlPlane(onlineServerWith(ha, haConfiguration(false))).notReadyReason()).isNull();
  }

  // Regression: #7118 a node whose Raft log failed must not report READY
  /**
   * The failure is reported BEFORE the server-status gate has been passed? No: a server that is not ONLINE yet
   * still answers with the status reason, because a node that has not started is not a node whose log writer
   * wedged. Pins the order so a future edit does not report the more alarming reason for the ordinary case.
   */
  @Test
  void aServerStillStartingReportsItsStatusNotTheLogFailure() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getRaftLogFailure()).thenReturn("at index 1: boom");
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.status(ArcadeDBServer.STATUS.STARTING);
    server.setHA(ha);

    assertThat(new ServerControlPlane(server).notReadyReason()).isEqualTo("Server not started yet");
  }

  /*
   * Regression test for issue #7622: before this fix, once a node's HA layer escalated a crash loop and gave
   * up restarting, {@code ServerControlPlane.isLive()} - the Kubernetes liveness probe - stayed unconditionally
   * {@code true}. Readiness already failed (the division's own lifecycle folds into
   * {@code RaftHAServer.isReadyForTraffic()}), so the node sat NotReady forever with liveness green: removed
   * from the Service, but never restarted, because the only probe that triggers a restart was the one still
   * passing. The SEVERE alert {@code HealthMonitor} raises at give-up said "a pod/process restart is the way
   * out", but nothing performed it.
   * <p>
   * {@code isLive()} now fails so that restart happens automatically - once (issue #7736): it consults
   * {@link HAServerPlugin#isCrashLoopRestartPending()}, not {@link HAServerPlugin#isCrashLoopEscalated()}, because an
   * escalation inherited by the restarted process must not fail liveness again and loop the pod forever.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #7622 liveness turns false once the HA layer escalated a crash loop
  @Test
  void isLiveWhenHaIsNotEnabled() {
    final ArcadeDBServer server = TestServerHelper.unstartedServer();

    assertThat(new ServerControlPlane(server).isLive()).isTrue();
  }

  // Regression: #7622 liveness turns false once the HA layer escalated a crash loop
  @Test
  void isLiveWhenHaHasNotEscalatedACrashLoop() {
    final ArcadeDBServer server = TestServerHelper.unstartedServer();
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.isCrashLoopEscalated()).thenReturn(false);
    server.setHA(ha);

    assertThat(new ServerControlPlane(server).isLive()).isTrue();
  }

  // Regression: #7622 liveness turns false once the HA layer escalated a crash loop
  @Test
  void isNotLiveOnceHaHasEscalatedACrashLoopInThisLifetime() {
    final ArcadeDBServer server = TestServerHelper.unstartedServer();
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.isCrashLoopEscalated()).thenReturn(true);
    when(ha.isCrashLoopRestartPending()).thenReturn(true);
    server.setHA(ha);

    assertThat(new ServerControlPlane(server).isLive()).isFalse();
  }

  // Regression: #7622 liveness turns false once the HA layer escalated a crash loop
  /**
   * Issue #7736: the restarted process inherits the recorded escalation and still crash-loops. Another restart
   * cannot help - the cause is served by the leader - so liveness stays green and the node parks, out of the
   * Service through readiness, instead of entering a perpetual CrashLoopBackOff.
   */
  @Test
  void isLiveWhenTheEscalationWasInheritedFromAPreviousLifetime() {
    final ArcadeDBServer server = TestServerHelper.unstartedServer();
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.isCrashLoopEscalated()).thenReturn(true);
    when(ha.isCrashLoopRestartPending()).thenReturn(false);
    server.setHA(ha);

    assertThat(new ServerControlPlane(server).isLive()).isTrue();
  }

  // Regression: #7519 readiness during the bootstrap window
  /**
   * Regression tests for the production half of issue #7519, the residue of #7259.
   * <p>
   * #7259 was closed by a test-harness gate: a test body no longer starts while the cluster's first-formation
   * bootstrap is still replacing database directories. Nothing gated a PRODUCTION client out of the same window.
   * The window is real and it is long: {@code SnapshotInstaller.install} downloads the leader's snapshot before it
   * touches the live files, so the local copy - the one the committed baseline has just decided against - stays
   * open and serving for the whole download, on every protocol. The node-wide {@code snapshotInstallInProgress}
   * 503 covers only the file swap at the end of it, and only HTTP.
   * <p>
   * {@code ServerControlPlane.notReadyReason()} now consults {@link HAServerPlugin#getBootstrapWindowReason()}, so
   * an orchestrator takes the pod out of the Service for the duration - which gates every protocol at once, not
   * just the one that has a 503 to send. Same shape, and the same OUTSIDE-{@code readinessRequiresHA} placement,
   * as the #7118 log-failure gate: that switch is opt-in because it gates a node that is merely BEHIND, and this
   * node is not behind, it is serving something the cluster did not adopt.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BootstrapWindow {
    private static final String INSTALLING =
        "The cluster's first-formation bootstrap is replacing 1 database(s) on this node from the leader's snapshot";

    private static ArcadeDBServer onlineServerWith(final HAServerPlugin ha, final ContextConfiguration configuration) {
      final FakeArcadeDBServer server = FakeArcadeDBServer.create(configuration);
      server.online();
      server.setHA(ha);
      return server;
    }

    private static ContextConfiguration haConfiguration(final boolean readinessRequiresHA) {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.HA_ENABLED, true);
      configuration.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, readinessRequiresHA);
      return configuration;
    }

    /** A caught-up node with no bootstrap install and nothing unreconciled is ready, as it always was. */
    @Test
    void readyWhileNoBootstrapWindowIsOpen() {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getBootstrapWindowReason()).thenReturn(null);
      when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
      when(ha.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);

      assertThat(new ServerControlPlane(onlineServerWith(ha, haConfiguration(true))).notReadyReason()).isNull();
    }

    /** The reported hole: a node whose directory is being replaced under it kept answering 200 on /ready. */
    @Test
    void notReadyWhileABootstrapInstallIsReplacingADatabase() {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getBootstrapWindowReason()).thenReturn(INSTALLING);

      final String reason = new ServerControlPlane(onlineServerWith(ha, haConfiguration(true))).notReadyReason();

      assertThat(reason).isEqualTo(INSTALLING);
    }

    /**
     * The point of the gate: {@code readinessRequiresHA} is off by default, so a gate behind it would leave the
     * deployment the issue describes - a cluster behind a Service, forming for the first time - exactly as it was.
     */
    @Test
    void notReadyDuringTheBootstrapWindowEvenWithoutReadinessRequiresHA() {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getBootstrapWindowReason()).thenReturn(INSTALLING);

      assertThat(new ServerControlPlane(onlineServerWith(ha, haConfiguration(false))).notReadyReason())
          .as("a node mid-bootstrap leaves the Service on a default-configured cluster too")
          .isEqualTo(INSTALLING);
    }

    /**
     * The state the window leaves behind (issue #6124): the peer that refused to overwrite its fresher copy keeps
     * serving a database whose file ids no other peer shares, durably, across restarts.
     */
    @Test
    void notReadyWhileHoldingACopyTheBootstrapBaselineDidNotAdopt() {
      final String unreconciled = "1 database(s) on this node hold a copy the cluster's committed bootstrap "
          + "baseline did not adopt";
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getBootstrapWindowReason()).thenReturn(unreconciled);

      assertThat(new ServerControlPlane(onlineServerWith(ha, haConfiguration(false))).notReadyReason())
          .isEqualTo(unreconciled);
    }

    /** A probe must answer, not 500. A plugin that throws is treated as having no signal. */
    @Test
    void aSignalThatCannotBeReadDoesNotFailTheProbe() {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getBootstrapWindowReason()).thenThrow(new IllegalStateException("plugin not initialized"));

      assertThat(new ServerControlPlane(onlineServerWith(ha, haConfiguration(false))).notReadyReason()).isNull();
    }

    /**
     * Order, pinned: the wedged log writer is the more fundamental report, because a node in both states cannot
     * finish the bootstrap install either. A future edit that reorders the two would report the less useful reason.
     */
    @Test
    void aWedgedLogWriterIsReportedBeforeTheBootstrapWindow() {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getRaftLogFailure()).thenReturn("at index 4711: java.io.IOException: No space left on device");
      when(ha.getBootstrapWindowReason()).thenReturn(INSTALLING);

      assertThat(new ServerControlPlane(onlineServerWith(ha, haConfiguration(false))).notReadyReason())
          .contains("No space left on device");
    }

    /**
     * A server that has not started yet answers with its status: a node that never came up is not a node whose
     * bootstrap is replacing a directory.
     */
    @Test
    void aServerStillStartingReportsItsStatusNotTheBootstrapWindow() {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getBootstrapWindowReason()).thenReturn(INSTALLING);
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.status(ArcadeDBServer.STATUS.STARTING);
      server.setHA(ha);

      assertThat(new ServerControlPlane(server).notReadyReason()).isEqualTo("Server not started yet");
    }
  }

  // Regression: #7872 a node in critical halt must not report READY
  /**
   * Regression test for the readiness half of issue #7872, the sibling of {@link ServerReadinessTest}.
   * <p>
   * The node-wide critical halt had two reporting defects at once, and this covers both.
   * <p>
   * First, the WORDING. The halt was reported through the generic {@code getReadinessSignal()} branch, so the 503
   * body said the node "is not yet in the Raft configuration or has not caught up" - which points at waiting, and
   * waiting is the one thing that cannot work: the state machine refuses every apply after the halt and will never
   * catch up. It now has its own branch and says so.
   * <p>
   * Second, the GATE. Because it was reported only through that branch, it was reported only when
   * {@code arcadedb.server.readinessRequiresHA} was on. With the switch off - the default - the node answered 204
   * while its state machine was dead. That is a strictly worse variant of the same reporting gap rather than a
   * deployment choice, so the check sits outside the switch, exactly where #7118 put the log-writer one and for the
   * same reason: that switch gates a node that is BEHIND, and this node is not behind, it has stopped.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CriticalHalt {
    private static ArcadeDBServer onlineServerWith(final HAServerPlugin ha, final ContextConfiguration configuration) {
      final FakeArcadeDBServer server = FakeArcadeDBServer.create(configuration);
      server.online();
      server.setHA(ha);
      return server;
    }

    private static ContextConfiguration haConfiguration(final boolean readinessRequiresHA) {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.HA_ENABLED, true);
      configuration.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, readinessRequiresHA);
      return configuration;
    }

    @Test
    void readyWhileTheStateMachineIsStillApplying() {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getCriticalHaltReason()).thenReturn(null);
      when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
      when(ha.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.READY);

      assertThat(new ServerControlPlane(onlineServerWith(ha, haConfiguration(true))).notReadyReason()).isNull();
    }

    /**
     * The wording. An operator told to wait for a node that has stopped applying reads the one message that points
     * at the one remedy that cannot work.
     */
    @Test
    void aHaltedNodeSaysItHaltedRatherThanThatItHasNotCaughtUp() {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getCriticalHaltReason()).thenReturn("at index 4711: unknown Raft log entry type");
      when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
      when(ha.getReadinessSignal(anyLong())).thenReturn(HAServerPlugin.READINESS_SIGNAL.NOT_READY);

      final String reason = new ServerControlPlane(onlineServerWith(ha, haConfiguration(true))).notReadyReason();

      assertThat(reason).contains("halted");
      assertThat(reason).contains("index 4711");
      assertThat(reason)
          .as("the generic 'has not caught up' points at waiting, which is exactly what cannot work here")
          .doesNotContain("has not caught up");
    }

    /**
     * The gate. With {@code readinessRequiresHA} off - the default - a node whose state machine is dead answered
     * 204 and stayed in the Service.
     */
    @Test
    void aHaltedNodeIsNotReadyEvenWithoutReadinessRequiresHA() {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getCriticalHaltReason()).thenReturn("at index 9: unexpected error applying a committed entry");

      assertThat(new ServerControlPlane(onlineServerWith(ha, haConfiguration(false))).notReadyReason())
          .as("a dead state machine removes the node from the Service on a default-configured cluster too")
          .isNotNull();
    }

    /**
     * A probe must answer, not 500. A plugin that throws while being asked is treated as having no signal, leaving
     * the remaining gates to decide - the same defensiveness the log-failure read has.
     */
    @Test
    void aSignalThatCannotBeReadDoesNotFailTheProbe() {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getCriticalHaltReason()).thenThrow(new IllegalStateException("plugin not initialized"));

      assertThat(new ServerControlPlane(onlineServerWith(ha, haConfiguration(false))).notReadyReason()).isNull();
    }

    /**
     * The log-writer failure is reported first when both hold. Neither is more recoverable than the other from the
     * probe's point of view, but the order has to be fixed rather than incidental, and the writer failure is the
     * one that can clear by itself.
     */
    @Test
    void theLogWriterFailureIsReportedFirstWhenBothHold() {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getRaftLogFailure()).thenReturn("on a log segment: disk full");
      when(ha.getCriticalHaltReason()).thenReturn("at index 9: boom");

      assertThat(new ServerControlPlane(onlineServerWith(ha, haConfiguration(false))).notReadyReason())
          .startsWith("Replication log writer has failed");
    }

    /**
     * And a node that has not started yet still reports its status: a node that never came up is not a node whose
     * state machine halted. Pins the order so a future edit does not report the more alarming reason for the
     * ordinary case.
     */
    @Test
    void aServerStillStartingReportsItsStatusNotTheHalt() {
      final HAServerPlugin ha = mock(HAServerPlugin.class);
      when(ha.getCriticalHaltReason()).thenReturn("at index 1: boom");
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.status(ArcadeDBServer.STATUS.STARTING);
      server.setHA(ha);

      assertThat(new ServerControlPlane(server).notReadyReason()).isEqualTo("Server not started yet");
    }
  }
}
