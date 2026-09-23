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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for the readiness half of issue #7872, the sibling of {@link Issue7118ReadinessRaftLogFailureTest}.
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
class Issue7872ReadinessCriticalHaltTest {

  private static ArcadeDBServer onlineServerWith(final HAServerPlugin ha, final ContextConfiguration configuration) {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getStatus()).thenReturn(ArcadeDBServer.STATUS.ONLINE);
    when(server.getConfiguration()).thenReturn(configuration);
    when(server.getHA()).thenReturn(ha);
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
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getStatus()).thenReturn(ArcadeDBServer.STATUS.STARTING);
    when(server.getHA()).thenReturn(ha);

    assertThat(new ServerControlPlane(server).notReadyReason()).isEqualTo("Server not started yet");
  }
}
