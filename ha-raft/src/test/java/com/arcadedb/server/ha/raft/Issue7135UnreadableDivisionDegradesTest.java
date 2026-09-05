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
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #7135: {@code getLivePeers()} and {@code isReadyForTraffic()} caught only
 * {@link java.io.IOException}, while Ratis throws an {@link IllegalStateException}
 * ("stateMachineUpdater is uninitialized") for the whole window in which an in-place division restart
 * re-initializes (issue #5271). Both methods therefore propagated an unchecked exception instead of
 * degrading to the fallback their javadoc promises.
 * <p>
 * That window is opened by the health monitor's {@code restartRatisIfNeeded()} on a CLOSED division, i.e.
 * exactly during the pod churn the readers are supposed to survive. After the #7040 fix
 * {@code configuredPeers()} - and with it {@code getStats()}, {@code getReplicaAddresses()} and the
 * Bolt/gRPC routing table - all read {@code getLivePeers()}, so one unguarded read 500s
 * {@code /api/v1/cluster} and aborts the health tick that is driving the recovery.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7135UnreadableDivisionDegradesTest {

  private static final String SERVER_LIST = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482";

  /** The exact exception Ratis raises while a division is being re-initialized in place. */
  private static IllegalStateException ratisRestartWindow() {
    return new IllegalStateException("stateMachineUpdater is uninitialized");
  }

  @Test
  void livePeersFallsBackToTheDeclaredListWhenTheDivisionThrowsUnchecked() throws Exception {
    final RaftHAServer raft = detachedServerWithUnreadableDivision();

    assertThat(raft.getLivePeers())
        .as("an unreadable division must degrade to the declared server list, not propagate")
        .extracting(peer -> ((RaftPeer) peer).getId().toString())
        .hasSize(3);
  }

  /**
   * The distinction {@code getCommittedPeersOrNull} exists for (issue #7136) must survive the widened catch:
   * "cannot read" is still no information, never the declared list dressed up as a committed membership.
   */
  @Test
  void committedPeersStillReportsNoInformationRatherThanTheDeclaredList() throws Exception {
    final RaftHAServer raft = detachedServerWithUnreadableDivision();

    assertThat(raft.getCommittedPeersOrNull()).isNull();
  }

  @Test
  void readinessFailsClosedWhenTheDivisionThrowsUnchecked() throws Exception {
    final RaftHAServer raft = detachedServerWithUnreadableDivision();

    assertThat(raft.isReadyForTraffic(10L))
        .as("the probe must answer NOT_READY, not 500")
        .isFalse();
  }

  /**
   * The blast radius #7135 is really about: every membership view behind {@code /api/v1/cluster} and the
   * client routing tables reads {@code getLivePeers()} through {@code configuredPeers()} since #7040.
   */
  @Test
  void theClusterStatusPayloadIsStillBuiltDuringTheRestartWindow() throws Exception {
    final RaftHAServer raft = detachedServerWithUnreadableDivision();

    assertThat(raft.getStats()).containsKey("replicas");
    assertThat(raft.getReplicaAddresses()).isNotNull();
  }

  /**
   * A {@link RaftHAServer} built from the static server list, with a Ratis server whose division cannot be
   * resolved because it is mid-restart. Ratis is never started; the mock stands in for the exact throw the
   * three hardened sibling readers ({@code isLeader}, {@code getLeaderId}, {@code getLastAppliedIndex})
   * already document and guard.
   */
  private static RaftHAServer detachedServerWithUnreadableDivision() throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);

    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getServerName()).thenReturn("ArcadeDB_0");

    final RaftHAServer raft = new RaftHAServer(mockServer, config);

    final RaftServer ratis = mock(RaftServer.class);
    when(ratis.getDivision(any())).thenThrow(ratisRestartWindow());

    final Field field = RaftHAServer.class.getDeclaredField("raftServer");
    field.setAccessible(true);
    field.set(raft, ratis);

    return raft;
  }
}
