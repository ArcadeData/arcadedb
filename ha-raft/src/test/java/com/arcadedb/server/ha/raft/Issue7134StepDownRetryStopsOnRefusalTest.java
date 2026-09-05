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
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression test for the retry half of issue #7134.
 * <p>
 * The phase-2 failure recovery re-checks {@code isLeader()} and then calls {@code stepDown()} up to three times
 * with 500ms between attempts. Once {@code stepDown()} refuses because this node is no longer the leader, that
 * refusal is not a transient failure - leadership has already moved, there is nothing left to hand off, and
 * every remaining attempt is guaranteed to refuse identically. Retrying burns ~1.5s on the commit path and,
 * worse, ends at the "all step-down attempts failed" branch, which under
 * {@code arcadedb.ha.stopServerOnReplicationFailure} stops a server whose leadership problem has already
 * resolved itself.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7134StepDownRetryStopsOnRefusalTest {

  private static final String NEW_LEADER = "peer-b_2435";

  /**
   * Drives the private recovery directly: reaching it through a real phase-2 failure would need a live cluster,
   * and the behaviour under test is entirely local to the retry loop.
   */
  private static void recover(final RaftReplicatedDatabase db) throws Exception {
    final Method m = RaftReplicatedDatabase.class
        .getDeclaredMethod("recoverLeadershipAfterPhase2Failure", String.class);
    m.setAccessible(true);
    m.invoke(db, "tx-1");
  }

  private static RaftReplicatedDatabase databaseWith(final RaftHAServer raft, final ArcadeDBServer server) {
    return new RaftReplicatedDatabase(server, mock(LocalDatabase.class), raft);
  }

  @Test
  void aRefusalStopsTheRetryLoopImmediatelyAndNeverStopsTheServer() throws Exception {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(true); // true when recovery starts, then leadership moves
    doThrow(new NotTheLeaderRefusalException("Refusing to step down",
        RaftPeerId.valueOf(NEW_LEADER))).when(raft).stepDown();

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_STOP_SERVER_ON_REPLICATION_FAILURE, true);
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(config);

    recover(databaseWith(raft, server));

    verify(raft, times(1)).stepDown();
    verify(server, never()).stop();
  }

  /**
   * Control: any OTHER step-down failure is still retried the full three times, because that one can succeed on
   * a later attempt. Without this the test above would also pass against a loop that never retries at all.
   */
  @Test
  void anOrdinaryStepDownFailureIsStillRetried() throws Exception {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(true);
    doThrow(new IllegalStateException("transfer timed out")).when(raft).stepDown();

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_STOP_SERVER_ON_REPLICATION_FAILURE, false);
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(config);

    recover(databaseWith(raft, server));

    verify(raft, times(3)).stepDown();
  }

  /** And a node that is not the leader when recovery starts never attempts a step-down at all. */
  @Test
  void aNonLeaderNeverAttemptsAStepDown() throws Exception {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(false);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);

    recover(databaseWith(raft, server));

    verify(raft, never()).stepDown();
  }
}
