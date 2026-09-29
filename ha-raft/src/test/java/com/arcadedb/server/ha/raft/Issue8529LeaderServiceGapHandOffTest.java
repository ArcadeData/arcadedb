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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Issue #8529: the #8491 hand-off moved leadership away only from a leader that was REPLACING one of its databases.
 * Three other states in which a leader cannot serve a database the cluster expects it to were only logged or alerted,
 * and nothing moved leadership to a peer that could:
 * <ol>
 *   <li>a database the committed bootstrap baseline says the cluster has, missing on this node (#7298);</li>
 *   <li>a pending bootstrap replacement - this node still holds the copy the baseline rejected (#8367);</li>
 *   <li>an unfilled stale-snapshot gap - entries this node's databases never received (#6111).</li>
 * </ol>
 * A leader cannot install from itself, so in each case it stayed wedged until an operator moved leadership by hand.
 */
class Issue8529LeaderServiceGapHandOffTest {

  private static final String MISSING_DB = "missing-here";
  private static final String KEPT_DB    = "kept-here";

  @TempDir
  private Path serverDir;

  private final List<ArcadeStateMachine> stateMachines = new ArrayList<>();

  @AfterEach
  void closeStateMachines() {
    for (final ArcadeStateMachine sm : stateMachines)
      try {
        sm.close();
      } catch (final IOException e) {
        // Teardown of a unit-test fixture: a close that fails must not replace the test's own verdict.
      }
    stateMachines.clear();
  }

  // -- the three states hand leadership off ------------------------------------------------------------------------

  @Test
  void aLeaderMissingADatabaseTheBaselineCommittedHandsOff() {
    final RaftHAServer raft = leader(true);
    final ArcadeStateMachine sm = stateMachine(raft);
    sm.markBootstrapUnreconciled(MISSING_DB); // marked, and no directory on disk: missing

    assertThat(sm.hasLeaderServiceGap()).isTrue();
    assertThat(sm.handOffLeadershipWhileReplacingDatabase()).isTrue();
    verify(raft).transferLeadership(anyLong());
  }

  @Test
  void aLeaderWithAPendingBootstrapReplacementHandsOff() throws Exception {
    final RaftHAServer raft = leader(true);
    final ArcadeStateMachine sm = stateMachine(raft);
    pendingBootstrapReplacements(sm).add(KEPT_DB);

    assertThat(sm.hasLeaderServiceGap()).isTrue();
    assertThat(sm.handOffLeadershipWhileReplacingDatabase()).isTrue();
    verify(raft).transferLeadership(anyLong());
  }

  @Test
  void aLeaderHoldingAStaleSnapshotGapHandsOff() throws Exception {
    final RaftHAServer raft = leader(true);
    final ArcadeStateMachine sm = stateMachine(raft);
    staleSnapshotAppliedFloor(sm).set(100L);

    assertThat(sm.hasLeaderServiceGap()).isTrue();
    assertThat(sm.handOffLeadershipWhileReplacingDatabase()).isTrue();
    verify(raft).transferLeadership(anyLong());
  }

  // -- what must NOT hand off --------------------------------------------------------------------------------------

  /**
   * The other half of the unreconciled mark: the #6124 guard KEPT a fresher local copy. The data is here and this
   * leader serves it; moving leadership would only put the cluster through an election for nothing.
   */
  @Test
  void aLeaderThatKeptItsOwnCopyDoesNotHandOff() throws Exception {
    final RaftHAServer raft = leader(true);
    final ArcadeStateMachine sm = stateMachine(raft);
    final Path kept = serverDir.resolve(KEPT_DB);
    Files.createDirectories(kept);
    Files.writeString(kept.resolve("schema.json"), "{}");
    sm.markBootstrapUnreconciled(KEPT_DB);

    assertThat(sm.hasLeaderServiceGap()).isFalse();
    assertThat(sm.handOffLeadershipWhileReplacingDatabase()).isFalse();
    verify(raft, never()).transferLeadership(anyLong());
  }

  @Test
  void aFollowerWithAGapDoesNotTransfer() throws Exception {
    final RaftHAServer raft = leader(false);
    final ArcadeStateMachine sm = stateMachine(raft);
    staleSnapshotAppliedFloor(sm).set(100L);
    pendingBootstrapReplacements(sm).add(KEPT_DB);

    assertThat(sm.handOffLeadershipWhileReplacingDatabase()).isFalse();
    verify(raft, never()).transferLeadership(anyLong());
  }

  @Test
  void aHealthyLeaderHasNoGap() {
    final RaftHAServer raft = leader(true);
    final ArcadeStateMachine sm = stateMachine(raft);

    assertThat(sm.hasLeaderServiceGap()).isFalse();
    assertThat(sm.handOffLeadershipWhileReplacingDatabase()).isFalse();
    verify(raft, never()).transferLeadership(anyLong());
  }

  // -- the health tick reaches it ----------------------------------------------------------------------------------

  /**
   * The path production takes: the health tick asks {@link RaftHAServer#queueReplacingDatabaseHandOff}, which used to
   * return before reaching the state machine unless a database was being REPLACED. A leader with a stale-snapshot gap
   * and nothing being replaced must still get its hand-off queued and run.
   */
  @Test
  void theHealthTickQueuesTheHandOffForAStaleSnapshotGap() throws Exception {
    final CountDownLatch transferred = new CountDownLatch(1);
    final RaftHAServer smRaft = leader(true);
    when(smRaft.transferLeadership(anyLong())).thenAnswer(invocation -> {
      transferred.countDown();
      return true;
    });
    final ArcadeStateMachine sm = stateMachine(smRaft);
    staleSnapshotAppliedFloor(sm).set(100L);
    assertThat(sm.getDatabasesBeingReplaced()).as("nothing is being replaced").isEmpty();

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482");
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getServerName()).thenReturn("ArcadeDB_0");
    when(server.getConfiguration()).thenReturn(config);
    final RaftHAServer tick = new RaftHAServer(server, config) {
      @Override
      public boolean isLeader() {
        return true;
      }
    };
    try {
      final Field field = RaftHAServer.class.getDeclaredField("stateMachine");
      field.setAccessible(true);
      field.set(tick, sm);

      tick.queueReplacingDatabaseHandOff(sm);

      assertThat(transferred.await(10, TimeUnit.SECONDS)).as("the queued hand-off transferred leadership").isTrue();
    } finally {
      tick.stop();
    }
  }

  // -- no leadership ping-pong -------------------------------------------------------------------------------------

  /**
   * A gap every node shares - a database missing on all of them, or a stale-snapshot gap after a cluster-wide crash -
   * has no peer that can close it. Each hand-off "succeeds" (leadership moves) and the next leader hands off in turn.
   * The pause between two hand-offs of one node must widen while its gap persists across them, so the cluster is not
   * put through an election every base interval forever; it restarts from the base once the gap is gone.
   */
  @Test
  void repeatedHandOffsWhileTheGapPersistsBackOff() throws Exception {
    final AtomicLong clock = new AtomicLong(1_000_000L);
    final AtomicInteger attempts = new AtomicInteger();
    final RaftHAServer raft = leader(true);
    when(raft.transferLeadership(anyLong())).thenAnswer(invocation -> {
      attempts.incrementAndGet();
      return true;
    });
    final ArcadeStateMachine sm = stateMachine(raft);
    sm.replacingLeaderHandOffClock = clock::get;
    staleSnapshotAppliedFloor(sm).set(100L);

    assertThat(sm.handOffLeadershipWhileReplacingDatabase()).isTrue();
    sm.resetReplacingLeaderHandOffBackOff(); // a follower tick: the gap persists
    clock.addAndGet(ArcadeStateMachine.REPLACING_LEADER_HAND_OFF_INTERVAL_MS);
    assertThat(sm.handOffLeadershipWhileReplacingDatabase()).as("re-elected: the second hand-off waits the base").isTrue();
    assertThat(attempts.get()).isEqualTo(2);

    sm.resetReplacingLeaderHandOffBackOff();
    clock.addAndGet(ArcadeStateMachine.REPLACING_LEADER_HAND_OFF_INTERVAL_MS);
    sm.handOffLeadershipWhileReplacingDatabase();
    assertThat(attempts.get()).as("the third waits a widened interval").isEqualTo(2);
    clock.addAndGet(ArcadeStateMachine.replacingLeaderHandOffIntervalMs(2) - ArcadeStateMachine.REPLACING_LEADER_HAND_OFF_INTERVAL_MS);
    sm.handOffLeadershipWhileReplacingDatabase();
    assertThat(attempts.get()).isEqualTo(3);

    // The gap is filled: the episode is over, and a new one starts from the base interval.
    staleSnapshotAppliedFloor(sm).set(-1L);
    sm.resetReplacingLeaderHandOffBackOff();
    staleSnapshotAppliedFloor(sm).set(100L);
    clock.addAndGet(ArcadeStateMachine.REPLACING_LEADER_HAND_OFF_INTERVAL_MS);
    sm.handOffLeadershipWhileReplacingDatabase();
    assertThat(attempts.get()).as("a new gap waits the base interval, not the widened one").isEqualTo(4);
  }

  // -- helpers -----------------------------------------------------------------------------------------------------

  private static RaftHAServer leader(final boolean isLeader) {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(isLeader);
    when(raft.transferLeadership(anyLong())).thenReturn(true);
    return raft;
  }

  private ArcadeStateMachine stateMachine(final RaftHAServer raft) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(config);
    when(server.existsDatabase(MISSING_DB)).thenReturn(false);
    when(server.existsDatabase(KEPT_DB)).thenReturn(false);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    sm.setRaftHAServer(raft);
    stateMachines.add(sm);
    return sm;
  }

  @SuppressWarnings("unchecked")
  private static Set<String> pendingBootstrapReplacements(final ArcadeStateMachine sm) throws Exception {
    final Field field = ArcadeStateMachine.class.getDeclaredField("bootstrapReplacementsPending");
    field.setAccessible(true);
    return (Set<String>) field.get(sm);
  }

  private static AtomicLong staleSnapshotAppliedFloor(final ArcadeStateMachine sm) throws Exception {
    final Field field = ArcadeStateMachine.class.getDeclaredField("staleSnapshotAppliedFloor");
    field.setAccessible(true);
    return (AtomicLong) field.get(sm);
  }
}
