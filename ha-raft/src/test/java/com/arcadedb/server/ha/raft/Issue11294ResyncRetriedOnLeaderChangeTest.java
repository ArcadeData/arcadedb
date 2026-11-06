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
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11294: a targeted resync of a quarantined database refused with "the leader is unknown" waited for the next
 * HealthMonitor tick, throttled to one attempt per snapshot-watchdog interval, although the refusal could only change
 * when a leader became known - which Ratis tells the state machine about through {@code notifyLeaderChanged}. In the
 * Jepsen run that reported it the retry came about six seconds after the refusal, with the leader elected before both.
 * <p>
 * The leader change now drives the attempt itself, without waiting out the throttle. These tests observe it through
 * the throttle slot, which the retry takes exactly when it decides to drive a resync and leaves alone otherwise.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11294ResyncRetriedOnLeaderChangeTest {
  private static final String     DB     = "jepsen";
  private static final RaftPeerId LEADER = RaftPeerId.valueOf("peer-a_2434");
  private static final RaftPeerId LOCAL  = RaftPeerId.valueOf("peer-d_2434");

  /** The reported sequence: quarantined while no leader is known, then the leader is learnt. */
  @Test
  void aQuarantineRefusedForAnUnknownLeaderIsRetriedAsSoonAsTheLeaderIsLearnt(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    final FakeRaftHAServer raft = follower();
    sm.setRaftHAServer(raft);
    try {
      sm.markStateDiverged(DB, DivergenceCause.UNDECODABLE_LOG_ENTRY);
      sm.retryUnfilledSnapshotGap();
      assertThat(readLastRetryMs(sm)).as("no leader is known, so nothing can be driven yet").isZero();

      raft.leaderId(LEADER);
      sm.notifyLeaderChanged(memberId(), LEADER);
      sm.awaitLifecycleTasksForTesting(30_000);

      assertThat(readLastRetryMs(sm))
          .as("the leader change is what the refusal was waiting for, so it drives the resync instead of the next tick")
          .isNotZero();
    } finally {
      sm.close();
    }
  }

  /**
   * A tick that tried a moment ago against a leader that has since gone holds the throttle slot for a whole watchdog
   * interval; the new leader does not wait that out.
   */
  @Test
  void aNewLeaderDoesNotWaitOutTheThrottleOfTheAttemptAgainstThePreviousOne(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    final FakeRaftHAServer raft = follower();
    raft.leaderId(LEADER);
    sm.setRaftHAServer(raft);
    try {
      sm.markStateDiverged(DB, DivergenceCause.UNDECODABLE_LOG_ENTRY);
      final long aMomentAgo = System.currentTimeMillis() - 1_000;
      lastRetryMs(sm).set(aMomentAgo);

      sm.retryUnfilledSnapshotGap();
      assertThat(readLastRetryMs(sm)).as("a tick inside the interval is still throttled").isEqualTo(aMomentAgo);

      sm.notifyLeaderChanged(memberId(), LEADER);
      sm.awaitLifecycleTasksForTesting(30_000);

      assertThat(readLastRetryMs(sm)).as("the leader change took the slot and drove the resync").isGreaterThan(aMomentAgo);
    } finally {
      sm.close();
    }
  }

  /**
   * Ratis sometimes notifies the same leader again within the same term. Nothing a refusal waits on has changed then, so
   * that callback leaves the throttle to pace the retries.
   */
  @Test
  void aSameTermReNotificationOfTheSameLeaderDoesNotBypassTheThrottle(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    final FakeRaftHAServer raft = follower();
    raft.leaderId(LEADER);
    raft.currentTerm(5L, 5L);
    sm.setRaftHAServer(raft);
    try {
      // The leader is first learnt while nothing is outstanding, so no resync is started (none could run here anyway)
      sm.notifyLeaderChanged(memberId(), LEADER);
      sm.awaitLifecycleTasksForTesting(30_000);
      assertThat(readLastRetryMs(sm)).isZero();

      sm.markStateDiverged(DB, DivergenceCause.UNDECODABLE_LOG_ENTRY);
      final long aMomentAgo = System.currentTimeMillis() - 1_000;
      lastRetryMs(sm).set(aMomentAgo);
      sm.notifyLeaderChanged(memberId(), LEADER);
      sm.awaitLifecycleTasksForTesting(30_000);

      assertThat(readLastRetryMs(sm)).isEqualTo(aMomentAgo);
    } finally {
      sm.close();
    }
  }

  /** Nothing outstanding: a leader change drives nothing, as the tick does not. */
  @Test
  void aHealthyNodeDrivesNothingOnALeaderChange(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    final FakeRaftHAServer raft = follower();
    raft.leaderId(LEADER);
    sm.setRaftHAServer(raft);
    try {
      sm.notifyLeaderChanged(memberId(), LEADER);
      sm.awaitLifecycleTasksForTesting(30_000);

      assertThat(readLastRetryMs(sm)).isZero();
    } finally {
      sm.close();
    }
  }

  /** A node elected leader cannot resync from itself: the hand-off of issue #8483 is its way out, not a resync. */
  @Test
  void aNodeElectedLeaderDoesNotTryToResyncFromItself(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    final FakeRaftHAServer raft = follower();
    raft.leader(true);
    raft.leaderId(LOCAL);
    raft.peerHttpAddress(LOCAL, "peer-d:2480");
    sm.setRaftHAServer(raft);
    try {
      sm.markStateDiverged(DB, DivergenceCause.UNDECODABLE_LOG_ENTRY);

      sm.notifyLeaderChanged(memberId(), LOCAL);
      sm.awaitLifecycleTasksForTesting(30_000);

      assertThat(readLastRetryMs(sm)).isZero();
      assertThat(sm.isDatabaseDiverged(DB)).isTrue();
    } finally {
      sm.close();
    }
  }

  private static FakeRaftHAServer follower() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(false);
    raft.localPeerId(LOCAL);
    raft.localHttpAddress("peer-d:2480");
    raft.peerHttpAddress(LEADER, "peer-a:2480");
    return raft;
  }

  private static RaftGroupMemberId memberId() {
    return RaftGroupMemberId.valueOf(LOCAL, RaftGroupId.valueOf(UUID.randomUUID()));
  }

  private static ArcadeStateMachine newStateMachine(final Path tempDir) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(new ArcadeDBServer(config));
    return sm;
  }

  private static AtomicLong lastRetryMs(final ArcadeStateMachine sm) throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("lastStaleSnapshotRetryMs");
    f.setAccessible(true);
    return (AtomicLong) f.get(sm);
  }

  private static long readLastRetryMs(final ArcadeStateMachine sm) throws Exception {
    return lastRetryMs(sm).get();
  }
}
