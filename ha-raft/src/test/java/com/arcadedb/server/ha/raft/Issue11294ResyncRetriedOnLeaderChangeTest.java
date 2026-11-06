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

import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11294: a targeted resync of a quarantined database refused with "the leader is unknown" waited for the next
 * HealthMonitor tick, throttled to one attempt per snapshot-watchdog interval, although the refusal could only change
 * when a leader became known - which Ratis tells the state machine about through {@code notifyLeaderChanged}. In the
 * Jepsen run that reported it the retry came about six seconds after the refusal, with the leader elected before both.
 * <p>
 * The leader change now drives the attempt itself, without waiting out the throttle. Two things are observed: the
 * throttle slot, which the retry takes exactly when it decides to drive a resync, and the resync itself, counted where
 * it resolves the leader's address to dial it. The leader's address is answered to the retry's precheck and refused to
 * that dial, so the resync stops there instead of starting a snapshot download nobody could serve, and every task it
 * queued has finished before the state machine is closed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11294ResyncRetriedOnLeaderChangeTest {
  private static final String                  DB        = "jepsen";
  private static final RaftPeerId              LEADER    = RaftPeerId.valueOf("peer-a_2434");
  private static final RaftPeerId              LOCAL     = RaftPeerId.valueOf("peer-d_2434");
  private static final Map<RaftPeerId, String> ADDRESSES = Map.of(LEADER, "peer-a:2480", LOCAL, "peer-d:2480");

  private final AtomicInteger resyncDials = new AtomicInteger();

  /** The reported sequence: quarantined while no leader is known, then the leader is learnt. */
  @Test
  void aQuarantineRefusedForAnUnknownLeaderIsRetriedAsSoonAsTheLeaderIsLearnt(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    final FakeRaftHAServer raft = follower();
    sm.setRaftHAServer(raft);
    try {
      sm.markStateDiverged(DB, DivergenceCause.UNDECODABLE_LOG_ENTRY);
      sm.retryUnfilledSnapshotGap();
      drainLifecycleTasks(sm);
      assertThat(retryStamp(sm).get()).as("no leader is known, so nothing can be driven yet").isZero();

      raft.leaderId(LEADER);
      sm.notifyLeaderChanged(memberId(), LEADER);
      drainLifecycleTasks(sm);

      assertThat(retryStamp(sm).get())
          .as("the leader change is what the refusal was waiting for, so it drives the resync instead of the next tick")
          .isNotZero();
      assertThat(resyncDials.get()).as("and the targeted resync of the quarantined database ran").isEqualTo(1);
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
      retryStamp(sm).set(aMomentAgo);

      sm.retryUnfilledSnapshotGap();
      drainLifecycleTasks(sm);
      assertThat(retryStamp(sm).get()).as("a tick inside the interval is still throttled").isEqualTo(aMomentAgo);
      assertThat(resyncDials.get()).isZero();

      sm.notifyLeaderChanged(memberId(), LEADER);
      drainLifecycleTasks(sm);

      assertThat(retryStamp(sm).get()).as("the leader change took the slot").isGreaterThan(aMomentAgo);
      assertThat(resyncDials.get()).as("and drove the resync").isEqualTo(1);
    } finally {
      sm.close();
    }
  }

  /**
   * Ratis sometimes notifies the same leader again within the same term. Nothing a refusal waits on has changed then,
   * so that callback leaves the throttle to pace the retries.
   */
  @Test
  void aSameTermReNotificationOfTheSameLeaderDoesNotBypassTheThrottle(@TempDir final Path tempDir) throws Exception {
    sameLeaderNotifiedAgain(tempDir, 5L, 5L);
  }

  /** ...and a re-notification of the same leader in a term that cannot be read is not taken for a new election either. */
  @Test
  void aReNotificationOfTheSameLeaderInAnUnreadableTermDoesNotBypassTheThrottle(@TempDir final Path tempDir) throws Exception {
    sameLeaderNotifiedAgain(tempDir, 5L, -1L);
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
      drainLifecycleTasks(sm);

      assertThat(retryStamp(sm).get()).isZero();
      assertThat(resyncDials.get()).isZero();
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
    sm.setRaftHAServer(raft);
    try {
      sm.markStateDiverged(DB, DivergenceCause.UNDECODABLE_LOG_ENTRY);

      sm.notifyLeaderChanged(memberId(), LOCAL);
      drainLifecycleTasks(sm);

      assertThat(retryStamp(sm).get()).isZero();
      assertThat(resyncDials.get()).isZero();
      assertThat(sm.isDatabaseDiverged(DB)).isTrue();
    } finally {
      sm.close();
    }
  }

  /**
   * The leader is first learnt in {@code firstTerm} while nothing is outstanding, so nothing is driven; then a quarantine
   * is raised, a tick holds the throttle slot, and the same leader is notified again with the term reading
   * {@code secondTerm}.
   */
  private void sameLeaderNotifiedAgain(final Path tempDir, final long firstTerm, final long secondTerm) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    final FakeRaftHAServer raft = follower();
    raft.leaderId(LEADER);
    raft.currentTerm(firstTerm, secondTerm);
    sm.setRaftHAServer(raft);
    try {
      sm.notifyLeaderChanged(memberId(), LEADER);
      drainLifecycleTasks(sm);
      assertThat(retryStamp(sm).get()).isZero();

      sm.markStateDiverged(DB, DivergenceCause.UNDECODABLE_LOG_ENTRY);
      final long aMomentAgo = System.currentTimeMillis() - 1_000;
      retryStamp(sm).set(aMomentAgo);
      sm.notifyLeaderChanged(memberId(), LEADER);
      drainLifecycleTasks(sm);

      assertThat(retryStamp(sm).get()).isEqualTo(aMomentAgo);
      assertThat(resyncDials.get()).isZero();
    } finally {
      sm.close();
    }
  }

  /**
   * A follower whose view of the leader's address answers the retry's precheck and refuses the resync's dial, counting
   * the dial: {@code PeerDialAddress.resolve} is the step every targeted resync takes before it downloads anything.
   */
  private FakeRaftHAServer follower() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(false);
    raft.localPeerId(LOCAL);
    raft.localHttpAddress(ADDRESSES.get(LOCAL));
    raft.peerHttpAddress(LEADER, ADDRESSES.get(LEADER));
    raft.on("getUnambiguousPeerHttpAddress", args -> {
      if (StackWalker.getInstance()
          .walk(frames -> frames.anyMatch(frame -> frame.getClassName().equals(PeerDialAddress.class.getName())))) {
        resyncDials.incrementAndGet();
        return null;
      }
      return args[0] == null ? null : ADDRESSES.get((RaftPeerId) args[0]);
    });
    return raft;
  }

  /**
   * Returns once the retry a leader change queued AND the resync that retry queued behind it have run: each await queues
   * a no-op behind what is already queued, and the resync is queued only while the retry runs.
   */
  private static void drainLifecycleTasks(final ArcadeStateMachine sm) throws Exception {
    sm.awaitLifecycleTasksForTesting(30_000);
    sm.awaitLifecycleTasksForTesting(30_000);
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

  private static AtomicLong retryStamp(final ArcadeStateMachine sm) {
    return sm.staleSnapshotRetryStampForTesting();
  }
}
