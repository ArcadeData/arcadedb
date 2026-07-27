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

import org.junit.jupiter.api.Test;

import static com.arcadedb.server.ha.raft.RaftHAServer.isReadyForTrafficState;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link RaftHAServer#isReadyForTrafficState} predicate behind the issue #4834 fix.
 * The readiness probe relies on this gate so a node does not advertise Ready before it has (re)joined the
 * Raft configuration and replayed the committed log. Advertising Ready too early during a Kubernetes
 * StatefulSet rolling restart lets the orchestrator terminate the next pod and drop the write quorum.
 *
 * Argument order: leaderPresent, localInConfig, leader, commitIndex, appliedIndex, maxLagEntries,
 * then the optional resyncInProgress and leaderReady flags.
 */
class RaftHAServerReadinessTest {

  @Test
  void leaderIsAlwaysReadyWhenInConfig() {
    // The leader is caught up with itself by definition; commit/applied are ignored.
    assertThat(isReadyForTrafficState(true, true, true, 5000, 100, 0)).isTrue();
  }

  @Test
  void noLeaderIsNotReady() {
    // Election not settled yet: no leader known.
    assertThat(isReadyForTrafficState(false, true, false, 100, 100, 100)).isFalse();
  }

  @Test
  void notInCurrentConfigIsNotReady() {
    // A restarted node that has not yet been admitted into the cluster configuration must not be Ready
    // even if it happens to recognize a leader and its local indices look caught up.
    assertThat(isReadyForTrafficState(true, false, false, 100, 100, 100)).isFalse();
  }

  @Test
  void caughtUpFollowerIsReady() {
    // commit == applied: fully replayed the committed log.
    assertThat(isReadyForTrafficState(true, true, false, 1000, 1000, 100)).isTrue();
  }

  @Test
  void followerWithinLagBoundIsReady() {
    // commit - applied == 50 <= 100: within the configured small bound.
    assertThat(isReadyForTrafficState(true, true, false, 1050, 1000, 100)).isTrue();
  }

  @Test
  void followerAtExactlyTheBoundIsReady() {
    // commit - applied == maxLag is within the bound (<=, not strict).
    assertThat(isReadyForTrafficState(true, true, false, 1100, 1000, 100)).isTrue();
  }

  @Test
  void followerBeyondLagBoundIsNotReady() {
    // The freshly restarted follower with a near-empty log: huge gap to the leader's commit index.
    assertThat(isReadyForTrafficState(true, true, false, 100000, 5, 100)).isFalse();
  }

  @Test
  void zeroBoundRequiresFullyCaughtUp() {
    assertThat(isReadyForTrafficState(true, true, false, 1000, 1000, 0)).isTrue();
    assertThat(isReadyForTrafficState(true, true, false, 1001, 1000, 0)).isFalse();
  }

  @Test
  void unreadableFollowerStateIsNotReady() {
    // Negative commit/applied means the Raft state could not be read this tick: fail closed.
    assertThat(isReadyForTrafficState(true, true, false, -1, 1000, 100)).as("negative commitIndex").isFalse();
    assertThat(isReadyForTrafficState(true, true, false, 1000, -1, 100)).as("negative appliedIndex").isFalse();
  }

  @Test
  void followerWithAppliedIndexAheadOfCommitIndexIsNotReady() {
    // appliedIndex > commitIndex is an inconsistent state: the negative lag must not be treated as "within
    // the bound" and report Ready - the probe must fail closed.
    assertThat(isReadyForTrafficState(true, true, false, 900, 1000, 100)).isFalse();
  }

  @Test
  void followerWithResyncInFlightIsNotReadyEvenWithinLagBound() {
    // Issue #5273: a follower whose data may be divergent (snapshot resync queued/running, or a
    // database still marked diverged after a WAL gap) must fail closed even when its raw applied-index
    // lag is within the bound (here commit == applied).
    assertThat(isReadyForTrafficState(true, true, false, 1000, 1000, 100, true)).isFalse();
    // Once the resync clears, the same caught-up follower is Ready again.
    assertThat(isReadyForTrafficState(true, true, false, 1000, 1000, 100, false)).isTrue();
  }

  @Test
  void leaderWithResyncFlagIsNotReady() {
    // Defensive: a resync flag set while this node believes it is leader still fails closed. The leader
    // never resyncs from a peer, so in practice this only guards a transitional inconsistency.
    assertThat(isReadyForTrafficState(true, true, true, 5000, 100, 0, true)).isFalse();
  }

  @Test
  void sixArgOverloadDefaultsToNoResync() {
    // The legacy 6-arg predicate behaves exactly as before (resyncInProgress defaults to false).
    assertThat(isReadyForTrafficState(true, true, false, 1000, 1000, 100)).isTrue();
  }

  @Test
  void freshlyElectedLeaderNotYetReadyIsNotReadyForTraffic() {
    // Issue #5453: a node that has just won an election reports the LEADER role before it has
    // committed its current-term no-op, and Ratis rejects writes with the retryable
    // LeaderNotReadyException until then. The probe must fail closed on that window.
    assertThat(isReadyForTrafficState(true, true, true, 1000, 1000, 100, false, false)).isFalse();
  }

  @Test
  void readyLeaderIsReadyForTraffic() {
    // Once Ratis reports the leader ready, the same node serves traffic.
    assertThat(isReadyForTrafficState(true, true, true, 1000, 1000, 100, false, true)).isTrue();
  }

  @Test
  void notYetReadyLeaderIsNotRescuedByZeroLag() {
    // A not-yet-ready leader must not fall through to the follower lag branch: a freshly elected
    // leader typically has commitIndex == appliedIndex, which would otherwise report Ready and
    // defeat the gate.
    assertThat(isReadyForTrafficState(true, true, true, 500, 500, 0, false, false)).isFalse();
    // ... and a leader whose applied index trails is equally not Ready.
    assertThat(isReadyForTrafficState(true, true, true, 5000, 100, 0, false, false)).isFalse();
  }

  @Test
  void followerIgnoresLeaderReadyFlag() {
    // leaderReady is a leader-only signal: a caught-up follower is Ready either way.
    assertThat(isReadyForTrafficState(true, true, false, 1000, 1000, 100, false, false)).isTrue();
    assertThat(isReadyForTrafficState(true, true, false, 1000, 1000, 100, false, true)).isTrue();
    // A lagging follower stays not Ready even if the cluster's leader happens to be ready.
    assertThat(isReadyForTrafficState(true, true, false, 100000, 5, 100, false, true)).isFalse();
  }

  @Test
  void resyncStillWinsOverAReadyLeader() {
    // The resync gate (#5273) is evaluated before the leader branch, so it still fails closed.
    assertThat(isReadyForTrafficState(true, true, true, 1000, 1000, 100, true, true)).isFalse();
  }

  @Test
  void sevenArgOverloadTreatsAnyLeaderAsReady() {
    // The legacy 7-arg predicate keeps its documented behaviour (leaderReady defaults to leader),
    // so existing callers are unaffected by the issue #5453 parameter.
    assertThat(isReadyForTrafficState(true, true, true, 5000, 100, 0, false)).isTrue();
  }

  @Test
  void sixArgOverloadTreatsAnyLeaderAsReady() {
    assertThat(isReadyForTrafficState(true, true, true, 5000, 100, 0)).isTrue();
  }
}
