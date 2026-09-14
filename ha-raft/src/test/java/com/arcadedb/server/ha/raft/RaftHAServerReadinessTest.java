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

import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;

import static com.arcadedb.server.ha.raft.RaftHAServer.isDivisionLifecycleHealthy;
import static com.arcadedb.server.ha.raft.RaftHAServer.isEmptyLogInMultiPeerCluster;
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

  // -----------------------------------------------------------------------------------------------
  // Issue #7131: local commit/applied lag alone cannot see a follower that is not receiving entries
  // at all (Ratis clamps a follower's commit index to its own flush index). emptyLogInMultiPeerCluster
  // closes the cold-rejoin half of that gap (a wiped/reformatted follower rejoining an established
  // multi-peer cluster). A leader-RPC-recency signal was tried for the complementary wedged-channel
  // case and removed after review: verified against Ratis 3.3.0 bytecode, granting a PRE_VOTE to ANY
  // candidate - not necessarily this follower's own recognized leader - refreshes the same timestamp,
  // so it did not reliably mean "still hearing from the leader" (PR #7605 review).
  // -----------------------------------------------------------------------------------------------

  @Test
  void emptyLogInMultiPeerClusterIsNotReady() {
    // A follower that just rejoined (wiped/reformatted) with nothing committed at all must not be Ready.
    assertThat(isReadyForTrafficState(true, true, false, 0, 0, 100, false, false, true)).isFalse();
  }

  @Test
  void nonEmptyLogIgnoresTheEmptyLogGate() {
    assertThat(isReadyForTrafficState(true, true, false, 1000, 1000, 100, false, false, false)).isTrue();
  }

  @Test
  void leaderRoleIgnoresTheEmptyLogGate() {
    // The gate applies to followers only; the leader branch returns before it is evaluated.
    assertThat(isReadyForTrafficState(true, true, true, 5000, 100, 0, false, true, true)).isTrue();
  }

  // isEmptyLogInMultiPeerCluster: the boundary claude-review caught (PR #7605) - index 0 is
  // RaftLog.LEAST_VALID_LOG_INDEX (the leader's first real committed entry), not "empty". An earlier
  // revision used commitIndex <= 0, which none of the isReadyForTrafficState cases above could catch since
  // they all pass emptyLogInMultiPeerCluster pre-reduced rather than feeding a real commitIndex through the
  // computation that produces it.

  @Test
  void genuinelyEmptyLogInMultiPeerClusterIsDetected() {
    // RaftLog.INVALID_LOG_INDEX (-1): nothing committed at all.
    assertThat(isEmptyLogInMultiPeerCluster(3, -1)).isTrue();
  }

  @Test
  void firstRealCommitIsNotTreatedAsEmpty() {
    // RaftLog.LEAST_VALID_LOG_INDEX (0): the cluster's very first entry has committed and this follower has
    // it. A brand-new multi-node cluster's followers are legitimately caught up here immediately after
    // bootstrap; the bug this regression-tests reported them not-ready.
    assertThat(isEmptyLogInMultiPeerCluster(3, 0)).isFalse();
  }

  @Test
  void singlePeerConfigurationNeverCountsAsEmpty() {
    // The gate only makes sense once there is more than one peer to have rejoined among; a single-node
    // "cluster" reaching commitIndex 0 for the first time is not a cold rejoin.
    assertThat(isEmptyLogInMultiPeerCluster(1, -1)).isFalse();
    assertThat(isEmptyLogInMultiPeerCluster(0, -1)).isFalse();
  }

  @Test
  void eightArgOverloadDefaultsToNoEmptyLogGate() {
    // Backward-compatible default for callers that predate issue #7131: the gate never fires.
    assertThat(isReadyForTrafficState(true, true, false, 0, 0, 100, false, false)).isTrue();
  }

  // -----------------------------------------------------------------------------------------------
  // Issue #7130: a RaftServer proxy can stay RUNNING while the per-group division underneath goes
  // CLOSED or EXCEPTION - every other field (leaderId, raft conf, commit/applied index) survives that
  // close and keeps reporting its last value, so the division's own lifecycle has to be an explicit,
  // first-evaluated gate. Same for ArcadeStateMachine.isHaltedAfterCriticalError().
  // -----------------------------------------------------------------------------------------------

  @Test
  void unhealthyDivisionLifecycleIsNotReadyEvenWhenEverythingElseLooksFine() {
    assertThat(isReadyForTrafficState(true, true, true, 1000, 1000, 100, false, true, false, false, false))
        .as("otherwise-ready leader with an unhealthy (CLOSED/EXCEPTION/PAUSED) division").isFalse();
    assertThat(isReadyForTrafficState(true, true, false, 1000, 1000, 100, false, false, false, false, false))
        .as("otherwise-ready follower with an unhealthy division").isFalse();
  }

  @Test
  void haltedAfterCriticalErrorIsNotReadyEvenWhenEverythingElseLooksFine() {
    assertThat(isReadyForTrafficState(true, true, true, 1000, 1000, 100, false, true, false, true, true))
        .isFalse();
  }

  @Test
  void healthyDivisionAndNotHaltedPreservesThePriorBehaviour() {
    assertThat(isReadyForTrafficState(true, true, true, 1000, 1000, 100, false, true, false, true, false))
        .isTrue();
    assertThat(isReadyForTrafficState(true, true, false, 1000, 1000, 100, false, false, false, true, false))
        .isTrue();
  }

  @Test
  void nineArgOverloadDefaultsToHealthyDivisionAndNotHalted() {
    // Backward-compatible default for callers that predate issue #7130: neither new gate fires.
    assertThat(isReadyForTrafficState(true, true, true, 1000, 1000, 100, false, true, false)).isTrue();
  }

  // -----------------------------------------------------------------------------------------------
  // Issue #7130 (review follow-up): the LifeCycle.State -> healthy-or-not mapping that isReadyForTraffic()
  // feeds into the final overload above is itself untested by every case above, since they all pass the
  // already-reduced boolean. Exercise the mapping directly against every LifeCycle.State value instead.
  // -----------------------------------------------------------------------------------------------

  @Test
  void onlyRunningIsHealthy() {
    assertThat(isDivisionLifecycleHealthy(LifeCycle.State.RUNNING)).isTrue();
  }

  @Test
  void everyOtherLifeCycleStateIsUnhealthy() {
    // CLOSED/EXCEPTION are the issue #7130 targets. PAUSED looked like a candidate for "healthy" too (it is
    // a normal, expected, self-recovering transition per ArcadeStateMachine.pause()'s javadoc) but Ratis's
    // own request handlers (append entries, request vote, client requests) reject with
    // ServerNotReadyException in every state but RUNNING - PAUSED included - so it is excluded here
    // (PR #7605 review; confirmed against RaftServerImpl's assertLifeCycleState(RUNNING) call sites in the
    // 3.3.0 bytecode).
    assertThat(isDivisionLifecycleHealthy(LifeCycle.State.NEW)).isFalse();
    assertThat(isDivisionLifecycleHealthy(LifeCycle.State.STARTING)).isFalse();
    assertThat(isDivisionLifecycleHealthy(LifeCycle.State.PAUSING)).isFalse();
    assertThat(isDivisionLifecycleHealthy(LifeCycle.State.PAUSED)).isFalse();
    assertThat(isDivisionLifecycleHealthy(LifeCycle.State.EXCEPTION)).isFalse();
    assertThat(isDivisionLifecycleHealthy(LifeCycle.State.CLOSING)).isFalse();
    assertThat(isDivisionLifecycleHealthy(LifeCycle.State.CLOSED)).isFalse();
  }
}
