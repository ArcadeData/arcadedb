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
import com.arcadedb.server.TestServerHelper;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.ProtoUtils;
import org.junit.jupiter.api.Test;

import static com.arcadedb.server.ha.raft.RaftHAServer.reportedCommitIndexOf;
import static com.arcadedb.server.ha.raft.TrafficReadiness.isBehindLeaderCommit;
import static com.arcadedb.server.ha.raft.TrafficReadiness.isDivisionLifecycleHealthy;
import static com.arcadedb.server.ha.raft.TrafficReadiness.isEmptyLogInMultiPeerCluster;
import static com.arcadedb.server.ha.raft.TrafficReadiness.isReadyForTrafficState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link TrafficReadiness#isReadyForTrafficState} predicate behind the issue #4834 fix.
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

  // isEmptyLogInMultiPeerCluster: the boundary the code review caught (PR #7605) - index 0 is
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
  // ---- #7619 ----
  // Regression test for issue #7619: the issue #7131 readiness gate ({@code isEmptyLogInMultiPeerCluster}) was
  // strictly subsumed by the {@code commitIndex < 0} check that follows it, and the case #7131 was actually about -
  // a follower whose replication channel is wedged, so heartbeats keep a leader known but no entries arrive - still
  // answered Ready. Ratis clamps a follower's commit index to its own flush index, so such a follower has
  // {@code commitIndex == appliedIndex} locally and reports lag {@code 0} however far the leader has moved on.
  //
  // The fix measures the follower's applied index against the commit index the LEADER reports, learned by the
  // health monitor over a follower-to-leader call. These tests drive it with raw indices rather than pre-reduced
  // booleans - the gap the issue called out in the #7131 tests - and through {@link RaftHAServer#isReadyForTraffic}
  // itself, with only the Ratis division and the leader call stood in for.

  private static final String SERVER_LIST = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482";
  private static final long   MAX_LAG     = 100L;

  // Regression for #7619: the empty log flag never changes the answer when computed from the same commit index
  // ---- the #7131 gate, fed a real commitIndex: it cannot change the answer --------------------------------------
  @Test
  void theEmptyLogFlagNeverChangesTheAnswerWhenComputedFromTheSameCommitIndex() {
    for (final long commitIndex : new long[] { -1L, 0L, 1L, 100L, 5000L })
      for (final long appliedIndex : new long[] { -1L, 0L, 1L, 100L, 5000L })
        for (final int peers : new int[] { 0, 1, 3 }) {
          final boolean withFlag = isReadyForTrafficState(true, true, false, commitIndex, appliedIndex, MAX_LAG, false,
              false, isEmptyLogInMultiPeerCluster(peers, commitIndex));
          final boolean withoutFlag = isReadyForTrafficState(true, true, false, commitIndex, appliedIndex, MAX_LAG,
              false, false, false);
          assertThat(withFlag).as("commit=%d applied=%d peers=%d", commitIndex, appliedIndex, peers)
              .isEqualTo(withoutFlag);
        }
  }

  // Regression for #7619: a follower whose local log stopped growing is not ready once the leader reports more
  // ---- the new gate, from raw indices ---------------------------------------------------------------------------
  @Test
  void aFollowerWhoseLocalLogStoppedGrowingIsNotReadyOnceTheLeaderReportsMore() {
    // The wedged channel: local commit == local applied == 100 (lag 0), leader has committed 5000.
    final long local = 100L;
    assertThat(isReadyForTrafficState(true, true, false, local, local, MAX_LAG, false, false, false, true, false,
        isBehindLeaderCommit(5000L, local, MAX_LAG))).isFalse();
    // Without the leader's figure the same follower reads Ready - exactly the bug.
    assertThat(isReadyForTrafficState(true, true, false, local, local, MAX_LAG, false, false, false, true, false,
        false)).isTrue();
  }

  // Regression for #7619: the leader gate is bounded by max lag inclusive
  @Test
  void theLeaderGateIsBoundedByMaxLagInclusive() {
    assertThat(isBehindLeaderCommit(1100L, 1000L, MAX_LAG)).as("exactly maxLag behind").isFalse();
    assertThat(isBehindLeaderCommit(1101L, 1000L, MAX_LAG)).as("one past maxLag").isTrue();
    assertThat(isBehindLeaderCommit(1000L, 1000L, 0L)).as("caught up with maxLag 0").isFalse();
    assertThat(isBehindLeaderCommit(1001L, 1000L, 0L)).as("one behind with maxLag 0").isTrue();
  }

  // Regression for #7619: the leader gate abstains without A leader figure and on A stale one
  @Test
  void theLeaderGateAbstainsWithoutALeaderFigureAndOnAStaleOne() {
    assertThat(isBehindLeaderCommit(-1L, 1000L, MAX_LAG)).as("no leader commit learned yet").isFalse();
    assertThat(isBehindLeaderCommit(500L, 1000L, MAX_LAG)).as("a sample older than local progress").isFalse();
    assertThat(isBehindLeaderCommit(5000L, -1L, MAX_LAG)).as("unreadable applied index: refused elsewhere").isFalse();
  }

  // Regression for #7619: the leader gate also catches the empty log case it was meant to cover
  @Test
  void theLeaderGateAlsoCatchesTheEmptyLogCaseItWasMeantToCover() {
    // A re-joined follower with an empty-but-valid log (applied 0) in a cluster at 5000: the #7131 flag misses it
    // (commitIndex 0 is not < 0), the leader figure does not.
    assertThat(isEmptyLogInMultiPeerCluster(3, 0L)).isFalse();
    assertThat(isReadyForTrafficState(true, true, false, 0L, 0L, MAX_LAG, false, false, false, true, false,
        isBehindLeaderCommit(5000L, 0L, MAX_LAG))).isFalse();
  }

  // Regression for #7619: the leader branch ignores the gate
  @Test
  void theLeaderBranchIgnoresTheGate() {
    assertThat(isReadyForTrafficState(true, true, true, 1000L, 1000L, MAX_LAG, false, true, false, true, false, true))
        .isTrue();
  }

  // Regression for #7619: the leader commit is read from the leaders own commit info
  @Test
  void theLeaderCommitIsReadFromTheLeadersOwnCommitInfo() {
    final RaftPeer leader = peer("leader", "localhost:2434");
    final RaftPeer other = peer("other", "localhost:2435");
    final List<RaftProtos.CommitInfoProto> infos = List.of(ProtoUtils.toCommitInfoProto(other, 12L),
        ProtoUtils.toCommitInfoProto(leader, 5000L));

    assertThat(reportedCommitIndexOf(infos, leader.getId())).isEqualTo(5000L);
    assertThat(reportedCommitIndexOf(infos, RaftPeerId.valueOf("absent"))).isEqualTo(RaftHAServer.NO_COMMIT_INFO);
    assertThat(reportedCommitIndexOf(null, leader.getId())).isEqualTo(RaftHAServer.NO_COMMIT_INFO);
  }

  // Regression for #7619: a wedged follower stops answering ready once the health tick learns the leader commit
  // ---- end to end through RaftHAServer ---------------------------------------------------------------------------
  @Test
  void aWedgedFollowerStopsAnsweringReadyOnceTheHealthTickLearnsTheLeaderCommit() throws Exception {
    final Fixture f = new Fixture(true, 100L, 100L);
    f.leaderCommit = 5000L;

    assertThat(f.raft.isReadyForTraffic(MAX_LAG)).as("before the leader figure is known: local lag only").isTrue();

    f.raft.refreshLeaderCommitIndex();

    assertThat(f.probes.get()).isEqualTo(1);
    assertThat(f.raft.getLeaderReportedCommitIndex()).isEqualTo(5000L);
    assertThat(f.raft.isReadyForTraffic(MAX_LAG)).as("local lag 0, leader 4900 entries ahead").isFalse();
  }

  // Regression for #7619: a follower within max lag of the leader stays ready
  @Test
  void aFollowerWithinMaxLagOfTheLeaderStaysReady() throws Exception {
    final Fixture f = new Fixture(true, 1000L, 1000L);
    f.leaderCommit = 1050L;

    f.raft.refreshLeaderCommitIndex();

    assertThat(f.raft.isReadyForTraffic(MAX_LAG)).isTrue();
  }

  // Regression for #7619: a failed leader call keeps the previous figure
  @Test
  void aFailedLeaderCallKeepsThePreviousFigure() throws Exception {
    final Fixture f = new Fixture(true, 100L, 100L);
    f.leaderCommit = 5000L;
    f.raft.refreshLeaderCommitIndex();
    f.leaderCommit = -1L; // the next call fails

    f.raft.refreshLeaderCommitIndex();

    assertThat(f.raft.getLeaderReportedCommitIndex()).isEqualTo(5000L);
    assertThat(f.raft.isReadyForTraffic(MAX_LAG)).isFalse();
  }

  // Regression for #7619: the leader is asked even when readiness does not consult ha
  @Test
  void theLeaderIsAskedEvenWhenReadinessDoesNotConsultHa() throws Exception {
    // Issue #8342: the follower's own stall signal in GET /api/v1/cluster reads the answer on every node, so the
    // probe no longer depends on arcadedb.server.readinessRequiresHA. Readiness itself still ignores it with the
    // flag off, because ServerControlPlane consults isReadyForTraffic() only when the flag is on.
    final Fixture f = new Fixture(false, 100L, 100L);
    f.leaderCommit = 5000L;

    f.raft.refreshLeaderCommitIndex();

    assertThat(f.probes.get()).isEqualTo(1);
    assertThat(f.raft.getLeaderReportedCommitIndex()).isEqualTo(5000L);
  }

  // Regression for #7619: the leader does not probe itself
  @Test
  void theLeaderDoesNotProbeItself() throws Exception {
    final Fixture f = new Fixture(true, 100L, 100L);
    when(f.info.isLeader()).thenReturn(true);

    f.raft.refreshLeaderCommitIndex();

    assertThat(f.probes.get()).isZero();
  }

  // Regression for #7619: an unreachable leader is probed with backoff not on every tick
  @Test
  void anUnreachableLeaderIsProbedWithBackoffNotOnEveryTick() throws Exception {
    final Fixture f = new Fixture(true, 100L, 100L);
    f.leaderCommit = -1L; // every call fails

    for (int i = 0; i < 10; i++)
      f.raft.refreshLeaderCommitIndex();

    // Probe, skip 1, probe, skip 3, probe, skip 7 (partly): three calls in ten ticks.
    assertThat(f.probes.get()).isEqualTo(3);
  }

  // Regression for #7619: a successful probe and A leader change re arm the backoff
  @Test
  void aSuccessfulProbeAndALeaderChangeReArmTheBackoff() throws Exception {
    final Fixture f = new Fixture(true, 100L, 100L);
    f.leaderCommit = -1L;
    f.raft.refreshLeaderCommitIndex(); // fails, skips the next tick
    f.raft.refreshLeaderCommitIndex(); // skipped
    assertThat(f.probes.get()).isEqualTo(1);

    // A new leader is asked straight away, however many times the old one failed.
    when(f.info.getLeaderId()).thenReturn(RaftPeerId.valueOf("third"));
    f.leaderCommit = 5000L;
    f.raft.refreshLeaderCommitIndex();
    assertThat(f.probes.get()).isEqualTo(2);
    assertThat(f.raft.getLeaderReportedCommitIndex()).isEqualTo(5000L);

    // After a success, the next tick probes again.
    f.raft.refreshLeaderCommitIndex();
    assertThat(f.probes.get()).isEqualTo(3);
  }

  // Regression for #7619: the backoff is bounded
  @Test
  void theBackoffIsBounded() {
    assertThat(RaftHAServer.leaderCommitProbeSkipTicksAfter(0)).isZero();
    assertThat(RaftHAServer.leaderCommitProbeSkipTicksAfter(1)).isEqualTo(1);
    assertThat(RaftHAServer.leaderCommitProbeSkipTicksAfter(2)).isEqualTo(3);
    assertThat(RaftHAServer.leaderCommitProbeSkipTicksAfter(3)).isEqualTo(7);
    assertThat(RaftHAServer.leaderCommitProbeSkipTicksAfter(4)).isEqualTo(RaftHAServer.LEADER_COMMIT_PROBE_MAX_SKIP_TICKS);
    assertThat(RaftHAServer.leaderCommitProbeSkipTicksAfter(30)).isEqualTo(RaftHAServer.LEADER_COMMIT_PROBE_MAX_SKIP_TICKS);
  }

  // Regression for #7619: the health tick asks for the leader commit
  @Test
  void theHealthTickAsksForTheLeaderCommit() {
    final AtomicInteger refreshes = new AtomicInteger();
    final HealthMonitor.HealthTarget target = new HealthMonitor.HealthTarget() {
      @Override
      public LifeCycle.State getRaftLifeCycleState() {
        return LifeCycle.State.RUNNING;
      }

      @Override
      public boolean isShutdownRequested() {
        return false;
      }

      @Override
      public void restartRatisIfNeeded() {
      }

      @Override
      public void refreshLeaderCommitIndex() {
        refreshes.incrementAndGet();
      }
    };
    final HealthMonitor monitor = new HealthMonitor(target, 0);

    monitor.tick();
    monitor.tick();

    assertThat(refreshes.get()).isEqualTo(2);
  }

  private static RaftPeer peer(final String id, final String address) {
    return RaftPeer.newBuilder().setId(id).setAddress(address).build();
  }

  /**
   * A {@link RaftHAServer} that believes it is a running follower of a three-peer group with the given local
   * commit/applied indices, whose leader answers the commit-index probe with {@link #leaderCommit}.
   */
  private static final class Fixture {
    final RaftHAServer  raft;
    final DivisionInfo  info;
    final AtomicInteger probes = new AtomicInteger();
    volatile long       leaderCommit;

    Fixture(final boolean readinessRequiresHa, final long localCommit, final long localApplied) throws Exception {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);
      config.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, readinessRequiresHa);

      final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");
      raft = new RaftHAServer(arcadeServer, config);

      final RaftPeer self = peer(raft.getLocalPeerId().toString(), "localhost:2434");
      final RaftPeer leader = peer("leader", "localhost:2435");
      final RaftPeer third = peer("third", "localhost:2436");

      info = mock(DivisionInfo.class);
      when(info.getLeaderId()).thenReturn(leader.getId());
      when(info.isLeader()).thenReturn(false);
      when(info.getLastAppliedIndex()).thenReturn(localApplied);
      when(info.getLifeCycleState()).thenReturn(LifeCycle.State.RUNNING);

      final RaftConfiguration conf = mock(RaftConfiguration.class);
      when(conf.getCurrentPeers()).thenReturn(List.of(self, leader, third));

      final RaftLog log = mock(RaftLog.class);
      when(log.getLastCommittedIndex()).thenReturn(localCommit);

      final RaftServer.Division division = mock(RaftServer.Division.class);
      when(division.getInfo()).thenReturn(info);
      when(division.getRaftConf()).thenReturn(conf);
      when(division.getRaftLog()).thenReturn(log);

      final RaftServer ratis = mock(RaftServer.class);
      when(ratis.getDivision(any())).thenReturn(division);

      final Field field = RaftHAServer.class.getDeclaredField("raftServer");
      field.setAccessible(true);
      field.set(raft, ratis);

      // Neither resyncing nor halted: the gates this test is not about stay open.
      final ArcadeStateMachine stateMachine = new ArcadeStateMachine();
      final Field smField = RaftHAServer.class.getDeclaredField("stateMachine");
      smField.setAccessible(true);
      smField.set(raft, stateMachine);

      raft.setLeaderCommitProber(target -> {
        assertThat(target.getId()).isEqualTo(info.getLeaderId());
        probes.incrementAndGet();
        return leaderCommit;
      });
    }
  }
}
