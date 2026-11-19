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

import com.arcadedb.log.LogManager;
import java.util.logging.Level;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.LifeCycle;

/**
 * Decides whether this node is ready to serve client traffic - the readiness probe a load balancer or Kubernetes asks:
 * the Raft division is running, the node is not behind the leader's commit index, no bootstrap window or critical halt
 * holds it, and an empty log in a multi-peer cluster is not mistaken for an up-to-date one.
 * <p>
 * Split out of {@link RaftHAServer}, which answers the probe through it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class TrafficReadiness {
  private final RaftHAServer ha;

  TrafficReadiness(final RaftHAServer ha) {
    this.ha = ha;
  }

  /**
   * Reports whether this node is ready to serve traffic for the readiness probe (issue #4834): a leader is
   * known, this node is a member of the current Raft configuration, and - for a follower - the local
   * applied index is within {@code maxLagEntries} of the commit index. A leader is Ready only once Ratis
   * reports it ready, which a freshly elected one is not until it has committed its current-term no-op
   * (issue #5453). Returns {@code false} before the Raft server has started, during shutdown, or when the
   * state cannot be read.
   * <p>
   * All inputs are read from a single {@code getDivision(...)} snapshot so leader/membership/commit/applied
   * come from one consistent view rather than re-resolving the division per field (cheaper, and avoids a
   * torn read if leadership changes mid-evaluation). Membership uses the <em>current</em> configuration
   * (the most recent conf in the log, which during a joint-consensus change may not yet be committed); that
   * is conservative for the targeted scenario - a wiped/lagging follower is not in it either way. See
   * {@link #isReadyForTrafficState} for the pure decision.
   */
  public boolean isReadyForTraffic(final long maxLagEntries) {
    final RaftServer server = ha.raftServer;
    if (server == null || ha.shutdownRequested)
      return false;
    // Read BEFORE the applied index below (issue #7619): the applied index only grows, so pairing it with a
    // leader figure taken no later than itself can only under-state the lag, never over-state it.
    final long leaderCommitIndex = ha.leaderReportedCommitIndex;
    try {
      final var division = server.getDivision(ha.raftGroup.getGroupId());
      final var info = division.getInfo();
      final var conf = division.getRaftConf();
      final boolean leaderPresent = info.getLeaderId() != null;
      final boolean localInConfig = conf != null && RaftHAServer.isPeerInConfig(conf.getCurrentPeers(), ha.localPeerId);
      final long commitIndex = division.getRaftLog().getLastCommittedIndex();
      final long appliedIndex = info.getLastAppliedIndex();
      // A snapshot resync in flight (or a database still diverged awaiting one) means this node's data
      // may be divergent; do not advertise Ready until it clears (issue #5273).
      final ArcadeStateMachine sm = ha.getStateMachine();
      final boolean resyncInProgress = sm != null && sm.isResyncInProgress();
      // Issue #7131: commitIndex/appliedIndex above are both local to this division, and Ratis clamps a
      // follower's commit index to its own flush index - a follower receiving no appends has
      // commitIndex == appliedIndex and reports lag 0 no matter how far behind the leader it really is.
      // emptyLogInMultiPeerCluster adds nothing to that: it is true only when commitIndex < 0, which the
      // predicate already refuses on its own (issue #7619). It is kept only because the predicate's overloads
      // and their tests carry it.
      final boolean emptyLogInMultiPeerCluster = isEmptyLogInMultiPeerCluster(
          conf == null ? 0 : conf.getCurrentPeers().size(), commitIndex);
      // Issue #7619, the wedged-channel case #7131 was really about: measure the applied index against the
      // commit index the LEADER reports, which the health monitor asks it for over a follower-to-leader call
      // that does not depend on the leader-to-follower replication channel being alive. A leader-RPC-recency
      // signal (lastRpcElapsedTimeMs) was tried for this before and removed, because a PRE_VOTE granted to any
      // candidate refreshes that same timestamp (review finding on PR #7605); a commit index has no such
      // ambiguity - it is exactly the quantity the lag is defined against.
      final boolean behindLeaderCommit = isBehindLeaderCommit(leaderCommitIndex, appliedIndex, maxLagEntries);
      // Issue #7130: the division's own Ratis lifecycle, read from this same snapshot so it cannot disagree
      // with leaderPresent/localInConfig/commitIndex/appliedIndex above. A RaftServer proxy can stay RUNNING
      // while the per-group division underneath goes CLOSED or EXCEPTION (issue #5271); every field this
      // method otherwise reads (leaderId, raft conf, commit/applied index) survives that close and keeps
      // reporting its last value, which is exactly how a dead division still answered Ready before this gate.
      final boolean divisionLifecycleHealthy = isDivisionLifecycleHealthy(info.getLifeCycleState());
      final boolean haltedAfterCriticalError = sm != null && sm.isHaltedAfterCriticalError();
      return isReadyForTrafficState(leaderPresent, localInConfig, info.isLeader(), commitIndex, appliedIndex,
          maxLagEntries, resyncInProgress, info.isLeaderReady(), emptyLogInMultiPeerCluster,
          divisionLifecycleHealthy, haltedAfterCriticalError, behindLeaderCommit);
    } catch (final Exception e) {
      // Catch Exception, not IOException: getLastAppliedIndex() above is documented to throw Ratis'
      // IllegalStateException while an in-place restart re-initializes the division (issue #5271), so the
      // narrow catch let /api/v1/ready answer HTTP 500 instead of NOT_READY (issue #7135). This method's
      // contract is to fail closed on unreadable state, and that is the answer for every read failure.
      // WARNING for the same reason as getCommittedPeersOrNull above: the broad catch must not let a real bug
      // hide behind a probe that merely answers NOT_READY.
      LogManager.instance().log(ha, Level.WARNING, "Cannot read Raft state for readiness probe", e);
      return false;
    }
  }

  /**
   * Whether {@code state} counts as healthy for the issue #7130 readiness gate. Only {@code RUNNING} is -
   * every other {@link LifeCycle.State} value, {@code PAUSED} included, is not ready.
   * <p>
   * {@code PAUSED} was initially included alongside {@code RUNNING} on the reasoning that
   * {@link ArcadeStateMachine#pause()}'s own javadoc verifies every Ratis {@code StateMachine.pause()} caller
   * is paired with a subsequent {@code reinitialize()} back to RUNNING, making it a brief, self-recovering
   * transition rather than a failure state. That reasoning conflated "will resolve itself shortly" with "can
   * serve traffic right now": {@code RaftServerImpl}'s own request handlers (append entries, request vote,
   * client requests) each assert the lifecycle is in the singleton {@code RUNNING} set before proceeding -
   * verified against Ratis 3.3.0 bytecode - and reject with {@code ServerNotReadyException} for any other
   * state, {@code PAUSED} included. A division legitimately PAUSED for a snapshot-install notification (the
   * common real-world trigger) therefore rejects traffic exactly like a CLOSED/EXCEPTION one would, for
   * however briefly; readiness must reflect that, not the fact that it is expected to end soon (review
   * finding on PR #7605).
   * <p>
   * Package-private, not private, specifically so this mapping - the one part of the #7130 fix with no other
   * test coverage, since every {@code isReadyForTrafficState} test passes the already-reduced boolean - can
   * be exercised directly against every {@link LifeCycle.State} value without faking a Ratis {@code Division}.
   */
  static boolean isDivisionLifecycleHealthy(final LifeCycle.State state) {
    return state == LifeCycle.State.RUNNING;
  }

  /**
   * The issue #7131 cold-rejoin signal: whether {@code commitIndex} means "this follower's log holds nothing
   * yet" in a configuration with more than one peer.
   * <p>
   * <b>It does not change the readiness answer for any input</b> (issue #7619): it is true only when
   * {@code commitIndex < 0}, and the follower branch of {@link #isReadyForTrafficState} refuses a negative
   * {@code commitIndex} on its own on the very next line. The follower it was meant to catch - one whose
   * replication channel is wedged, so its local log stops growing - is caught by {@link #isBehindLeaderCommit}
   * instead, which compares against the leader's commit index rather than this follower's clamped one.
   * <p>
   * Strictly {@code < 0} ({@code RaftLog.INVALID_LOG_INDEX}), not {@code <= 0}: index {@code 0} is
   * {@code RaftLog.LEAST_VALID_LOG_INDEX}, the leader's first real committed entry, not "empty". An earlier
   * revision used {@code <= 0}, which reported a follower genuinely caught up to a brand-new multi-node
   * cluster's very first commit as not-ready - confirmed against Ratis 3.3.0 bytecode ({@code RaftLogIndex}'s
   * two named constants) and missed by every {@code isReadyForTrafficState} test because none of them fed a
   * real, un-reduced {@code commitIndex} through this computation (review finding on PR #7605).
   * <p>
   * Package-private, not private, for the same reason as {@link #isDivisionLifecycleHealthy}: so this
   * boundary can be exercised directly instead of only through a live Ratis {@code Division}.
   */
  static boolean isEmptyLogInMultiPeerCluster(final int peerCount, final long commitIndex) {
    return peerCount > 1 && commitIndex < 0;
  }

  /**
   * Pure decision function behind {@link #isReadyForTraffic(long)}, split out so the predicate can be
   * unit-tested without the Ratis state plumbing. Returns {@code true} only when a leader is present and
   * this node is in the current configuration, and either this node is the leader (treated as caught up
   * with itself by this overload) or - as a follower - the lag {@code commitIndex - appliedIndex} is in
   * {@code [0, maxLagEntries]}. A negative {@code commitIndex}/{@code appliedIndex} (state not readable this
   * tick) or a negative lag ({@code appliedIndex > commitIndex}, an inconsistent state) returns
   * {@code false} so the probe fails closed rather than advertising Ready on unreadable/inconsistent state.
   * Production code calls the 8-argument overload, which additionally requires the leader to be ready.
   */
  static boolean isReadyForTrafficState(final boolean leaderPresent, final boolean localInConfig,
      final boolean leader, final long commitIndex, final long appliedIndex, final long maxLagEntries) {
    return isReadyForTrafficState(leaderPresent, localInConfig, leader, commitIndex, appliedIndex, maxLagEntries,
        false);
  }

  /**
   * Overload of {@link #isReadyForTrafficState(boolean, boolean, boolean, long, long, long)} that also
   * fails closed while a snapshot resync is in flight (issue #5273). A follower that is downloading a
   * snapshot - or that still has a database marked diverged after a WAL version gap and awaiting its
   * resync - may hold divergent data, so it must not advertise Ready until the resync clears, even if
   * its raw applied-index lag happens to be within {@code maxLagEntries}. The leader never resyncs from
   * a peer, so this gate only affects followers in practice.
   */
  static boolean isReadyForTrafficState(final boolean leaderPresent, final boolean localInConfig,
      final boolean leader, final long commitIndex, final long appliedIndex, final long maxLagEntries,
      final boolean resyncInProgress) {
    return isReadyForTrafficState(leaderPresent, localInConfig, leader, commitIndex, appliedIndex, maxLagEntries,
        resyncInProgress, leader);
  }

  /**
   * Overload of {@link #isReadyForTrafficState(boolean, boolean, boolean, long, long, long, boolean)} that
   * gates the leader branch on Ratis' own readiness signal (issue #5453) instead of assuming the role
   * implies the ability to serve. A node that has just won an election reports the leader role before it
   * has committed its current-term no-op, and rejects writes with the retryable
   * {@code LeaderNotReadyException} until then; advertising Ready in that window makes a client's first
   * write absorb retries until its {@code arcadedb.ha.quorumTimeout} budget expires.
   * <p>
   * {@code leaderReady} is a separate input rather than a redefinition of {@code leader} because a
   * not-yet-ready leader must not fall through to the follower lag branch: a freshly elected leader
   * typically has {@code commitIndex == appliedIndex} and would be reported Ready there anyway. The flag
   * is meaningful only when {@code leader} is true; a follower's readiness is decided by its lag.
   * <p>
   * Kept as the direct 8-argument entry point for existing callers and tests: it forwards to the 9-argument
   * overload with {@code emptyLogInMultiPeerCluster=false}. That flag never changes the answer when it is
   * computed from the same {@code commitIndex} (see {@link #isEmptyLogInMultiPeerCluster}), so this overload's
   * lag-only behaviour is exactly the production one minus the #7130 and #7619 inputs. Production code calls
   * the final, 12-argument overload directly.
   */
  static boolean isReadyForTrafficState(final boolean leaderPresent, final boolean localInConfig,
      final boolean leader, final long commitIndex, final long appliedIndex, final long maxLagEntries,
      final boolean resyncInProgress, final boolean leaderReady) {
    return isReadyForTrafficState(leaderPresent, localInConfig, leader, commitIndex, appliedIndex, maxLagEntries,
        resyncInProgress, leaderReady, false);
  }

  /**
   * Overload of {@link #isReadyForTrafficState(boolean, boolean, boolean, long, long, long, boolean, boolean)}
   * that adds the issue #7131 empty-log flag: this follower's local log holds nothing yet, in a configuration
   * that has more than one peer. Applies to followers only; the leader branch returns before it is evaluated.
   * <p>
   * As production computes it the flag is redundant (issue #7619): {@link #isEmptyLogInMultiPeerCluster} is
   * true only for a negative {@code commitIndex}, which the follower branch refuses anyway. A follower that is
   * not receiving entries - empty log or not - is caught by the {@code behindLeaderCommit} input of the
   * 12-argument overload, which is measured against the leader's commit index instead of this follower's own.
   */
  static boolean isReadyForTrafficState(final boolean leaderPresent, final boolean localInConfig,
      final boolean leader, final long commitIndex, final long appliedIndex, final long maxLagEntries,
      final boolean resyncInProgress, final boolean leaderReady, final boolean emptyLogInMultiPeerCluster) {
    return isReadyForTrafficState(leaderPresent, localInConfig, leader, commitIndex, appliedIndex, maxLagEntries,
        resyncInProgress, leaderReady, emptyLogInMultiPeerCluster, true, false);
  }

  /**
   * Overload that additionally folds in the issue #7130 gap: a {@code RaftServer} proxy can stay
   * {@code RUNNING} while the per-group division underneath goes {@code CLOSED} or {@code EXCEPTION}
   * (issue #5271) - the node then rejects every vote/append with {@code ServerNotReadyException} while
   * every field the overloads above read (leader id, raft configuration, commit/applied index) keeps
   * reporting its last value from before the close, so none of those gates see it. Also fails closed once
   * {@code ArcadeStateMachine.isHaltedAfterCriticalError()} is set, giving that flag its first production
   * caller: the halt is meant to take the node out of service, and without this it stayed Ready with every
   * {@code applyTransaction} failing.
   * <p>
   * Both are evaluated before every other gate, including {@code leaderPresent}/{@code localInConfig}: once
   * the division is unhealthy or the node is halted, none of the other fields can be trusted either. Forwards
   * to the 12-argument overload with no leader commit index known ({@code behindLeaderCommit=false}).
   */
  static boolean isReadyForTrafficState(final boolean leaderPresent, final boolean localInConfig,
      final boolean leader, final long commitIndex, final long appliedIndex, final long maxLagEntries,
      final boolean resyncInProgress, final boolean leaderReady, final boolean emptyLogInMultiPeerCluster,
      final boolean divisionLifecycleHealthy, final boolean haltedAfterCriticalError) {
    return isReadyForTrafficState(leaderPresent, localInConfig, leader, commitIndex, appliedIndex, maxLagEntries,
        resyncInProgress, leaderReady, emptyLogInMultiPeerCluster, divisionLifecycleHealthy, haltedAfterCriticalError,
        false);
  }

  /**
   * Overload that closes the wedged-channel case of issue #7131 (issue #7619). The lag every overload above
   * measures, {@code commitIndex - appliedIndex}, is between two indices local to this division, and Ratis clamps
   * a follower's commit index to its own flush index. A follower whose inbound replication channel is wedged -
   * heartbeats still arrive, so a leader is known and nothing is resyncing, but no entries do - therefore has
   * {@code commitIndex == appliedIndex} and reports lag {@code 0} however far the leader has moved on.
   * {@code behindLeaderCommit} is the same lag measured against the commit index the leader itself reports (see
   * {@link #isBehindLeaderCommit}); when it exceeds {@code maxLagEntries} the follower is not Ready. Applies to
   * followers only: the leader branch returns before it is evaluated. This is the production entry point;
   * {@link #isReadyForTraffic(long)} calls it directly.
   */
  static boolean isReadyForTrafficState(final boolean leaderPresent, final boolean localInConfig,
      final boolean leader, final long commitIndex, final long appliedIndex, final long maxLagEntries,
      final boolean resyncInProgress, final boolean leaderReady, final boolean emptyLogInMultiPeerCluster,
      final boolean divisionLifecycleHealthy, final boolean haltedAfterCriticalError,
      final boolean behindLeaderCommit) {
    if (!divisionLifecycleHealthy)
      return false;
    if (haltedAfterCriticalError)
      return false;
    if (!leaderPresent || !localInConfig)
      return false;
    if (resyncInProgress)
      return false;
    if (leader)
      return leaderReady;
    if (emptyLogInMultiPeerCluster)
      return false;
    if (commitIndex < 0 || appliedIndex < 0)
      return false;
    if (behindLeaderCommit)
      return false;
    final long lag = commitIndex - appliedIndex;
    return lag >= 0 && lag <= maxLagEntries;
  }

  /**
   * Whether this follower's {@code appliedIndex} is more than {@code maxLagEntries} behind the commit index its
   * leader last reported (issue #7619). {@code false} when no leader commit index is known yet
   * ({@code leaderCommitIndex < 0}) - the gate then abstains and readiness falls back to the local lag, which is
   * the behaviour before this gate existed - and when {@code appliedIndex} is negative, which the predicate
   * refuses on its own. A leader figure below the applied index (a sample older than this follower's progress)
   * is not a lag at all.
   * <p>
   * Package-private so the boundary can be exercised directly with raw indices.
   */
  static boolean isBehindLeaderCommit(final long leaderCommitIndex, final long appliedIndex,
      final long maxLagEntries) {
    if (leaderCommitIndex < 0 || appliedIndex < 0)
      return false;
    return leaderCommitIndex - appliedIndex > maxLagEntries;
  }
}
