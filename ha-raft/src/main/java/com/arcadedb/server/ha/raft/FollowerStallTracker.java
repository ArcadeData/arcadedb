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

/**
 * The follower's own view of the zero-progress stall the leader's {@link ClusterMonitor} reports as
 * {@link ClusterMonitor.ReplicaStatus#STALLED} (issue #8342).
 * <p>
 * A follower whose log stops receiving entries while the term does not change has nothing local to go on: Ratis
 * clamps its commit index to the entries it holds, so {@code commitIndex == appliedIndex}, and its applied term
 * matches the current term, so neither the persistent-lag check nor the stale-term check can see it. The only
 * figure that shows the gap is the leader's commit index, which {@link RaftHAServer#refreshLeaderCommitIndex()}
 * asks the leader for over a follower-to-leader call. This tracker compares the two with the rule the leader
 * applies to the same replica: more than the lag threshold behind, with no progress at all for
 * {@link ClusterMonitor#ZERO_PROGRESS_STALL_GRACE_MS}.
 * <p>
 * Progress is either the applied index or the local log's last index moving. The log index is what the leader's
 * {@code matchIndex} measures, so a follower whose appends still arrive but whose state machine is busy applying
 * one large entry is not called stalled here while the leader calls it healthy.
 * <p>
 * Written only by the health-monitor thread ({@link #observe}, {@link #reset}); {@link #current()} is read from
 * HTTP workers, hence the single volatile snapshot.
 */
final class FollowerStallTracker {

  /**
   * A stall in progress, as last observed.
   *
   * @param leaderCommitIndex the commit index the leader last reported
   * @param appliedIndex      this node's applied index, unchanged since the spell began
   * @param lag               {@code leaderCommitIndex - appliedIndex}
   * @param stalledForMs      how long this node has made no progress while that far behind
   */
  record Stall(long leaderCommitIndex, long appliedIndex, long lag, long stalledForMs) {
  }

  private final long           graceMs;
  private       long           baselineAppliedIndex = -1L;
  private       long           baselineLogIndex     = -1L;
  private       long           zeroProgressSinceMs  = -1L;
  private volatile Stall       stall;

  FollowerStallTracker() {
    this(ClusterMonitor.ZERO_PROGRESS_STALL_GRACE_MS);
  }

  FollowerStallTracker(final long graceMs) {
    this.graceMs = graceMs;
  }

  /**
   * Records one health tick.
   *
   * @param nowMs             the tick's wall-clock time
   * @param eligible          whether this node is a running follower of a known leader with no resync in flight.
   *                          A resync has its own alert and legitimately applies nothing for a while
   * @param leaderCommitIndex the commit index the leader last reported, or a negative value when none is known.
   *                          Every value is a commit index a leader really reported, so a stale one can only
   *                          under-state the lag
   * @param appliedIndex      this node's applied index, or a negative value when it cannot be read
   * @param logIndex          the last index of this node's Raft log, or a negative value when it cannot be read
   * @param lagThreshold      lag (entries) at or below which this node is not considered behind
   */
  void observe(final long nowMs, final boolean eligible, final long leaderCommitIndex, final long appliedIndex,
      final long logIndex, final long lagThreshold) {
    if (!eligible || leaderCommitIndex < 0 || appliedIndex < 0) {
      reset();
      return;
    }
    final boolean progressed = (baselineAppliedIndex >= 0 && appliedIndex > baselineAppliedIndex)
        || (baselineLogIndex >= 0 && logIndex > baselineLogIndex);
    baselineAppliedIndex = appliedIndex;
    baselineLogIndex = logIndex;

    final long lag = leaderCommitIndex - appliedIndex;
    if (lag <= lagThreshold || progressed) {
      zeroProgressSinceMs = -1L;
      stall = null;
      return;
    }
    if (zeroProgressSinceMs == -1L)
      zeroProgressSinceMs = nowMs;
    final long stalledForMs = nowMs - zeroProgressSinceMs;
    stall = stalledForMs >= graceMs ? new Stall(leaderCommitIndex, appliedIndex, lag, stalledForMs) : null;
  }

  /** Forgets the spell and the progress baseline: the next observation starts from scratch. */
  void reset() {
    baselineAppliedIndex = -1L;
    baselineLogIndex = -1L;
    zeroProgressSinceMs = -1L;
    stall = null;
  }

  /** The stall in progress, or {@code null} when this node is not stalled behind its leader. */
  Stall current() {
    return stall;
  }
}
