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
 * The follower's own view of a leader that is not reaching it since its Raft layer was restarted in place (issue
 * #8953, the follower-side half of #8898).
 * <p>
 * After an in-place restart the new division reports {@code RUNNING}, which is factually right for it, while the
 * leader's appends can keep landing on the closed instance it replaced. The follower cannot see the leader's failing
 * appends, and its time since the last leader contact is no help either: a follower the leader does not reach times
 * out into a candidate, its pre-vote is rejected and it comes back as a follower with a fresh contact timestamp, so
 * that figure never grows past one election timeout. What it can see is that the replication path is still unproven
 * since the restart (no replicated entry, no newer term with a known leader, see
 * {@link RaftHAServer#isReplicationPathUnprovenSinceRestart()}) while either no leader has made itself known at all,
 * or the leader reports a commit index past everything this node holds - entries a live path would have delivered.
 * <p>
 * The two arms are not equally conclusive, and {@link Unreachable#leaderKnown()} says which one is reported. A leader
 * that reports entries this node does not hold is direct evidence that its appends are not arriving. No leader known
 * is also what every node of a cluster that has lost its quorum sees, so on that arm the condition may be a symptom
 * of the cluster rather than of this node's path.
 * <p>
 * The condition must hold on every tick for a grace before it is reported, so the heartbeat and catch-up a healthy
 * path delivers within an election timeout of the restart never surface as one. A node whose path is genuinely
 * idle (no new entry on the leader) is never reported: a leader that is known and has nothing new to send gives no
 * evidence either way, and the leader-side channel reset ({@code arcadedb.ha.peerChannelResetDuration}) covers it.
 * <p>
 * Written only by the health-monitor thread ({@link #observe}, {@link #reset}); {@link #current()} is read from HTTP
 * workers, hence the single volatile snapshot.
 */
final class LeaderReachSinceRestartTracker {

  /**
   * A leader not reaching this node, as last observed.
   *
   * @param unreachableForMs how long the condition has held, uninterrupted
   * @param leaderKnown      {@code true} when a leader is known and reports entries this node does not hold (direct
   *                         evidence), {@code false} when no leader has made itself known (which a cluster without a
   *                         quorum shows too)
   */
  record Unreachable(long unreachableForMs, boolean leaderKnown) {
  }

  private          long        sinceMs = -1L;
  private volatile Unreachable current;

  /**
   * Pure predicate for one tick.
   *
   * @param pathUnproven      whether the replication path is still unproven since the last in-place restart
   * @param isLeader          whether this node leads: a leader has nobody to be reached by
   * @param leaderKnown       whether this division currently knows a leader
   * @param leaderCommitIndex the commit index a leader last reported, negative when none is known
   * @param heldIndex         the highest index this node holds (last log entry or applied index), negative when it
   *                          cannot be read
   */
  static boolean leaderNotReaching(final boolean pathUnproven, final boolean isLeader, final boolean leaderKnown,
      final long leaderCommitIndex, final long heldIndex) {
    if (!pathUnproven || isLeader)
      return false;
    if (!leaderKnown)
      return true;
    return leaderCommitIndex >= 0 && heldIndex >= 0 && leaderCommitIndex > heldIndex;
  }

  /**
   * Records one health tick.
   *
   * @param nowMs       the tick's wall-clock time
   * @param holds       whether {@link #leaderNotReaching} holds on this tick
   * @param leaderKnown whether a leader was known on this tick, reported with the condition
   * @param graceMs     how long it must hold, uninterrupted, before it is reported
   */
  void observe(final long nowMs, final boolean holds, final boolean leaderKnown, final long graceMs) {
    if (!holds) {
      reset();
      return;
    }
    if (sinceMs == -1L)
      sinceMs = nowMs;
    final long elapsed = nowMs - sinceMs;
    current = elapsed >= graceMs ? new Unreachable(elapsed, leaderKnown) : null;
  }

  /** Forgets the spell: the next observation starts from scratch. */
  void reset() {
    sinceMs = -1L;
    current = null;
  }

  /** The condition being reported, or {@code null} when it is not. */
  Unreachable current() {
    return current;
  }
}
