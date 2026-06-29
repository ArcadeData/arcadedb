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
 * Pure state machine that turns a sequence of (appliedIndex, committedIndex, now) observations on a
 * follower into a concise resync narrative. The signal is the local <i>apply backlog</i>
 * ({@code committedIndex - appliedIndex}): entries the follower has received/committed but not yet
 * applied to its state machine. This is what a follower can observe locally - it is NOT the distance
 * from the leader's commit index, which a follower cannot read directly. The backlog spikes as a
 * restarting follower streams entries in faster than it applies them, which is the catch-up signal.
 * Emits one STARTED line when the backlog reaches {@code catchupLagThreshold} (a genuine post-restart
 * catch-up, not the handful-of-entries steady-state backlog under write load), throttled PROGRESS lines
 * while it drains, and one FINISHED line when it falls back within a tenth of that threshold. The
 * start/finish hysteresis prevents flip-flopping on normal backlog. Driven by the follower-only
 * health-monitor tick. Holds no clock or logger of its own so it can be unit-tested deterministically;
 * the caller supplies {@code now} and logs the returned message.
 */
public final class FollowerResyncProgressTracker {

  public enum Event {NONE, STARTED, PROGRESS, FINISHED}

  public record Tick(Event event, String message) {
    static final Tick NONE = new Tick(Event.NONE, null);
  }

  private final long progressIntervalMs;
  // Minimum lag (entries) that starts the narrative, and the smaller lag at which it finishes. The
  // gap between them is deliberate hysteresis so steady-state replication lag - which under write load
  // hovers a few entries behind and momentarily reaches zero - never flip-flops the narrative. Only a
  // genuine post-restart catch-up crosses the start threshold.
  private final long startLagThreshold;
  private final long finishLagThreshold;

  private boolean active        = false;
  private long    startMs       = 0;
  private long    startApplied  = 0;
  private long    lastProgressMs = 0;

  public FollowerResyncProgressTracker(final long progressIntervalMs, final long catchupLagThreshold) {
    this.progressIntervalMs = progressIntervalMs;
    this.startLagThreshold = Math.max(1L, catchupLagThreshold);
    this.finishLagThreshold = startLagThreshold / 10L;
  }

  public Tick onTick(final long appliedIndex, final long committedIndex, final long nowMs) {
    if (appliedIndex < 0 || committedIndex < 0)
      return Tick.NONE; // transient read failure: do not change state

    // Apply backlog: entries this follower has committed (received) but not yet applied to its state
    // machine. This is what is observable locally; it is NOT the distance from the leader's commit
    // index, which a follower cannot read directly. The backlog spikes as a restarting follower streams
    // entries in faster than it applies them, which is the catch-up signal we narrate.
    final long backlog = committedIndex - appliedIndex;

    if (!active) {
      if (backlog < startLagThreshold)
        return Tick.NONE;
      active = true;
      startMs = nowMs;
      startApplied = appliedIndex;
      lastProgressMs = nowMs;
      return new Tick(Event.STARTED, String.format(
          "HA resync started (mode=catch-up): %d entries to apply (applied=%d, committed=%d)",
          backlog, appliedIndex, committedIndex));
    }

    if (backlog <= finishLagThreshold) {
      active = false;
      final long durationMs = nowMs - startMs;
      final long applied = appliedIndex - startApplied;
      return new Tick(Event.FINISHED, String.format(
          "HA resync finished (mode=catch-up, duration=%dms, result=ok): applied %d entries to index %d",
          durationMs, applied, appliedIndex));
    }

    if (nowMs - lastProgressMs >= progressIntervalMs) {
      lastProgressMs = nowMs;
      final long elapsedMs = Math.max(1L, nowMs - startMs);
      final long applied = appliedIndex - startApplied;
      final double ratePerSec = applied * 1000.0 / elapsedMs;
      return new Tick(Event.PROGRESS, String.format(
          "catch-up: applied=%d, committed=%d (%d to apply, ~%.0f entries/s)",
          appliedIndex, committedIndex, backlog, ratePerSec));
    }

    return Tick.NONE;
  }
}
