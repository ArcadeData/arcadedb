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
 * Pure state machine that turns a sequence of (appliedIndex, leaderCommitIndex, now) observations on a
 * follower into a concise resync narrative: one STARTED line when the follower is first seen behind the
 * leader after a (re)start, throttled PROGRESS lines while it catches up via Raft log replay, and one
 * FINISHED line when it draws level. Driven by the follower-only health-monitor tick. Holds no clock or
 * logger of its own so it can be unit-tested deterministically; the caller supplies {@code now} and logs
 * the returned message.
 */
public final class FollowerResyncProgressTracker {

  public enum Event {NONE, STARTED, PROGRESS, FINISHED}

  public record Tick(Event event, String message) {
    static final Tick NONE = new Tick(Event.NONE, null);
  }

  private final long progressIntervalMs;

  private boolean active        = false;
  private long    startMs       = 0;
  private long    startApplied  = 0;
  private long    lastProgressMs = 0;

  public FollowerResyncProgressTracker(final long progressIntervalMs) {
    this.progressIntervalMs = progressIntervalMs;
  }

  public Tick onTick(final long appliedIndex, final long leaderCommitIndex, final long nowMs) {
    if (appliedIndex < 0 || leaderCommitIndex < 0)
      return Tick.NONE; // transient read failure: do not change state

    final long behind = leaderCommitIndex - appliedIndex;

    if (!active) {
      if (behind <= 0)
        return Tick.NONE;
      active = true;
      startMs = nowMs;
      startApplied = appliedIndex;
      lastProgressMs = nowMs;
      return new Tick(Event.STARTED, String.format(
          "HA resync started (mode=catch-up): %d entries behind leader (applied=%d, leader=%d)",
          behind, appliedIndex, leaderCommitIndex));
    }

    if (behind <= 0) {
      active = false;
      final long durationMs = nowMs - startMs;
      final long caughtUp = appliedIndex - startApplied;
      return new Tick(Event.FINISHED, String.format(
          "HA resync finished (mode=catch-up, duration=%dms, result=ok): caught up %d entries to index %d",
          durationMs, caughtUp, appliedIndex));
    }

    if (nowMs - lastProgressMs >= progressIntervalMs) {
      lastProgressMs = nowMs;
      final long elapsedMs = Math.max(1L, nowMs - startMs);
      final long caughtUp = appliedIndex - startApplied;
      final double ratePerSec = caughtUp * 1000.0 / elapsedMs;
      return new Tick(Event.PROGRESS, String.format(
          "catch-up: applied=%d/%d (%d behind, ~%.0f entries/s)",
          appliedIndex, leaderCommitIndex, behind, ratePerSec));
    }

    return Tick.NONE;
  }
}
