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

import static com.arcadedb.server.ha.raft.RaftHAServer.isPersistentlyLagging;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link RaftHAServer#isPersistentlyLagging} predicate behind the issue #4840 fix.
 * The recovery it gates ({@code recoverFromPersistentLag()} -> {@code triggerSnapshotDownload()}) closes
 * and re-downloads the live database, so it must fire only for a follower that is genuinely stuck, never
 * for one that is merely slow but still applying. Before the fix a follower applying steady-state
 * AppendEntries one entry at a time never set the {@code catchingUp} flag, so a sustained-write workload
 * misclassified a healthy slow follower as lagging and resynced it needlessly.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftHAServerStaleFollowerLagTest {

  // Argument order: commitIndex, appliedIndex, lagThreshold, previousAppliedIndex

  @Test
  void stuckFollowerBeyondThresholdFires() {
    // Applied frozen at 100 while commit raced to 2000, lag 1900 > 1000, no progress since last tick.
    assertThat(isPersistentlyLagging(2000, 100, 1000, 100)).isTrue();
  }

  @Test
  void slowButProgressingFollowerDoesNotFire() {
    // Applied advanced 100 -> 200 since the previous tick. Still 1900 behind commit, but actively
    // replaying the backlog one entry at a time: healthy-but-slow, must not be resynced (issue #4840).
    assertThat(isPersistentlyLagging(2100, 200, 1000, 100)).isFalse();
  }

  @Test
  void evenSingleEntryProgressDoesNotFire() {
    // A single applied entry since the previous tick is enough to prove the follower is not stuck.
    assertThat(isPersistentlyLagging(2100, 101, 1000, 100)).isFalse();
  }

  @Test
  void lagWithinThresholdDoesNotFire() {
    // commit - applied == lagThreshold is NOT beyond the threshold (strict >).
    assertThat(isPersistentlyLagging(1100, 100, 1000, 100)).isFalse();
    // Comfortably within the threshold.
    assertThat(isPersistentlyLagging(150, 100, 1000, 100)).isFalse();
  }

  @Test
  void firstObservationWithoutPriorSampleFiresOnLagAlone() {
    // previousAppliedIndex < 0: no baseline to measure progress against, so the lag alone decides. The
    // HealthMonitor's persistence window still requires the condition to repeat before it acts, and the
    // next tick (which has a baseline) detects any progress.
    assertThat(isPersistentlyLagging(2000, 100, 1000, -1)).isTrue();
  }

  @Test
  void caughtUpFollowerDoesNotFire() {
    assertThat(isPersistentlyLagging(100, 100, 1000, 90)).isFalse();
  }

  @Test
  void unreadableStateDoesNotFire() {
    assertThat(isPersistentlyLagging(-1, 100, 1000, 100)).as("negative commitIndex").isFalse();
    assertThat(isPersistentlyLagging(2000, -1, 1000, 100)).as("negative appliedIndex").isFalse();
  }

  @Test
  void sustainedWriteWorkloadNeverFiresAcrossManyTicks() {
    // Simulate the issue scenario: a healthy follower applies one entry per tick while the leader keeps
    // committing ahead of it, so the absolute lag stays large forever. Threading the applied index from
    // each tick into the next (as isFollowerLaggingBeyond does), the predicate must report "not stuck"
    // on every tick after the first, so the HealthMonitor streak never persists and no resync triggers.
    long previousApplied = -1;
    int laggingTicks = 0;
    for (int tick = 0; tick < 20; tick++) {
      final long applied = 100 + tick;     // advances by one every tick
      final long commit = 2000 + tick * 2; // leader commits faster: lag grows, follower never stuck
      if (isPersistentlyLagging(commit, applied, 1000, previousApplied))
        laggingTicks++;
      previousApplied = applied;
    }
    // Only the very first tick (no baseline) can report lagging; every subsequent tick sees progress.
    assertThat(laggingTicks).isEqualTo(1);
  }

  @Test
  void genuinelyStuckFollowerFiresEveryTick() {
    // The applied index is frozen (e.g. apply thread wedged) while commit keeps advancing.
    long previousApplied = -1;
    int laggingTicks = 0;
    for (int tick = 0; tick < 20; tick++) {
      final long applied = 100;            // frozen
      final long commit = 2000 + tick * 2; // commit advances
      if (isPersistentlyLagging(commit, applied, 1000, previousApplied))
        laggingTicks++;
      previousApplied = applied;
    }
    assertThat(laggingTicks).isEqualTo(20);
  }
}
