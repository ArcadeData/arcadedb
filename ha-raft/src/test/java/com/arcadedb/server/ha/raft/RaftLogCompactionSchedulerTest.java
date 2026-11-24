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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Unit tests for {@link RaftLogCompactionScheduler}, the periodic snapshot trigger that keeps the
 * Raft log bounded on a low-write cluster where the count-based auto-snapshot threshold is never
 * reached (issue #5345).
 */
class RaftLogCompactionSchedulerTest {

  static final class FakeCompactionTarget implements RaftLogCompactionScheduler.CompactionTarget {
    // Written by the scheduler's thread for an early checkpoint (issue #12587)
    final    List<Long> requestedGaps     = new CopyOnWriteArrayList<>();
    volatile boolean    shutdownRequested = false;
    volatile boolean    failSnapshot      = false;
    volatile long       usableSpace       = 900L;
    volatile long       totalSpace        = 1000L;
    volatile long       snapshotIndex     = 42L;

    @Override
    public boolean isShutdownRequested() {
      return shutdownRequested;
    }

    @Override
    public long triggerSnapshot(final long creationGap) {
      requestedGaps.add(creationGap);
      if (failSnapshot)
        throw new IllegalStateException("snapshot request refused");
      return snapshotIndex;
    }

    @Override
    public long getRaftStorageUsableSpaceBytes() {
      return usableSpace;
    }

    @Override
    public long getRaftStorageTotalSpaceBytes() {
      return totalSpace;
    }

    @Override
    public String getRaftStorageDescription() {
      return "/tmp/raft-storage-test";
    }
  }

  private static RaftLogCompactionScheduler newScheduler(final FakeCompactionTarget target) {
    return new RaftLogCompactionScheduler(target, 300_000L, 64L, 20);
  }

  @Test
  void schedulerIsDisabledWhenIntervalIsNotPositive() {
    final FakeCompactionTarget target = new FakeCompactionTarget();
    final RaftLogCompactionScheduler scheduler = new RaftLogCompactionScheduler(target, 0L, 64L, 20);
    scheduler.start();
    try {
      assertThat(scheduler.isRunning()).isFalse();
    } finally {
      scheduler.stop();
    }
  }

  @Test
  void schedulerStartsAndStopsWhenIntervalIsPositive() {
    final FakeCompactionTarget target = new FakeCompactionTarget();
    final RaftLogCompactionScheduler scheduler = newScheduler(target);
    scheduler.start();
    try {
      assertThat(scheduler.isRunning()).isTrue();
    } finally {
      scheduler.stop();
    }
    assertThat(scheduler.isRunning()).isFalse();
  }

  @Test
  void normalTickRequestsSnapshotWithConfiguredCreationGap() {
    final FakeCompactionTarget target = new FakeCompactionTarget();
    final RaftLogCompactionScheduler scheduler = newScheduler(target);

    scheduler.tick();

    assertThat(target.requestedGaps).containsExactly(64L);
  }

  @Test
  void creationGapIsClampedToAtLeastOne() {
    final FakeCompactionTarget target = new FakeCompactionTarget();
    final RaftLogCompactionScheduler scheduler = new RaftLogCompactionScheduler(target, 300_000L, 0L, 20);

    scheduler.tick();

    assertThat(target.requestedGaps).containsExactly(1L);
  }

  @Test
  void diskPressureDropsCreationGapToOne() {
    final FakeCompactionTarget target = new FakeCompactionTarget();
    // 100 free of 1000 total = 10% free, below the 20% threshold
    target.usableSpace = 100L;
    final RaftLogCompactionScheduler scheduler = newScheduler(target);

    scheduler.tick();

    assertThat(scheduler.isUnderDiskPressure()).isTrue();
    assertThat(target.requestedGaps).containsExactly(1L);
  }

  @Test
  void freeSpaceExactlyAtThresholdIsNotDiskPressure() {
    final FakeCompactionTarget target = new FakeCompactionTarget();
    target.usableSpace = 200L;
    final RaftLogCompactionScheduler scheduler = newScheduler(target);

    scheduler.tick();

    assertThat(scheduler.isUnderDiskPressure()).isFalse();
    assertThat(target.requestedGaps).containsExactly(64L);
  }

  @Test
  void unknownVolumeSizeIsNotReportedAsDiskPressure() {
    final FakeCompactionTarget target = new FakeCompactionTarget();
    target.totalSpace = 0L;
    target.usableSpace = 0L;
    final RaftLogCompactionScheduler scheduler = newScheduler(target);

    scheduler.tick();

    assertThat(scheduler.isUnderDiskPressure()).isFalse();
    assertThat(target.requestedGaps).containsExactly(64L);
  }

  @Test
  void diskPressureCheckIsSkippedWhenThresholdIsNotPositive() {
    final FakeCompactionTarget target = new FakeCompactionTarget();
    target.usableSpace = 1L;
    final RaftLogCompactionScheduler scheduler = new RaftLogCompactionScheduler(target, 300_000L, 64L, 0);

    scheduler.tick();

    assertThat(scheduler.isUnderDiskPressure()).isFalse();
    assertThat(target.requestedGaps).containsExactly(64L);
  }

  @Test
  void tickIsSuppressedWhileShutdownIsRequested() {
    final FakeCompactionTarget target = new FakeCompactionTarget();
    target.shutdownRequested = true;
    final RaftLogCompactionScheduler scheduler = newScheduler(target);

    scheduler.tick();

    assertThat(target.requestedGaps).isEmpty();
  }

  @Test
  void snapshotFailureDoesNotEscapeTheSchedulerThread() {
    final FakeCompactionTarget target = new FakeCompactionTarget();
    target.failSnapshot = true;
    final RaftLogCompactionScheduler scheduler = newScheduler(target);

    scheduler.tickSafely();
    scheduler.tickSafely();

    // Both ticks ran: the first failure did not kill the periodic task.
    assertThat(target.requestedGaps).containsExactly(64L, 64L);
  }

  @Test
  void diskPressureWarningIsThrottledToOnePerWindow() {
    final FakeCompactionTarget target = new FakeCompactionTarget();
    target.usableSpace = 10L;
    final RaftLogCompactionScheduler scheduler = newScheduler(target);
    final AtomicLong now = new AtomicLong(1_000L);
    scheduler.setClock(now::get);

    assertThat(scheduler.shouldWarnAboutDiskPressure()).isTrue();
    assertThat(scheduler.shouldWarnAboutDiskPressure()).isFalse();

    now.addAndGet(RaftLogCompactionScheduler.DISK_WARNING_THROTTLE_MS - 1);
    assertThat(scheduler.shouldWarnAboutDiskPressure()).isFalse();

    now.addAndGet(1);
    assertThat(scheduler.shouldWarnAboutDiskPressure()).isTrue();
  }

  /**
   * Ratis answers a request below the creation gap with a SUCCESS reply carrying the <i>existing</i>
   * snapshot index, so the scheduler must only treat a strictly higher index as a real compaction.
   */
  @Test
  void repeatedTicksAtTheSameIndexAreNotCountedAsNewCompactions() {
    final FakeCompactionTarget target = new FakeCompactionTarget();
    final RaftLogCompactionScheduler scheduler = newScheduler(target);

    scheduler.tick();
    assertThat(scheduler.getLastSnapshotIndex()).isEqualTo(42L);

    scheduler.tick();
    assertThat(scheduler.getLastSnapshotIndex()).isEqualTo(42L);

    target.snapshotIndex = 100L;
    scheduler.tick();
    assertThat(scheduler.getLastSnapshotIndex()).isEqualTo(100L);
  }

  /**
   * After a RECOVER restart Ratis already holds a snapshot at some index, which the first (no-op) tick
   * observes. That index must be recorded as the baseline rather than reported as work this scheduler
   * did, while a genuine later advance still registers.
   */
  @Test
  void firstTickSeedsTheBaselineIndexWithoutClaimingACompaction() {
    final FakeCompactionTarget target = new FakeCompactionTarget();
    target.snapshotIndex = 24_269L; // a snapshot Ratis loaded at startup
    final RaftLogCompactionScheduler scheduler = newScheduler(target);

    assertThat(scheduler.getLastSnapshotIndex()).isEqualTo(-1L);

    scheduler.tick();
    assertThat(scheduler.getLastSnapshotIndex()).isEqualTo(24_269L);
    assertThat(scheduler.hasReportedCompaction()).isFalse();

    target.snapshotIndex = 24_400L;
    scheduler.tick();
    assertThat(scheduler.getLastSnapshotIndex()).isEqualTo(24_400L);
    assertThat(scheduler.hasReportedCompaction()).isTrue();
  }

  /**
   * A failed first tick must not consume the baseline: otherwise the next successful tick would report
   * a pre-existing startup snapshot as a compaction, which is exactly what the baseline suppresses.
   */
  @Test
  void aFailedFirstTickDoesNotConsumeTheBaseline() {
    final FakeCompactionTarget target = new FakeCompactionTarget();
    target.snapshotIndex = -1L; // Ratis refused the request
    final RaftLogCompactionScheduler scheduler = newScheduler(target);

    scheduler.tick();
    assertThat(scheduler.getLastSnapshotIndex()).isEqualTo(-1L);
    assertThat(scheduler.hasReportedCompaction()).isFalse();

    // A pre-existing startup snapshot observed by the first SUCCESSFUL tick is still only a baseline.
    target.snapshotIndex = 24_269L;
    scheduler.tick();
    assertThat(scheduler.getLastSnapshotIndex()).isEqualTo(24_269L);
    assertThat(scheduler.hasReportedCompaction()).isFalse();

    target.snapshotIndex = 24_400L;
    scheduler.tick();
    assertThat(scheduler.hasReportedCompaction()).isTrue();
  }

  /**
   * The sentinel distinction is load-bearing: a negative reading means "volume size unknown" and must
   * never be read as pressure, while a genuine zero free bytes on a sized volume is the disk-full case
   * this scheduler exists to catch.
   */
  @Test
  void negativeUsableSpaceIsUnknownButZeroUsableSpaceIsPressure() {
    final FakeCompactionTarget unknown = new FakeCompactionTarget();
    unknown.usableSpace = -1L;
    final RaftLogCompactionScheduler unknownScheduler = newScheduler(unknown);
    unknownScheduler.tick();
    assertThat(unknownScheduler.isUnderDiskPressure()).isFalse();
    assertThat(unknown.requestedGaps).containsExactly(64L);

    final FakeCompactionTarget full = new FakeCompactionTarget();
    full.usableSpace = 0L;
    final RaftLogCompactionScheduler fullScheduler = newScheduler(full);
    fullScheduler.tick();
    assertThat(fullScheduler.isUnderDiskPressure()).isTrue();
    assertThat(full.requestedGaps).containsExactly(1L);
  }

  /**
   * Percentage arithmetic must not overflow: long products of petabyte-scale byte counts wrap negative
   * and would silently under-report a nearly full volume.
   */
  @Test
  void diskPressureIsDetectedOnPetabyteScaleVolumes() {
    final FakeCompactionTarget target = new FakeCompactionTarget();
    target.totalSpace = Long.MAX_VALUE / 2L;
    target.usableSpace = target.totalSpace / 100L; // 1% free, well below the 20% threshold
    final RaftLogCompactionScheduler scheduler = newScheduler(target);

    scheduler.tick();

    assertThat(scheduler.isUnderDiskPressure()).isTrue();
    assertThat(target.requestedGaps).containsExactly(1L);
  }

  @Test
  void diskPressureStateClearsWhenSpaceIsReclaimed() {
    final FakeCompactionTarget target = new FakeCompactionTarget();
    target.usableSpace = 50L;
    final RaftLogCompactionScheduler scheduler = newScheduler(target);

    scheduler.tick();
    assertThat(scheduler.isUnderDiskPressure()).isTrue();

    target.usableSpace = 800L;
    scheduler.tick();
    assertThat(scheduler.isUnderDiskPressure()).isFalse();
    assertThat(target.requestedGaps).containsExactly(1L, 64L);
  }

  // Regression for #12587: an early checkpoint is taken on the scheduler's thread with the gap it was asked with
  @Test
  void anEarlyCheckpointIsTakenWithTheGapItWasAskedWith() {
    final FakeCompactionTarget target = new FakeCompactionTarget();
    final RaftLogCompactionScheduler scheduler = newScheduler(target);
    scheduler.start();
    try {
      assertThat(scheduler.requestEarlyCheckpoint(75_000L)).as("queued").isTrue();
      await().atMost(Duration.ofSeconds(10)).until(() -> !target.requestedGaps.isEmpty());
      assertThat(target.requestedGaps)
          .as("the early checkpoint's own gap, not minEntries: a checkpoint another trigger took meanwhile is not taken twice")
          .containsExactly(75_000L);
    } finally {
      scheduler.stop();
    }
  }

  // Regression for #12587: arcadedb.ha.snapshotInterval=0 turns the periodic tick off, not the early checkpoints
  @Test
  void anEarlyCheckpointIsTakenWhenThePeriodicCompactionIsDisabled() {
    final FakeCompactionTarget target = new FakeCompactionTarget();
    final RaftLogCompactionScheduler scheduler = new RaftLogCompactionScheduler(target, 0L, 64L, 20);
    scheduler.start();
    try {
      assertThat(scheduler.isRunning()).as("no periodic tick").isFalse();
      assertThat(scheduler.acceptsEarlyCheckpoints()).isTrue();
      scheduler.requestEarlyCheckpoint(30L);
      await().atMost(Duration.ofSeconds(10)).until(() -> !target.requestedGaps.isEmpty());
      assertThat(target.requestedGaps).containsExactly(30L);
    } finally {
      scheduler.stop();
    }
    assertThat(scheduler.acceptsEarlyCheckpoints()).isFalse();
  }

  // Regression for #12587: disk pressure purges as far as possible, whoever asked for the checkpoint
  @Test
  void anEarlyCheckpointUnderDiskPressureUsesTheSmallestGap() {
    final FakeCompactionTarget target = new FakeCompactionTarget();
    target.usableSpace = 100L;
    final RaftLogCompactionScheduler scheduler = newScheduler(target);
    scheduler.start();
    try {
      scheduler.requestEarlyCheckpoint(30L);
      await().atMost(Duration.ofSeconds(10)).until(() -> !target.requestedGaps.isEmpty());
      assertThat(target.requestedGaps).containsExactly(RaftLogCompactionScheduler.DISK_PRESSURE_CREATION_GAP);
    } finally {
      scheduler.stop();
    }
  }

  // Regression for #12587: the apply thread asks, so a request to a scheduler that is not running is refused without a throw
  @Test
  void anEarlyCheckpointAskedOfASchedulerNotRunningIsRefused() {
    final FakeCompactionTarget target = new FakeCompactionTarget();
    final RaftLogCompactionScheduler scheduler = newScheduler(target);

    assertThat(scheduler.requestEarlyCheckpoint(30L)).as("not started yet: asked again with the next entry").isFalse();
    scheduler.start();
    scheduler.stop();
    assertThat(scheduler.requestEarlyCheckpoint(30L)).as("stopped").isFalse();

    assertThat(target.requestedGaps).isEmpty();
  }

  // Regression for #12587: a failed early checkpoint neither escapes nor kills the scheduler's thread
  @Test
  void aFailedEarlyCheckpointDoesNotKillTheSchedulerThread() {
    final FakeCompactionTarget target = new FakeCompactionTarget();
    target.failSnapshot = true;
    final RaftLogCompactionScheduler scheduler = newScheduler(target);
    scheduler.start();
    try {
      scheduler.requestEarlyCheckpoint(30L);
      scheduler.requestEarlyCheckpoint(31L);
      await().atMost(Duration.ofSeconds(10)).until(() -> target.requestedGaps.size() == 2);
      assertThat(target.requestedGaps).containsExactly(30L, 31L);
    } finally {
      scheduler.stop();
    }
  }

  // Regression for #12587: the early checkpoint is asked at three quarters of the auto-trigger, never at 0
  @Test
  void theEarlyCheckpointGapIsThreeQuartersOfTheAutoTrigger() {
    assertThat(ArcadeStateMachine.earlyCheckpointGapFor(100_000L)).isEqualTo(75_000L);
    assertThat(ArcadeStateMachine.earlyCheckpointGapFor(40L)).isEqualTo(30L);
    assertThat(ArcadeStateMachine.earlyCheckpointGapFor(1L)).isEqualTo(1L);
    assertThat(ArcadeStateMachine.earlyCheckpointGapFor(0L)).as("no auto-trigger, no early checkpoint").isZero();
  }
}
