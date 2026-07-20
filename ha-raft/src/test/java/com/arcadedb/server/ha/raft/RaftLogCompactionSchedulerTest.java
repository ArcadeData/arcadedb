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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RaftLogCompactionScheduler}, the periodic snapshot trigger that keeps the
 * Raft log bounded on a low-write cluster where the count-based auto-snapshot threshold is never
 * reached (issue #5345).
 */
class RaftLogCompactionSchedulerTest {

  static final class FakeCompactionTarget implements RaftLogCompactionScheduler.CompactionTarget {
    final    List<Long> requestedGaps     = new ArrayList<>();
    volatile boolean    shutdownRequested = false;
    volatile boolean    failSnapshot      = false;
    volatile long       usableSpace       = 900L;
    volatile long       totalSpace        = 1000L;

    @Override
    public boolean isShutdownRequested() {
      return shutdownRequested;
    }

    @Override
    public long triggerSnapshot(final long creationGap) {
      requestedGaps.add(creationGap);
      if (failSnapshot)
        throw new IllegalStateException("snapshot request refused");
      return 42L;
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
}
