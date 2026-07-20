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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.logging.Level;

/**
 * Periodic Raft snapshot trigger that keeps the segmented Raft log bounded in time rather than in
 * entry count.
 * <p>
 * Ratis purges log segments only up to the latest snapshot index, and its built-in auto-snapshot is
 * driven purely by a count of applied entries ({@code arcadedb.ha.snapshotThreshold}). On a low or
 * moderate write cluster that count is effectively never reached between process restarts, so the
 * snapshot index stays frozen at the value captured at startup, nothing is purged, and the log grows
 * monotonically until the volume is full. Once the log writer hits {@code No space left on device},
 * Ratis marks the log permanently failed at that index and the node rejects every subsequent
 * {@code APPEND_ENTRIES} until it is restarted.
 * <p>
 * ArcadeDB's snapshot is a zero-byte marker file - the database files on disk are already the
 * durable state, see {@link ArcadeStateMachine#takeSnapshot()} - so triggering one is cheap enough
 * to do on a wall-clock schedule. Each tick asks the <b>local</b> Ratis server to create a snapshot;
 * this runs on leader and followers alike because every Ratis server purges its own log against its
 * own snapshot index.
 * <p>
 * A tick carries a creation gap: Ratis completes the request without doing any work when fewer than
 * that many entries have been applied since the last snapshot, so an idle cluster costs nothing.
 * When free space on the Raft storage volume drops below the configured percentage the gap is
 * lowered to 1, purging as aggressively as Ratis allows, and a throttled WARNING surfaces the
 * condition before the disk fills.
 * <p>
 * <b>Note on pool metrics:</b> this class owns a single-thread scheduled executor, but it is
 * deliberately not registered with the engine's {@code PoolMetrics} binder. That binder covers pools
 * that do query or storage work whose saturation an operator must see; this one runs one bounded task
 * every few minutes with no queue to back up, alongside the other HA housekeeping timers
 * ({@link HealthMonitor}, the lag monitor), none of which are surfaced there either.
 */
public final class RaftLogCompactionScheduler {

  /**
   * Minimal surface of {@link RaftHAServer} the scheduler depends on. Kept small for testing.
   */
  public interface CompactionTarget {
    boolean isShutdownRequested();

    /**
     * Requests a snapshot of the local Ratis server, skipped by Ratis when fewer than
     * {@code creationGap} entries have been applied since the last snapshot.
     *
     * @return the resulting snapshot index, or a negative value when no snapshot was taken
     */
    long triggerSnapshot(long creationGap);

    /**
     * Free bytes on the volume hosting the Raft storage directory, or a non-positive value when it
     * cannot be determined.
     */
    long getRaftStorageUsableSpaceBytes();

    /**
     * Total bytes of the volume hosting the Raft storage directory, or a non-positive value when it
     * cannot be determined.
     */
    long getRaftStorageTotalSpaceBytes();

    /** Human-readable Raft storage location, used only in log messages. */
    String getRaftStorageDescription();
  }

  /** Creation gap used once the Raft storage volume is under disk pressure: purge as far as possible. */
  static final long DISK_PRESSURE_CREATION_GAP = 1L;

  /** At most one disk-pressure WARNING per this window, matching the engine-wide saturation convention. */
  static final long DISK_WARNING_THROTTLE_MS = 60_000L;

  private final    CompactionTarget         target;
  private final    long                     intervalMs;
  // Creation gap for a normal tick. Clamped to >= 1 because Ratis reads a gap of 0 as "use the
  // configured default", which would silently reinstate the count-based-only behaviour this fixes.
  private final    long                     minEntries;
  private final    int                      minFreeSpacePerc;
  private volatile ScheduledExecutorService executor;
  private volatile boolean                  underDiskPressure = false;
  // Wall-clock time (ms) of the last emitted disk-pressure WARNING; -1 = never warned.
  private          long                     lastDiskWarningMs = -1L;
  // Highest snapshot index observed so far, so an idle tick (which Ratis answers with the unchanged
  // index) is not reported as a compaction. Read/written only on the single scheduler thread.
  private          long                     lastSnapshotIndex = -1L;
  // Whether any tick has completed yet. The first tick only records the index it observes: after a
  // RECOVER restart Ratis already holds a snapshot at some index N, and reporting that pre-existing N
  // as "compacted at index N" would be a phantom compaction line for work this scheduler never did.
  private          boolean                  firstTickObserved = false;
  // Whether a real compaction (an index advance after the baseline tick) has ever been reported.
  private          boolean                  reportedCompaction = false;
  // Injectable for deterministic tests; defaults to the system clock.
  private          LongSupplier             clock             = System::currentTimeMillis;

  public RaftLogCompactionScheduler(final CompactionTarget target, final long intervalMs, final long minEntries,
      final int minFreeSpacePerc) {
    this.target = target;
    this.intervalMs = intervalMs;
    this.minEntries = Math.max(1L, minEntries);
    this.minFreeSpacePerc = minFreeSpacePerc;
  }

  /** Package-private test hook to drive the warning throttle deterministically. */
  void setClock(final LongSupplier clock) {
    this.clock = clock;
  }

  public void start() {
    if (intervalMs <= 0) {
      LogManager.instance().log(this, Level.FINE,
          "Raft log compaction scheduler disabled (arcadedb.ha.snapshotInterval=%d)", intervalMs);
      return;
    }
    executor = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread t = new Thread(r, "arcadedb-raft-log-compaction");
      t.setDaemon(true);
      return t;
    });
    // Start one full interval in so the first tick does not race Ratis's initial election and log replay.
    executor.scheduleWithFixedDelay(this::tickSafely, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    LogManager.instance().log(this, Level.FINE,
        "Raft log compaction scheduler started (interval=%dms, minEntries=%d, minFreeSpacePerc=%d%%)",
        intervalMs, minEntries, minFreeSpacePerc);
  }

  public void stop() {
    final ScheduledExecutorService current = executor;
    if (current != null) {
      current.shutdownNow();
      executor = null;
    }
  }

  /** Visible for tests: whether the periodic task is scheduled. */
  boolean isRunning() {
    return executor != null;
  }

  /** Visible for tests: whether the last tick observed the Raft storage volume under disk pressure. */
  boolean isUnderDiskPressure() {
    return underDiskPressure;
  }

  /** Visible for tests: highest snapshot index observed, or -1 before the first tick. */
  long getLastSnapshotIndex() {
    return lastSnapshotIndex;
  }

  /** Visible for tests: whether a real compaction (an advance past the baseline) was ever reported. */
  boolean hasReportedCompaction() {
    return reportedCompaction;
  }

  /**
   * Wrapper that swallows every throwable. A {@code scheduleWithFixedDelay} task that throws is
   * silently cancelled, which would turn a single transient snapshot refusal into the permanent loss
   * of log compaction - exactly the failure mode this class exists to prevent.
   */
  void tickSafely() {
    try {
      tick();
    } catch (final Throwable t) {
      LogManager.instance().log(this, Level.WARNING,
          "Raft log compaction tick failed: %s", t.getMessage());
    }
  }

  /**
   * Package-private for tests. Runs one compaction check synchronously.
   */
  void tick() {
    if (target.isShutdownRequested())
      return;

    final boolean pressure = evaluateDiskPressure();
    final long creationGap = pressure ? DISK_PRESSURE_CREATION_GAP : minEntries;

    final long snapshotIndex = target.triggerSnapshot(creationGap);
    // Ratis answers a short-circuited (below the creation gap) request with a SUCCESS reply carrying the
    // existing snapshot index, so a bare "index >= 0" check would announce a compaction on every idle
    // tick. Only report when the index actually moved, which is the only case where segments became
    // purgeable.
    if (snapshotIndex > lastSnapshotIndex) {
      lastSnapshotIndex = snapshotIndex;
      if (firstTickObserved) {
        reportedCompaction = true;
        HALog.log(this, HALog.BASIC, "Raft log compaction: snapshot at index %d (creationGap=%d)", snapshotIndex,
            creationGap);
      }
    }
    firstTickObserved = true;
  }

  /**
   * Recomputes the disk-pressure state from the current free/total bytes of the Raft storage volume
   * and emits a throttled WARNING while the condition holds.
   *
   * @return {@code true} when free space is strictly below the configured percentage
   */
  private boolean evaluateDiskPressure() {
    if (minFreeSpacePerc <= 0) {
      underDiskPressure = false;
      return false;
    }

    final long total = target.getRaftStorageTotalSpaceBytes();
    final long usable = target.getRaftStorageUsableSpaceBytes();
    if (total <= 0 || usable < 0) {
      // Volume size unknown (e.g. the directory does not exist yet): never guess pressure from it.
      underDiskPressure = false;
      return false;
    }

    // Double arithmetic rather than long products: both usable*100 and total*minFreeSpacePerc can
    // overflow independently on very large volumes, and a wrapped negative product would silently
    // under-report pressure. A double's 53-bit mantissa is far more precision than a free-space
    // percentage comparison needs.
    final boolean pressure = (double) usable / (double) total * 100.0 < minFreeSpacePerc;
    underDiskPressure = pressure;

    if (pressure && shouldWarnAboutDiskPressure())
      LogManager.instance().log(this, Level.WARNING,
          "Raft storage '%s' is low on space: %d of %d bytes free (below %d%%). Forcing snapshot and log purge. "
              + "Raft storage must be sized for the log, not just the databases - see arcadedb.ha.snapshotInterval "
              + "and arcadedb.ha.snapshotThreshold",
          target.getRaftStorageDescription(), usable, total, minFreeSpacePerc);

    return pressure;
  }

  /**
   * Package-private for tests. Returns {@code true} at most once per {@link #DISK_WARNING_THROTTLE_MS}
   * window, recording the emission time when it does.
   */
  boolean shouldWarnAboutDiskPressure() {
    final long now = clock.getAsLong();
    if (lastDiskWarningMs >= 0 && now - lastDiskWarningMs < DISK_WARNING_THROTTLE_MS)
      return false;
    lastDiskWarningMs = now;
    return true;
  }
}
