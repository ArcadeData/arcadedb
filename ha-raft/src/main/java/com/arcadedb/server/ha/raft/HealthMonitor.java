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
import org.apache.ratis.util.LifeCycle;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.logging.Level;

/**
 * Background health check that detects a Ratis server stuck in CLOSED or EXCEPTION
 * state after a network partition and triggers in-place recovery via
 * {@link HealthTarget#restartRatisIfNeeded()}.
 * <p>
 * It also detects a persistently lagging follower (issue #3893): a follower that diverged and
 * whose snapshot download failed on a quiet cluster, where no new log entry arrives to re-trigger
 * recovery. When the lag has persisted for the configured duration, it re-arms the download via
 * {@link HealthTarget#recoverFromPersistentLag()}. Disabled when {@code staleFollowerLagThreshold <= 0}.
 */
public final class HealthMonitor {

  /**
   * Minimal surface of RaftHAServer that the monitor depends on. Kept small for testing.
   */
  public interface HealthTarget {
    LifeCycle.State getRaftLifeCycleState();

    boolean isShutdownRequested();

    void restartRatisIfNeeded();

    /**
     * Returns {@code true} when this node is a follower lagging more than {@code lagThreshold}
     * entries behind the commit index while NOT actively catching up and with no snapshot
     * download already pending. Implementations must return {@code false} for the leader and
     * whenever the state cannot be determined.
     */
    default boolean isFollowerLaggingBeyond(final long lagThreshold) {
      return false;
    }

    /**
     * Re-arms a snapshot download from the leader for a persistently lagging follower.
     */
    default void recoverFromPersistentLag() {
    }
  }

  private final    HealthTarget             target;
  private final    long                     intervalMs;
  private final    long                     staleFollowerLagThreshold;
  private final    long                     staleFollowerRecoveryDurationMs;
  private volatile ScheduledExecutorService executor;
  // Wall-clock time (ms) when the current uninterrupted lag streak was first observed; -1 = not lagging.
  private          long                     lagObservedSinceMs = -1;
  // Injectable for deterministic tests; defaults to the system clock.
  private          LongSupplier             clock              = System::currentTimeMillis;

  public HealthMonitor(final HealthTarget target, final long intervalMs) {
    this(target, intervalMs, 0L, 0L);
  }

  public HealthMonitor(final HealthTarget target, final long intervalMs, final long staleFollowerLagThreshold,
      final long staleFollowerRecoveryDurationMs) {
    this.target = target;
    this.intervalMs = intervalMs;
    this.staleFollowerLagThreshold = staleFollowerLagThreshold;
    this.staleFollowerRecoveryDurationMs = staleFollowerRecoveryDurationMs;
  }

  /** Package-private test hook to drive the persistence logic deterministically. */
  void setClock(final LongSupplier clock) {
    this.clock = clock;
  }

  public void start() {
    if (intervalMs <= 0) {
      LogManager.instance().log(this, Level.FINE, "HealthMonitor disabled (interval=%d)", intervalMs);
      return;
    }
    executor = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread t = new Thread(r, "arcadedb-raft-health-monitor");
      t.setDaemon(true);
      return t;
    });
    // Startup grace period: skip the first 2*intervalMs while Ratis completes its initial election.
    executor.scheduleWithFixedDelay(this::tickSafely, intervalMs * 2, intervalMs, TimeUnit.MILLISECONDS);
  }

  public void stop() {
    final ScheduledExecutorService current = executor;
    if (current != null) {
      current.shutdownNow();
      executor = null;
    }
  }

  /**
   * Package-private for tests. Runs one check synchronously.
   */
  void tick() {
    if (target.isShutdownRequested())
      return;
    final LifeCycle.State state = target.getRaftLifeCycleState();
    if (state == LifeCycle.State.CLOSED || state == LifeCycle.State.EXCEPTION) {
      HALog.log(this, HALog.BASIC, "Health monitor detected Ratis %s state, attempting recovery", state);
      target.restartRatisIfNeeded();
      // A Ratis restart will reinitialize the state machine and re-detect any snapshot gap, so
      // drop any pending stale-follower streak to avoid a redundant download right after recovery.
      lagObservedSinceMs = -1;
      return;
    }
    checkStaleFollower();
  }

  /**
   * Re-arms a snapshot download when a follower has lagged beyond the threshold, without actively
   * catching up, for at least {@link #staleFollowerRecoveryDurationMs}. The first observation only
   * starts the streak (so a single transient tick never triggers), and any tick where the lag is
   * gone resets it (issue #3893).
   */
  private void checkStaleFollower() {
    if (staleFollowerLagThreshold <= 0)
      return; // disabled

    if (!target.isFollowerLaggingBeyond(staleFollowerLagThreshold)) {
      lagObservedSinceMs = -1;
      return;
    }

    final long now = clock.getAsLong();
    if (lagObservedSinceMs == -1) {
      lagObservedSinceMs = now; // first observation; require persistence before acting
      return;
    }

    if (now - lagObservedSinceMs >= staleFollowerRecoveryDurationMs) {
      LogManager.instance().log(this, Level.WARNING,
          "Persistent follower lag beyond threshold %d for %dms, triggering snapshot recovery",
          staleFollowerLagThreshold, now - lagObservedSinceMs);
      target.recoverFromPersistentLag();
      lagObservedSinceMs = -1; // reset; the next streak re-arms only if the lag persists again
    }
  }

  private void tickSafely() {
    try {
      tick();
    } catch (final Throwable t) {
      LogManager.instance().log(this, Level.SEVERE, "Error in HealthMonitor tick: %s", t, t.getMessage());
    }
  }
}
