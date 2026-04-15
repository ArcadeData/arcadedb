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
import java.util.logging.Level;

/**
 * Background health check that detects a Ratis server stuck in CLOSED or EXCEPTION
 * state after a network partition and triggers in-place recovery via
 * {@link HealthTarget#restartRatisIfNeeded()}.
 */
public final class HealthMonitor {

  /**
   * Minimal surface of RaftHAServer that the monitor depends on. Kept small for testing.
   */
  public interface HealthTarget {
    LifeCycle.State getRaftLifeCycleState();

    boolean isShutdownRequested();

    void restartRatisIfNeeded();
  }

  private final    HealthTarget             target;
  private final    long                     intervalMs;
  private volatile ScheduledExecutorService executor;

  public HealthMonitor(final HealthTarget target, final long intervalMs) {
    this.target = target;
    this.intervalMs = intervalMs;
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
