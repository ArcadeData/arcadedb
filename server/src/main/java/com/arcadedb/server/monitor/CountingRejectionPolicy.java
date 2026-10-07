/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.monitor;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The JDK's abort and caller-runs policies, plus a count of the saturations they handled, so an instance pool's
 * executor row can publish them (issue #8856): {@link PoolMetrics#statsOf(ThreadPoolExecutor)} reads a caller-runs
 * count into {@code tasks.caller_run_fallbacks}, and the owner hands {@link #getSaturations()} of an abort policy to
 * {@link PoolMetrics#bindInstancePool} as {@code tasks.rejected}.
 * <p>
 * Only a rejection by a RUNNING executor is counted. One refused because the executor was shut down is the owner
 * stopping, not the pool being undersized, and counting it would put a number on every stop that reads as a sizing
 * problem. The behaviour on such a rejection is the JDK policy's own: abort throws, caller-runs discards.
 */
public final class CountingRejectionPolicy implements RejectedExecutionHandler {
  private final boolean    callerRuns;
  private final AtomicLong saturations = new AtomicLong();

  private CountingRejectionPolicy(final boolean callerRuns) {
    this.callerRuns = callerRuns;
  }

  /** Throws {@link RejectedExecutionException}, as {@link ThreadPoolExecutor.AbortPolicy} does. */
  public static CountingRejectionPolicy abort() {
    return new CountingRejectionPolicy(false);
  }

  /**
   * Runs the task on the submitter, as {@link ThreadPoolExecutor.CallerRunsPolicy} does - including discarding it,
   * silently and uncounted, once the executor is shut down: the submitter gets no signal that its task was dropped.
   */
  public static CountingRejectionPolicy callerRuns() {
    return new CountingRejectionPolicy(true);
  }

  public boolean isCallerRuns() {
    return callerRuns;
  }

  /** Cumulative tasks a running executor could not queue: rejected (abort) or run on the submitter (caller-runs). */
  public long getSaturations() {
    return saturations.get();
  }

  @Override
  public void rejectedExecution(final Runnable task, final ThreadPoolExecutor executor) {
    final boolean running = !executor.isShutdown();
    if (running)
      saturations.incrementAndGet();

    if (!callerRuns)
      throw new RejectedExecutionException("Task " + task + " rejected from " + executor);
    if (running)
      task.run();
  }
}
