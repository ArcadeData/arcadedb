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
package com.arcadedb.server.ha.raft;

import com.arcadedb.log.LogManager;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * The bounded wait {@link ArcadeStateMachine#close()} runs for every executor it stops, its own and those of the
 * helpers it owns (issues #8182 and #8364). {@code shutdownNow()} interrupts a running task and does not wait for it,
 * so a task past its last interruption point outlives the close that believed it stopped it; this is the second half
 * of that close.
 */
final class ExecutorTermination {

  private ExecutorTermination() {
  }

  /**
   * Waits, until {@code deadlineNanos}, for an executor that has already been shut down. Skipped when the caller IS
   * that executor's worker: a task closing its own owner would otherwise wait out the whole bound for the one thread
   * that cannot terminate while it waits - itself. The executor's task keeps running past the bound and is only
   * logged.
   *
   * @param logSource   the owner the log line is attributed to
   * @param worker      the executor's current worker as its thread factory recorded it, or null if it never made one
   *                    or its thread factory is not ours
   * @param threadName  the executor's thread name, for the log line only
   * @param consequence what a task still running past the bound may still be doing, for the log line only
   *
   * @return false if the caller was interrupted while waiting, for the caller to restore the flag once every wait of
   * its close is done - restoring it at once would short-circuit the waits still to come
   */
  static boolean await(final Object logSource, final ExecutorService executor, final Thread worker,
      final String threadName, final long deadlineNanos, final String consequence) {
    if (worker == Thread.currentThread())
      return true;
    // Logged as the budget THIS wait had: the waits of one close share one deadline, so a later one can get far less
    // than the whole bound.
    final long budgetNanos = Math.max(0L, deadlineNanos - System.nanoTime());
    try {
      if (!executor.awaitTermination(budgetNanos, TimeUnit.NANOSECONDS))
        LogManager.instance().log(logSource, Level.WARNING,
            "State machine closed while a task on '%s' was still running after a %d ms wait; it keeps running in the "
                + "background and may still %s", null, threadName, TimeUnit.NANOSECONDS.toMillis(budgetNanos),
            consequence);
      return true;
    } catch (final InterruptedException e) {
      return false;
    }
  }
}
