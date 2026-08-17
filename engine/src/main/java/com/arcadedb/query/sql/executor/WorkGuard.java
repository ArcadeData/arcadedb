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
package com.arcadedb.query.sql.executor;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;

/**
 * Cooperative abort check for a loop whose length is a property of the data rather than of the statement.
 * <p>
 * The point of a guard rather than a step is granularity. A checkpoint placed <em>between</em> two batches of
 * rows - which is all {@code TimeoutStep} and {@code AccumulatingTimeoutStep} can be - bounds nothing when the
 * unbounded work happens inside one batch: a filter that scans a hundred million records before yielding its
 * first row spends the whole scan inside a single {@code hasNext()}, and the enclosing step is not re-entered
 * until that call returns. A guard put in the scan loop itself is inside the unbounded thing (issues #6216,
 * #6266).
 * <p>
 * The deadline is read once from the {@link CommandContext} and then only compared, so nesting cannot extend
 * the budget: a subquery, a procedure call or a per-row expansion all share the command's single deadline. The
 * clock is read only when a deadline is actually configured, so with {@code arcadedb.command.timeout} at its
 * default of 0 a check costs one field load and one comparison, and no syscall.
 * <p>
 * {@link #forCommand(CommandContext, String)} additionally consumes a pending thread interrupt so that a
 * cancelled query stops rather than running to the end. The flag is CLEARED rather than restored, matching
 * {@code ShortestPathStep} and {@code SQLFunctionShortestPath}: the exception aborts the whole call, and
 * leaving the flag set would poison the next task to run on a pooled query thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class WorkGuard {
  /**
   * Iterations between two checks in {@link #checkPeriodically(int)}. One less than a power of two so the
   * throttle is a single AND, and small enough that the work between two checks stays well under a
   * millisecond for any realistic inner-loop body.
   */
  private static final int CHECK_INTERVAL_MASK = 1023;

  /**
   * Shared instance for the default configuration - no deadline and no interrupt check. Returned instead of a
   * fresh object so that guarding a hot loop costs nothing at all when the operator is disabled, which is the
   * case for every query on a server that has not set {@code arcadedb.command.timeout}.
   */
  private static final WorkGuard UNBOUNDED = new WorkGuard("the command", null, Long.MAX_VALUE, false);

  private final String        what;
  private final CommandContext context;
  private final long          deadline;
  private final boolean       interruptible;

  private WorkGuard(final String what, final CommandContext context, final long deadline, final boolean interruptible) {
    this.what = what;
    this.context = context;
    this.deadline = deadline;
    this.interruptible = interruptible;
  }

  /**
   * Returns a guard bound by the command's deadline (see {@link CommandContext#getCommandDeadline()}) and by a
   * thread interrupt. The deadline belongs to the command, not to this guard, so creating one per step
   * execution, per row or per nested plan does not hand the statement a fresh budget each time.
   *
   * @param context the command context the deadline is read from; {@code null} leaves only the interrupt check
   * @param what    what to name in the abort message, e.g. {@code "algo.pageRank()"}
   */
  public static WorkGuard forCommand(final CommandContext context, final String what) {
    // No shared instance here even when nothing is configured: the guard still tests the interrupt flag, and
    // it has to name the caller when it aborts.
    if (context == null)
      return new WorkGuard(what, null, Long.MAX_VALUE, true);

    return new WorkGuard(what, context, context.getCommandDeadline(), true);
  }

  /**
   * Returns a guard bound by the command's deadline only, leaving the thread interrupt flag alone. Used on the
   * generic executor paths, where a pending interrupt may have been set for a reason unrelated to this
   * statement and consuming it would change behaviour beyond the timeout this guard exists to enforce.
   */
  public static WorkGuard forCommandDeadline(final CommandContext context) {
    if (context == null)
      return UNBOUNDED;

    final long deadline = context.getCommandDeadline();
    if (deadline == Long.MAX_VALUE)
      return UNBOUNDED;

    return new WorkGuard("the command", context, deadline, false);
  }

  /**
   * Aborts the call if the query thread was interrupted or the command deadline has passed. Call from a
   * loop whose single iteration already costs enough to swallow a flag test.
   */
  public void check() {
    if (interruptible && Thread.interrupted())
      throw new CommandExecutionException(what + " has been interrupted");
    if (deadline < Long.MAX_VALUE && System.currentTimeMillis() > deadline)
      // The bound is named by the context rather than assumed to be the setting: a SQL TIMEOUT clause pins its
      // own deadline here too, and reporting the configuration's value for it would be a lie.
      throw new TimeoutException(what + " exceeded the " + context.getCommandDeadlineDescription());
  }

  /**
   * {@link #check()} throttled for a hot inner loop whose single iteration is too small to justify a flag
   * test of its own. Checking only at the enclosing loop leaves abort latency proportional to the inner
   * loop's length, and that length is data - node2vec's context window may span a whole walk, so one walk
   * alone is O(walkLength&sup2;). Testing every 1024 iterations instead bounds the latency by a fixed amount
   * of work at the cost of one AND and one branch per iteration.
   * <p>
   * "Every 1024" describes a counter that keeps climbing. The counter belongs to the caller, so a loop whose
   * counter <em>restarts</em> - the negative-sampling loop runs {@code ns} from 0 again for every
   * (position, context) pair - also tests on its first iteration every time round. That is deliberate rather
   * than a redundancy to remove: it costs one flag test per restart, and it is what keeps a small
   * {@code negSamples} responsive, since the enclosing context checkpoint fires only about once every 1024
   * positions when the window is narrow.
   *
   * @param iterationCounter the caller's loop counter; its absolute value does not matter, only that it
   *                         advances by one per iteration
   */
  public void checkPeriodically(final int iterationCounter) {
    if ((iterationCounter & CHECK_INTERVAL_MASK) == 0)
      check();
  }
}
