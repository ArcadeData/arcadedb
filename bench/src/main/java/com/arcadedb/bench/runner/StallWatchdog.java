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
package com.arcadedb.bench.runner;

import java.time.Duration;
import java.util.Arrays;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Takes a thread dump of the server when the case worker is stuck: its stack has not changed (same frames) for at least the
 * threshold. One dump per stall; a new stall needs a changed stack in between. The budget of dumps is the watchdog's, and the
 * runner keeps one watchdog for the whole run, so {@code maxDumps} bounds the run rather than a case.
 * <p>
 * {@link #sample()} is called periodically by one scheduler thread; {@link #dumpsTaken()} may be read from another.
 */
public final class StallWatchdog {
  private static final StackTraceElement[] NO_FRAMES = new StackTraceElement[0];

  private final Supplier<StackTraceElement[]> workerStack;
  private final LongSupplier                  monoNanos;
  private final long                          thresholdNanos;
  private final int                           maxDumps;
  private final Function<Integer, Boolean>    dump;

  private StackTraceElement[] lastStack = NO_FRAMES;
  private long                sameSince;
  // whether the current stall has been handled (dumped, or the dump failed): only a changed stack starts a new one
  private boolean             handled;
  private int                 taken;
  private int                 withoutDump;

  /**
   * @param workerStack the current stack of the worker; an empty one (no worker, or one that ended) is never a stall
   * @param monoNanos   a monotonic clock
   * @param threshold   how long an unchanged stack must last to count as a stall
   * @param maxDumps    the most dumps this watchdog ever takes
   * @param dump        takes dump number {@code n} (1-based, counting the ones taken); false when none could be taken, which
   *                    spends nothing of the budget
   */
  public StallWatchdog(final Supplier<StackTraceElement[]> workerStack, final LongSupplier monoNanos, final Duration threshold,
      final int maxDumps, final Function<Integer, Boolean> dump) {
    this.workerStack = workerStack;
    this.monoNanos = monoNanos;
    this.thresholdNanos = threshold.toNanos();
    this.maxDumps = maxDumps;
    this.dump = dump;
  }

  /** Looks at the worker's stack once; dumps when it has been the same for the threshold and this stall has no dump yet. */
  public synchronized void sample() {
    final StackTraceElement[] stack = workerStack.get();
    final long now = monoNanos.getAsLong();
    if (stack == null || stack.length == 0) {
      forget();
      return;
    }
    if (!Arrays.equals(stack, lastStack)) {
      lastStack = stack;
      sameSince = now;
      handled = false;
      return;
    }
    if (handled || now - sameSince < thresholdNanos)
      return;
    handled = true;
    if (taken < maxDumps && Boolean.TRUE.equals(dump.apply(taken + 1)))
      taken++;
    else
      withoutDump++;
  }

  /** Forgets the stack seen so far, so the next case's worker starts with no stall; the budget is kept. */
  public synchronized void reset() {
    forget();
  }

  /** The dumps taken so far over the life of this watchdog. */
  public synchronized int dumpsTaken() {
    return taken;
  }

  /**
   * The stalls seen so far over the life of this watchdog that got no dump: the image cannot take one (native), the dump failed,
   * or the budget was spent.
   */
  public synchronized int stallsWithoutDump() {
    return withoutDump;
  }

  private void forget() {
    lastStack = NO_FRAMES;
    handled = false;
  }
}
