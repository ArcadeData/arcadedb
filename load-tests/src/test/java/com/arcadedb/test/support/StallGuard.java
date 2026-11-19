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
package com.arcadedb.test.support;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Backs off, and eventually gives up, a load-test loop that has to wait for rows another thread is still inserting.
 * <p>
 * The loops that draw ids from a {@link TypeIdSupplier} start together with the writers that fill the tables, so the first answers are
 * {@code null} by design. Retrying at once re-issues the {@code SELECT ... SKIP ? LIMIT ?} in a tight loop while the load is being
 * measured, and a supply that never comes (more likes asked for than users ever created) used to spin until the class timeout.
 * Call {@link #stall()} each time the supply has nothing: it sleeps one poll interval and, once the wait has lasted longer than the
 * timeout without a {@link #progress()} in between, fails with the reason. Not thread safe: one guard per consuming loop.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class StallGuard {

  @FunctionalInterface
  interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  private final String       what;
  private final long         timeoutMs;
  private final long         timeoutNanos;
  private final long         pollMs;
  private final LongSupplier nanoClock;
  private final Sleeper      sleeper;
  private       boolean      stalled;
  private       long         stalledSinceNanos;

  /**
   * @param what      what the loop is waiting for, as the failure message words it
   * @param timeoutMs how long the loop may wait without progress before it fails
   * @param pollMs    how long each {@link #stall()} sleeps
   */
  StallGuard(final String what, final long timeoutMs, final long pollMs) {
    this(what, timeoutMs, pollMs, System::nanoTime, Thread::sleep);
  }

  StallGuard(final String what, final long timeoutMs, final long pollMs, final LongSupplier nanoClock, final Sleeper sleeper) {
    this.what = what;
    this.timeoutMs = timeoutMs;
    this.timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    this.pollMs = pollMs;
    this.nanoClock = nanoClock;
    this.sleeper = sleeper;
  }

  /** The loop got what it was waiting for: the next stall starts a new wait. */
  void progress() {
    stalled = false;
  }

  /**
   * The loop has nothing to work on: pauses one poll interval.
   *
   * @throws IllegalStateException when the wait has lasted longer than the timeout, or the thread was interrupted (the flag is kept)
   */
  void stall() {
    final long now = nanoClock.getAsLong();
    if (!stalled) {
      stalled = true;
      stalledSinceNanos = now;
    } else if (now - stalledSinceNanos >= timeoutNanos)
      throw new IllegalStateException(
          "Gave up after waiting " + timeoutMs + " ms for " + what + ": the table stayed empty, or every id it holds was already used");

    try {
      sleeper.sleep(pollMs);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for " + what, e);
    }
  }
}
