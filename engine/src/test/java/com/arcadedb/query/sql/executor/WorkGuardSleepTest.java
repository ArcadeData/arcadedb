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

import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.RunningQueryRegistry;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link WorkGuard#sleep(long)}: a wait that takes the same time as {@link Thread#sleep(long)} when nothing bounds it, and ends
 * with the deadline or the termination of the command when something does.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
// A hang detector, not a latency bound
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class WorkGuardSleepTest {
  /** A tripwire between a wait that stops with its command and one that runs to the end of a duration this long. */
  private static final long TRIPWIRE_MS = 10_000;

  @Test
  void aDurationThatIsNotPositiveIsNoWait() {
    final WorkGuard unbounded = WorkGuard.forCommandDeadline(null);
    final WorkGuard bounded = WorkGuard.forDeadline("the wait", System.currentTimeMillis() + 60_000);

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    assertThatCode(() -> {
      for (final WorkGuard guard : new WorkGuard[] { unbounded, bounded })
        for (final long millis : new long[] { 0, -1, Long.MIN_VALUE })
          guard.sleep(millis);
    }).doesNotThrowAnyException();
    watch.assertGaveUpWithin(TRIPWIRE_MS, "a wait that is not made from one that is");
  }

  @Test
  void aGuardWithNothingToCheckWaitsTheWholeDuration() throws Exception {
    final long begin = System.nanoTime();
    WorkGuard.forCommandDeadline(null).sleep(200);
    // A lower bound, on the monotonic clock: a stall only makes it more true
    assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin)).isGreaterThanOrEqualTo(200L);
  }

  @Test
  void aBoundedGuardWaitsTheWholeDurationWhenTheBoundIsFarAway() throws Exception {
    final WorkGuard guard = WorkGuard.forDeadline("the wait", System.currentTimeMillis() + 60_000);

    // More than two slices, so the wait is made of several
    final long begin = System.nanoTime();
    guard.sleep(350);
    assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin)).isGreaterThanOrEqualTo(350L);
  }

  @Test
  void aDurationOfAnySizeEndsWithTheDeadline() {
    final WorkGuard guard = WorkGuard.forDeadline("the wait", System.currentTimeMillis() + 300);

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> guard.sleep(Long.MAX_VALUE)).isInstanceOf(TimeoutException.class);
    watch.assertGaveUpWithin(TRIPWIRE_MS, "a wait that stops at the deadline from one that never ends");
  }

  @Test
  void aDeadlineThatHasPassedEndsTheWaitBeforeItStarts() {
    final WorkGuard guard = WorkGuard.forDeadline("the wait", System.currentTimeMillis() - 1);

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> guard.sleep(60_000)).isInstanceOf(TimeoutException.class);
    watch.assertGaveUpWithin(TRIPWIRE_MS, "a wait that is refused at once from one that runs to its end");
  }

  @Test
  void aTerminatedStatementEndsTheWaitBeforeItStarts() {
    final RunningQueryRegistry registry = new RunningQueryRegistry();
    try (final RunningQuery statement = registry.register("db", "admin", "test", null, null, RunningQuery.Client.NONE)) {
      final WorkGuard guard = WorkGuard.forCommandDeadline(null);
      statement.terminate("root");

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> guard.sleep(60_000)).isInstanceOf(QueryTerminatedException.class);
      watch.assertGaveUpWithin(TRIPWIRE_MS, "a wait that is refused at once from one that runs to its end");
    }
  }

  @Test
  void anInterruptEndsTheWaitAsItDoesForThreadSleep() {
    final WorkGuard unbounded = WorkGuard.forCommandDeadline(null);
    final WorkGuard bounded = WorkGuard.forDeadline("the wait", System.currentTimeMillis() + 60_000);

    for (final WorkGuard guard : new WorkGuard[] { unbounded, bounded }) {
      Thread.currentThread().interrupt();
      try {
        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        assertThatThrownBy(() -> guard.sleep(60_000)).isInstanceOf(InterruptedException.class);
        watch.assertGaveUpWithin(TRIPWIRE_MS, "a wait that stops at an interrupt from one that runs to its end");
      } finally {
        // InterruptedException clears the flag already: this only keeps a failed assertion from leaking it to the next test
        Thread.interrupted();
      }
    }
  }
}
