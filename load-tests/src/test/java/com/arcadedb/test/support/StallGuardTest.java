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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11653: {@code createLike} spun with no sleep and no bound once an id supplier answered {@code null}. The clock and the sleep
 * of {@link StallGuard} are injected, so these tests run on a fake clock that only the fake sleep advances: nothing here depends on how
 * long the machine takes, and no test sleeps.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class StallGuardTest {
  private       long       nowNanos;
  private final List<Long> sleeps = new ArrayList<>();

  private StallGuard guard(final long timeoutMs, final long pollMs) {
    return new StallGuard("a User id", timeoutMs, pollMs, () -> nowNanos, millis -> {
      sleeps.add(millis);
      nowNanos += TimeUnit.MILLISECONDS.toNanos(millis);
    });
  }

  /** The consumer loop of {@code createLike}: draw from the supplier, backing off through the guard while it has nothing. */
  private static Integer drawWhenAvailable(final Supplier<Integer> supplier, final StallGuard guard) {
    Integer id;
    while ((id = supplier.get()) == null)
      guard.stall();
    guard.progress();
    return id;
  }

  @Test
  void aSupplierThatFillsUpLaterIsWaitedForNotSpunOn() {
    final AtomicInteger calls = new AtomicInteger();
    final Supplier<Integer> supplier = () -> calls.incrementAndGet() <= 3 ? null : 42;

    final Integer id = drawWhenAvailable(supplier, guard(60_000, 250));

    assertThat(id).isEqualTo(42);
    assertThat(calls).hasValue(4);
    // one pause per empty answer, each of the poll interval: the loop does not re-query the server back to back
    assertThat(sleeps).containsExactly(250L, 250L, 250L);
  }

  @Test
  void aSupplierThatNeverFillsUpFailsWithTheReasonInsteadOfSpinningUntilTheClassTimeout() {
    final AtomicInteger calls = new AtomicInteger();
    final Supplier<Integer> supplier = () -> {
      calls.incrementAndGet();
      return null;
    };

    assertThatThrownBy(() -> drawWhenAvailable(supplier, guard(1_000, 250)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("a User id")
        .hasMessageContaining("1000 ms");

    // 1000 ms at one pause per 250 ms: four pauses, five questions, and then it stops asking
    assertThat(calls).hasValue(5);
    assertThat(sleeps).hasSize(4);
  }

  @Test
  void progressStartsTheWaitAgain() {
    final StallGuard guard = guard(1_000, 250);

    guard.stall();
    guard.stall();
    guard.stall();
    guard.progress();

    // had progress() not reset the window, the fifth stall overall would be the one that fails
    for (int i = 0; i < 4; i++)
      guard.stall();
    assertThat(sleeps).hasSize(7);

    assertThatThrownBy(guard::stall).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void anInterruptStopsTheWaitAndKeepsTheFlag() {
    final StallGuard guard = new StallGuard("a Photo id", 60_000, 250, () -> nowNanos, millis -> {
      throw new InterruptedException();
    });

    try {
      assertThatThrownBy(guard::stall)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Interrupted")
          .hasMessageContaining("a Photo id")
          .hasCauseInstanceOf(InterruptedException.class);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      // do not leak the flag into the next test of this thread
      Thread.interrupted();
    }
  }
}
