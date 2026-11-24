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

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class StallWatchdogTest {
  private static final Duration THRESHOLD = Duration.ofSeconds(40);

  /** A worker whose stack the test sets, a clock the test moves, and a dump function that records the numbers it was asked for. */
  private static final class Rig {
    StackTraceElement[] stack = frames("a");
    long nanos;
    long progress;
    boolean dumpSucceeds = true;
    final List<Integer> dumps = new ArrayList<>();
    final StallWatchdog watchdog = new StallWatchdog(() -> stack, () -> progress, () -> nanos, THRESHOLD, 3, n -> {
      dumps.add(n);
      return dumpSucceeds;
    });

    /** Samples every 5 s, as the runner's scheduler does, for {@code seconds} of the fake clock. */
    void sampleFor(final int seconds) {
      for (int s = 0; s < seconds; s += 5) {
        watchdog.sample();
        nanos += TimeUnit.SECONDS.toNanos(5);
      }
      watchdog.sample();
    }
  }

  private static StackTraceElement[] frames(final String method) {
    return new StackTraceElement[] { new StackTraceElement("W", method, "W.java", 1),
        new StackTraceElement("W", "run", "W.java", 2) };
  }

  @Test
  void unchangedStackFor40sDumpsOnce() {
    final Rig rig = new Rig();
    rig.sampleFor(35);
    assertThat(rig.dumps).isEmpty();
    // 40 s and well past it: still one stall, so still one dump
    rig.sampleFor(120);
    assertThat(rig.dumps).containsExactly(1);
    assertThat(rig.watchdog.dumpsTaken()).isEqualTo(1);
  }

  @Test
  void unchangedStackWithAdvancingProgressNeverDumps() {
    // a multi-threaded worker parked in Future.get for a whole repetition, or a query loop repeating the same frames
    final Rig rig = new Rig();
    for (int i = 0; i < 100; i++) {
      rig.progress++;
      rig.sampleFor(5);
    }
    assertThat(rig.dumps).isEmpty();
    assertThat(rig.watchdog.stallsWithoutDump()).isZero();
  }

  @Test
  void unchangedStackAndNoProgressDumpsOnce() {
    final Rig rig = new Rig();
    rig.progress = 17;
    rig.sampleFor(35);
    assertThat(rig.dumps).isEmpty();
    rig.sampleFor(120);
    assertThat(rig.dumps).containsExactly(1);
    assertThat(rig.watchdog.stallsWithoutDump()).isZero();
  }

  @Test
  void progressEndsAStallSoTheNextOneDumpsAgain() {
    final Rig rig = new Rig();
    rig.sampleFor(60);
    rig.progress++;
    rig.sampleFor(60);
    assertThat(rig.dumps).containsExactly(1, 2);
  }

  @Test
  void changingStackNeverDumps() {
    final Rig rig = new Rig();
    for (int i = 0; i < 100; i++) {
      rig.stack = frames("m" + (i % 2));
      rig.sampleFor(5);
    }
    assertThat(rig.dumps).isEmpty();
    assertThat(rig.watchdog.dumpsTaken()).isZero();
  }

  @Test
  void newStallAfterProgressDumpsAgain() {
    final Rig rig = new Rig();
    rig.sampleFor(60);
    rig.stack = frames("b");
    rig.sampleFor(60);
    assertThat(rig.dumps).containsExactly(1, 2);
    assertThat(rig.watchdog.dumpsTaken()).isEqualTo(2);
  }

  @Test
  void budgetOfThreeIsPerRun() {
    final Rig rig = new Rig();
    // one watchdog for the run: the stalls of five cases share the budget
    for (int c = 0; c < 5; c++) {
      rig.watchdog.reset();
      rig.stack = frames("case" + c);
      rig.sampleFor(60);
    }
    assertThat(rig.dumps).containsExactly(1, 2, 3);
    assertThat(rig.watchdog.dumpsTaken()).isEqualTo(3);
    // the stalls past the budget are still counted, as stalls without a dump
    assertThat(rig.watchdog.stallsWithoutDump()).isEqualTo(2);
  }

  @Test
  void dumpReturningFalseCountsNothing() {
    final Rig rig = new Rig();
    rig.dumpSucceeds = false;
    rig.sampleFor(60);
    assertThat(rig.dumps).containsExactly(1);
    assertThat(rig.watchdog.dumpsTaken()).isZero();
    assertThat(rig.watchdog.stallsWithoutDump()).isEqualTo(1);
    // the budget is untouched: a later stall that can be dumped gets number 1
    rig.dumpSucceeds = true;
    rig.stack = frames("b");
    rig.sampleFor(60);
    assertThat(rig.dumps).containsExactly(1, 1);
    assertThat(rig.watchdog.dumpsTaken()).isEqualTo(1);
  }

  @Test
  void emptyStackIsNoStall() {
    // a worker that has not started or has ended has no frames: that is not a stall
    final Rig rig = new Rig();
    rig.stack = new StackTraceElement[0];
    rig.sampleFor(120);
    assertThat(rig.dumps).isEmpty();
    assertThat(rig.watchdog.stallsWithoutDump()).isZero();
  }
}
