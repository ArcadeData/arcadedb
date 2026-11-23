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

import com.arcadedb.bench.model.CaseResult;
import com.arcadedb.bench.model.Metric;
import com.arcadedb.bench.workload.Scale;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchRunnerTest {
  private volatile boolean spin = true;

  @AfterEach
  void releaseSpinner() {
    spin = false;
  }

  private static final WorkloadContext CTX = new WorkloadContext(null, Scale.XS, 1L, new JSONObject());

  private abstract static class Fake implements Workload {
    final AtomicInteger calls     = new AtomicInteger();
    final AtomicInteger teardowns = new AtomicInteger();

    @Override
    public String name() {
      return "fake";
    }

    @Override
    public void prepare(final WorkloadContext ctx, final String caseName) throws Exception {
    }

    @Override
    public void teardown(final WorkloadContext ctx) {
      teardowns.incrementAndGet();
    }

    static Measurement m(final double v) {
      return new Measurement(List.of(new Metric("t", "ms", Metric.Better.LOWER, v, new double[] { v })));
    }
  }

  @Test
  void onCaseSeesEveryCaseInOrder() {
    final Fake w = new Fake() {
      @Override
      public List<String> cases() {
        return List.of("a", "b", "c");
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) {
        if (caseName.equals("b"))
          throw new IllegalStateException("boom");
        return m(1);
      }
    };
    final List<List<CaseResult>> seen = new ArrayList<>();
    final List<CaseResult> results = new BenchRunner(1, 0, Duration.ofSeconds(10)).run(w, CTX, null, seen::add);
    assertThat(seen).hasSize(3);
    assertThat(seen.get(0)).extracting(CaseResult::caseName).containsExactly("a");
    final List<CaseResult> last = seen.get(2);
    assertThat(last).extracting(CaseResult::caseName).containsExactly("a", "b", "c");
    assertThat(last).extracting(CaseResult::status)
        .containsExactly(CaseResult.Status.OK, CaseResult.Status.FAILED, CaseResult.Status.OK);
    assertThat(last).isEqualTo(results);
    assertThatThrownBy(() -> last.add(results.get(0))).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void warmupIsDiscarded() {
    final Fake w = new Fake() {
      @Override
      public List<String> cases() {
        return List.of("a");
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) {
        return m(calls.incrementAndGet());
      }
    };
    final List<CaseResult> results = new BenchRunner(3, 2, Duration.ofSeconds(10)).run(w, CTX, n -> true);
    assertThat(results).hasSize(1);
    final CaseResult r = results.get(0);
    assertThat(r.status()).isEqualTo(CaseResult.Status.OK);
    assertThat(r.metrics().get(0).values()).containsExactly(3, 4, 5);
    assertThat(r.metrics().get(0).median()).isEqualTo(4.0);
    assertThat(r.scale()).isEqualTo("XS");
  }

  @Test
  void failedWithMetricsKeepsMetrics() {
    final Fake w = new Fake() {
      @Override
      public List<String> cases() {
        return List.of("a");
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) throws Exception {
        throw new CaseFailedWithMetrics("recall too low", m(0.5).perRep());
      }
    };
    final List<CaseResult> results = new BenchRunner(1, 0, Duration.ofSeconds(10)).run(w, CTX, n -> true);
    assertThat(results).hasSize(1);
    assertThat(results.get(0).status()).isEqualTo(CaseResult.Status.FAILED);
    assertThat(results.get(0).error()).contains("recall too low");
    assertThat(results.get(0).metrics()).hasSize(1);
    assertThat(results.get(0).metrics().get(0).name()).isEqualTo("t");
    assertThat(results.get(0).metrics().get(0).median()).isEqualTo(0.5);
  }

  @Test
  void failingCaseIsRecordedAndRunContinues() {
    final Fake w = new Fake() {
      @Override
      public List<String> cases() {
        return List.of("a", "b");
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) {
        if (caseName.equals("a"))
          throw new OutOfMemoryError("boom");
        return m(1);
      }
    };
    final List<CaseResult> results = new BenchRunner(1, 0, Duration.ofSeconds(10)).run(w, CTX, n -> true);
    assertThat(results).hasSize(2);
    assertThat(results.get(0).caseName()).isEqualTo("a");
    assertThat(results.get(0).status()).isEqualTo(CaseResult.Status.FAILED);
    assertThat(results.get(0).error()).contains("boom");
    assertThat(results.get(1).status()).isEqualTo(CaseResult.Status.OK);
  }

  // a remote client wraps the transport failure: "Error on executing remote operation" alone says nothing of what broke
  @Test
  void failureRecordsTheWholeCauseChain() {
    final Fake w = new Fake() {
      @Override
      public List<String> cases() {
        return List.of("a");
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) {
        throw new IllegalStateException("Error on executing remote operation 'command'",
            new UncheckedIOException(new IOException("HTTP/1.1 header parser received no bytes")));
      }
    };
    final List<CaseResult> results = new BenchRunner(1, 0, Duration.ofSeconds(10)).run(w, CTX, n -> true);
    assertThat(results.get(0).status()).isEqualTo(CaseResult.Status.FAILED);
    assertThat(results.get(0).error())
        .startsWith("java.lang.IllegalStateException: Error on executing remote operation 'command'")
        .contains("caused by java.io.IOException: HTTP/1.1 header parser received no bytes");
  }

  @Test
  void timeoutIsRecorded() {
    final Fake w = new Fake() {
      @Override
      public List<String> cases() {
        return List.of("slow");
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) throws Exception {
        Thread.sleep(5_000);
        return m(1);
      }
    };
    final List<CaseResult> results = new BenchRunner(1, 0, Duration.ofMillis(200)).run(w, CTX, n -> true);
    assertThat(results).hasSize(1);
    assertThat(results.get(0).status()).isEqualTo(CaseResult.Status.TIMEOUT);
  }

  @Test
  void teardownRunsAfterFailure() {
    final Fake w = new Fake() {
      @Override
      public List<String> cases() {
        return List.of("a");
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) {
        throw new IllegalStateException("x");
      }
    };
    new BenchRunner(1, 0, Duration.ofSeconds(10)).run(w, CTX, n -> true);
    assertThat(w.teardowns.get()).isEqualTo(1);
  }

  @Test
  void timeoutDoesNotBlockNextCase() {
    final Fake w = new Fake() {
      @Override
      public List<String> cases() {
        return List.of("a", "b");
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) {
        if (caseName.equals("a"))
          while (spin)
            Thread.onSpinWait();
        else
          calls.incrementAndGet();
        return m(1);
      }
    };
    final List<CaseResult> results = new BenchRunner(1, 0, Duration.ofMillis(200), Duration.ofMillis(100)).run(w, CTX,
        n -> true);
    assertThat(results).hasSize(2);
    assertThat(results.get(0).status()).isEqualTo(CaseResult.Status.TIMEOUT);
    assertThat(results.get(0).error()).contains("teardown skipped");
    // the abandoned worker still loads the same server: a case measured next to it would record a wrong number
    assertThat(results.get(1).status()).isEqualTo(CaseResult.Status.FAILED);
    assertThat(results.get(1).error()).contains("not started").contains("'a'");
    assertThat(w.calls.get()).isZero();
    assertThat(w.teardowns.get()).isZero();
  }

  @Test
  void aCaseAfterAStoppedTimeoutStillRuns() {
    final Fake w = new Fake() {
      @Override
      public List<String> cases() {
        return List.of("slow", "next");
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) throws Exception {
        if (caseName.equals("slow"))
          Thread.sleep(5_000);
        return m(1);
      }
    };
    final List<CaseResult> results = new BenchRunner(1, 0, Duration.ofMillis(200)).run(w, CTX, n -> true);
    assertThat(results.get(0).status()).isEqualTo(CaseResult.Status.TIMEOUT);
    assertThat(results.get(1).status()).isEqualTo(CaseResult.Status.OK);
  }

  @Test
  void anInterruptedCallerStartsNoCase() {
    final Fake w = new Fake() {
      @Override
      public List<String> cases() {
        return List.of("a", "b");
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) {
        calls.incrementAndGet();
        return m(1);
      }
    };
    Thread.currentThread().interrupt();
    final List<CaseResult> results;
    try {
      results = new BenchRunner(1, 0, Duration.ofSeconds(10)).run(w, CTX, n -> true);
    } finally {
      assertThat(Thread.interrupted()).as("the interrupt status is kept for the caller").isTrue();
    }
    assertThat(results).extracting(CaseResult::status).containsExactly(CaseResult.Status.FAILED, CaseResult.Status.FAILED);
    assertThat(results.get(0).error()).contains("not started").contains("interrupted");
    assertThat(w.calls.get()).isZero();
  }

  @Test
  void noRepetitionIsRefused() {
    assertThatThrownBy(() -> new BenchRunner(0, 0, Duration.ofSeconds(1))).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("reps");
  }

  @Test
  void aNegativeWarmupIsRefused() {
    assertThatThrownBy(() -> new BenchRunner(1, -1, Duration.ofSeconds(1))).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("warmup");
  }

  @Test
  void zeroWarmupIsAllowed() {
    assertThat(new BenchRunner(1, 0, Duration.ofSeconds(1))).isNotNull();
  }

  @Test
  void teardownRunsAfterCooperativeTimeout() {
    final Fake w = new Fake() {
      @Override
      public List<String> cases() {
        return List.of("slow");
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) throws Exception {
        Thread.sleep(5_000);
        return m(1);
      }
    };
    final List<CaseResult> results = new BenchRunner(1, 0, Duration.ofMillis(200)).run(w, CTX, n -> true);
    assertThat(results.get(0).status()).isEqualTo(CaseResult.Status.TIMEOUT);
    assertThat(results.get(0).error()).doesNotContain("teardown skipped");
    assertThat(w.teardowns.get()).isEqualTo(1);
  }

  @Test
  void anAbandonedWorkerThatSwallowsItsInterruptStartsNoFurtherRepetition() throws Exception {
    final Fake w = new Fake() {
      @Override
      public List<String> cases() {
        return List.of("swallow");
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) {
        calls.incrementAndGet();
        final long until = System.nanoTime() + 400_000_000L;
        while (System.nanoTime() < until)
          if (Thread.interrupted()) // a measure that clears the flag and carries on
            Thread.onSpinWait();
        return m(1);
      }
    };
    final List<CaseResult> results = new BenchRunner(5, 0, Duration.ofMillis(100), Duration.ofMillis(10)).run(w, CTX,
        n -> true);
    assertThat(results.get(0).status()).isEqualTo(CaseResult.Status.TIMEOUT);
    Thread.sleep(1_500); // the worker's first measure ends after 400 ms; a second one would have started by now
    assertThat(w.calls.get()).isEqualTo(1);
  }

  @Test
  void anAbandonedWorkerAlsoStopsTheWorkloadsThatFollow() {
    final Fake hung = new Fake() {
      @Override
      public List<String> cases() {
        return List.of("a");
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) {
        while (spin)
          Thread.onSpinWait();
        return m(1);
      }
    };
    final Fake next = new Fake() {
      @Override
      public String name() {
        return "next";
      }

      @Override
      public List<String> cases() {
        return List.of("x");
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) {
        calls.incrementAndGet();
        return m(1);
      }
    };
    final BenchRunner runner = new BenchRunner(1, 0, Duration.ofMillis(200), Duration.ofMillis(100));
    runner.run(hung, CTX, n -> true);
    assertThat(runner.hasAbandonedWorker()).isTrue();
    final List<CaseResult> results = runner.run(next, CTX, n -> true);
    // the worker still burns host CPU: a later workload on a fresh server would be measured beside it
    assertThat(results.get(0).status()).isEqualTo(CaseResult.Status.FAILED);
    assertThat(results.get(0).error()).contains("not started").contains("'a'");
    assertThat(next.calls.get()).isZero();
  }

  // #12839: a case of 2 warm-ups + 5 reps + a data load timed out as a whole although no single build came near the cap
  @Test
  void theTimeoutAppliesToEachStepNotToTheWholeCase() {
    final Fake w = new Fake() {
      @Override
      public List<String> cases() {
        return List.of("a");
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) throws Exception {
        Thread.sleep(600);
        return m(calls.incrementAndGet());
      }
    };
    // 6 steps of 600 ms = 3.6 s in total, each one far under the 2 s step timeout
    final List<CaseResult> results = new BenchRunner(5, 1, Duration.ofSeconds(2)).run(w, CTX, n -> true);
    assertThat(results.get(0).status()).isEqualTo(CaseResult.Status.OK);
    assertThat(results.get(0).metrics().get(0).values()).hasSize(5);
  }

  @Test
  void aSlowRepetitionTimesOutAndIsNamed() {
    final Fake w = new Fake() {
      @Override
      public List<String> cases() {
        return List.of("a");
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) throws Exception {
        if (calls.incrementAndGet() == 4) // warm-up 1, then reps 1..3
          Thread.sleep(5_000);
        return m(1);
      }
    };
    final List<CaseResult> results = new BenchRunner(3, 1, Duration.ofMillis(300)).run(w, CTX, n -> true);
    assertThat(results.get(0).status()).isEqualTo(CaseResult.Status.TIMEOUT);
    assertThat(results.get(0).error()).contains("rep 3/3").contains("300 ms");
  }

  @Test
  void prepareIsBoundedByItsOwnTimeoutNotByTheStepTimeout() {
    final Fake w = new Fake() {
      @Override
      public List<String> cases() {
        return List.of("a");
      }

      @Override
      public void prepare(final WorkloadContext ctx, final String caseName) throws Exception {
        Thread.sleep(600); // a data load longer than one repetition may take
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) {
        return m(1);
      }
    };
    final List<CaseResult> results = new BenchRunner(1, 0, Duration.ofMillis(200), Duration.ofSeconds(5), Duration.ZERO,
        Duration.ofSeconds(5)).run(w, CTX, n -> true);
    assertThat(results.get(0).status()).isEqualTo(CaseResult.Status.OK);
  }

  @Test
  void aSlowPrepareTimesOutAndIsNamed() {
    final Fake w = new Fake() {
      @Override
      public List<String> cases() {
        return List.of("a");
      }

      @Override
      public void prepare(final WorkloadContext ctx, final String caseName) throws Exception {
        Thread.sleep(5_000);
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) {
        calls.incrementAndGet();
        return m(1);
      }
    };
    final List<CaseResult> results = new BenchRunner(1, 0, Duration.ofSeconds(10), Duration.ofMillis(200), Duration.ZERO,
        Duration.ofSeconds(5)).run(w, CTX, n -> true);
    assertThat(results.get(0).status()).isEqualTo(CaseResult.Status.TIMEOUT);
    assertThat(results.get(0).error()).contains("prepare").contains("200 ms");
    assertThat(w.calls.get()).isZero();
  }

  @Test
  void theCaseCapEndsACaseWhoseStepsEachFit() {
    final Fake w = new Fake() {
      @Override
      public List<String> cases() {
        return List.of("a");
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) throws Exception {
        calls.incrementAndGet();
        Thread.sleep(300);
        return m(1);
      }
    };
    final List<CaseResult> results = new BenchRunner(20, 0, Duration.ofSeconds(10), Duration.ofSeconds(10), Duration.ofSeconds(1),
        Duration.ofSeconds(5)).run(w, CTX, n -> true);
    assertThat(results.get(0).status()).isEqualTo(CaseResult.Status.TIMEOUT);
    assertThat(results.get(0).error()).contains("case exceeded 1000 ms");
    assertThat(w.calls.get()).isLessThan(20);
  }

  @Test
  void aNegativeCaseCapIsRefused() {
    assertThatThrownBy(() -> new BenchRunner(1, 0, Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofSeconds(-1)))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("case timeout");
  }

  /** A runner whose clocks move by the given amounts between the start and the end of a case. */
  private static List<CaseResult> runWithClockMoves(final long wallMillis, final long monoNanos, final boolean fail) {
    final Fake w = new Fake() {
      @Override
      public List<String> cases() {
        return List.of("a");
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) {
        if (fail)
          throw new IllegalStateException("boom");
        return m(1);
      }
    };
    final AtomicLong wall = new AtomicLong(1_000_000L);
    final AtomicLong mono = new AtomicLong(0L);
    final AtomicInteger wallReads = new AtomicInteger();
    final AtomicInteger monoReads = new AtomicInteger();
    // the first read is the case start, the second the case end
    final BenchRunner.ClockPair clocks = new BenchRunner.ClockPair(
        () -> wallReads.getAndIncrement() == 0 ? wall.get() : wall.addAndGet(wallMillis),
        () -> monoReads.getAndIncrement() == 0 ? mono.get() : mono.addAndGet(monoNanos));
    return new BenchRunner(1, 0, Duration.ofSeconds(10), Duration.ofSeconds(10), Duration.ZERO, Duration.ofSeconds(5), clocks)
        .run(w, CTX, null);
  }

  @Test
  void clockJumpMarksHostNotQuiet() {
    final CaseResult r = runWithClockMoves(5_000, 1_000_000_000L, false).get(0);
    assertThat(r.notes().getString("hostNotQuiet", "")).contains("4000 ms");
  }

  @Test
  void smallDriftIsQuiet() {
    final CaseResult r = runWithClockMoves(1_900, 1_000_000_000L, false).get(0);
    assertThat(r.notes().has("hostNotQuiet")).isFalse();
  }

  @Test
  void noteDoesNotChangeStatus() {
    assertThat(runWithClockMoves(5_000, 1_000_000_000L, false).get(0).status()).isEqualTo(CaseResult.Status.OK);
  }

  @Test
  void theNoteIsAddedToAFailedCaseToo() {
    final CaseResult r = runWithClockMoves(5_000, 1_000_000_000L, true).get(0);
    assertThat(r.status()).isEqualTo(CaseResult.Status.FAILED);
    assertThat(r.notes().has("hostNotQuiet")).isTrue();
  }
}
