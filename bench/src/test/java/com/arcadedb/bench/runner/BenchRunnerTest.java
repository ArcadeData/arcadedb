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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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
    public void prepare(final WorkloadContext ctx, final String caseName) {
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
}
