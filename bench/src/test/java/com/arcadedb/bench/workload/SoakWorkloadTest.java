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
package com.arcadedb.bench.workload;

import com.arcadedb.bench.model.Metric;
import com.arcadedb.bench.runner.BenchRunner;
import com.arcadedb.bench.runner.CaseFailedWithMetrics;
import com.arcadedb.bench.runner.LatencyRecorder;
import com.arcadedb.bench.runner.Measurement;
import com.arcadedb.bench.workload.mixed.MixedPhase;
import com.arcadedb.bench.workload.soak.SoakSampler;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

class SoakWorkloadTest {
  private static final double FLOOR  = 500_000_000.0;
  private static final long   SECOND = 1_000_000_000L;

  /** {@code n} pairs a 5-minute window apart, {@code base + perHour * hour}, plus a wobble of {@code noise} either way. */
  private static double[][] series(final int n, final double windowHours, final double base, final double perHour, final double noise) {
    final double[][] pairs = new double[n][];
    for (int i = 0; i < n; i++) {
      final double hour = (i + 0.5) * windowHours;
      pairs[i] = new double[] { hour, base + perHour * hour + ((i & 1) == 0 ? noise : -noise) };
    }
    return pairs;
  }

  private static double[][] floors(final double perHour) {
    return series(24, 5 / 60.0, FLOOR, perHour, FLOOR * 0.002);
  }

  private static double[][] perMinute(final double base, final double perHour) {
    return series(120, 1 / 60.0, base, perHour, 1);
  }

  /**
   * A 2 h soak sampled every 10 s, warm-up 15 min, floor windows 5 min: heap, threads and files rise by {@code warmRise} of
   * their base over the warm-up and by {@code lateRise} of it over what follows, linearly, with a small sawtooth on the heap.
   */
  private static SoakSampler soak(final double warmRise, final double lateRise, final int minutes) {
    final int n = minutes * 6;
    final long[] offsets = new long[n];
    final long[] heaps = new long[n];
    final int[] threads = new int[n];
    final int[] files = new int[n];
    final double warmEnd = 15 * 60;
    final double total = 120 * 60;
    for (int i = 0; i < n; i++) {
      final double t = i * 10;
      final double rise = t < warmEnd ? warmRise * t / warmEnd : warmRise + lateRise * (t - warmEnd) / (total - warmEnd);
      offsets[i] = (long) (t * SECOND);
      heaps[i] = (long) (FLOOR * (1 + rise) + (i % 5) * FLOOR * 0.01);
      threads[i] = (int) Math.round(100 * (1 + rise));
      files[i] = (int) Math.round(200 * (1 + rise));
    }
    return SoakSampler.replay(Duration.ofSeconds(10), Duration.ofMinutes(5), SoakWorkload.WARMUP, SoakWorkload.WINDOW, offsets, heaps,
        threads, files);
  }

  @Test
  void aRiseDuringTheWarmUpOnlyPasses() throws Exception {
    // the server grows to its working size in the first minutes (threads 100 -> 150, files 200 -> 300, heap x1.5), then holds
    final Measurement measurement = SoakWorkload.judge(latencies(), soak(0.5, 0, 120), List.of());
    assertThat(measurement.perRep()).extracting(Metric::name)
        .contains("heap_floor_slope_per_hour", "thread_slope_per_hour", "open_files_slope_per_hour");
  }

  @Test
  void theSameRiseAfterTheWarmUpStillFails() {
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> SoakWorkload.judge(latencies(), soak(0, 0.5, 120), List.of()));
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).contains("heap_floor_slope_per_hour").contains("thread_slope_per_hour")
        .contains("open_files_slope_per_hour");
  }

  @Test
  void aSoakCutShortAfterItsWarmUpFailsForTooFewSamples() {
    // 40 minutes: 25 after the warm-up, 5 heap floor windows of 5 minutes, one short of a slope
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> SoakWorkload.judge(latencies(), soak(0.5, 0, 40), List.of()));
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).contains("heap_floor_slope_per_hour: too few samples, 5 heap floors");
  }

  /** A window of the mixed phase that ran each loop once, with {@code failures}. */
  private static MixedPhase.Result window(final String... failures) {
    final Map<String, LatencyRecorder> latencies = new LinkedHashMap<>();
    for (final String loop : MixedPhase.LOOPS) {
      final LatencyRecorder recorder = new LatencyRecorder(16);
      recorder.record(5_000_000L);
      latencies.put(loop, recorder);
    }
    return new MixedPhase.Result(latencies, 1_000_000L, MixedPhase.LOOPS.size(), 1.0, List.of(failures));
  }

  /** A phase whose window returns {@code result} (running {@code during} first) and whose check throws {@code checkFailure}. */
  private static SoakWorkload.Phase phase(final MixedPhase.Result result, final Runnable during, final String checkFailure,
      final AtomicInteger checks) {
    return new SoakWorkload.Phase() {
      @Override
      public MixedPhase.Result run(final Duration window) {
        during.run();
        return result;
      }

      @Override
      public void check() {
        checks.incrementAndGet();
        if (checkFailure != null)
          throw new IllegalStateException(checkFailure);
      }
    };
  }

  @Test
  void aCancelledSoakStillPublishesItsMetricsAndKeepsTheInterrupt() {
    final AtomicInteger checks = new AtomicInteger();
    // the runner's deadline: the window returns early with the interrupt set, as MixedPhase.run does
    final SoakWorkload.Phase cancelled = phase(window("window: interrupted after 5000 ms"), () -> Thread.currentThread().interrupt(),
        null, checks);
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> SoakWorkload.soak(cancelled, soak(0, 0, 120), SoakWorkload.WINDOW));
    final boolean kept = Thread.interrupted();
    assertThat(e).isNotNull();
    assertThat(kept).isTrue();
    assertThat(e.getMessage()).contains("window: interrupted");
    assertThat(e.metrics()).extracting(Metric::name)
        .contains("ingest_p99_ms", "max_commit_gap_ms", "heap_floor_slope_per_hour", "thread_slope_per_hour", "open_files_slope_per_hour");
    assertThat(checks).hasValue(0);
  }

  @Test
  void theCheckRunsOnlyAfterACleanWindow() {
    final AtomicInteger checks = new AtomicInteger();
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> SoakWorkload.soak(phase(window("ddl: boom"), () -> {
        }, "the expected counts were lost", checks), soak(0, 0, 120), SoakWorkload.WINDOW));
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).isEqualTo("ddl: boom");
    assertThat(checks).hasValue(0);
  }

  @Test
  void aFailedCheckAfterACleanWindowFailsTheCaseWithItsMessage() {
    final AtomicInteger checks = new AtomicInteger();
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> SoakWorkload.soak(phase(window(), () -> {
        }, "MixDoc holds 9 documents, expected 10", checks), soak(0, 0, 120), SoakWorkload.WINDOW));
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).isEqualTo("MixDoc holds 9 documents, expected 10");
    assertThat(checks).hasValue(1);
  }

  @Test
  void aCleanSoakPassesItsCheck() throws Exception {
    final AtomicInteger checks = new AtomicInteger();
    final Measurement measurement = SoakWorkload.soak(phase(window(), () -> {
    }, null, checks), soak(0, 0, 120), SoakWorkload.WINDOW);
    assertThat(checks).hasValue(1);
    assertThat(measurement.perRep()).extracting(Metric::name).contains("ingest_p99_ms", "heap_floor_slope_per_hour");
  }

  @Test
  void aHeapFloorOfZeroFailsTheCase() {
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> SoakWorkload.judge(latencies(), series(24, 5 / 60.0, 0, 0, 0), perMinute(80, 0), perMinute(300, 0), List.of()));
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).contains("heap_floor_slope_per_hour: no heap floor above zero");
    assertThat(e.metrics()).extracting(Metric::name).doesNotContain("heap_floor_slope_per_hour");
  }

  private static List<Metric> latencies() {
    final List<Metric> metrics = new ArrayList<>();
    metrics.add(Metrics.of("ingest_p99_ms", "ms", Metric.Better.LOWER, 12));
    return metrics;
  }

  private static Metric metric(final List<Metric> metrics, final String name) {
    return metrics.stream().filter(m -> m.name().equals(name)).findFirst().orElseThrow();
  }

  @Test
  void runsOnlyOnSaturdayLaneScaleAndTriggers() {
    final SoakWorkload soak = new SoakWorkload();
    assertThat(soak.name()).isEqualTo("soak");
    assertThat(soak.cases()).containsExactly("mixed-2h");
    assertThat(soak.cases(Scale.XS)).isEmpty();
    assertThat(soak.cases(Scale.S)).isEmpty();
    assertThat(soak.cases(Scale.M)).containsExactly("mixed-2h");
    assertThat(soak.cases(Scale.L)).isEmpty();
    assertThat(soak.runsOn("release")).isFalse();
    assertThat(soak.runsOn("nightly")).isTrue();
    assertThat(soak.runsOn("manual")).isTrue();
    assertThat(soak.reps()).hasValue(1);
    assertThat(soak.warmup()).hasValue(0);
    assertThat(soak.stepLimit()).contains(Duration.ofMinutes(150));
    assertThat(SoakWorkload.WINDOW).isEqualTo(Duration.ofMinutes(120));
    assertThat(SoakWorkload.WARMUP).isEqualTo(Duration.ofMinutes(15));
    // the run's 30 min step and 120 min case cap would cut the soak: it gets its own step, and a case cap that holds it
    final BenchRunner runner = new BenchRunner(3, 1, Duration.ofMinutes(30), Duration.ofMinutes(30), Duration.ofMinutes(120));
    assertThat(runner.stepLimitOf(soak)).isEqualTo(Duration.ofMinutes(150));
    assertThat(runner.caseLimitOf(soak)).isEqualTo(Duration.ofMinutes(180));
  }

  @Test
  void aGrowingHeapFailsTheCase() {
    final List<Metric> metrics = latencies();
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> SoakWorkload.judge(metrics, floors(FLOOR * 0.10), perMinute(80, 0), perMinute(300, 0), List.of()));
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).contains("heap_floor_slope_per_hour").doesNotContain("thread_slope_per_hour")
        .doesNotContain("open_files_slope_per_hour");
    // the slope is relative to the mean floor: about 0.10 per hour
    final Metric heap = metric(e.metrics(), "heap_floor_slope_per_hour");
    assertThat(heap.unit()).isEqualTo("per_hour");
    assertThat(heap.median()).isBetween(0.08, 0.12);
    assertThat(e.metrics()).extracting(Metric::name).contains("ingest_p99_ms", "thread_slope_per_hour", "open_files_slope_per_hour");
  }

  @Test
  void aThreadLeakFailsTheCase() {
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> SoakWorkload.judge(latencies(), floors(0), perMinute(80, 12), perMinute(300, 0), List.of()));
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).contains("thread_slope_per_hour").doesNotContain("heap_floor_slope_per_hour");
    assertThat(metric(e.metrics(), "thread_slope_per_hour").median()).isBetween(11.0, 13.0);
  }

  @Test
  void anOpenFileLeakFailsTheCase() {
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> SoakWorkload.judge(latencies(), floors(0), perMinute(80, 0), perMinute(300, 25), List.of()));
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).contains("open_files_slope_per_hour").doesNotContain("thread_slope_per_hour");
  }

  @Test
  void aCutShortSoakFailsForTooFewSamples() {
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> SoakWorkload.judge(latencies(), series(4, 5 / 60.0, FLOOR, 0, 0), perMinute(80, 0), perMinute(300, 0), List.of()));
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).contains("heap_floor_slope_per_hour").contains("too few samples");
    // the series that had enough are still published
    assertThat(e.metrics()).extracting(Metric::name).contains("thread_slope_per_hour").doesNotContain("heap_floor_slope_per_hour");
  }

  @Test
  void aFailedWindowFailsTheCaseWithItsSlopes() {
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> SoakWorkload.judge(latencies(), floors(0), perMinute(80, 0), perMinute(300, 0), List.of("ddl: boom")));
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).isEqualTo("ddl: boom");
    assertThat(e.metrics()).extracting(Metric::name).contains("heap_floor_slope_per_hour", "thread_slope_per_hour");
  }

  @Test
  void aSteadyServerPasses() throws Exception {
    final Measurement measurement = SoakWorkload.judge(latencies(), floors(0), perMinute(80, 0), perMinute(300, 0), List.of());
    final List<Metric> metrics = measurement.perRep();
    assertThat(metrics).extracting(Metric::name)
        .containsExactly("ingest_p99_ms", "heap_floor_slope_per_hour", "thread_slope_per_hour", "open_files_slope_per_hour");
    assertThat(Math.abs(metric(metrics, "heap_floor_slope_per_hour").median())).isLessThan(0.01);
    assertThat(Math.abs(metric(metrics, "thread_slope_per_hour").median())).isLessThan(1.0);
  }
}
