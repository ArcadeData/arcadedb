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
import com.arcadedb.bench.runner.CaseFailedWithMetrics;
import com.arcadedb.bench.runner.Measurement;
import com.arcadedb.bench.runner.Workload;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.bench.workload.mixed.MixedPhase;
import com.arcadedb.bench.workload.soak.Slope;
import com.arcadedb.bench.workload.soak.SoakSampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;

/**
 * The mixed phase ({@link MixedPhase}) for one window of two hours, while a {@link SoakSampler} watches the server for leaks: the
 * least-squares slope per hour of its heap floor (relative to the mean floor), of its thread count and of its open files. A
 * slope over its limit, a series with too few samples for a slope, a failed loop or a database that does not hold what the loops
 * acknowledged fails the case with its metrics, the failure naming each limit crossed.
 * <p>
 * The slopes are published with unit {@code per_hour}, never judged by the gate: their baseline is near zero by design, so
 * relative bands would judge night-to-night noise; the limits of this case are the judgment. It runs at M only (the Saturday
 * lane) and on nightly and manual runs, never on a release run. The samples of the first {@link #WARMUP} are left out of every
 * slope: a server grows its pools, files and heap to their working size there.
 */
public class SoakWorkload implements Workload {
  static final String CASE = "mixed-2h";

  static final String HEAP_METRIC    = "heap_floor_slope_per_hour";
  static final String THREADS_METRIC = "thread_slope_per_hour";
  static final String FILES_METRIC   = "open_files_slope_per_hour";
  static final String UNIT           = "per_hour";

  /** The heap floor may grow by at most 5% of its mean per hour. */
  static final double HEAP_LIMIT    = 0.05;
  /** The thread count may grow by at most 10 per hour. */
  static final double THREADS_LIMIT = 10;
  /** The open files may grow by at most 10 per hour. */
  static final double FILES_LIMIT   = 10;

  static final Duration WINDOW     = Duration.ofMinutes(120);
  /** The first stretch of the window, left out of every slope: the server grows to its working size there. */
  static final Duration WARMUP     = Duration.ofMinutes(15);
  static final Duration STEP_LIMIT = Duration.ofMinutes(150);

  private static final Logger LOGGER = LoggerFactory.getLogger(SoakWorkload.class);

  /** The window and the check of the mixed phase; the seam that lets a test run the measure step without a server. */
  interface Phase {
    MixedPhase.Result run(Duration window) throws Exception;

    void check() throws InterruptedException;
  }

  private final Duration window;
  private final Duration floorWindow;
  private final Duration warmup;
  private MixedPhase     phase;
  private SoakSampler    lastSampler;

  public SoakWorkload() {
    this(WINDOW, SoakSampler.FLOOR_WINDOW, WARMUP);
  }

  /** A shorter soak, for the IT only: the window, the heap floor window it is cut into and the warm-up left out of the slopes. */
  SoakWorkload(final Duration window, final Duration floorWindow, final Duration warmup) {
    this.window = window;
    this.floorWindow = floorWindow;
    this.warmup = warmup;
  }

  @Override
  public String name() {
    return "soak";
  }

  @Override
  public List<String> cases() {
    return List.of(CASE);
  }

  /** Two hours fit the Saturday lane (M) only. */
  @Override
  public List<String> cases(final Scale scale) {
    return scale == Scale.M ? cases() : List.of();
  }

  @Override
  public boolean runsOn(final String trigger) {
    return "nightly".equals(trigger) || "manual".equals(trigger);
  }

  @Override
  public OptionalInt reps() {
    return OptionalInt.of(1);
  }

  @Override
  public OptionalInt warmup() {
    return OptionalInt.of(0);
  }

  @Override
  public Optional<Duration> stepLimit() {
    return Optional.of(STEP_LIMIT);
  }

  @Override
  public void prepare(final WorkloadContext ctx, final String caseName) {
    if (!CASE.equals(caseName))
      throw new IllegalArgumentException("Unknown case: " + caseName);
    MixedWorkload.prepareDatabase(ctx);
    phase = new MixedPhase(ctx.server(), ctx.seed(), ctx.scale());
  }

  @Override
  public Measurement measure(final WorkloadContext ctx, final String caseName) throws Exception {
    final SoakSampler sampler = new SoakSampler(ctx.server(), floorWindow, warmup, window);
    lastSampler = sampler;
    final MixedPhase current = phase;
    return soak(new Phase() {
      @Override
      public MixedPhase.Result run(final Duration w) throws Exception {
        return current.run(w);
      }

      @Override
      public void check() throws InterruptedException {
        current.check();
      }
    }, sampler, window);
  }

  /**
   * One soak: {@code phase} runs for {@code window} while {@code sampler} samples, and the check runs after a clean window only
   * (after a failed one it could only say that its expectations were lost), its message joining the problems. A window cut short
   * by an interrupt (the runner's deadline) still publishes what it measured in a {@link CaseFailedWithMetrics}, and the
   * interrupt stays set for the runner to see.
   */
  static Measurement soak(final Phase phase, final SoakSampler sampler, final Duration window) throws Exception {
    final MixedPhase.Result result;
    sampler.start();
    try {
      result = phase.run(window);
    } finally {
      sampler.close();
    }
    LOGGER.info("Soak: {} samples, {} operations in {} s", sampler.samples(), result.operations(), result.seconds());
    final List<String> problems = new ArrayList<>(result.failures());
    if (problems.isEmpty())
      try {
        phase.check();
      } catch (final IllegalStateException e) {
        problems.add(e.getMessage());
      }
    return judge(MixedWorkload.latencyMetrics(result, 3), sampler, problems);
  }

  @Override
  public void teardown(final WorkloadContext ctx) {
    phase = null;
  }

  /** The sampler of the last repetition, null before the first: its series, for the IT to keep. */
  SoakSampler lastSampler() {
    return lastSampler;
  }

  /** What the last repetition acknowledged: base and ingested documents; -1 before the first. */
  long expectedDocs() {
    return phase != null ? phase.expectedDocs() : -1L;
  }

  /** As {@link #judge(List, double[][], double[][], double[][], List)} over the series of {@code sampler}, warm-up left out. */
  static Measurement judge(final List<Metric> metrics, final SoakSampler sampler, final List<String> problems)
      throws CaseFailedWithMetrics {
    return judge(metrics, sampler.heapFloors(), sampler.threads(), sampler.files(), problems);
  }

  /**
   * Adds the three slopes to {@code metrics} and judges them against their limits: the heap floor slope relative to the mean floor
   * ({@code {hourOffset, bytes}} pairs), the thread and open-file slopes as they are. Throws the metrics in a
   * {@link CaseFailedWithMetrics} naming each limit crossed, each series too short for a slope and each of {@code problems}.
   */
  static Measurement judge(final List<Metric> metrics, final double[][] floors, final double[][] threads, final double[][] files,
      final List<String> problems) throws CaseFailedWithMetrics {
    final List<String> failures = new ArrayList<>(problems);
    final OptionalDouble heap = slope(floors);
    final double meanFloor = mean(floors);
    if (heap.isEmpty())
      failures.add(tooFew(HEAP_METRIC, floors.length, "heap floors"));
    else if (meanFloor <= 0)
      failures.add(HEAP_METRIC + ": no heap floor above zero");
    else
      judge(metrics, failures, HEAP_METRIC, heap.getAsDouble() / meanFloor, HEAP_LIMIT);
    final OptionalDouble threadSlope = slope(threads);
    if (threadSlope.isEmpty())
      failures.add(tooFew(THREADS_METRIC, threads.length, "per-minute thread counts"));
    else
      judge(metrics, failures, THREADS_METRIC, threadSlope.getAsDouble(), THREADS_LIMIT);
    final OptionalDouble fileSlope = slope(files);
    if (fileSlope.isEmpty())
      failures.add(tooFew(FILES_METRIC, files.length, "per-minute open file counts"));
    else
      judge(metrics, failures, FILES_METRIC, fileSlope.getAsDouble(), FILES_LIMIT);
    if (!failures.isEmpty())
      throw new CaseFailedWithMetrics(String.join("; ", failures), metrics);
    return new Measurement(metrics);
  }

  private static void judge(final List<Metric> metrics, final List<String> failures, final String name, final double value,
      final double limit) {
    LOGGER.info("Soak {}: {}", name, value);
    metrics.add(Metrics.of(name, UNIT, Metric.Better.LOWER, value));
    if (value > limit)
      failures.add(name + " is " + value + ", over its limit of " + limit);
  }

  private static String tooFew(final String name, final int samples, final String what) {
    return name + ": too few samples, " + samples + " " + what + " where a slope needs " + Slope.MIN_SAMPLES;
  }

  private static OptionalDouble slope(final double[][] pairs) {
    final double[] hours = new double[pairs.length];
    final double[] values = new double[pairs.length];
    for (int i = 0; i < pairs.length; i++) {
      hours[i] = pairs[i][0];
      values[i] = pairs[i][1];
    }
    return Slope.perHour(hours, values);
  }

  private static double mean(final double[][] pairs) {
    if (pairs.length == 0)
      return 0;
    double sum = 0;
    for (final double[] pair : pairs)
      sum += pair[1];
    return sum / pairs.length;
  }
}
