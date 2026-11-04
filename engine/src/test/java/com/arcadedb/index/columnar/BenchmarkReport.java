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
package com.arcadedb.index.columnar;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.StallAwareStopwatch;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * The samples of one benchmark run, per metric and variant, written as JSON for {@code engine/src/test/scripts/columnar_bench_compare.py}
 * (plan 7 measurement protocol, #10662). Report only: nothing here asserts a time.
 * <p>
 * JSON: {@code {benchmark, label, machine{...}, metrics{<name>{unit, variants{<variant>[samples]}}}}}. The machine state is
 * taken when the report is created, so its load average is the one at the start of the run. System properties read:
 * <ul>
 * <li>{@code columnar.bench.label}: without it {@link #write()} writes nothing;</li>
 * <li>{@code columnar.bench.out}: the directory of the JSON files, default {@code target/columnar-bench};</li>
 * <li>{@code columnar.bench.rounds}: see {@link #rounds()};</li>
 * <li>{@code columnar.bench.rev} and {@code columnar.bench.dirty}: the git revision and dirty file count, set by
 * {@code columnar-bench.sh}.</li>
 * </ul>
 */
public final class BenchmarkReport {
  public static final String ON  = "on";
  public static final String OFF = "off";

  private final String                  benchmark;
  private final JSONObject              machine = machine();
  private final Map<String, Metric>     metrics = new LinkedHashMap<>();

  private static final class Metric {
    private final String                unit;
    private final Map<String, double[]> variants = new LinkedHashMap<>();

    private Metric(final String unit) {
      this.unit = unit;
    }
  }

  public BenchmarkReport(final String benchmark) {
    this.benchmark = benchmark;
  }

  /** The measured rounds of every benchmark, from {@code -Dcolumnar.bench.rounds} (default 5). */
  public static int rounds() {
    return Integer.getInteger("columnar.bench.rounds", 5);
  }

  /** Adds one sample of the metric for the variant; the first sample of a metric fixes its unit. */
  public void add(final String metric, final String unit, final String variant, final double value) {
    final Metric m = metrics.computeIfAbsent(metric, k -> new Metric(unit));
    final double[] samples = m.variants.get(variant);
    final double[] grown = samples == null ? new double[1] : Arrays.copyOf(samples, samples.length + 1);
    grown[grown.length - 1] = value;
    m.variants.put(variant, grown);
  }

  /** The samples of the metric for the variant, in the order they were added; empty when none. */
  public double[] samples(final String metric, final String variant) {
    final Metric m = metrics.get(metric);
    final double[] samples = m == null ? null : m.variants.get(variant);
    return samples == null ? new double[0] : samples.clone();
  }

  /** The median; of an even count, the mean of the middle two; 0 of none. The array is not reordered. */
  public static double median(final double[] values) {
    if (values.length == 0)
      return 0;
    final double[] sorted = values.clone();
    Arrays.sort(sorted);
    final int middle = sorted.length / 2;
    return sorted.length % 2 == 1 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
  }

  /** {@code (max - min) / median}; 0 when the median is 0. */
  public static double spread(final double[] values) {
    final double median = median(values);
    if (median == 0)
      return 0;
    double min = Double.MAX_VALUE;
    double max = -Double.MAX_VALUE;
    for (final double v : values) {
      min = Math.min(min, v);
      max = Math.max(max, v);
    }
    return (max - min) / median;
  }

  /**
   * Runs {@code runNanos} twice per round, {@code warmups} unrecorded rounds then {@code rounds} recorded ones, as the variants
   * {@value #ON} and {@value #OFF} in an order that rotates (on/off, off/on, ...) so a drift of the machine weighs on both alike. Each
   * run's nanoseconds are added as a sample in ms of its variant. Both variants run the same code (the tuning switches of #10662 are
   * gone), so their difference is the noise of the metric in this run, and the reports compare variant by variant with those of the
   * lever-era harness.
   */
  public void pairs(final int warmups, final int rounds, final String metric, final LongSupplier runNanos) {
    pairs(warmups, rounds, recorder -> recorder.nanos(metric, runNanos.getAsLong()));
  }

  /** One run of a round, which may time several metrics (for example each query of a group and their sum). */
  @FunctionalInterface
  public interface Round {
    void run(Recorder recorder);
  }

  /** Records the metrics of one run under its variant; the samples of a warm-up run are dropped. */
  public interface Recorder {
    /** {@value #ON} or {@value #OFF}. */
    String variant();

    /** Adds the nanoseconds as a sample in ms of the metric, unless the run is a warm-up. */
    void nanos(String metric, long nanos);
  }

  /**
   * As {@link #pairs(int, int, String, LongSupplier)}, with each run recording any number of metrics; a run may pick what it measures
   * by its {@link Recorder#variant()}.
   */
  public void pairs(final int warmups, final int rounds, final Round round) {
    for (int r = 0; r < warmups + rounds; r++)
      for (int k = 0; k < 2; k++) {
        final boolean on = (r + k) % 2 == 0;
        final boolean recorded = r >= warmups;
        final String variant = on ? ON : OFF;
        final Recorder recorder = new Recorder() {
          @Override
          public String variant() {
            return variant;
          }

          @Override
          public void nanos(final String metric, final long nanos) {
            if (recorded)
              add(metric, "ms", variant, nanos / 1_000_000d);
          }
        };
        round.run(recorder);
      }
  }

  /**
   * Runs the body and returns its elapsed nanoseconds minus the JVM stall observed in the same window, the discount of
   * {@link StallAwareStopwatch} at nanosecond resolution (a query of a small data set takes less than its millisecond resolution).
   */
  public static long effectiveNanos(final Runnable body) {
    final long startStall = StallAwareStopwatch.jvmStallNanos();
    final long start = System.nanoTime();
    body.run();
    return Math.max(0, System.nanoTime() - start - (StallAwareStopwatch.jvmStallNanos() - startStall));
  }

  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("benchmark", benchmark);
    json.put("label", System.getProperty("columnar.bench.label", ""));
    json.put("machine", machine);
    final JSONObject all = new JSONObject();
    for (final Map.Entry<String, Metric> entry : metrics.entrySet()) {
      final JSONObject variants = new JSONObject();
      for (final Map.Entry<String, double[]> variant : entry.getValue().variants.entrySet()) {
        final JSONArray samples = new JSONArray();
        for (final double v : variant.getValue())
          samples.put(v);
        variants.put(variant.getKey(), samples);
      }
      all.put(entry.getKey(), new JSONObject().put("unit", entry.getValue().unit).put("variants", variants));
    }
    json.put("metrics", all);
    return json;
  }

  /**
   * Writes the report to {@code ${columnar.bench.out:-target/columnar-bench}/<benchmark>-<label>-<epochMs>.json}.
   *
   * @return the file written, or null without {@code -Dcolumnar.bench.label}
   */
  public Path write() {
    final String label = System.getProperty("columnar.bench.label");
    if (label == null || label.isBlank())
      return null;
    try {
      final Path dir = Path.of(System.getProperty("columnar.bench.out", "target/columnar-bench"));
      Files.createDirectories(dir);
      final Path file = dir.resolve(benchmark + "-" + label + "-" + System.currentTimeMillis() + ".json");
      Files.writeString(file, toJSON().toString(2), StandardCharsets.UTF_8);
      return file;
    } catch (final IOException e) {
      throw new UncheckedIOException("Cannot write the benchmark report of " + benchmark, e);
    }
  }

  /** The state of the machine and the JVM now, as the verdicts of the measurement protocol need it. */
  public static JSONObject machine() {
    final Runtime runtime = Runtime.getRuntime();
    return new JSONObject()//
        .put("cores", runtime.availableProcessors())//
        .put("os", System.getProperty("os.name") + " " + System.getProperty("os.version"))//
        .put("arch", System.getProperty("os.arch"))//
        .put("java.version", System.getProperty("java.version"))//
        .put("maxMemory", runtime.maxMemory())//
        .put("loadAverage", ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage())//
        .put("columnar.bench.rev", System.getProperty("columnar.bench.rev", "unknown"))//
        .put("columnar.bench.dirty", Integer.getInteger("columnar.bench.dirty", -1))//
        .put("startedAt", Instant.now().toString());
  }
}
