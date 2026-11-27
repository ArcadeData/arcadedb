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
package com.arcadedb.bench.workload.issues;

import com.arcadedb.bench.model.Metric;
import com.arcadedb.bench.runner.CaseFailedWithMetrics;
import com.arcadedb.bench.runner.Measurement;
import com.arcadedb.bench.workload.Metrics;
import com.arcadedb.bench.workload.RemoteClients;
import com.arcadedb.bench.workload.Scale;
import com.arcadedb.remote.RemoteDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A case judged by the exponent of a sweep: the same question at several sizes, the least-squares slope of the log of its
 * median on the log of the size. Each size has an answer of a known number of rows, checked once before timing.
 */
public class ExponentScenario implements IssueScenario {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExponentScenario.class);

  /**
   * The question at one size of the sweep and what it must answer: {@code expected} rows, or, with {@code counted}, one row
   * whose {@code n} is {@code expected}.
   */
  public record Point(double size, Statement statement, Map<String, Object> params, long expected, boolean counted) {
  }

  /** The points of the sweep, smallest first, for a scale and a seed. */
  @FunctionalInterface
  public interface Sweep {
    List<Point> of(Scale scale, long seed);
  }

  private final int    issue;
  private final double limit;
  private final Sweep  sweep;
  private final Loader loader;

  private List<Point> points;
  private int[]       batches;

  public ExponentScenario(final int issue, final double limit, final Sweep sweep, final Loader loader) {
    this.issue = issue;
    this.limit = limit;
    this.sweep = sweep;
    this.loader = loader;
  }

  @Override
  public int issue() {
    return issue;
  }

  @Override
  public double limit() {
    return limit;
  }

  @Override
  public void bind(final Scale scale, final long seed) {
    points = sweep.of(scale, seed);
    if (points.size() < 2)
      throw new IllegalArgumentException(caseName() + ": a sweep needs at least two sizes");
    batches = null;
  }

  @Override
  public void load(final RemoteDatabase db, final Scale scale, final long seed) {
    loader.load(db, scale, seed);
  }

  @Override
  public void checkForms(final RemoteDatabase db) {
    for (final Point p : points) {
      if (p.expected() < 1)
        throw new IllegalStateException(caseName() + ": the point of size " + p.size() + " expects nothing, it would time nothing");
      final long answered = p.counted() ? RemoteClients.count(db, p.statement().language(), p.statement().text(), p.params())
          : IssueScenario.drain(db, p.statement(), p.params());
      if (answered != p.expected())
        throw new IllegalStateException(caseName() + ": at size " + p.size() + " the query answered " + answered
            + (p.counted() ? " as its count" : " rows") + ", expected " + p.expected());
    }
    LOGGER.info("{}: every size of the sweep answers what it must", caseName());
  }

  /** Runs one statement and reads its answer to the end; the unit of every timing. */
  protected void execute(final RemoteDatabase db, final Statement statement, final Map<String, Object> params) {
    IssueScenario.drain(db, statement, params);
  }

  @Override
  public Measurement measure(final RemoteDatabase db, final IssueSampler sampler, final Scale scale) throws CaseFailedWithMetrics {
    final int n = points.size();
    final Runnable[] forms = new Runnable[n];
    final double[] sizes = new double[n];
    for (int i = 0; i < n; i++) {
      final Point p = points.get(i);
      forms[i] = () -> execute(db, p.statement(), p.params());
      sizes[i] = p.size();
    }
    if (batches == null) {
      batches = new int[n];
      for (int i = 0; i < n; i++)
        batches[i] = IssueSampler.batchFor(calibrate(sampler, forms[i], IssueScenario.calibrationMs(scale) / n),
            IssueScenario.roundTargetMs(scale) / n);
      LOGGER.info("{}: rounds of {} executions at each size", caseName(), Arrays.toString(batches));
    }
    sampler.rotateFor(forms, WARMUP_SAMPLES, batches, IssueScenario.warmupSampleMs(scale));
    IssueSampler.Samples timed = sampler.rotateFor(forms, SAMPLES, batches, IssueScenario.sampleTargetMs(scale));
    IssueJudge.Verdict verdict = IssueJudge.exponent(sizes, timed.medianMs(), limit);
    if (verdict.failed()) {
      // the runner calls measure for its warm-ups too: one noisy repetition must not fail the case, a regression fails twice
      LOGGER.warn("{}: {}; timing the samples once more", caseName(), verdict.reason());
      final IssueJudge.Verdict first = verdict;
      timed = sampler.rotateFor(forms, SAMPLES, batches, IssueScenario.sampleTargetMs(scale));
      verdict = IssueJudge.confirm(first, IssueJudge.exponent(sizes, timed.medianMs(), limit));
    }
    LOGGER.info("{}: medians {} ms at sizes {}: {}", caseName(), Arrays.toString(timed.medianMs()), Arrays.toString(sizes),
        verdict.reason());
    return publish(verdict, timed.medianMs(), timed.elapsedMs() / 1_000.0);
  }

  /**
   * The metrics of a repetition: the exponent, the median at each size ({@code size_<i>_ms},
   * smallest first, so a drift of one size is visible) and the time of the timed samples. A failed verdict throws them with
   * its reason, so they are kept on the failed case.
   */
  public static Measurement publish(final IssueJudge.Verdict verdict, final double[] medians, final double repSeconds)
      throws CaseFailedWithMetrics {
    final List<Metric> metrics = new ArrayList<>();
    metrics.add(Metrics.of("exponent", "exponent", Metric.Better.LOWER, verdict.value()));
    for (int i = 0; i < medians.length; i++)
      metrics.add(Metrics.of("size_" + (i + 1) + "_ms", "ms", Metric.Better.LOWER, medians[i]));
    metrics.add(Metrics.seconds("rep_s", repSeconds));
    if (verdict.failed())
      throw new CaseFailedWithMetrics(verdict.reason(), metrics);
    return new Measurement(metrics);
  }

  private static double calibrate(final IssueSampler sampler, final Runnable form, final double forMs) {
    final long start = sampler.nanos();
    int runs = 0;
    double elapsedMs;
    do {
      form.run();
      runs++;
      elapsedMs = (sampler.nanos() - start) / 1_000_000.0;
    } while (elapsedMs < forMs || runs < 3);
    return elapsedMs / runs;
  }
}
