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
import com.arcadedb.bench.workload.Scale;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A case judged by the ratio of two forms of the same question on the same server: the form the issue made slow over a
 * reference form that never was. One execution of a form runs each of its statements once, with the next parameter set of the
 * cycle; both forms walk the same cycle. The two forms must answer the same rows, or the ratio measures nothing.
 */
public class RatioScenario implements IssueScenario {
  private static final Logger       LOGGER     = LoggerFactory.getLogger(RatioScenario.class);
  private static final List<String> ZERO_COUNT = List.of("{\"n\":0}");

  /** The parameter sets an execution cycles through, derived from the scale and the seed; at least one (may be empty). */
  @FunctionalInterface
  public interface Params {
    List<Map<String, Object>> of(Scale scale, long seed);
  }

  private final int             issue;
  private final double          limit;
  private final List<Statement> slow;
  private final List<Statement> reference;
  private final Params          params;
  private final Loader          loader;

  // appended to a mismatch where the reference counted 0 and the slow form did not
  private String                    zeroReferenceHint;
  private List<Map<String, Object>> bound;
  // sized by the first repetition of the case, the runner's first warm-up
  private int                       batch;

  public RatioScenario(final int issue, final double limit, final List<Statement> slow, final List<Statement> reference,
      final Params params, final Loader loader) {
    if (slow.size() != reference.size())
      throw new IllegalArgumentException("issue " + issue + ": the forms must have as many statements, the ratio counts executions");
    this.issue = issue;
    this.limit = limit;
    this.slow = slow;
    this.reference = reference;
    this.params = params;
    this.loader = loader;
  }

  /** A hint for a mismatch where the reference form's {@code count(*) AS n} is 0 and the slow form's is not. */
  public RatioScenario hintWhenReferenceCountsZero(final String hint) {
    this.zeroReferenceHint = hint;
    return this;
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
    bound = params.of(scale, seed);
    if (bound.isEmpty())
      throw new IllegalArgumentException(caseName() + ": no parameter set");
    batch = 0;
  }

  @Override
  public void load(final RemoteDatabase db, final Scale scale, final long seed) {
    loader.load(db, scale, seed);
  }

  /**
   * For every parameter set and statement, both forms must answer the same rows: in order when both statements order their
   * answer, as a multiset otherwise (a form that falls back to a scan answers in bucket order, and must then be judged by its
   * factor, not fail here).
   */
  @Override
  public void checkForms(final RemoteDatabase db) {
    int slowTotal = 0;
    int referenceTotal = 0;
    final List<String> mismatches = new ArrayList<>();
    for (int p = 0; p < bound.size(); p++)
      for (int s = 0; s < slow.size(); s++) {
        final List<String> slowRows = new ArrayList<>(rows(db, true, p, s));
        final List<String> referenceRows = new ArrayList<>(rows(db, false, p, s));
        slowTotal += slowRows.size();
        referenceTotal += referenceRows.size();
        if (!(slow.get(s).ordered() && reference.get(s).ordered())) {
          Collections.sort(slowRows);
          Collections.sort(referenceRows);
        }
        if (mismatches.isEmpty() && !slowRows.equals(referenceRows))
          mismatches.add("parameter set " + p + ", statement " + s + ": slow " + slowRows + ", reference " + referenceRows
              + (zeroReferenceHint != null && referenceRows.equals(ZERO_COUNT) ? " (" + zeroReferenceHint + ")" : ""));
      }
    if (slowTotal != referenceTotal)
      throw new IllegalStateException(caseName() + ": the slow form answered " + slowTotal + " rows, the reference form "
          + referenceTotal + " rows");
    if (slowTotal == 0)
      throw new IllegalStateException(caseName() + ": both forms answered no row, the ratio would time nothing");
    if (!mismatches.isEmpty())
      throw new IllegalStateException(caseName() + ": the forms' rows differ at " + abbreviate(mismatches.getFirst()));
    LOGGER.info("{}: both forms answer the same {} rows", caseName(), slowTotal);
  }

  private static String abbreviate(final String text) {
    return text.length() <= 1_000 ? text : text.substring(0, 1_000) + "...";
  }

  /** The rows, as JSON text, of statement {@code s} of a form with parameter set {@code p}. */
  protected List<String> rows(final RemoteDatabase db, final boolean slowForm, final int p, final int s) {
    final Statement statement = (slowForm ? slow : reference).get(s);
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = db.query(statement.language(), statement.text(), bound.get(p))) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        rows.add(r.toJSON().toString());
      }
    }
    return rows;
  }

  /** Runs one statement and reads its answer to the end; the unit of every timing. */
  protected void execute(final RemoteDatabase db, final Statement statement, final Map<String, Object> params) {
    IssueScenario.drain(db, statement, params);
  }

  @Override
  public Measurement measure(final RemoteDatabase db, final IssueSampler sampler, final Scale scale) throws CaseFailedWithMetrics {
    final Runnable slowForm = form(db, slow);
    final Runnable referenceForm = form(db, reference);
    if (batch == 0) {
      batch = IssueSampler.batchFor(calibrate(sampler, slowForm, referenceForm, IssueScenario.calibrationMs(scale)),
          IssueScenario.roundTargetMs(scale));
      LOGGER.info("{}: rounds of {} executions of each form", caseName(), batch);
    }
    sampler.alternateFor(slowForm, referenceForm, WARMUP_SAMPLES, batch, IssueScenario.warmupSampleMs(scale));
    IssueSampler.Samples timed = sampler.alternateFor(slowForm, referenceForm, SAMPLES, batch, IssueScenario.sampleTargetMs(scale));
    IssueJudge.Verdict verdict = IssueJudge.ratio(timed.medianMs()[0], timed.medianMs()[1], limit);
    if (verdict.failed()) {
      // the runner calls measure for its warm-ups too: one noisy repetition must not fail the case, a regression fails twice
      LOGGER.warn("{}: {}; timing the samples once more", caseName(), verdict.reason());
      final IssueJudge.Verdict first = verdict;
      timed = sampler.alternateFor(slowForm, referenceForm, SAMPLES, batch, IssueScenario.sampleTargetMs(scale));
      verdict = IssueJudge.confirm(first, IssueJudge.ratio(timed.medianMs()[0], timed.medianMs()[1], limit));
    }
    LOGGER.info("{}: {}", caseName(), verdict.reason());
    return publish(verdict, timed.medianMs()[0], timed.medianMs()[1], timed.elapsedMs() / 1_000.0);
  }

  /** The metrics of a repetition; a failed verdict throws them with its reason, so they are kept on the failed case. */
  public static Measurement publish(final IssueJudge.Verdict verdict, final double slowMs, final double referenceMs,
      final double repSeconds) throws CaseFailedWithMetrics {
    final List<Metric> metrics = List.of(Metrics.of("factor", "factor", Metric.Better.LOWER, verdict.value()),
        Metrics.of("slow_ms", "ms", Metric.Better.LOWER, slowMs), Metrics.of("reference_ms", "ms", Metric.Better.LOWER, referenceMs),
        Metrics.seconds("rep_s", repSeconds));
    if (verdict.failed())
      throw new CaseFailedWithMetrics(verdict.reason(), metrics);
    return new Measurement(metrics);
  }

  /**
   * The milliseconds of one execution of both forms, measured over at least {@code forMs}. A cold estimate only sizes the
   * rounds: the samples are bounded by time, so it cannot shorten a repetition.
   */
  private static double calibrate(final IssueSampler sampler, final Runnable slowForm, final Runnable referenceForm,
      final double forMs) {
    final long start = sampler.nanos();
    int pairs = 0;
    double elapsedMs;
    do {
      slowForm.run();
      referenceForm.run();
      pairs++;
      elapsedMs = (sampler.nanos() - start) / 1_000_000.0;
    } while (elapsedMs < forMs || pairs < 3);
    return elapsedMs / pairs;
  }

  // the parameters are built once in bind: the timed loop only sends them
  private Runnable form(final RemoteDatabase db, final List<Statement> statements) {
    final int[] next = { 0 };
    final int cycle = bound.size();
    return () -> {
      final Map<String, Object> p = bound.get(next[0]);
      next[0] = (next[0] + 1) % cycle;
      for (int s = 0; s < statements.size(); s++)
        execute(db, statements.get(s), p);
    };
  }
}
