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
import com.arcadedb.serializer.json.JSONObject;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

/**
 * Runs the cases of a workload: prepare, warm-ups (discarded), then the measured repetitions. A failing or timed-out case is
 * recorded and the run goes on with the next case.
 */
public final class BenchRunner {
  private final int      reps;
  private final int      warmup;
  private final Duration caseTimeout;
  private final Duration interruptGrace;

  private static final String WORKER_STILL_RUNNING = "teardown skipped: worker still running";

  // set by runCase for run(), on the caller's thread
  private boolean lastWorkerStopped = true;
  // the case whose worker outlived its timeout: it still loads the host, so no later case of any workload is measured
  private String  abandoned;

  public BenchRunner(final int reps, final int warmup, final Duration caseTimeout) {
    this(reps, warmup, caseTimeout, Duration.ofSeconds(5));
  }

  BenchRunner(final int reps, final int warmup, final Duration caseTimeout, final Duration interruptGrace) {
    if (reps < 1)
      throw new IllegalArgumentException("reps must be at least 1, got " + reps);
    if (warmup < 0)
      throw new IllegalArgumentException("warmup must not be negative, got " + warmup);
    this.reps = reps;
    this.warmup = warmup;
    this.caseTimeout = caseTimeout;
    this.interruptGrace = interruptGrace;
  }

  public List<CaseResult> run(final Workload w, final WorkloadContext ctx, final Predicate<String> caseFilter) {
    final List<CaseResult> results = new ArrayList<>();
    for (final String caseName : w.cases()) {
      if (caseFilter != null && !caseFilter.test(caseName))
        continue;
      final CaseResult result;
      if (Thread.currentThread().isInterrupted())
        result = result(w, ctx, caseName, List.of(), CaseResult.Status.FAILED, "not started: the run was interrupted");
      else if (abandoned != null)
        result = result(w, ctx, caseName, List.of(), CaseResult.Status.FAILED,
            "not started: the worker of case '" + abandoned + "' is still running");
      else {
        result = runCase(w, ctx, caseName);
        if (!lastWorkerStopped)
          abandoned = caseName;
      }
      results.add(result);
    }
    return results;
  }

  /** Whether a timed-out worker could not be stopped: every later case is then recorded as not started. */
  public boolean hasAbandonedWorker() {
    return abandoned != null;
  }

  // each case gets its own worker thread, so a worker hung past an interrupt is abandoned and cannot block the next case
  private CaseResult runCase(final Workload w, final WorkloadContext ctx, final String caseName) {
    final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
      final Thread t = new Thread(r, "bench-runner");
      t.setDaemon(true);
      return t;
    });
    final long deadline = System.nanoTime() + caseTimeout.toNanos();
    // a flag of its own: a measure() that swallows the interrupt must not let the worker start another repetition
    final AtomicBoolean cancelled = new AtomicBoolean();
    final Future<CaseResult> future = executor.submit(() -> {
      try {
        return execute(w, ctx, caseName, cancelled);
      } catch (final CaseFailedWithMetrics e) {
        return result(w, ctx, caseName, e.metrics(), CaseResult.Status.FAILED, e.getMessage());
      } catch (final Throwable t) {
        return result(w, ctx, caseName, List.of(), CaseResult.Status.FAILED, t.getClass().getName() + ": " + t.getMessage());
      }
    });
    CaseResult result;
    boolean workerStopped = true;
    try {
      result = future.get(Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
    } catch (final TimeoutException e) {
      cancelled.set(true);
      future.cancel(true);
      executor.shutdownNow();
      workerStopped = awaitStopped(executor);
      result = result(w, ctx, caseName, List.of(), CaseResult.Status.TIMEOUT,
          "case exceeded " + caseTimeout.toMillis() + " ms" + (workerStopped ? "" : "; " + WORKER_STILL_RUNNING));
    } catch (final InterruptedException e) {
      cancelled.set(true);
      future.cancel(true);
      executor.shutdownNow();
      Thread.currentThread().interrupt();
      workerStopped = false;
      result = result(w, ctx, caseName, List.of(), CaseResult.Status.FAILED, "interrupted; " + WORKER_STILL_RUNNING);
    } catch (final Exception e) {
      result = result(w, ctx, caseName, List.of(), CaseResult.Status.FAILED, e.getClass().getName() + ": " + e.getMessage());
    } finally {
      executor.shutdown();
    }
    lastWorkerStopped = workerStopped;
    if (workerStopped) {
      try {
        w.teardown(ctx);
      } catch (final Throwable t) {
        if (result.status() == CaseResult.Status.OK)
          result = result(w, ctx, caseName, result.metrics(), CaseResult.Status.FAILED,
              "teardown: " + t.getClass().getName() + ": " + t.getMessage());
      }
    }
    return result;
  }

  private boolean awaitStopped(final ExecutorService executor) {
    try {
      return executor.awaitTermination(interruptGrace.toNanos(), TimeUnit.NANOSECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /** An abandoned worker (its case timed out) ends at the next repetition instead of measuring on. */
  private static void stopIfCancelled(final AtomicBoolean cancelled) throws InterruptedException {
    if (cancelled.get() || Thread.currentThread().isInterrupted())
      throw new InterruptedException("case abandoned");
  }

  private CaseResult execute(final Workload w, final WorkloadContext ctx, final String caseName, final AtomicBoolean cancelled)
      throws Exception {
    w.prepare(ctx, caseName);
    for (int i = 0; i < warmup; i++) {
      stopIfCancelled(cancelled);
      w.measure(ctx, caseName);
    }

    // metric name -> per-rep medians, in first-seen order
    final Map<String, List<Double>> values = new LinkedHashMap<>();
    final Map<String, Metric> first = new LinkedHashMap<>();
    for (int i = 0; i < reps; i++) {
      stopIfCancelled(cancelled);
      for (final Metric m : w.measure(ctx, caseName).perRep()) {
        first.putIfAbsent(m.name(), m);
        values.computeIfAbsent(m.name(), k -> new ArrayList<>()).add(m.median());
      }
    }
    final List<Metric> metrics = new ArrayList<>(first.size());
    for (final Map.Entry<String, Metric> e : first.entrySet()) {
      final List<Double> list = values.get(e.getKey());
      final double[] v = new double[list.size()];
      for (int i = 0; i < v.length; i++)
        v[i] = list.get(i);
      final Metric f = e.getValue();
      metrics.add(new Metric(f.name(), f.unit(), f.better(), Stats.median(v), v));
    }
    return result(w, ctx, caseName, metrics, CaseResult.Status.OK, null);
  }

  private CaseResult result(final Workload w, final WorkloadContext ctx, final String caseName, final List<Metric> metrics,
      final CaseResult.Status status, final String error) {
    final JSONObject params = ctx.params() == null ? new JSONObject() : ctx.params();
    return new CaseResult(w.name(), caseName, ctx.scale().name(), params, reps, warmup, metrics, status, error);
  }
}
