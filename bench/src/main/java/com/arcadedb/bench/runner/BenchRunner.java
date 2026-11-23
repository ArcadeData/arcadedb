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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

/**
 * Runs the cases of a workload: prepare, warm-ups (discarded), then the measured repetitions. A failing or timed-out case is
 * recorded and the run goes on with the next case.
 * <p>
 * Each step has its own timeout: {@code prepare} (which may load the data set) is bounded by the prepare timeout, and every
 * warm-up and repetition by the step timeout. An optional case cap bounds the whole case, as a hang detector over the sum. A
 * single deadline over the whole case would make a workload whose repetitions are each well inside the limit time out only
 * because it has many of them (#12839).
 */
public final class BenchRunner {
  private final int      reps;
  private final int      warmup;
  private final Duration stepTimeout;
  private final Duration prepareTimeout;
  // zero = no cap over the whole case
  private final Duration caseTimeout;
  private final Duration interruptGrace;

  /** The wall clock stepping this far ahead of the monotonic one during a case means the host slept or was suspended. */
  private static final long HOST_NOT_QUIET_MILLIS = 1000;

  /** The two clocks read at the start and end of each case; replaceable so a test can fake a sleeping host. */
  record ClockPair(LongSupplier wallMillis, LongSupplier monoNanos) {
    static final ClockPair SYSTEM = new ClockPair(System::currentTimeMillis, System::nanoTime);
  }

  private final ClockPair clocks;

  private static final String WORKER_STILL_RUNNING = "teardown skipped: worker still running";

  // set by runCase for run(), on the caller's thread
  private boolean lastWorkerStopped = true;
  // the case whose worker outlived its timeout: it still loads the host, so no later case of any workload is measured
  private String  abandoned;

  /** {@code stepTimeout} bounds prepare and every warm-up and repetition; the case as a whole has no cap. */
  public BenchRunner(final int reps, final int warmup, final Duration stepTimeout) {
    this(reps, warmup, stepTimeout, stepTimeout, Duration.ZERO);
  }

  /**
   * @param stepTimeout    the limit of each warm-up and each measured repetition
   * @param prepareTimeout the limit of {@code prepare}, which may load the data set
   * @param caseTimeout    the limit of the whole case, prepare included; {@link Duration#ZERO} for none
   */
  public BenchRunner(final int reps, final int warmup, final Duration stepTimeout, final Duration prepareTimeout,
      final Duration caseTimeout) {
    this(reps, warmup, stepTimeout, prepareTimeout, caseTimeout, Duration.ofSeconds(5));
  }

  BenchRunner(final int reps, final int warmup, final Duration stepTimeout, final Duration interruptGrace) {
    this(reps, warmup, stepTimeout, stepTimeout, Duration.ZERO, interruptGrace);
  }

  BenchRunner(final int reps, final int warmup, final Duration stepTimeout, final Duration prepareTimeout,
      final Duration caseTimeout, final Duration interruptGrace) {
    this(reps, warmup, stepTimeout, prepareTimeout, caseTimeout, interruptGrace, ClockPair.SYSTEM);
  }

  BenchRunner(final int reps, final int warmup, final Duration stepTimeout, final Duration prepareTimeout,
      final Duration caseTimeout, final Duration interruptGrace, final ClockPair clocks) {
    if (reps < 1)
      throw new IllegalArgumentException("reps must be at least 1, got " + reps);
    if (warmup < 0)
      throw new IllegalArgumentException("warmup must not be negative, got " + warmup);
    if (!stepTimeout.isPositive())
      throw new IllegalArgumentException("step timeout must be positive, got " + stepTimeout);
    if (!prepareTimeout.isPositive())
      throw new IllegalArgumentException("prepare timeout must be positive, got " + prepareTimeout);
    if (caseTimeout.isNegative())
      throw new IllegalArgumentException("case timeout must not be negative (zero = no cap), got " + caseTimeout);
    this.reps = reps;
    this.warmup = warmup;
    this.stepTimeout = stepTimeout;
    this.prepareTimeout = prepareTimeout;
    this.caseTimeout = caseTimeout;
    this.interruptGrace = interruptGrace;
    this.clocks = clocks;
  }

  /** The step the worker is in, when it started and how long it may take; replaced by the worker as it moves on. */
  private record Step(String name, long startNanos, long timeoutNanos) {
    long overdueNanos(final long now) {
      return now - startNanos - timeoutNanos;
    }
  }

  public List<CaseResult> run(final Workload w, final WorkloadContext ctx, final Predicate<String> caseFilter) {
    return run(w, ctx, caseFilter, done -> {
    });
  }

  /**
   * As {@link #run(Workload, WorkloadContext, Predicate)}, and after every case (once its teardown is done) hands {@code onCase}
   * an immutable copy of this workload's results so far, so a caller can persist them while the run goes on.
   */
  public List<CaseResult> run(final Workload w, final WorkloadContext ctx, final Predicate<String> caseFilter,
      final Consumer<List<CaseResult>> onCase) {
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
        final long wallStart = clocks.wallMillis().getAsLong();
        final long monoStart = clocks.monoNanos().getAsLong();
        final CaseResult ran = runCase(w, ctx, caseName);
        final long wallDelta = clocks.wallMillis().getAsLong() - wallStart;
        final long monoDeltaMillis = TimeUnit.NANOSECONDS.toMillis(clocks.monoNanos().getAsLong() - monoStart);
        final long ahead = wallDelta - monoDeltaMillis;
        result = ahead > HOST_NOT_QUIET_MILLIS ?
            ran.withNote("hostNotQuiet", "wall clock ran " + ahead + " ms ahead of the monotonic clock") :
            ran;
        if (!lastWorkerStopped)
          abandoned = caseName;
      }
      results.add(result);
      onCase.accept(List.copyOf(results));
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
    final long caseStart = System.nanoTime();
    final long caseTimeoutNanos = caseTimeout.toNanos();
    final AtomicReference<Step> step = new AtomicReference<>(new Step("prepare", caseStart, prepareTimeout.toNanos()));
    // a flag of its own: a measure() that swallows the interrupt must not let the worker start another repetition
    final AtomicBoolean cancelled = new AtomicBoolean();
    final Future<CaseResult> future = executor.submit(() -> {
      try {
        return execute(w, ctx, caseName, cancelled, step);
      } catch (final CaseFailedWithMetrics e) {
        return result(w, ctx, caseName, e.metrics(), CaseResult.Status.FAILED, e.getMessage());
      } catch (final Throwable t) {
        return result(w, ctx, caseName, List.of(), CaseResult.Status.FAILED, describe(t));
      }
    });
    CaseResult result;
    boolean workerStopped = true;
    try {
      result = await(future, step, caseStart, caseTimeoutNanos);
    } catch (final TimeoutException e) {
      cancelled.set(true);
      future.cancel(true);
      executor.shutdownNow();
      workerStopped = awaitStopped(executor);
      result = result(w, ctx, caseName, List.of(), CaseResult.Status.TIMEOUT,
          e.getMessage() + (workerStopped ? "" : "; " + WORKER_STILL_RUNNING));
    } catch (final InterruptedException e) {
      cancelled.set(true);
      future.cancel(true);
      executor.shutdownNow();
      Thread.currentThread().interrupt();
      workerStopped = false;
      result = result(w, ctx, caseName, List.of(), CaseResult.Status.FAILED, "interrupted; " + WORKER_STILL_RUNNING);
    } catch (final Exception e) {
      result = result(w, ctx, caseName, List.of(), CaseResult.Status.FAILED, describe(e));
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
              "teardown: " + describe(t));
      }
    }
    return result;
  }

  /**
   * Waits for the worker, re-reading its current step after every wake-up: the wait ends at the earlier of the current step's
   * deadline and the case cap, and a timeout is declared only when one of them has passed. The exception message names it.
   */
  private static CaseResult await(final Future<CaseResult> future, final AtomicReference<Step> step, final long caseStart,
      final long caseTimeoutNanos) throws InterruptedException, ExecutionException, TimeoutException {
    while (true) {
      final Step s = step.get();
      final long now = System.nanoTime();
      final long stepOverdue = s.overdueNanos(now);
      if (stepOverdue >= 0)
        throw new TimeoutException(s.name() + " exceeded " + TimeUnit.NANOSECONDS.toMillis(s.timeoutNanos()) + " ms");
      long wait = -stepOverdue;
      if (caseTimeoutNanos > 0) {
        final long caseLeft = caseStart + caseTimeoutNanos - now;
        if (caseLeft <= 0)
          throw new TimeoutException(
              "case exceeded " + TimeUnit.NANOSECONDS.toMillis(caseTimeoutNanos) + " ms (during " + s.name() + ")");
        wait = Math.min(wait, caseLeft);
      }
      try {
        return future.get(wait, TimeUnit.NANOSECONDS);
      } catch (final TimeoutException e) {
        // the worker may have moved on to a later step meanwhile: look again
      }
    }
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

  private CaseResult execute(final Workload w, final WorkloadContext ctx, final String caseName, final AtomicBoolean cancelled,
      final AtomicReference<Step> step) throws Exception {
    w.prepare(ctx, caseName);
    final long stepNanos = stepTimeout.toNanos();
    for (int i = 0; i < warmup; i++) {
      stopIfCancelled(cancelled);
      step.set(new Step("warm-up " + (i + 1) + "/" + warmup, System.nanoTime(), stepNanos));
      w.measure(ctx, caseName);
    }

    // metric name -> per-rep medians, in first-seen order
    final Map<String, List<Double>> values = new LinkedHashMap<>();
    final Map<String, Metric> first = new LinkedHashMap<>();
    for (int i = 0; i < reps; i++) {
      stopIfCancelled(cancelled);
      step.set(new Step("rep " + (i + 1) + "/" + reps, System.nanoTime(), stepNanos));
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

  /**
   * The failure and every cause under it, {@code "A: msg; caused by B: msg"}. A remote client reports a transport failure as
   * a generic "Error on executing remote operation" whose cause is the only part that says what broke. A cycle in the chain
   * ends the walk.
   */
  static String describe(final Throwable t) {
    final StringBuilder b = new StringBuilder();
    final Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable c = t; c != null && seen.add(c); c = c.getCause()) {
      if (c != t)
        b.append("; caused by ");
      b.append(c.getClass().getName()).append(": ").append(c.getMessage());
    }
    return b.toString();
  }

  private CaseResult result(final Workload w, final WorkloadContext ctx, final String caseName, final List<Metric> metrics,
      final CaseResult.Status status, final String error) {
    final JSONObject params = ctx.params() == null ? new JSONObject() : ctx.params();
    return new CaseResult(w.name(), caseName, ctx.scale().name(), params, reps, warmup, metrics, status, error);
  }
}
