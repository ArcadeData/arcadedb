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
import com.arcadedb.bench.workload.ramp.OpenLoop;
import com.arcadedb.bench.workload.ramp.RampSearch;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleFunction;

/**
 * The open-loop capacity of the graph OLTP point lookup: arrivals on a fixed schedule ({@link OpenLoop}, latency from the
 * scheduled start), the rate raised from 100/s by 25% per step until a step breaks (p99 over 50 ms, errors over 0.1%, any
 * dropped arrival), then 3 bisections ({@link RampSearch}). One ramp is one repetition, so the workload declares 3 repetitions
 * and 1 warm-up instead of the run's.
 * <p>
 * The graph is {@link GraphOltpWorkload}'s, built or reused through it. A fixed pool of 128 clients, one
 * {@link RemoteDatabase} each, opened and warmed by one untimed query in {@code prepare} so no connection is set up inside a
 * timed step; an arrival that finds every client busy is a drop. The requests run on virtual threads: the remote client
 * waits for its HTTP answer on a {@code CompletableFuture} and holds no monitor across the network call, so a waiting request
 * never pins its carrier.
 * <p>
 * A knee the client could not drive is not a server number: when more than 1% of the knee step's arrivals started more than
 * 1 ms late, {@code client_limited} is 1 and the case fails, its metrics kept. A ramp that never breaks reports the ceiling it
 * reached with {@code ceiling_reached} 1 rather than a knee; one that breaks at the first step reports a knee of 0 and fails.
 * The flags {@code break_drop}, {@code break_p99}, {@code break_errors} and {@code break_unfinished} say what broke the last
 * failing step, the one that bounded the knee, and the reason is logged per ramp. {@code steps} and the flags are informational:
 * their units ({@code steps}, {@code flag}) are never judged by the gate.
 * <p>
 * One {@link OpenLoop} serves every ramp of the case: a request left unfinished by a ramp still holds its client in the next,
 * so no {@link RemoteDatabase} ever carries two requests at once.
 */
public class RampWorkload implements Workload {
  static final String   CASE         = "knee-point-lookup-cypher";
  static final int      CLIENTS      = 128;
  static final double   START_PER_S  = 100;
  static final double   FACTOR       = 1.25;
  static final int      MAX_COARSE   = 40;
  static final int      BISECTIONS   = 3;
  static final Duration WARM         = Duration.ofSeconds(2);
  /** The share of the knee step's arrivals that may start late before the knee counts as the client's, not the server's. */
  static final double   LATE_LIMIT   = 0.01;
  static final String   FLAG         = "flag";
  /**
   * Untimed lookups in {@code prepare}, round robin over the clients, after each client's first: the first step of the first
   * ramp (100/s) must not meet a server whose query path is still cold, or the whole case fails on a knee of 0.
   */
  static final int      WARM_LOOKUPS = 1_000;

  private static final Logger LOGGER = LoggerFactory.getLogger(RampWorkload.class);

  private static final int      REPS     = 3;
  private static final int      WARMUP   = 1;
  private static final String   LANGUAGE = "opencypher";
  private static final String   QUERY    = "MATCH (u:User {id: $id}) RETURN u.id AS n";
  private static final String   GRAPH_CASE = "point-lookup-cypher";

  // null = the spec's length for the scale
  private final Duration stepOverride;
  private final Duration warm;
  private final int      maxCoarse;
  private final double   lateLimit;

  private final GraphOltpWorkload graph = new GraphOltpWorkload();
  private RemoteDatabase[]        clients;
  private int[]                   userIds;
  private ExecutorService         executor;
  private OpenLoop                loop;
  // the ramps measured since prepare: the first warmup() of them are the runner's discarded warm-ups
  private int                     measures;

  public RampWorkload() {
    this(null, WARM, MAX_COARSE, LATE_LIMIT);
  }

  /**
   * Shorter steps and a lower ceiling, so the IT ends in about a minute, and the IT's own late-start limit: a shared laptop or
   * CI host stalls the scheduler thread for a few ms at a time (measured with no server running: 0.3-0.9% of arrivals late on
   * a loaded 12-core laptop), close to the spec's 1%, which the unit tests pin. The spec's values come from the public constructor.
   */
  RampWorkload(final Duration step, final Duration warm, final int maxCoarse, final double lateLimit) {
    this.stepOverride = step;
    this.warm = warm;
    this.maxCoarse = maxCoarse;
    this.lateLimit = lateLimit;
  }

  @Override
  public String name() {
    return "ramp";
  }

  @Override
  public List<String> cases() {
    return List.of(CASE);
  }

  /**
   * Left out at S: one ramp takes about 5 minutes and the case runs four, which the weeknight S lane has no room for. The ramp
   * runs from M up, every Saturday; XS keeps it for the integration test.
   */
  @Override
  public List<String> cases(final Scale scale) {
    return scale == Scale.S ? List.of() : cases();
  }

  @Override
  public OptionalInt reps() {
    return OptionalInt.of(REPS);
  }

  @Override
  public OptionalInt warmup() {
    return OptionalInt.of(WARMUP);
  }

  /** 10 s at XS and S, 20 s at M and L, unless the IT's override says otherwise. */
  Duration stepLength(final Scale scale) {
    if (stepOverride != null)
      return stepOverride;
    return switch (scale) {
      case XS, S -> Duration.ofSeconds(10);
      case M, L -> Duration.ofSeconds(20);
    };
  }

  @Override
  public void prepare(final WorkloadContext ctx, final String caseName) {
    if (!CASE.equals(caseName))
      throw new IllegalArgumentException("Unknown case: " + caseName);
    startCase();
    closeClients();
    graph.prepare(ctx, GRAPH_CASE);
    userIds = graph.userIds();
    if (userIds == null || userIds.length == 0)
      throw new IllegalStateException("the graph has no user to look up");
    executor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("ramp-request-", 0).factory());
    clients = new RemoteDatabase[CLIENTS];
    for (int c = 0; c < CLIENTS; c++) {
      clients[c] = RemoteClients.open(ctx.server(), GraphOltpWorkload.GRAPH_DATABASE);
      // the connection is set up here, untimed, not by the first arrival of a step
      lookup(c);
    }
    for (int i = 0; i < WARM_LOOKUPS; i++)
      lookup(i % CLIENTS);
    loop = new OpenLoop(OpenLoop.SYSTEM, CLIENTS, this::lookup, executor);
  }

  @Override
  public Measurement measure(final WorkloadContext ctx, final String caseName) throws CaseFailedWithMetrics {
    final Duration length = stepLength(ctx.scale());
    // the case's own loop, so a request an earlier ramp left running still holds its client here
    final OpenLoop open = loop;
    return counted(() -> ramp(rate -> open.runStep(rate, warm, length), START_PER_S, FACTOR, maxCoarse, BISECTIONS, lateLimit));
  }

  /** One ramp, as {@link #counted} runs it. */
  interface Ramp {
    Measurement run() throws CaseFailedWithMetrics;
  }

  /** Starts counting the case's ramps afresh: the next {@link #warmup()} of them are warm-ups. */
  void startCase() {
    measures = 0;
  }

  /**
   * Runs one ramp of the case. The runner cannot tell a warm-up from a measured repetition, so the workload counts its ramps:
   * a failed warm-up ramp (a knee of 0 on a still-cold server, a client-limited knee) is logged and its metrics returned for
   * the runner to discard, instead of failing the whole case with the warm-up's numbers. A measured ramp's failure fails the
   * case.
   */
  Measurement counted(final Ramp ramp) throws CaseFailedWithMetrics {
    final boolean warmUp = measures++ < WARMUP;
    try {
      return ramp.run();
    } catch (final CaseFailedWithMetrics e) {
      if (!warmUp)
        throw e;
      LOGGER.warn("Warm-up ramp failed, not counted: {}", e.getMessage());
      return new Measurement(e.metrics());
    }
  }

  @Override
  public void teardown(final WorkloadContext ctx) {
    closeClients();
    graph.teardown(ctx);
    userIds = null;
  }

  /** One point lookup of a random user on client {@code c}; an empty answer is an error. */
  private void lookup(final int c) {
    final int id = userIds[ThreadLocalRandom.current().nextInt(userIds.length)];
    try (final ResultSet rs = clients[c].query(LANGUAGE, QUERY, Map.<String, Object>of("id", id))) {
      if (!rs.hasNext())
        throw new IllegalStateException("no row for user " + id);
      rs.next();
    }
  }

  private void closeClients() {
    loop = null;
    if (executor != null) {
      executor.shutdownNow();
      executor = null;
    }
    if (clients != null) {
      for (final RemoteDatabase db : clients)
        if (db != null)
          db.close();
      clients = null;
    }
  }

  /**
   * One ramp over {@code step}, keeping the last passing step and the last broken one. The last passing step is the knee's
   * (the search moves its passing rate only on a pass), so its late starts decide {@code client_limited}; the last broken step
   * is the one that bounded the knee (the search moves its failing rate only on a break), so it sets the break flags.
   */
  static Measurement ramp(final DoubleFunction<OpenLoop.StepResult> step, final double start, final double factor,
      final int maxCoarse, final int bisections) throws CaseFailedWithMetrics {
    return ramp(step, start, factor, maxCoarse, bisections, LATE_LIMIT);
  }

  private static Measurement ramp(final DoubleFunction<OpenLoop.StepResult> step, final double start, final double factor,
      final int maxCoarse, final int bisections, final double lateLimit) throws CaseFailedWithMetrics {
    final OpenLoop.StepResult[] last = new OpenLoop.StepResult[2];
    final RampSearch.Knee knee = RampSearch.search(rate -> {
      final OpenLoop.StepResult r = step.apply(rate);
      last[r.broke() ? 1 : 0] = r;
      return r;
    }, start, factor, maxCoarse, bisections);
    final OpenLoop.StepResult broke = last[1];
    LOGGER.info("Ramp: knee {}/s, p99 {} ms, {} steps, {}", String.format(Locale.ROOT, "%.0f", knee.ratePerS()),
        String.format(Locale.ROOT, "%.2f", knee.p99Ms()), knee.steps(), knee.ceilingReached() || broke == null ? "ceiling reached" :
            String.format(Locale.ROOT, "bounded by the %.0f/s step: %s (late starts %d of %d arrivals)", broke.ratePerS(),
                knee.breakReason(), broke.lateStarts(), broke.arrivals()));
    return publish(knee, last[0], last[1], lateLimit);
  }

  /**
   * What broke a step, as 0/1 flags in the order drop, p99, errors, unfinished; all 0 for no step (a ramp that reached its
   * ceiling).
   */
  static int[] breakFlags(final OpenLoop.StepResult step) {
    if (step == null)
      return new int[4];
    return new int[] { step.dropped() > 0 ? 1 : 0, step.p99OverLimit() ? 1 : 0, step.errorsOverLimit() ? 1 : 0,
        step.unfinished() > 0 ? 1 : 0 };
  }

  /**
   * The metrics of one ramp. Throws, the metrics attached, when the first step already broke or the knee step's late starts
   * say the client, not the server, set the knee.
   *
   * @param kneeStep  the step the knee was measured on; null when no step passed
   * @param breakStep the last step that broke, which bounded the knee; null when none did
   */
  static Measurement publish(final RampSearch.Knee knee, final OpenLoop.StepResult kneeStep, final OpenLoop.StepResult breakStep)
      throws CaseFailedWithMetrics {
    return publish(knee, kneeStep, breakStep, LATE_LIMIT);
  }

  private static Measurement publish(final RampSearch.Knee knee, final OpenLoop.StepResult kneeStep,
      final OpenLoop.StepResult breakStep, final double lateLimit) throws CaseFailedWithMetrics {
    final boolean clientLimited = kneeStep != null && kneeStep.lateStarts() > kneeStep.arrivals() * lateLimit;
    final int[] broke = breakFlags(breakStep);
    final List<Metric> metrics = new ArrayList<>(9);
    metrics.add(Metrics.rate("knee_per_s", "ops/s", knee.ratePerS()));
    metrics.add(Metrics.of("knee_p99_ms", "ms", Metric.Better.LOWER, knee.p99Ms()));
    metrics.add(Metrics.of("steps", "steps", Metric.Better.LOWER, knee.steps()));
    metrics.add(flag("client_limited", clientLimited ? 1 : 0));
    metrics.add(flag("ceiling_reached", knee.ceilingReached() ? 1 : 0));
    metrics.add(flag("break_drop", broke[0]));
    metrics.add(flag("break_p99", broke[1]));
    metrics.add(flag("break_errors", broke[2]));
    metrics.add(flag("break_unfinished", broke[3]));
    if (knee.ratePerS() == 0)
      throw new CaseFailedWithMetrics(
          String.format(Locale.ROOT, "the first step (%.0f/s) already broke: %s", START_PER_S, knee.breakReason()), metrics);
    if (clientLimited)
      throw new CaseFailedWithMetrics(String.format(Locale.ROOT,
          "client limited: %d of %d arrivals of the knee step (%.0f/s) started more than 1 ms late (over %.0f%%), the knee is the "
              + "client's, not the server's", kneeStep.lateStarts(), kneeStep.arrivals(), knee.ratePerS(), lateLimit * 100),
          metrics);
    return new Measurement(metrics);
  }

  /** An informational 0/1 metric: 1 is the state a reader should look at, and the gate never judges the unit. */
  private static Metric flag(final String name, final int value) {
    return Metrics.of(name, FLAG, Metric.Better.LOWER, value);
  }
}
