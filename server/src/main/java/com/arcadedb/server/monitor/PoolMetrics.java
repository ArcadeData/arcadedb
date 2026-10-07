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
package com.arcadedb.server.monitor;

import com.arcadedb.database.async.AsyncCommandPool;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.log.LogManager;
import com.arcadedb.index.sparsevector.SparseVectorScoringPool;
import com.arcadedb.query.ParallelScanProducerPool;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.utility.DedicatedThreadPool.PoolStats;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.logging.Level;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;

/**
 * Micrometer binding for the engine's executor pools so they show up in {@code /api/v1/metrics}
 * and the {@link io.micrometer.core.instrument.logging.LoggingMeterRegistry} alongside JVM
 * thread / GC / memory gauges. Both the {@link QueryEngineManager} pool (general query
 * parallelism) and the {@link SparseVectorScoringPool} (sparse-vector top-K fan-out) are
 * exposed under the same shape - one set of gauges per pool, tagged with {@code pool=<name>} -
 * so a Grafana panel can switch between them with a single label selector.
 * <p>
 * Lives in the server module because the engine's pom only pulls Micrometer at test scope; the
 * pools themselves expose framework-agnostic {@code PoolStats} records, and this binder
 * translates them into Micrometer gauges.
 * <p>
 * <b>Two registration paths.</b> {@link #bindTo} binds the JVM-wide singleton pools, reached through their
 * {@code getInstance()} accessors, once per metrics install. {@link #bindInstancePool} is for a pool that belongs
 * to a server or a state-machine instance instead - the security permission-refresh worker, the HA security seed
 * and catch-up workers (issue #7856) - and hands back a handle the owner closes when the instance goes away, so a
 * restart does not leave a row reading a pool that no longer exists.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class PoolMetrics implements MeterBinder {
  /**
   * Registers gauges for the {@link QueryEngineManager} pool and the
   * {@link SparseVectorScoringPool} on the given registry.
   * <p>
   * Each gauge re-reads its source pool's {@code PoolStats} record on scrape - the record is a
   * tiny allocation and Micrometer scrape intervals are typically tens of seconds, so the cost is
   * negligible.
   * <p>
   * The per-pool {@code Supplier<PoolStats>} buys MAINTENANCE, not scrape cost: every gauge still
   * calls it once, exactly as the six separate suppliers it replaced did, so the record is still
   * built once per gauge per scrape. What changes is that a new component on {@code PoolStats}
   * reaches every pool's row by being read in one place, instead of having to be threaded through
   * four call sites' argument lists - which is how the previous shape would have absorbed
   * {@code reclaimedTasks}.
   * <p>
   * {@code callerRunFallbacks} and {@code reclaimedTasks} are strictly-monotonic counters so we
   * register them as gauges that expose the cumulative count; downstream tools (Prometheus
   * {@code rate()}, etc.) can derive a rate as needed.
   */
  @Override
  public void bindTo(final MeterRegistry registry) {
    final QueryEngineManager qem = QueryEngineManager.getInstance();
    bindPool(registry, "query", "QueryEngineManager parallel-query pool", qem::getExecutorStats);

    final SparseVectorScoringPool svsp = SparseVectorScoringPool.getInstance();
    bindPool(registry, "sparse_vector", "SparseVectorScoringPool top-K fan-out pool", svsp::getPoolStats);
    bindSparseVectorSplit(registry, svsp);

    // Dedicated pool for the BLOCKING parallel-scan producers (issues #4948/#4950): unbounded task queue by
    // design (backpressure comes from each query's bounded result queue), so callerRunFallbacks is always 0
    // and queueCapacityRemaining reports -1 (not applicable); queue_depth is the signal to watch.
    final ParallelScanProducerPool pspp = ParallelScanProducerPool.getInstance();
    bindPool(registry, "parallel_scan", "ParallelScanProducerPool bucket-scan producer pool", pspp::getPoolStats);

    // The pool that runs commands dispatched with awaitResponse=false (issue #6303, item 3). Its caller-runs count is
    // the one an operator most wants to see here: a fallback means the pool was saturated and the command ran on the
    // HTTP worker that submitted it, so a client that explicitly asked NOT to wait for the answer waited for it.
    final AsyncCommandPool acp = AsyncCommandPool.getInstance();
    bindPool(registry, "async_command", "AsyncCommandPool asynchronously dispatched command/query pool",
        acp::getPoolStats);

    // Not a pool, but a graph data-integrity signal surfaced the same way. A monotonic FunctionCounter
    // (not a gauge) so dashboards can compute a rate() and alert on a sudden spike of corruption,
    // complementing the throttled WARNING already emitted by GhostEdgeReporter.
    FunctionCounter.builder("arcadedb.graph.ghost_edges_skipped", GhostEdgeReporter.class,
            c -> GhostEdgeReporter.getTotalSkipped())
        .description("Cumulative ghost (dangling) edges skipped during graph traversal since startup. "
            + "Sustained growth indicates a data-integrity anomaly, e.g. incomplete HA replication or a partially rolled-back transaction.")
        .register(registry);
  }

  /**
   * Three extra gauges on the {@code sparse_vector} row that explain the pool's own numbers
   * (issue #4085).
   * <p>
   * A sparse-vector top-K decides per query whether to split itself into RID ranges, and a query
   * that stays serial never touches the pool at all. Without these, an idle {@code sparse_vector}
   * row is ambiguous in a way an operator cannot resolve: nobody is querying, everybody is querying
   * but the load gate has switched splitting off, the queries are too small to qualify, or something
   * is broken - all four look the same. {@code queries.in_flight} separates "no queries" from "no
   * splitting", {@code queries.split} confirms splitting has ever happened, and {@code pool.reserved}
   * shows the capacity currently claimed, which is what the gate decides against.
   * <p>
   * Registered only for this pool. The other pools take work as it is handed to them and have no
   * equivalent decision to explain, so their rows leave these blank rather than reporting a
   * meaningless zero.
   */
  private static void bindSparseVectorSplit(final MeterRegistry registry, final SparseVectorScoringPool pool) {
    final Tags tags = Tags.of(Tag.of("pool", "sparse_vector"));
    Gauge.builder("arcadedb.executor.pool.reserved", () -> pool.getReservedWorkers())
        .description("Sparse-vector scoring: workers currently claimed by in-flight range splits").tags(tags)
        .register(registry);
    Gauge.builder("arcadedb.executor.queries.in_flight", () -> pool.getInFlightQueries())
        .description("Sparse-vector scoring: top-K queries executing on caller threads right now, split or serial - the "
            + "number the split gate is compared against. Once these alone can keep the pool busy, queries stop splitting "
            + "and run on their caller's thread. Deliberately excludes queries already running on a worker (the per-bucket "
            + "fan-out), which occupy capacity rather than competing for it and show up under pool.active instead; counting "
            + "them here would let one multi-bucket query read as several and gate unrelated ones.")
        .tags(tags).register(registry);
    Gauge.builder("arcadedb.executor.queries.split", () -> pool.getSplitQueryCount())
        .description("Sparse-vector scoring: cumulative top-K queries split into parallel RID ranges since startup. "
            + "Counts the decision, not the outcome: a range submitted to a full queue runs inline on the caller under the "
            + "caller-runs policy, so a query counted here can still have executed serially. Read alongside "
            + "tasks.caller_run_fallbacks, which is where that shows up. It also records only WHETHER a query split, not into "
            + "how many ranges - and width is what explains the tail-latency band the default shows at moderate concurrency, "
            + "where some queries claim a wide split and others get none. A distribution of granted partition counts is the "
            + "gauge that would show it; this one cannot.")
        .tags(tags).register(registry);
  }

  private static List<Meter> bindPool(final MeterRegistry registry, final String poolTag, final String description,
      final Supplier<PoolStats> stats) {
    final Tags tags = Tags.of(Tag.of("pool", poolTag));
    final List<Meter> meters = new ArrayList<>(8);
    meters.add(Gauge.builder("arcadedb.executor.pool.size", () -> stats.get().poolSize())
        .description(description + ": currently allocated worker threads").tags(tags).register(registry));
    meters.add(Gauge.builder("arcadedb.executor.pool.active", () -> stats.get().activeThreads())
        .description(description + ": worker threads currently running a task").tags(tags).register(registry));
    meters.add(Gauge.builder("arcadedb.executor.queue.depth", () -> stats.get().queueDepth())
        .description(description + ": tasks waiting in the queue").tags(tags).register(registry));
    meters.add(Gauge.builder("arcadedb.executor.queue.capacity_remaining", () -> stats.get().queueCapacityRemaining())
        .description(description + ": queue slots free before saturation triggers caller-runs fallback").tags(tags)
        .register(registry));
    meters.add(Gauge.builder("arcadedb.executor.tasks.completed", () -> stats.get().completedTasks())
        .description(description + ": cumulative tasks finished by pool threads").tags(tags).register(registry));
    meters.add(Gauge.builder("arcadedb.executor.tasks.caller_run_fallbacks", () -> stats.get().callerRunFallbacks())
        .description(
            description + ": cumulative tasks that ran on the submitter's thread because the queue was full. Sustained growth means the pool is undersized for the workload.")
        .tags(tags).register(registry));
    meters.add(Gauge.builder("arcadedb.executor.tasks.reclaimed", () -> stats.get().reclaimedTasks())
        .description(description + ": cumulative queued tasks taken back out of the queue and run by the thread that "
            + "was about to wait for them (issue #6568). Unlike caller_run_fallbacks this is not a sizing signal - the "
            + "pool was busy, not full - but sustained growth alongside a high pool.active says the fan-out callers are "
            + "spending their time doing the pool's work, which is the shape that used to deadlock.")
        .tags(tags).register(registry));
    return meters;
  }

  /** An instance-pool gauge: work a pool did not take because a task already queued or running covers it. */
  public static final String COALESCED_GAUGE = "arcadedb.executor.tasks.coalesced";

  /** An instance-pool gauge: work an abort-policy pool refused while running, which nothing then ran (issue #8856). */
  public static final String REJECTED_GAUGE = "arcadedb.executor.tasks.rejected";

  /** What {@link #bindInstancePool} hands back when another binding already publishes the row: owns nothing. */
  private static final Closeable NOTHING_OWNED = () -> {
  };

  /**
   * Publishes a pool that belongs to a server or a state-machine instance rather than to the JVM (issue #7856),
   * under the same {@code pool=<poolTag>} row shape as the singleton pools plus {@code tasks.coalesced}.
   * <p>
   * The pools this exists for are one-worker, one-slot executors that refuse a task when one is already queued,
   * on the ground that the queued one reads its inputs when it RUNS and therefore covers the refused one. That
   * makes a refusal coalescing rather than loss, which is why the count is published as {@code coalesced} and
   * not as {@code caller_run_fallbacks} - those pools never run anything on the submitter - but it is still the
   * number an operator wants during a burst: one climbing on a quiet node is a worker that is not draining.
   *
   * @see #bindInstancePool(MeterRegistry, String, String, Supplier, LongSupplier, LongSupplier)
   */
  public static Closeable bindInstancePool(final MeterRegistry registry, final String poolTag, final String description,
      final Supplier<PoolStats> stats, final LongSupplier coalesced) {
    return bindInstancePool(registry, poolTag, description, stats, coalesced, null);
  }

  /**
   * Publishes an instance pool with the per-pool gauges that fit its saturation semantics (issue #8856). Each pool
   * decides what its refusals mean, so each of the two extra gauges is published only where it applies, and a pool
   * that does not publish one shows "-" in Studio rather than a zero that would read as "never happened":
   * <ul>
   * <li>{@code coalesced} - a task not run because one already queued or running covers it: the design working,
   * not loss (the one-slot security workers, the HA channel-recovery database hand-off);</li>
   * <li>{@code rejected} - a task an abort-policy pool refused while running, which nothing ran in its place: loss,
   * whatever the submitter then does about it (a retry, a failed future, an operator-facing log line).</li>
   * </ul>
   * A caller-runs pool needs neither: its saturations are the shared row's {@code caller_run_fallbacks}.
   * <p>
   * <b>Every supplier is read on every scrape</b>, so it should resolve the pool through the owner rather than
   * capture an executor that the owner may replace. None may throw or block.
   * <p>
   * <b>A second binding of a tag already published owns nothing.</b> Two servers in one JVM (every in-process HA
   * test) register identical meter ids, and Micrometer answers the second registration with the first one's
   * meter. Owning it would let the second server's shutdown delete the row the first one is still publishing, so
   * the row stays the first binder's, exactly as every other {@code arcadedb.*} meter on the shared registry does.
   * The flip side, kept deliberately: when the first binder stops, the row goes with it even if a sibling still runs.
   * Handing it over would make one row silently switch to another instance's pool, whose cumulative counters restart
   * from that instance's own, and a {@code server} tag is not available - it would give the {@code arcadedb.executor.*}
   * name a tag key the singleton rows lack, which Prometheus refuses for one meter name. One server per JVM, the
   * production shape, is unaffected.
   *
   * @param coalesced the {@code tasks.coalesced} reading, or {@code null} when the pool never coalesces
   * @param rejected  the {@code tasks.rejected} reading, or {@code null} when the pool never refuses running work
   *
   * @return a handle whose {@link Closeable#close()} removes the meters this call registered; idempotent
   */
  public static Closeable bindInstancePool(final MeterRegistry registry, final String poolTag, final String description,
      final Supplier<PoolStats> stats, final LongSupplier coalesced, final LongSupplier rejected) {
    final Tags tags = Tags.of(Tag.of("pool", poolTag));
    // Keyed on a gauge every row has, not on an optional one: a pool publishing neither extra would otherwise always
    // look unpublished, and a second binding would take the first one's meters as its own.
    if (registry.find("arcadedb.executor.pool.size").tags(tags).gauge() != null) {
      LogManager.instance().log(PoolMetrics.class, Level.FINE,
          "Executor pool row '%s' is already published by another binding in this JVM; not registering it twice",
          poolTag);
      return NOTHING_OWNED;
    }

    final List<Meter> meters = bindPool(registry, poolTag, description, stats);
    if (coalesced != null)
      meters.add(Gauge.builder(COALESCED_GAUGE, coalesced::getAsLong)
          .description(description + ": cumulative tasks the pool did not run because one already queued or running "
              + "covers them, plus any refused while the owner was stopping. Harmless by itself - the covering task "
              + "reads its inputs when it runs - but a count climbing while tasks.completed does not is a worker that "
              + "is not draining.")
          .tags(tags).register(registry));
    if (rejected != null)
      meters.add(Gauge.builder(REJECTED_GAUGE, rejected::getAsLong)
          .description(description + ": cumulative tasks the pool refused because its queue was full, which nothing ran "
              + "in their place. Rejections while the owner was stopping are not counted. Any growth means work was "
              + "dropped; what that costs depends on the submitter, which logs each one.")
          .tags(tags).register(registry));

    final AtomicBoolean closed = new AtomicBoolean();
    return () -> {
      if (closed.compareAndSet(false, true))
        for (final Meter meter : meters)
          registry.remove(meter);
    };
  }

  /**
   * A {@link PoolStats} reading of a plain {@link ThreadPoolExecutor}, for the instance pools that are not
   * {@code DedicatedThreadPool}s. They never reclaim a task, so that counter is {@code 0}. Neither do they run one
   * on the submitter, unless their rejection policy is a caller-runs {@link CountingRejectionPolicy}, whose count is
   * then {@code callerRunFallbacks} (issue #8856); an abort policy's count is {@link #bindInstancePool}'s
   * {@code tasks.rejected} instead. An unbounded queue reports {@code -1} slots remaining, as the singleton pools do.
   */
  public static PoolStats statsOf(final ThreadPoolExecutor executor) {
    return statsOf(executor,
        executor.getRejectedExecutionHandler() instanceof CountingRejectionPolicy policy && policy.isCallerRuns() ?
            policy.getSaturations() :
            0L);
  }

  /**
   * As {@link #statsOf(ThreadPoolExecutor)}, for a pool that falls back to running a task on the submitter outside its
   * rejection policy and counts that itself.
   */
  public static PoolStats statsOf(final ThreadPoolExecutor executor, final long callerRunFallbacks) {
    final int depth = executor.getQueue().size();
    final int remaining = executor.getQueue().remainingCapacity();
    // An unbounded queue's remaining capacity is Integer.MAX_VALUE minus what it holds, not MAX_VALUE itself: compare
    // the total, or a queue holding one task would report two billion free slots.
    final boolean unbounded = (long) depth + remaining >= Integer.MAX_VALUE;
    return new PoolStats(executor.getPoolSize(), executor.getActiveCount(), depth, unbounded ? -1 : remaining,
        executor.getCompletedTaskCount(), callerRunFallbacks, 0L);
  }
}
