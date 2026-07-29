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
package com.arcadedb.index.sparsevector;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * JVM-wide dedicated executor for per-segment parallel scoring of {@code LSM_SPARSE_VECTOR}
 * top-K queries. Lazy singleton; daemon threads, so the JVM exits cleanly even if no caller
 * ever invokes {@link #close()}.
 * <p>
 * <b>Why a dedicated pool, not {@code QueryEngineManager}'s.</b> Sparse-vector scoring tasks
 * are fine-grained: each task scores a few thousand postings against the query in tens of
 * milliseconds. Long-running graph algorithms (PageRank, connected components) run for seconds
 * on the same {@code QueryEngineManager} pool; mixing the two produces bad-neighbour latency
 * for both - scoring queries queue behind graph chunks, and graph algorithms queue behind
 * waves of scoring tasks. Isolating into its own pool sized for fast, frequent fan-out keeps
 * each workload's tail latency predictable.
 * <p>
 * <b>"No JDK common ForkJoinPool" rule.</b> The common pool is shared with user-supplied
 * scripts (Gremlin, Polyglot, custom SQL functions) and has no back-pressure; long-running
 * engine work there can starve user code, the JDK reference handler, and parallel GC. Sparse
 * scoring belongs here, not on {@code ForkJoinPool.commonPool()}. See
 * {@link com.arcadedb.query.QueryEngineManager} class javadoc for the full rule.
 * <p>
 * <b>Graceful degradation.</b> The queue is bounded; on saturation the rejection handler runs
 * the task inline on the submitter's thread instead of throwing, so a scoring fan-out facing a
 * saturated pool degrades to single-threaded execution per chunk - loses parallelism, but always
 * returns a correct top-K. The fallback count is exposed via {@link #getPoolStats()} so
 * dashboards can surface saturation.
 * <p>
 * <b>Two kinds of work land here (#4085).</b> {@code SQLFunctionVectorSparseNeighbors} fans out
 * per-bucket {@code topK} calls when a query targets multiple buckets (partitioned types, or types
 * with multiple physical buckets), and {@link PaginatedSparseVectorEngine#topK} splits a single
 * index's traversal into RID ranges. The two never nest: a fan-out already running on a worker will
 * not split again, which {@link #isPoolThread()} enforces, because a nested fan-out on a bounded
 * queue deadlocks rather than degrading.
 * <p>
 * <b>Range splitting is not free, which is why it is adaptive.</b> Each range prunes against its own
 * top-K watermark instead of the global one, so it does more work than its share - about 1.9x total
 * CPU for an 8-way split on a learned-sparse corpus. A query claims the workers it wants up front
 * ({@link #tryReserveWorkers(int)}), and the claim is refused once the queries already in flight can
 * keep the pool's worth of threads busy by themselves. Measured on an 18-worker box at 500k
 * documents: one client 13.7 -> 3.1 ms p50 with every query split, four clients 13.6 -> 4.1 ms and
 * throughput up 61%, sixteen clients within 5% of serial throughput with 2 queries out of 8289
 * split. So the split shows up when there are cores going spare and steps out of the way when there
 * are not. The pool is sized
 * through the {@link GlobalConfiguration#SPARSE_VECTOR_SCORING_POOL_THREADS} / {@code _QUEUE_SIZE}
 * knobs, and the split through {@code SPARSE_VECTOR_SCORING_MAX_PARTITIONS} /
 * {@code _MIN_POSTINGS_FOR_PARTITIONING}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class SparseVectorScoringPool {

  // Lazy-init via the initialization-on-demand holder idiom. The pool only allocates its
  // ThreadPoolExecutor when something actually calls {@link #getInstance()} - which today is
  // either the server-side {@link com.arcadedb.server.monitor.PoolMetrics} binder (when a
  // server starts), or a future caller in PaginatedSparseVectorEngine.topK once #4085 wires
  // the dispatch. Embedded JVMs that never touch sparse vectors pay zero allocations for this
  // class. The holder class is not loaded until the {@code getInstance} call inside
  // {@link Holder} runs, which the JLS guarantees is thread-safe and at-most-once.
  private static final class Holder {
    static final SparseVectorScoringPool INSTANCE = new SparseVectorScoringPool();
  }

  /** Floor for the auto-sized thread count when {@code SPARSE_VECTOR_SCORING_POOL_THREADS=0}. */
  private static final int DEFAULT_THREADS_FLOOR = 2;

  /**
   * How much caller-side concurrency is treated as enough to keep the pool busy without help:
   * splitting stops once {@code inFlightQueries * this > poolSize}, i.e. at half the pool's worth of
   * concurrent queries.
   * <p>
   * A literal rather than a setting, deliberately. It shapes only the middle of the range - an
   * operator who wants splitting off, or always on, already has
   * {@link GlobalConfiguration#SPARSE_VECTOR_SCORING_MAX_PARTITIONS} for both - so exposing it would
   * add a knob that is hard to reason about and easy to set wrongly. Measured at 1, 4 and 16
   * concurrent clients on an 18-worker pool: full split when idle, still a 61% throughput gain at
   * four, and within 5% of serial at sixteen. If a machine is found where that shape is wrong, this
   * is the number to revisit, and it is named so it can be found.
   */
  private static final int CALLER_LOAD_GATE_FACTOR = 2;

  /**
   * How far the ranges of concurrently forced splits may oversubscribe the pool before the setting is
   * assumed to be working against the query that asked for it.
   * <p>
   * Mechanistic rather than a client count: {@code inFlight * partitions} ranges competing for
   * {@code ceiling} workers is when a query's own ranges start queueing behind its neighbours', which
   * is what turns the split from a latency win into a latency loss. Two independent harnesses put the
   * crossover at this factor - between 4 and 8 clients forcing 8 ranges on 18 workers, and around 4
   * clients forcing 8 on 12 - and it is a rough shape rather than a prediction, which is why it drives
   * only a log line and never a decision. The crossover itself is hardware and workload specific and
   * is deliberately not quoted in the setting's documentation.
   */
  private static final int SELF_DEFEATING_OVERSUBSCRIPTION = 2;

  /**
   * Marks a thread as belonging to this pool. Read by {@link #isPoolThread()} to stop a fan-out
   * from nesting inside another one, which on a pool with a bounded queue is a deadlock rather
   * than a slowdown: the outer tasks occupy every worker and block waiting on inner tasks that sit
   * in the queue with nobody left to run them. The caller-runs rejection policy does not save it,
   * because the queue accepts long before it rejects.
   * <p>
   * Set on the worker itself rather than per task so it survives whatever the task does, and so a
   * task that spawns further work inherits nothing it should not.
   */
  private static final ThreadLocal<Boolean> POOL_THREAD = new ThreadLocal<>();

  private final ThreadPoolExecutor executor;
  /** Workers claimed by in-flight range fan-outs; see {@link #tryReserveWorkers(int)}. */
  private final AtomicInteger      reservedWorkers      = new AtomicInteger();
  /** Caller-thread top-K calls in flight JVM-wide, split or not; see {@link #queryStarted()}. */
  private final AtomicInteger      inFlightQueries      = new AtomicInteger();
  /** Top-K calls running on a worker, i.e. the per-bucket fan-out. Occupy capacity, never split. */
  private final AtomicInteger      poolThreadQueries    = new AtomicInteger();
  /** Cumulative queries that were split into RID ranges rather than run on the caller thread. */
  private final AtomicLong         splitQueries         = new AtomicLong();
  private final AtomicLong         callerRunCount       = new AtomicLong();
  // Throttle for the WARNING log emitted on saturation: at most one entry per minute. Same
  // shape as the QueryEngineManager pool's throttle - operators see one nudge in the console
  // when scoring starts queueing badly and can correlate against {@link #getPoolStats}.
  // Millisecond precision is plenty for a 60 000 ms window. Initialised to 0L (not
  // {@code Long.MIN_VALUE}) so the {@code now - last} subtraction does not overflow on the
  // first saturation event.
  private static final long        SATURATION_WARN_INTERVAL_MS = 60_000L;
  private final AtomicLong         lastSaturationWarnMs = new AtomicLong(0L);
  private final AtomicLong         lastExplicitSplitWarnMs = new AtomicLong(0L);

  private SparseVectorScoringPool() {
    // 0 = auto-size to available cores (with a floor of {@link #DEFAULT_THREADS_FLOOR}). Any
    // explicit positive value wins, so an operator can pin the pool size to e.g. half the cores
    // on a box that also runs Gremlin / Polyglot scripts that compete for CPU. Negative values
    // are coerced to the floor for safety; the configuration validator already rejects them at
    // GlobalConfiguration parse time, but the coercion here makes test-side bypasses safe too.
    // A negative configured value silently falling back to a default is exactly the kind of
    // misconfiguration that hides bad sizing - log it at WARNING so operators see something
    // when their setting did not stick.
    final int configuredThreads = GlobalConfiguration.SPARSE_VECTOR_SCORING_POOL_THREADS.getValueAsInteger();
    if (configuredThreads < 0)
      LogManager.instance().log(this, Level.WARNING,
          "Sparse-vector scoring pool: negative configured thread count (%d), falling back to auto-size (max(%d, cores))",
          configuredThreads, DEFAULT_THREADS_FLOOR);
    final int threads = configuredThreads > 0
        ? configuredThreads
        : Math.max(DEFAULT_THREADS_FLOOR, Runtime.getRuntime().availableProcessors());
    final int configuredQueueSize = GlobalConfiguration.SPARSE_VECTOR_SCORING_QUEUE_SIZE.getValueAsInteger();
    if (configuredQueueSize < 0)
      LogManager.instance().log(this, Level.WARNING,
          "Sparse-vector scoring pool: negative configured queue size (%d), falling back to default 1024",
          configuredQueueSize);
    final int queueSize = configuredQueueSize > 0 ? configuredQueueSize : 1024;
    final AtomicInteger workerSeq = new AtomicInteger();

    final ThreadPoolExecutor pool = new ThreadPoolExecutor(
        threads, threads,
        60L, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(queueSize),
        r -> {
          final Thread t = new Thread(() -> {
            POOL_THREAD.set(Boolean.TRUE);
            r.run();
          }, "ArcadeDB-SparseVectorScorer-" + workerSeq.incrementAndGet());
          t.setDaemon(true);
          return t;
        },
        // CallerRunsPolicy with a fallback counter and throttled WARNING. When the queue is full
        // we run the task on the submitter; the alternative (throwing RejectedExecutionException)
        // would force every caller to also implement a fallback path, which is more error-prone
        // than just doing it once here. The log line nudges operators that the pool needs to
        // grow; sizing knobs return alongside the dispatch wiring (see follow-up #4085).
        (task, exec) -> {
          final long fallbacks = callerRunCount.incrementAndGet();
          final long now = System.currentTimeMillis();
          final long last = lastSaturationWarnMs.get();
          if (now - last > SATURATION_WARN_INTERVAL_MS && lastSaturationWarnMs.compareAndSet(last, now)) {
            LogManager.instance().log(this, Level.WARNING,
                "Sparse-vector scoring pool saturated: queue full (capacity=%d, threads=%d), running task on caller thread (cumulative caller-runs fallbacks=%d).",
                exec.getQueue().remainingCapacity() + exec.getQueue().size(), exec.getMaximumPoolSize(), fallbacks);
          }
          if (!exec.isShutdown())
            task.run();
          else if (task instanceof RunnableFuture<?> future)
            // #4961: a task rejected because the pool is shut down would otherwise be neither run
            // nor completed, leaving any caller blocked in an untimed Future.get() hanging forever.
            future.cancel(false);
        });
    pool.allowCoreThreadTimeOut(true);
    this.executor = pool;
  }

  public static SparseVectorScoringPool getInstance() {
    return Holder.INSTANCE;
  }

  /**
   * The dedicated executor. Callers that fork query work for parallel scoring should submit
   * here; the executor's bounded queue + caller-runs rejection policy handles back-pressure
   * automatically, so callers do not need a "if pool is full, do it inline" branch of their own.
   */
  public ExecutorService getExecutorService() {
    return executor;
  }

  /**
   * Configured number of threads for parallel scoring fan-out. Callers can read this to size
   * a single query's split count - forking more chunks than the pool has threads is wasteful;
   * forking fewer leaves cores idle. Returns the configured ceiling, not the live worker count
   * ({@link ThreadPoolExecutor#getPoolSize()} may be lower if no scoring has happened yet).
   */
  public int getMaxParallelism() {
    return executor.getMaximumPoolSize();
  }

  /**
   * True when the calling thread is one of this pool's workers, i.e. when it is already executing a
   * fan-out task. Callers use it to refuse to fan out again - see {@link #POOL_THREAD}. Also true on
   * a thread that ran a task inline through the caller-runs policy only if that thread is itself a
   * worker; a plain caller running a rejected task stays a non-pool thread, which is correct: it has
   * a whole worker's worth of nothing else to do.
   */
  public static boolean isPoolThread() {
    return Boolean.TRUE.equals(POOL_THREAD.get());
  }

  /**
   * Whether the queries already in flight on caller threads can keep this pool's worth of threads
   * busy without any help, which is the point at which splitting stops paying for itself.
   * <p>
   * Deliberately the only place this comparison is written. Both the gate that refuses a split and
   * the warning that reports one granted anyway ask this question, and they have to agree by
   * construction rather than by two authors keeping two expressions in step.
   */
  private boolean callersAloneCanSaturate() {
    return inFlightQueries.get() * CALLER_LOAD_GATE_FACTOR > executor.getMaximumPoolSize();
  }

  /**
   * Claim up to {@code desired} workers for a range fan-out, returning how many were actually granted
   * (possibly 0). The caller must hand back exactly what it was granted with
   * {@link #releaseWorkers(int)}.
   * <p>
   * <b>Why a reservation instead of just reading how busy the pool is.</b> Splitting a query buys
   * latency by burning extra CPU - each range prunes against its own weaker watermark - so it is only
   * worth doing with capacity nobody else wants. Sizing that from {@link ThreadPoolExecutor#getActiveCount()}
   * looked sufficient and measured otherwise: at 16 concurrent clients on an 18-worker pool it still
   * split 57% of queries and gave up 14% of throughput, because every client samples an
   * instantaneously-idle pool at the same moment and they all decide to split. A reservation is a
   * claim rather than an observation, so the capacity a query is about to use is already gone from
   * the next query's view of it.
   */
  public int tryReserveWorkers(final int desired) {
    if (desired <= 0)
      return 0;
    final int ceiling = executor.getMaximumPoolSize();
    // Callers are the load the pool cannot see. A query runs on its caller's thread, so N concurrent
    // queries already occupy N threads before a single task is submitted, and getActiveCount() stays
    // near zero right up until the machine is full. Measured on an 18-worker box: at 16 concurrent
    // clients the pool looks idle at every sampling instant, splits a third of the queries anyway,
    // and gives up throughput for it. Refusing to split once the callers alone can keep the pool's
    // worth of threads busy is what makes the default safe under load, and it costs nothing when
    // idle - one caller against 18 workers splits all the way.
    if (callersAloneCanSaturate())
      return 0;
    // Work on the pool that never went through a reservation - the per-bucket fan-out submits
    // straight to the executor - still occupies workers, so it counts against what is grantable or a
    // split would oversubscribe against it.
    //
    // Counted rather than read from ThreadPoolExecutor.getActiveCount(), which looks like the
    // obvious source and is a trap: it takes the pool's main lock and walks the worker set, so every
    // query serializes on it. Measured at 16 concurrent clients, with splitting almost entirely
    // gated off, it cost 29% of throughput on its own - a decision path more expensive than the
    // decision. Two atomic reads do the same job.
    final int onPool = poolThreadQueries.get();
    while (true) {
      final int current = reservedWorkers.get();
      final int free = ceiling - current - onPool;
      if (free <= 0)
        return 0;
      final int grant = Math.min(desired, free);
      if (reservedWorkers.compareAndSet(current, current + grant))
        return grant;
    }
  }

  /**
   * Registers a sparse-vector top-K as in flight, whatever shape it ends up taking. Callers must
   * pair it with {@link #queryFinished()}. Counting queries rather than tasks is the point: the
   * concurrency that matters for the split decision arrives on caller threads, which never touch
   * this pool unless a query decides to fan out.
   * <p>
   * <b>Queries already running on a worker are counted separately.</b> The per-bucket fan-out submits
   * one {@code topK} per bucket, so a single user query against an 8-bucket type would otherwise
   * register as eight and trip the load gate for unrelated queries on an otherwise quiet box. Those
   * eight are real load, but of a different kind: they occupy workers, which
   * {@link #tryReserveWorkers(int)} subtracts from what it can grant, whereas the caller-thread count
   * measures the load nothing else can see. Splitting the two keeps each honest. The pairing stays
   * symmetric either way, since a thread's pool membership never changes.
   */
  public void queryStarted() {
    if (isPoolThread())
      poolThreadQueries.incrementAndGet();
    else
      inFlightQueries.incrementAndGet();
  }

  public void queryFinished() {
    if (isPoolThread())
      poolThreadQueries.decrementAndGet();
    else
      inFlightQueries.decrementAndGet();
  }

  /** Sparse-vector top-K calls currently executing across the JVM. Test and diagnostics hook. */
  public int getInFlightQueries() {
    return inFlightQueries.get();
  }

  /**
   * Records that a query was split into RID ranges. Counted here rather than only per index so an
   * operator can tell "nothing is splitting" apart from "nothing is querying" - the two look
   * identical on the pool's own gauges, since a query that stays serial never touches this pool.
   */
  public void querySplit() {
    splitQueries.incrementAndGet();
  }

  /** Cumulative queries split into RID ranges since JVM start. */
  public long getSplitQueryCount() {
    return splitQueries.get();
  }

  /**
   * Throttled WARNING for a split that only happened because an operator configured an explicit
   * partition count, at a moment the adaptive default would have refused it.
   * <p>
   * Behaviour is unchanged - an explicit setting is a deliberate "do not throttle me" and is still
   * honoured. The point is that the setting is JVM-wide and long-lived, so whoever configured it
   * months ago is rarely the person watching latency today; without a signal, the trade it makes
   * (throughput for latency, under exactly the load where that hurts) is invisible. Same 60-second
   * throttle as the saturation warning, so a busy server gets one nudge rather than a stream.
   */
  public void warnExplicitSplitUnderLoad(final int partitions) {
    final int ceiling = executor.getMaximumPoolSize();
    final int inFlight = inFlightQueries.get();
    // Two independent reasons to speak, and the contention one alone is not enough.
    //
    // callersAloneCanSaturate is "the adaptive default would have refused this", asked through the
    // same predicate the gate uses rather than a copy of it, so tuning the gate cannot make this
    // warning lie. But it only trips once callers alone could keep the pool busy - half the pool's
    // worth of queries - and the measured point where a forced split starts making the FORCING
    // query slower arrives earlier than that. On an 18-thread pool the gate needs 10 queries in
    // flight; the crossover sat between 4 and 8. So a purely contention-based trigger stays silent
    // through the region where the setting has already turned against the person who set it.
    final boolean wouldHaveBeenRefused = callersAloneCanSaturate();
    final boolean defeatsItself =
        (long) inFlight * partitions > (long) ceiling * SELF_DEFEATING_OVERSUBSCRIPTION;
    if (!wouldHaveBeenRefused && !defeatsItself)
      return;
    final long now = System.currentTimeMillis();
    final long last = lastExplicitSplitWarnMs.get();
    if (now - last > SATURATION_WARN_INTERVAL_MS && lastExplicitSplitWarnMs.compareAndSet(last, now))
      LogManager.instance().log(this, Level.WARNING,
          "Sparse-vector top-K split into %d ranges by an explicit %s=%d with %d queries in flight (including this one) on a "
              + "%d-thread pool. The adaptive default (0) would have kept this query on its caller thread, and past a handful of "
              + "concurrent queries a forced split stops helping anything at all: measured at 16 concurrent clients on an 18-thread "
              + "pool, a forced 8-way split returned 0.52x the throughput of no split, a median 1.85x worse AND a p99 2.5x worse, for "
              + "1.9x the CPU per query. There is no regime above light concurrency where this setting wins - not median, not "
              + "throughput, not tail. Set %s=0 to let the engine decide per query.",
          partitions, GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.getKey(),
          GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.getValueAsInteger(), inFlight, ceiling,
          GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.getKey(),
          GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.getKey());
  }

  /**
   * Claim for an operator who configured an explicit partition count: granted whether or not the pool
   * is busy, but never beyond what the pool can actually hold.
   * <p>
   * Saturates at the pool ceiling rather than adding blindly. An explicit setting means "do not
   * throttle me", and it still does not - the caller splits into the ranges it asked for whatever
   * this returns, including 0. What it must not do is <i>claim</i> capacity that does not exist: the
   * count is what every other query's gate reads, and a query recording four times the pool's worth
   * of workers would stop every concurrent query from splitting for its whole duration, far beyond
   * the contention it actually causes. Past the ceiling the extra ranges are queued, not running, so
   * counting them would describe parallelism the machine is not providing.
   *
   * @return the number actually recorded, which the caller must hand back with
   *         {@link #releaseWorkers(int)} - not the number it asked for.
   */
  public int reserveWorkers(final int count) {
    if (count <= 0)
      return 0;
    final int ceiling = executor.getMaximumPoolSize();
    while (true) {
      final int current = reservedWorkers.get();
      final int granted = Math.min(count, ceiling - current);
      if (granted <= 0)
        return 0;
      if (reservedWorkers.compareAndSet(current, current + granted))
        return granted;
    }
  }

  /** Hands back workers claimed through {@link #tryReserveWorkers} / {@link #reserveWorkers}. */
  public void releaseWorkers(final int count) {
    if (count > 0)
      reservedWorkers.addAndGet(-count);
  }

  /** Workers currently claimed by in-flight range fan-outs. Test and diagnostics hook. */
  public int getReservedWorkers() {
    return reservedWorkers.get();
  }

  public PoolStats getPoolStats() {
    return new PoolStats(
        executor.getPoolSize(),
        executor.getActiveCount(),
        executor.getQueue().size(),
        executor.getQueue().remainingCapacity(),
        executor.getCompletedTaskCount(),
        callerRunCount.get());
  }

  /**
   * Snapshot of the scoring pool's load at one instant. Same shape as
   * {@link com.arcadedb.query.QueryEngineManager.PoolStats} so a single dashboard can render
   * both. Sustained growth in {@code callerRunFallbacks} signals that scoring is queueing
   * faster than the pool can drain it - bump
   * {@link GlobalConfiguration#SPARSE_VECTOR_SCORING_POOL_THREADS} or
   * {@link GlobalConfiguration#SPARSE_VECTOR_SCORING_QUEUE_SIZE} to absorb the burst.
   */
  public record PoolStats(int poolSize, int activeThreads, int queueDepth, int queueCapacityRemaining,
                          long completedTasks, long callerRunFallbacks) {
  }

  /**
   * Best-effort shutdown for tests and tooling. Production lifecycle relies on the daemon-thread
   * default: workers die when the JVM exits, no explicit close is required.
   */
  public void close() {
    executor.shutdownNow();
  }
}
