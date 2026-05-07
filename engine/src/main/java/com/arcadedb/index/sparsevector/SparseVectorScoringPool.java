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
 * <b>Wired since #4085.</b> {@code SQLFunctionVectorSparseNeighbors} fans out per-bucket
 * {@code topK} calls onto this pool when a query targets multiple buckets (partitioned types,
 * or types with multiple physical buckets). Within a single bucket's index the scoring still
 * runs serially - per-segment RID-range partitioning was investigated but deferred because the
 * absolute gain is small relative to network + serialization overhead at the latencies the
 * serial 10M benchmark already lands at. The pool is sized through the
 * {@link GlobalConfiguration#SPARSE_VECTOR_SCORING_POOL_THREADS} / {@code _QUEUE_SIZE} knobs.
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

  private final ThreadPoolExecutor executor;
  private final AtomicLong         callerRunCount       = new AtomicLong();
  // Throttle for the WARNING log emitted on saturation: at most one entry per minute. Same
  // shape as the QueryEngineManager pool's throttle - operators see one nudge in the console
  // when scoring starts queueing badly and can correlate against {@link #getPoolStats}.
  // Millisecond precision is plenty for a 60 000 ms window. Initialised to 0L (not
  // {@code Long.MIN_VALUE}) so the {@code now - last} subtraction does not overflow on the
  // first saturation event.
  private static final long        SATURATION_WARN_INTERVAL_MS = 60_000L;
  private final AtomicLong         lastSaturationWarnMs = new AtomicLong(0L);

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
          final Thread t = new Thread(r, "ArcadeDB-SparseVectorScorer-" + workerSeq.incrementAndGet());
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
