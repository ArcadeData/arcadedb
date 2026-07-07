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
package com.arcadedb.query;

import com.arcadedb.GlobalConfiguration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * JVM-wide dedicated executor for the BLOCKING producer tasks of parallel bucket scans
 * ({@code FetchFromTypeExecutionStep.syncPullParallel}). Lazy singleton; daemon threads, so the
 * JVM exits cleanly even if nothing ever shuts it down.
 * <p>
 * <b>Why not the {@link QueryEngineManager} pool (issues #4948, #4950).</b> Scan producers hold
 * a pool thread for a whole bucket scan and BLOCK on the per-query bounded result queue whenever
 * the consumer is slower than the producers - which is the common case. On the shared pool this
 * had two failure modes:
 * <ul>
 * <li><b>Self-deadlock (#4948):</b> the shared pool's caller-runs rejection ran the whole bucket
 * scan synchronously on the CONSUMER thread, before the ResultSet was even returned. For any
 * bucket larger than the result queue, {@code put()} filled the queue and blocked forever: the
 * only thread that could drain it was the thread doing the {@code put()}. Under sustained load
 * the wedged HTTP workers accumulated until the server stopped answering.</li>
 * <li><b>Pool colonization (#4950):</b> a few concurrent parallel-scan queries pinned every
 * shared worker in a blocked {@code put()}, so non-blocking compute users (graph algorithms,
 * Cypher operators) stalled behind them and their p99 became coupled to the slowest scan
 * consumer in the JVM.</li>
 * </ul>
 * Isolating the blocking producers here keeps {@link QueryEngineManager} strictly for
 * non-blocking compute chunks.
 * <p>
 * <b>Why the task queue is UNBOUNDED with no caller-runs policy</b> (a deliberate deviation from
 * the "bounded queue + caller-runs" rule documented on {@link QueryEngineManager}): caller-runs
 * on a BLOCKING producer is exactly the #4948 self-deadlock, and aborting would fail queries
 * spuriously under load. The queued items are small task objects - one per bucket per in-flight
 * query, each also holding its per-worker context copy (a shallow copy of the caller's variables
 * map) - so the queue cannot grow past (concurrent queries x buckets); the actual memory
 * backpressure is enforced by each query's bounded RESULT queue, which stalls producers until
 * the consumer drains. A queued task simply starts later; progress is guaranteed because every
 * consumer drains its own queue independently of this pool. That guarantee assumes a producer
 * thread never becomes the CONSUMER of another parallel scan; today sub-steps are plain bucket
 * iterators so it cannot happen, and {@code FetchFromTypeExecutionStep} additionally degrades to a
 * sequential scan when planned on a {@link ProducerThread}, keeping the assumption structural.
 * <p>
 * <b>Operator note: concurrency ceiling.</b> Each producer occupies its thread for the whole
 * bucket scan whenever its consumer is slower (the common case), so the number of concurrently
 * PROGRESSING parallel scans is roughly (pool threads / buckets per query); further queries'
 * producers wait in the queue until threads free up - they are delayed, never deadlocked, since
 * every consumer drains independently of this pool. Producers of an abandoned (never drained nor
 * closed) ResultSet release their thread after an inactivity timeout. Size the pool with
 * {@code arcadedb.parallelScanProducerPoolThreads} if scan-heavy concurrency needs more headroom;
 * the setting is read ONCE at the pool's lazy initialization (first parallel scan in the JVM), so
 * changing it later has no effect until restart. The {@code pool=parallel_scan} queue-depth gauge
 * is the saturation signal to watch.
 * <p>
 * <b>"No JDK common ForkJoinPool" rule.</b> See the {@link QueryEngineManager} class javadoc;
 * blocking producer work belongs on its own dedicated pool, never on the common pool.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ParallelScanProducerPool {

  private static final class Holder {
    static final ParallelScanProducerPool INSTANCE = new ParallelScanProducerPool();
  }

  /**
   * Marker thread type for this pool's workers, so the planner can recognize (and refuse) a parallel scan
   * whose consumer would run ON a producer thread - the one shape that would break the pool's progress
   * guarantee (see class javadoc). Mirrors the {@code DatabaseAsyncExecutorImpl.AsyncThread} pattern.
   */
  public static final class ProducerThread extends Thread {
    private ProducerThread(final Runnable target, final String name) {
      super(target, name);
    }
  }

  /** Floor for the auto-sized thread count. */
  private static final int DEFAULT_THREADS_FLOOR = 2;

  private final ThreadPoolExecutor executor;

  private ParallelScanProducerPool() {
    // 0 = auto-size to available cores (with a floor of DEFAULT_THREADS_FLOOR). An explicit positive value
    // wins, so an operator can cap the pool on very high core-count machines where (cores) blocking
    // producers would be excessive. Negative values are coerced to auto for safety.
    final int configuredThreads = GlobalConfiguration.PARALLEL_SCAN_PRODUCER_POOL_THREADS.getValueAsInteger();
    final int threads = configuredThreads > 0
        ? configuredThreads
        : Math.max(DEFAULT_THREADS_FLOOR, Runtime.getRuntime().availableProcessors());
    final AtomicInteger workerSeq = new AtomicInteger();

    final ThreadPoolExecutor pool = new ThreadPoolExecutor(
        threads, threads,
        60L, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(), // unbounded ON PURPOSE: see class javadoc
        r -> {
          final Thread t = new ProducerThread(r, "ArcadeDB-ParallelScanProducer-" + workerSeq.incrementAndGet());
          t.setDaemon(true);
          return t;
        });
    pool.allowCoreThreadTimeOut(true);
    this.executor = pool;
  }

  public static ParallelScanProducerPool getInstance() {
    return Holder.INSTANCE;
  }

  /** The dedicated executor for blocking parallel-scan producer tasks. */
  public ExecutorService getExecutorService() {
    return executor;
  }

  /**
   * Live pool statistics for the metrics binder (Studio "Executor Pools" card).
   * {@code queueCapacityRemaining} is reported as {@code -1} (not applicable): the task queue is
   * unbounded by design, and a near-2^31 constant would read oddly next to the bounded pools.
   * {@code queueDepth} is the saturation signal for this pool.
   */
  public PoolStats getPoolStats() {
    return new PoolStats(
        executor.getPoolSize(),
        executor.getActiveCount(),
        executor.getQueue().size(),
        -1,
        executor.getCompletedTaskCount(),
        0L); // no caller-runs policy on this pool by design (see class javadoc)
  }

  public record PoolStats(int poolSize, int activeThreads, int queueDepth, int queueCapacityRemaining,
                          long completedTasks, long callerRunFallbacks) {
  }
}
