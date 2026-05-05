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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.java.JavaQueryEngine;
import com.arcadedb.query.polyglot.PolyglotQueryEngine;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.SQLScriptQueryEngine;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.*;

/**
 * JVM-wide registry of query-language engines plus the default executor any query-time
 * parallelism should fork to.
 * <p>
 * <b>"No JDK common ForkJoinPool" rule.</b> Engine and server code MUST NOT submit work to the
 * common pool (no {@code parallelStream()}, no {@code Arrays.parallelSort} on hot paths, no
 * {@code CompletableFuture.supplyAsync} / {@code runAsync} without an explicit executor). The
 * common pool is shared with user-supplied scripts (Gremlin, Polyglot, SQL functions) and has
 * no back-pressure - long-running ArcadeDB work there can starve user code, the JDK reference
 * handler, and parallel GC. Fork to this manager's {@link #getExecutorService()} (sized via
 * {@link GlobalConfiguration#QUERY_PARALLELISM_POOL_THREADS}) for general query parallelism, or
 * to a feature-specific dedicated pool for hot, fine-grained workloads (e.g. sparse-vector
 * scoring). Two pre-existing common-pool callers are tracked for migration:
 * <ul>
 *   <li>{@code GraphBatch} bulk-load uses {@code Arrays.parallelSort} on the vertex-key array.</li>
 *   <li>{@code ArcadeStateMachine.notifyInstallSnapshotFromLeader} forks the snapshot download via
 *       {@code CompletableFuture.supplyAsync} (no explicit executor).</li>
 * </ul>
 * Both run during operational events (bulk import, HA snapshot install) rather than the per-query
 * hot path; migrating them off the common pool is queued as follow-up work.
 */
public class QueryEngineManager {
  private static final QueryEngineManager                         INSTANCE        = new QueryEngineManager();
  private final        Map<String, QueryEngine.QueryEngineFactory> implementations = new HashMap<>();
  private final        ThreadPoolExecutor                          executorService;
  // Per-pool counter the {@link RejectedExecutionHandler} below increments every time the queue
  // saturates and the task falls back to the caller. ThreadPoolExecutor itself doesn't expose
  // a "rejected" count when CallerRunsPolicy is in use (CallerRuns silently runs the task on
  // the submitter's thread), so we track it ourselves to surface saturation in dashboards.
  private final        AtomicLong                                  callerRunCount   = new AtomicLong();
  // Throttle for the WARNING log emitted on saturation: at most one entry per minute, regardless
  // of how often the queue actually overflows. The metric counter ticks every event so the full
  // rate is still visible through {@link #getExecutorStats()}; the log line is the
  // operator-noticing nudge that something needs attention. Millisecond precision is plenty
  // here - the throttle window is 60 000 ms.
  // <p>
  // Initialised to {@code 0L} (not {@code Long.MIN_VALUE}!) so the first {@code now - last}
  // subtraction returns a sane "very large" positive value and the first saturation always logs.
  // Using {@code Long.MIN_VALUE} would overflow the subtraction and silently suppress the
  // first-ever log line.
  private static final long                                        SATURATION_WARN_INTERVAL_MS = 60_000L;
  private final        AtomicLong                                  lastSaturationWarnMs        = new AtomicLong(0L);

  private QueryEngineManager() {
    // Pool sizing: explicit knob first, then "as many threads as cores (min 2)". Configurable so
    // operators can cap or expand without rebuild; the previous hardcoded {@code max(2, cpuCount)}
    // is preserved as the default behaviour when the knob is left at its default of 0.
    final int configured = GlobalConfiguration.QUERY_PARALLELISM_POOL_THREADS.getValueAsInteger();
    final int maxThreads = configured > 0 ? configured : Math.max(2, Runtime.getRuntime().availableProcessors());
    // Bound the queue so a runaway producer can't OOM the JVM. CallerRunsPolicy gives us
    // graceful degradation: when the queue saturates, the submitter (which was going to block
    // waiting for the result anyway) runs the task itself - the query loses parallelism but
    // never fails. We wrap the standard CallerRunsPolicy to also tick {@link #callerRunCount}
    // for observability.
    final int queueSize = Math.max(1, GlobalConfiguration.QUERY_PARALLELISM_QUEUE_SIZE.getValueAsInteger());
    final AtomicInteger workerSeq = new AtomicInteger();
    final ThreadPoolExecutor pool = new ThreadPoolExecutor(maxThreads, maxThreads, 60L, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(queueSize), r -> {
      final Thread t = new Thread(r, "ArcadeDB-QueryWorker-" + workerSeq.incrementAndGet());
      t.setDaemon(true);
      return t;
    }, (task, executor) -> {
      final long fallbacks = callerRunCount.incrementAndGet();
      // Throttled WARNING. Compare-and-swap ensures only one thread per interval logs - the
      // others increment the counter and skip the log. The message names the affected pool
      // and the cumulative fallback count so the operator can grep their console and see at a
      // glance whether saturation is one bad burst or sustained pressure.
      final long now = System.currentTimeMillis();
      final long last = lastSaturationWarnMs.get();
      if (now - last > SATURATION_WARN_INTERVAL_MS && lastSaturationWarnMs.compareAndSet(last, now)) {
        LogManager.instance().log(this, Level.WARNING,
            "Query parallelism pool saturated: queue full (capacity=%d, threads=%d), running task on caller thread (cumulative caller-runs fallbacks=%d). "
                + "Consider raising arcadedb.queryParallelismPoolThreads or arcadedb.queryParallelismQueueSize if this persists.",
            executor.getQueue().remainingCapacity() + executor.getQueue().size(), executor.getMaximumPoolSize(), fallbacks);
      }
      if (!executor.isShutdown())
        task.run();
    });
    pool.allowCoreThreadTimeOut(true);
    executorService = pool;

    // REGISTER ALL THE SUPPORTED LANGUAGE FROM POLYGLOT ENGINE
    for (final String language : PolyglotQueryEngine.PolyglotQueryEngineFactory.getSupportedLanguages())
      register(new PolyglotQueryEngine.PolyglotQueryEngineFactory(language));

    register(new JavaQueryEngine.JavaQueryEngineFactory());
    register(new SQLQueryEngine.SQLQueryEngineFactory());
    register(new SQLScriptQueryEngine.SQLScriptQueryEngineFactory());

    // REGISTER QUERY ENGINES IF AVAILABLE ON CLASSPATH AT RUN-TIME
    register("com.arcadedb.query.opencypher.query.OpenCypherQueryEngineFactory");
    register("com.arcadedb.mongo.query.MongoQueryEngineFactory");
    register("com.arcadedb.graphql.query.GraphQLQueryEngineFactory");
    register("com.arcadedb.redis.query.RedisQueryEngineFactory");

    // REGISTER OPENCYPHER AS DEFAULT "cypher" ENGINE, SO CYPHER WORKS EVEN WITHOUT GREMLIN MODULE
    final QueryEngine.QueryEngineFactory openCypherFactory = implementations.get("opencypher");
    if (openCypherFactory != null)
      implementations.put("cypher", openCypherFactory);

    // REGISTER GREMLIN AND ITS CYPHER ENGINE (OVERRIDES "cypher" WITH GREMLIN-BASED IMPLEMENTATION IF AVAILABLE)
    register("com.arcadedb.gremlin.query.GremlinQueryEngineFactory");
  }

  public static QueryEngineManager getInstance() {
    return INSTANCE;
  }

  public void register(final String className) {
    try {

      register((QueryEngine.QueryEngineFactory) Class.forName(className).getConstructor().newInstance());
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Unable to register engine '%s' (%s)", className, e.getMessage());
    }
  }

  public void register(final QueryEngine.QueryEngineFactory impl) {
    implementations.put(impl.getLanguage().toLowerCase(Locale.ENGLISH), impl);
  }

  public QueryEngine getEngine(final String language, final DatabaseInternal database) {
    final QueryEngine.QueryEngineFactory impl = implementations.get(language.toLowerCase(Locale.ENGLISH));
    if (impl == null)
      throw new IllegalArgumentException("Query engine '" + language + "' was not found. Check your configuration");
    return impl.getInstance(database);
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  /**
   * Point-in-time snapshot of the query-parallelism pool's load. Used by metrics exporters and
   * for ad-hoc operational debugging. All five values are read under no lock and are not
   * mutually consistent (the pool may transition between reads), but each individual reading is
   * safe.
   */
  public PoolStats getExecutorStats() {
    return new PoolStats(
        executorService.getPoolSize(),
        executorService.getActiveCount(),
        executorService.getQueue().size(),
        executorService.getQueue().remainingCapacity(),
        executorService.getCompletedTaskCount(),
        callerRunCount.get());
  }

  /**
   * Snapshot of the query-parallelism pool counters at one instant.
   *
   * @param poolSize         live thread count (workers currently allocated; can be lower than
   *                         the configured max if no work has needed them yet thanks to
   *                         {@code allowCoreThreadTimeOut}).
   * @param activeThreads    threads currently running a task.
   * @param queueDepth       tasks waiting in the queue.
   * @param queueCapacityRemaining how many more tasks the queue can accept before triggering
   *                         the rejection policy. Approaching zero means saturation is imminent
   *                         and {@code callerRunFallbacks} is about to start ticking.
   * @param completedTasks   monotonically increasing count of tasks finished by pool threads
   *                         (excludes tasks that ran on the caller's thread via the rejection
   *                         policy fallback).
   * @param callerRunFallbacks number of tasks that the rejection policy redirected to the
   *                         submitter thread because the queue was full. Sustained growth means
   *                         the pool is undersized for the workload and queries are losing
   *                         parallelism (still correct, just slower).
   */
  public record PoolStats(int poolSize, int activeThreads, int queueDepth, int queueCapacityRemaining,
                          long completedTasks, long callerRunFallbacks) {
  }

  public void close() {
    executorService.shutdownNow();
  }

  public List<String> getAvailableLanguages() {
    final List<String> available = new ArrayList<>();
    for (final QueryEngine.QueryEngineFactory impl : implementations.values()) {
      available.add(impl.getLanguage());
    }
    return available;
  }
}
