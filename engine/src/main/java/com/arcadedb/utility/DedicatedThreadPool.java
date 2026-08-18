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
package com.arcadedb.utility;

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
 * The shape every JVM-wide dedicated engine executor has in common: the {@link ThreadPoolExecutor} itself, the
 * {@link PoolStats} the metrics binder reads, and the caller-runs rejection policy with its throttled saturation
 * WARNING (issue #6324, item 4).
 * <p>
 * <b>Why it exists.</b> {@code QueryEngineManager}, {@code SparseVectorScoringPool}, {@code ParallelScanProducerPool}
 * and {@code AsyncCommandPool} each carried a near-identical copy of all three, down to a {@code PoolStats} record
 * with the same six components in the same order. The cost had already shown up as drift rather than as a
 * hypothetical: three copies of the saturation warning, three different sentences, one of which forgot to say which
 * settings to raise. More usefully than the ~100 lines it removes, this is what lets the {@code engine-concurrency}
 * skill's checklist for a new pool be ENFORCED instead of remembered - a pool that extends this has a bounded queue,
 * caller-runs, a throttled warning and a {@code PoolStats} to bind, or it says in its own constructor which of those
 * it is deliberately not doing.
 * <p>
 * <b>What it deliberately does not own.</b> The pools' reasons for existing, which is everything interesting about
 * them: why blocking producers may not share a pool with non-blocking compute, why a dispatched DDL statement cannot
 * run on an async worker, why a sparse-vector fan-out must not nest. Those stay in each subclass's javadoc, where a
 * reader looking for that pool will find them.
 * <p>
 * <b>"No JDK common ForkJoinPool" rule.</b> See {@code com.arcadedb.query.QueryEngineManager}'s class javadoc. Every
 * pool built here exists because of it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class DedicatedThreadPool {

  /**
   * The queue capacity that asks for an UNBOUNDED task queue, which also implies {@link SaturationPolicy#NONE}: a
   * queue that never rejects has nothing to hand back to a caller. Only {@code ParallelScanProducerPool} uses it, and
   * its class javadoc carries the argument for the deviation.
   */
  public static final int UNBOUNDED_QUEUE = 0;

  /** Floor for the auto-sized thread count, i.e. what {@link #autoSizeThreads(int)} gives on a single-core box. */
  public static final int DEFAULT_THREADS_FLOOR = 2;

  /** Default bounded-queue capacity for a pool whose sizing setting is left at 0. */
  public static final int DEFAULT_QUEUE_SIZE = 1024;

  /**
   * At most one WARNING per minute per pool per subject. The counter behind {@link PoolStats#callerRunFallbacks()}
   * ticks on every saturation event, so the full rate stays visible through the metrics; the log line is only the
   * nudge that makes an operator go and look.
   * <p>
   * Visible to subclasses because a pool with a periodic warning of its own throttles it the same way and to the same
   * window, and two windows drifting apart is the kind of small inconsistency an operator reads as a clue.
   */
  protected static final long WARN_THROTTLE_INTERVAL_MS = 60_000L;

  protected final ThreadPoolExecutor executor;

  private final SaturationPolicy saturationPolicy;
  /**
   * The whole saturation WARNING as a format string, composed ONCE at construction rather than assembled per event.
   * Two reasons, and the second is the one that matters: a saturating pool is the last place to be concatenating
   * strings, and the pool's name has to be LITERAL text in the message rather than an argument, or an operator
   * grepping their console for "Query parallelism pool saturated" - and a log-capturing test doing the same - finds
   * a bare "%s saturated" instead. Takes exactly three arguments: queue capacity, thread count, cumulative fallbacks.
   */
  private final String     saturationWarnMessage;
  private final boolean    boundedQueue;
  private final AtomicLong callerRunCount       = new AtomicLong();
  /**
   * Initialised to {@code 0L} and not {@code Long.MIN_VALUE}: the throttle below subtracts it from
   * {@code System.currentTimeMillis()}, and {@code MIN_VALUE} would overflow that subtraction and silently suppress
   * the first-ever warning - the one that matters most.
   */
  private final AtomicLong lastSaturationWarnMs = new AtomicLong(0L);

  // The nested types below are declared AFTER the fields, not at the top where the idiom usually puts them: PMD's
  // FieldDeclarationsShouldBeAtStartOfClass counts a nested type as the end of the field section and reports every
  // field that follows one. Same reason AsyncCommandPool keeps its Holder down here.

  /** What a pool does with a task its bounded queue refused. */
  public enum SaturationPolicy {
    /**
     * Run it on the thread that submitted it, unless the pool is shut down - a task rejected by a shut-down pool is
     * neither run nor completed, so any caller blocked in an untimed {@code Future.get()} would hang for ever
     * (issue #4961); cancelling its future ends the wait instead.
     */
    CALLER_RUNS,
    /**
     * Run it on the submitting thread ALWAYS, shut-down pool included. For a pool whose tasks hand back no future
     * there is nothing to cancel and nothing to return, so a task that is neither run nor completed leaves the
     * submitter's in-flight count raised for ever.
     */
    CALLER_RUNS_EVEN_WHEN_SHUT_DOWN,
    /** No rejection handler at all. Legal only with {@link #UNBOUNDED_QUEUE}, which never rejects. */
    NONE
  }

  /** Creates one of a pool's worker threads. The body is already wrapped with whatever the pool sets per worker. */
  @FunctionalInterface
  public interface WorkerFactory {
    Thread newWorker(Runnable body, String name);
  }

  /**
   * Point-in-time snapshot of a pool's load, read by the metrics binder ({@code pool=<name>} gauges, Studio's
   * "Executor Pools" card) and by tests. The six values are read under no lock and are not mutually consistent - the
   * pool may transition between two of them - but each individual reading is safe.
   *
   * @param poolSize               live thread count. Can be below the configured maximum when no work has needed the
   *                               rest yet, since every pool here allows core threads to time out
   * @param activeThreads          threads currently running a task
   * @param queueDepth             tasks waiting in the queue. The saturation signal for an unbounded pool, which has
   *                               no capacity to run out of
   * @param queueCapacityRemaining how many more tasks the queue accepts before the rejection policy takes over, or
   *                               {@code -1} for an unbounded queue - not a near-2^31 constant, which would read
   *                               oddly next to the bounded pools
   * @param completedTasks         cumulative tasks finished BY POOL THREADS, so a task that ran on its submitter
   *                               under caller-runs is counted by the next field and not by this one
   * @param callerRunFallbacks     cumulative tasks the rejection policy redirected to the submitting thread. Sustained
   *                               growth means the pool is undersized for the workload
   */
  public record PoolStats(int poolSize, int activeThreads, int queueDepth, int queueCapacityRemaining,
                          long completedTasks, long callerRunFallbacks) {
  }

  /**
   * @param threadNamePrefix      worker thread names are this plus a per-pool sequence number
   * @param threads               core = max thread count, already resolved (see {@link #autoSizeThreads(int)})
   * @param queueCapacity         bounded-queue capacity, or {@link #UNBOUNDED_QUEUE}
   * @param saturationPolicy      what happens to a task the queue refuses
   * @param workerFactory         creates each worker, so a pool can subclass {@link Thread} (to be recognizable by
   *                              type) or set a thread-local on the worker itself rather than per task
   * @param poolDescription       how the pool names itself in its saturation warning, e.g. "Query parallelism pool"
   * @param saturationConsequence what the caller-runs fallback costs THIS pool's callers, appended to the warning, or
   *                              null when running the task inline costs nothing worth naming
   * @param sizingSettingKeys     the settings the warning tells the operator to raise
   */
  protected DedicatedThreadPool(final String threadNamePrefix, final int threads, final int queueCapacity,
      final SaturationPolicy saturationPolicy, final WorkerFactory workerFactory, final String poolDescription,
      final String saturationConsequence, final GlobalConfiguration... sizingSettingKeys) {
    if (queueCapacity == UNBOUNDED_QUEUE && saturationPolicy != SaturationPolicy.NONE)
      throw new IllegalArgumentException(
          "An unbounded queue never rejects, so it cannot have the " + saturationPolicy + " saturation policy");
    if (queueCapacity != UNBOUNDED_QUEUE && saturationPolicy == SaturationPolicy.NONE)
      throw new IllegalArgumentException(
          "A bounded queue must say what happens to the task it refuses: " + threadNamePrefix + " has no policy");

    this.saturationPolicy = saturationPolicy;
    this.boundedQueue = queueCapacity != UNBOUNDED_QUEUE;
    this.saturationWarnMessage = poolDescription
        + " saturated: queue full (capacity=%d, threads=%d), running the task on the submitting thread"
        + (saturationConsequence != null ? " - " + saturationConsequence : "")
        + " (cumulative caller-runs fallbacks=%d)." + raiseSettingsAdvice(sizingSettingKeys);

    final AtomicInteger workerSeq = new AtomicInteger();
    final ThreadPoolExecutor pool = new ThreadPoolExecutor(threads, threads, 60L, TimeUnit.SECONDS,
        boundedQueue ? new LinkedBlockingQueue<>(queueCapacity) : new LinkedBlockingQueue<>(),
        r -> {
          final Thread t = workerFactory.newWorker(r, threadNamePrefix + workerSeq.incrementAndGet());
          t.setDaemon(true);
          return t;
        });
    if (saturationPolicy != SaturationPolicy.NONE)
      // Set after construction rather than through the constructor so the handler can capture `this`: the executor
      // has to exist before a lambda that reads this pool's counters can be built.
      pool.setRejectedExecutionHandler((task, exec) -> runOnCaller(task, queueCapacity, exec.getMaximumPoolSize()));
    pool.allowCoreThreadTimeOut(true);
    this.executor = pool;
  }

  /**
   * The auto-sizing every one of these pools shares: an explicit positive setting wins, anything else means "as many
   * threads as cores, and never fewer than {@link #DEFAULT_THREADS_FLOOR}". A negative value is coerced rather than
   * refused - {@code GlobalConfiguration} already rejects one at parse time, and the coercion keeps a test-side
   * bypass safe.
   */
  public static int autoSizeThreads(final int configuredThreads) {
    return configuredThreads > 0 ?
        configuredThreads :
        Math.max(DEFAULT_THREADS_FLOOR, Runtime.getRuntime().availableProcessors());
  }

  /** The bounded-queue counterpart of {@link #autoSizeThreads(int)}: an explicit positive value, or the default. */
  public static int queueSizeOrDefault(final int configuredQueueSize) {
    return configuredQueueSize > 0 ? configuredQueueSize : DEFAULT_QUEUE_SIZE;
  }

  /** The default worker: a plain daemon thread. */
  public static Thread plainWorker(final Runnable body, final String name) {
    return new Thread(body, name);
  }

  /**
   * The caller-runs rejection policy: count the saturation, report it at most once a minute, and hand the task to
   * whatever {@link #runRejectedTask(Runnable)} decides.
   * <p>
   * Takes its numbers as arguments rather than reading them off the executor because this is the path a real pool
   * cannot be made to take on demand without queueing a thousand blocking tasks first, and the subclasses whose safety
   * argument is ABOUT this path drive it directly from their tests.
   *
   * @param task          the task the queue refused
   * @param queueCapacity the queue's capacity, for the message
   * @param poolThreads   the pool's thread count, for the message
   */
  public final void runOnCaller(final Runnable task, final int queueCapacity, final int poolThreads) {
    final long fallbacks = callerRunCount.incrementAndGet();
    final long now = System.currentTimeMillis();
    final long last = lastSaturationWarnMs.get();
    if (now - last > WARN_THROTTLE_INTERVAL_MS && lastSaturationWarnMs.compareAndSet(last, now))
      LogManager.instance().log(this, Level.WARNING, saturationWarnMessage, null, queueCapacity, poolThreads, fallbacks);

    runRejectedTask(task);
  }

  /** " Raise 'a' or 'b' if this persists.", or nothing when the pool has no sizing settings to name. */
  private static String raiseSettingsAdvice(final GlobalConfiguration[] sizingSettingKeys) {
    if (sizingSettingKeys.length == 0)
      return "";
    final StringBuilder advice = new StringBuilder(" Raise ");
    for (int i = 0; i < sizingSettingKeys.length; i++) {
      if (i > 0)
        advice.append(i == sizingSettingKeys.length - 1 ? " or " : ", ");
      advice.append('\'').append(sizingSettingKeys[i].getKey()).append('\'');
    }
    return advice.append(" if this persists.").toString();
  }

  /**
   * Reopens the saturation-warning throttle immediately, so a test can observe the WARNING without waiting out a
   * window some earlier test in the same JVM already consumed. A test seam, and a deliberate one: the alternative
   * that was in use - reflection on the private field - breaks silently on a rename, with no compile error to say so.
   */
  public void resetSaturationWarningThrottle() {
    lastSaturationWarnMs.set(0L);
  }

  /**
   * Runs a task the queue refused. Overridable for a pool that has to mark the borrowed thread for the duration -
   * the submitter IS one of this pool's tasks while it runs one, and everything that asks has to get that answer
   * there too.
   */
  protected void runRejectedTask(final Runnable task) {
    if (saturationPolicy == SaturationPolicy.CALLER_RUNS_EVEN_WHEN_SHUT_DOWN || !executor.isShutdown())
      task.run();
    else if (task instanceof RunnableFuture<?> future)
      future.cancel(false);
  }

  /** The dedicated executor. Its queue plus its rejection policy are the whole back-pressure story. */
  public ExecutorService getExecutorService() {
    return executor;
  }

  /**
   * Configured thread ceiling, not the live worker count: a caller sizing a fan-out wants to know how wide the pool
   * can get, and {@link ThreadPoolExecutor#getPoolSize()} is lower until the threads have been needed.
   */
  public int getMaxParallelism() {
    return executor.getMaximumPoolSize();
  }

  /** Live pool statistics for the metrics binder and for tests. */
  public PoolStats getPoolStats() {
    return new PoolStats(executor.getPoolSize(), executor.getActiveCount(), executor.getQueue().size(),
        boundedQueue ? executor.getQueue().remainingCapacity() : -1, executor.getCompletedTaskCount(),
        callerRunCount.get());
  }

  /**
   * Best-effort shutdown for tests and tooling. The production lifecycle relies on the daemon-thread default: the
   * workers die when the JVM exits and nothing has to remember to close anything.
   */
  public void close() {
    executor.shutdownNow();
  }
}
