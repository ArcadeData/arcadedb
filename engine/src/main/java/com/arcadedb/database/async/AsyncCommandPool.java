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
package com.arcadedb.database.async;

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
 * JVM-wide dedicated executor for the <b>DDL</b> dispatched through the asynchronous API - a
 * {@code database.async().command(...)} that parses to a {@code DDLStatement}, which is what an HTTP
 * {@code POST /command} with {@code awaitResponse=false} carrying {@code CREATE INDEX} or {@code REBUILD INDEX}
 * reaches. Lazy singleton; daemon threads, so the JVM exits cleanly whether or not anything shuts it down.
 * <p>
 * <b>Why DDL cannot run on the async worker threads (issue #6303, item 3).</b> The DDL that matters here -
 * {@code CREATE INDEX}, a manual index create, {@code REBUILD INDEX} - is built by SCANNING the data, so it must
 * first make the async executor quiesce: commit every worker's open batch and hold the workers still while it scans
 * (issue #6281). That cannot be done from one of those workers. The quiescence enqueues a task on every worker
 * INCLUDING the caller's own, and the only consumer of a worker's queue is that worker, so before the guard of #6281
 * such a command parked for ever - measured: the surefire fork does not merely fail, it never finishes. #6281 turned
 * the hang into a clear {@code NeedRetryException} and a workaround ("run it synchronously"); it did not give the
 * operation back. Running it here does.
 * <p>
 * <b>And why ONLY DDL comes here.</b> A worker is not merely a thread: it owns a batch transaction across up to
 * {@link com.arcadedb.GlobalConfiguration#ASYNC_TX_BATCH_SIZE} tasks, and it is the unit
 * {@code ThreadBucketSelectionStrategy} pins a bucket to - so "as many workers as buckets" is a documented way to
 * make concurrent asynchronous writers never contend for the same pages. Sending ordinary write commands to a pool
 * sized for the machine instead breaks that arithmetic, and does so quietly: measured as MVCC conflicts and a
 * duplicated row in {@code AsyncInsertTest}. So the relocation is exactly as wide as the problem - the statements
 * that could not run on a worker at all.
 * <p>
 * <b>Why one JVM-wide pool and not one per database.</b> What lands here is bursty and rare: the fire-and-forget
 * administrative statement, never the ingestion path. A pool per database would size itself against a per-database
 * burst that does not exist, and would multiply idle threads by the number of open databases in a process that
 * cycles through many. The task carries its own database, so there is nothing per-database about the threads
 * themselves - and they carry no {@code DatabaseContext} across tasks, which is what keeps a shared thread from
 * leaking one database's binding into another's command.
 * <p>
 * <b>Graceful degradation.</b> The queue is bounded and saturation runs the command on the submitting thread rather
 * than refusing it. That is a real cost worth naming: for {@code awaitResponse=false} the submitter is an HTTP worker,
 * so its 202 is delayed by however long the command takes - the client asked not to wait and waits anyway. The
 * alternative is worse in both directions: throwing loses the command outright, and an unbounded queue would accept
 * an unbounded backlog of commands nobody is waiting on. The fallback is counted and reported through
 * {@code pool=async_command}, so an operator can see it happening rather than infer it from latency.
 * <p>
 * It also has a consequence the task side has to honour, and does: on that path the command runs on a thread that
 * already has a {@code DatabaseContext} and very possibly an open transaction of its own, so
 * {@code DatabaseAsyncExecutorImpl.runCommand} touches neither unless it created them. The submitter is marked as a
 * pool thread for the duration ({@link #isPoolThread()}) so everything that asks "is this thread itself a dispatched
 * command?" gets the right answer there too.
 * <p>
 * <b>"No JDK common ForkJoinPool" rule.</b> See the {@link com.arcadedb.query.QueryEngineManager} class javadoc.
 * A command is arbitrary user work of unbounded duration and belongs on a dedicated pool.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class AsyncCommandPool {

  /** Floor for the auto-sized thread count when {@code arcadedb.asyncCommandPoolThreads} is 0. */
  private static final int  DEFAULT_THREADS_FLOOR       = 2;
  private static final int  DEFAULT_QUEUE_SIZE          = 1024;
  /** At most one saturation WARNING per minute, the same throttle the other engine pools use. */
  private static final long SATURATION_WARN_INTERVAL_MS = 60_000L;

  /**
   * Marks a thread as one of this pool's, so {@code DatabaseAsyncExecutorImpl.waitCompletion} can tell that its
   * caller IS one of the commands it would otherwise wait for.
   * <p>
   * A command that reaches the async barrier - every scan-based index build does - would otherwise wait for the set
   * of in-flight commands to empty while being a member of that set: a self-deadlock, and exactly the shape of the
   * one #6281 fixed on the workers, reintroduced one pool over. Set on the worker itself rather than per task so it
   * survives whatever the task does.
   * <p>
   * Note it is set on a shared JVM-wide pool, so it says "some command of some database is running on this thread",
   * not which. That is enough for the only decision it drives, and it is the honest one: waiting for a PEER command
   * on a pool with a bounded queue is not safe either - the peer may be behind this task in the queue with no thread
   * left to run it.
   * <p>
   * <b>Which makes the exemption CROSS-DATABASE, and deliberately so.</b> A thread running database A's command is
   * exempt from waiting on database B's in-flight commands too, should it reach B's executor - a script or trigger
   * that spans databases is the shape that would. The same argument covers it: B's commands would be peers on this
   * one bounded queue, so waiting for them is the same unsafe wait, and what the barrier is actually for - B's
   * workers' open batches - is served by the per-worker markers, which run either way. Narrowing it to "all but my
   * own command" would need per-thread task identity and would buy nothing this reasoning does not already give.
   */
  private static final ThreadLocal<Boolean> POOL_THREAD = new ThreadLocal<>();

  private final ThreadPoolExecutor executor;
  private final AtomicLong         callerRunCount       = new AtomicLong();
  private final AtomicLong         lastSaturationWarnMs = new AtomicLong(0L);

  /**
   * Initialization-on-demand holder: the pool allocates its threads only when something first asks for it.
   * <p>
   * That laziness is real for an embedded or CLI JVM and NOT for a server, where {@code PoolMetrics.bindTo} asks for
   * the instance at startup to register its gauges - so a server builds the pool whether or not a single statement is
   * ever dispatched to it. Same as {@code SparseVectorScoringPool} and {@code ParallelScanProducerPool}, and the
   * price of the gauges being there before the first saturation rather than after it; said out loud because "pays
   * nothing until used" would otherwise read as true of the deployment where it is not.
   * <p>
   * Declared BELOW the fields rather than at the top where the idiom is usually written, because PMD's
   * {@code FieldDeclarationsShouldBeAtStartOfClass} counts a nested class as the end of the field section and would
   * report every field above as misplaced.
   */
  private static final class Holder {
    static final AsyncCommandPool INSTANCE = new AsyncCommandPool();
  }

  private AsyncCommandPool() {
    final int configuredThreads = GlobalConfiguration.ASYNC_COMMAND_POOL_THREADS.getValueAsInteger();
    final int threads = configuredThreads > 0 ?
        configuredThreads :
        Math.max(DEFAULT_THREADS_FLOOR, Runtime.getRuntime().availableProcessors());
    final int configuredQueueSize = GlobalConfiguration.ASYNC_COMMAND_QUEUE_SIZE.getValueAsInteger();
    final int queueSize = configuredQueueSize > 0 ? configuredQueueSize : DEFAULT_QUEUE_SIZE;
    final AtomicInteger workerSeq = new AtomicInteger();

    final ThreadPoolExecutor pool = new ThreadPoolExecutor(threads, threads, 60L, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(queueSize), r -> {
      // A PLAIN Thread, and that is the whole point: DatabaseAsyncExecutorImpl recognizes its own workers by
      // class and owner, so a command running here is - correctly - not one of them, and the barrier it needs
      // can be satisfied instead of refused.
      final Thread t = new Thread(() -> {
        POOL_THREAD.set(Boolean.TRUE);
        r.run();
      }, "ArcadeDB-AsyncCommand-" + workerSeq.incrementAndGet());
      t.setDaemon(true);
      return t;
    }, (task, exec) -> runOnCaller(task, queueSize, exec.getMaximumPoolSize()));
    pool.allowCoreThreadTimeOut(true);
    this.executor = pool;
  }

  /**
   * The caller-runs rejection policy: a saturated pool executes the command on the thread that submitted it.
   * <p>
   * Package-private and taking its numbers as arguments rather than reading them off the executor, because this is
   * the path most of the pool's safety argument is ABOUT - the self-deadlock exemption, and the runner's
   * "touch no transaction and no context we did not create" contract both exist for it - and it is the one a real
   * pool cannot be made to take on demand without queueing a thousand blocking statements. Driven directly by
   * {@code AsyncCommandPoolCallerRunsTest}.
   */
  void runOnCaller(final Runnable task, final int queueCapacity, final int poolThreads) {
    final long fallbacks = callerRunCount.incrementAndGet();
    final long now = System.currentTimeMillis();
    final long last = lastSaturationWarnMs.get();
    if (now - last > SATURATION_WARN_INTERVAL_MS && lastSaturationWarnMs.compareAndSet(last, now))
      LogManager.instance().log(AsyncCommandPool.class, Level.WARNING,
          "Asynchronous command pool saturated: queue full (capacity=%d, threads=%d), running the command on the "
              + "submitting thread - a caller that asked not to wait for the response will wait for it "
              + "(cumulative caller-runs fallbacks=%d). Raise '%s' or '%s'", null,
          queueCapacity, poolThreads, fallbacks,
          GlobalConfiguration.ASYNC_COMMAND_POOL_THREADS.getKey(), GlobalConfiguration.ASYNC_COMMAND_QUEUE_SIZE.getKey());

    // MARKED AS A POOL THREAD FOR THE DURATION, even though it is the submitter's. The flag means "this thread is
    // currently BEING an asynchronously dispatched command", and everything that reads it needs that to be true
    // here too: a CREATE INDEX run inline would otherwise reach the barrier and wait for the in-flight-command set
    // it is itself a member of. Restored rather than cleared, so a submitter that is already running one command
    // (a command that dispatches another) is left as it was.
    final boolean previous = isPoolThread();
    POOL_THREAD.set(Boolean.TRUE);
    try {
      // Run it UNCONDITIONALLY, including on a shut-down pool - unlike the pools that hand back a Future, which
      // can cancel one instead. There is no future here to cancel and no result to return: the submitter has
      // already counted this command as in flight, so a task that is neither run nor completed leaves that count
      // raised for ever and every later waitCompletion() on that database waits out its whole budget. Nothing
      // shuts this pool down today; this is the answer that stays correct if something ever does.
      task.run();
    } finally {
      if (previous)
        POOL_THREAD.set(Boolean.TRUE);
      else
        POOL_THREAD.remove();
    }
  }

  public static AsyncCommandPool getInstance() {
    return Holder.INSTANCE;
  }

  /**
   * Whether the calling thread is one of this pool's workers, i.e. whether it IS an asynchronously dispatched
   * command. Static and holder-free on purpose: asking must not be the thing that creates the pool.
   */
  public static boolean isPoolThread() {
    return Boolean.TRUE.equals(POOL_THREAD.get());
  }

  /** The dedicated executor. Its bounded queue plus caller-runs rejection is the whole back-pressure story. */
  public ExecutorService getExecutorService() {
    return executor;
  }

  /** Live pool statistics for the metrics binder (Studio "Executor Pools" card). */
  public PoolStats getPoolStats() {
    return new PoolStats(executor.getPoolSize(), executor.getActiveCount(), executor.getQueue().size(),
        executor.getQueue().remainingCapacity(), executor.getCompletedTaskCount(), callerRunCount.get());
  }

  public record PoolStats(int poolSize, int activeThreads, int queueDepth, int queueCapacityRemaining,
                          long completedTasks, long callerRunFallbacks) {
  }
}
