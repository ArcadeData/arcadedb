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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DocumentCallback;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ErrorRecordCallback;
import com.arcadedb.engine.WALFile;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.engine.timeseries.TimeSeriesRowSource;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.utility.StringUtils;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

public class DatabaseAsyncExecutorImpl implements DatabaseAsyncExecutor {
  private final DatabaseInternal     database;
  private final ContextConfiguration configuration;
  // Volatile + lifecycleLock-guarded: createThreads/shutdownThreads/kill mutate this field; readers
  // (scheduleTask, getThreadCount, getStats, ...) snapshot it locally before iterating so they cannot
  // observe a half-built or just-nulled array. See lifecycleLock for the publish discipline.
  private volatile AsyncThread[]     executorThreads;
  private final Object               lifecycleLock                 = new Object();
  // #4961: these settings are written by user threads and read by the worker threads, so they must
  // be volatile or a change applied after createThreads() may never become visible to the workers.
  private volatile int               parallelLevel                 = 1;
  private volatile int               commitEvery;
  private volatile int               backPressurePercentage        = 0;
  private volatile boolean           transactionUseWAL             = true;
  private volatile WALFile.FlushType transactionSync               = WALFile.FlushType.NO;
  private volatile long              checkForStalledQueuesMaxDelay = 5_000;
  // #5062 review r4 (point 1): grace period each worker gets to exit on its own before shutdown
  // escalates to interrupt() and again before giving up with a WARNING. Package-private (and
  // volatile: written by tests, read by the closing thread) so shutdown regression tests can shrink
  // it instead of waiting the production 10s.
  volatile         long              shutdownJoinTimeoutMs         = 10_000;
  private final AtomicLong           transactionCounter            = new AtomicLong();
  private final AtomicLong           commandRoundRobinIndex        = new AtomicLong();
  private final AtomicLong           tsAppendCounter               = new AtomicLong();
  /**
   * Commands and queries of THIS database submitted to the JVM-wide {@link AsyncCommandPool} and not yet finished
   * (issue #6303, item 3). They no longer sit in a worker's queue, so they are no longer counted by walking the
   * workers - and both {@link #waitCompletion(long)} and {@link #isProcessing()} have to keep answering about them,
   * or moving the dispatch off the workers would quietly change what those two mean.
   */
  private final AtomicInteger        commandsInFlight              = new AtomicInteger();
  /** Monitor {@link #awaitCommands} parks on; notified when the last in-flight command lands. */
  private final Object               commandsLock                  = new Object();

  // #5062 review r2 (point 1): producer-side backstop for a worker wedged inside user code. After
  // this many consecutive stall windows (checkForStalledQueuesMaxDelay each, 60s with the defaults)
  // in which the target worker completed NO task while its queue stayed full, offerWaiting throws
  // instead of letting the caller hang forever. Progress-gated so a single legitimately slow task
  // does not trip it (the #4953 false positive). Only the window DURATION is tunable, via the
  // volatile setCheckForStalledQueuesMaxDelay(); this count and the cross-slot one below are fixed.
  private static final int STALLED_NO_PROGRESS_WINDOWS = 12;

  // #5062 review r3 (point 1): same progress gating for the cross-slot-park branch. A worker parked
  // handing a task to another queue with a flat completed count is much more likely to sit in a
  // genuine scheduling cycle than a wedged one (it only reaches offerWaiting after exhausting its
  // help-deferral budget), so the bound is far smaller than the wedged-worker one - but it must
  // exceed 2, the old head-identity bound that a peer merely busy on one slow task could flatten,
  // or the #4953 false positive recurs on producers targeting a budget-exhausted worker. 15s with
  // the default 5s window: genuine capped cycles still fail loudly in bounded time.
  private static final int STALLED_CROSS_SLOT_NO_PROGRESS_WINDOWS = 3;

  /**
   * Serializes {@link #quiesceWorkers()} and is held for the WHOLE quiescence, which is what makes a nested one
   * safe: a second quiescence taken while the workers are already parked would schedule park tasks behind tasks
   * nothing is going to run, and wait for them for ever. Reentrant, so the nested case is recognized by hold count
   * and simply rides on the outer one; a DIFFERENT thread waits its turn instead, which serializes concurrent index
   * builds on one database rather than deadlocking them.
   */
  private final ReentrantLock quiesceLock = new ReentrantLock();

  /**
   * The verbs every {@code DDLStatement} of the SQL grammar begins with, used by {@link #mayContainDDL} to answer
   * "this script has no DDL in it" without parsing. Kept in step with the grammar by
   * {@code AsyncDDLKeywordCoverageTest}.
   */
  private static final String[] DDL_LEADING_KEYWORDS = { "CREATE", "DROP", "ALTER", "REBUILD", "TRUNCATE", "REFRESH",
      "COMPACT" };

  // SPECIAL TASKS
  public final static DatabaseAsyncTask FORCE_EXIT = new DatabaseAsyncTask() {
    @Override
    public void execute(final AsyncThread async, final DatabaseInternal database) {
      // NO ACTIONS
    }

    @Override
    public String toString() {
      return "FORCE_EXIT";
    }
  };

  // #4961: read by worker threads, written by user threads: must be volatile.
  private volatile OkCallback    onOkCallback;
  private volatile ErrorCallback onErrorCallback;
  private final    AtomicLong    counterScheduledTasks = new AtomicLong();

  public class AsyncThread extends Thread {
    public final    BlockingQueue<DatabaseAsyncTask> queue;
    public final    DatabaseInternal                 database;
    public volatile boolean                          shutdown      = false;
    public volatile boolean                          forceShutdown = false;
    public          AtomicBoolean                    executingTask = new AtomicBoolean(false);
    public          long                             count         = 0;
    // #4953: monotonic count of tasks this worker has finished (successfully or not). Producers
    // blocked on this worker's full queue use it as a progress probe: only written by the worker,
    // read by any thread, hence volatile. Package-private (instead of private) so the backstop
    // progress-reset regression test can simulate a slow-but-progressing peer deterministically (a
    // real progressing peer frees queue slots, ending the park before enough windows elapse).
    volatile         long                            completedTaskCount    = 0;
    // #4953: true while this worker is parked in scheduleTask waiting to hand a task to a queue.
    // Combined with a flat completedTaskCount it identifies a genuine cross-scheduling stall.
    private volatile boolean                         waitingCrossSlotOffer = false;
    // #5062 review (point 1): tasks polled from the own queue while help-waiting are NOT executed
    // re-entrantly (a nested execution could commit or roll back the suspended task's partial
    // writes, breaking per-task atomicity); they are parked here, in poll order, and run by the run
    // loop once the current task unwinds. Thread-confined: only this worker touches it.
    // Memory bound (#5062 review r3, point 3): at most queueCapacity task references
    // (ASYNC_OPERATIONS_QUEUE_SIZE / parallelLevel, so one extra queue's worth per worker in the
    // worst case), a transient spike under sustained cross-slot pressure that drains as soon as the
    // current task unwinds.
    private final    ArrayDeque<DatabaseAsyncTask>   helpDeferredTasks     = new ArrayDeque<>();
    // Capacity of the own queue, doubling as the helpDeferredTasks budget (see offerHelping).
    private final    int                             queueCapacity;

    private AsyncThread(final DatabaseInternal database, final int id) {
      super("AsyncExecutor-" + database.getName() + "-" + id);
      // #5418: DAEMON. These workers inherit the daemon flag of whatever application thread happened to
      // touch the async API first, so on a leaked Database they kept the embedder's JVM from ever exiting.
      // The graceful drain still happens in close(), which the JVM shutdown hook installed by
      // DatabaseFactory reaches before daemon threads are stopped.
      setDaemon(true);
      this.database = database;

      int queueSize =
          database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_SIZE) / parallelLevel;
      if (queueSize < 1)
        queueSize = 1;
      this.queueCapacity = queueSize;

      final String cfgQueueImpl =
          database.getConfiguration().getValueAsString(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_IMPL);
      if ("fast".equalsIgnoreCase(cfgQueueImpl))
        // #5066: DisruptorBlockingQueue (MPMC, lock-free CAS-claimed sequences) instead of
        // PushPullBlockingQueue: the latter is an explicit single-producer/single-consumer design
        // with no CAS on the tail, while this queue has many producers (any application thread,
        // cross-scheduling workers, completion markers, the closing thread), so concurrent offers
        // could silently lose tasks; it also lacks remove(Object), which scheduleTask's
        // post-shutdown undo relies on. Same library, same jar, capacity rounded up to a power of 2.
        this.queue = new DisruptorBlockingQueue<>(queueSize);
      else if ("standard".equalsIgnoreCase(cfgQueueImpl))
        this.queue = new ArrayBlockingQueue<>(queueSize);
      else {
        // WARNING AND THEN USE THE DEFAULT
        LogManager.instance()
            .log(this, Level.WARNING, "Error on async operation queue implementation setting: %s is not supported",
                cfgQueueImpl);
        this.queue = new ArrayBlockingQueue<>(queueSize);
      }

      backPressurePercentage = database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_BACK_PRESSURE);
    }

    public boolean isShutdown() {
      return shutdown;
    }

    @Override
    public void run() {
      DatabaseContext.INSTANCE.init(database);

      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).perThreadBucketSelection = true;
      database.getTransaction().setUseWAL(transactionUseWAL);
      database.setWALFlush(transactionSync);
      database.getTransaction().begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED); // FORCE THE LOWEST LEVEL
      // OF ISOLATION

      while (!forceShutdown) {
        try {
          // TASKS PARKED BY THE HELP-WHILE-WAITING PATH RUN FIRST, IN POLL ORDER, NOW THAT THE TASK
          // THAT WAS SUSPENDED WHILE THEY WERE POLLED HAS FULLY UNWOUND (SEE offerHelping)
          final DatabaseAsyncTask message =
              helpDeferredTasks.isEmpty() ? queue.poll(500, TimeUnit.MILLISECONDS) : helpDeferredTasks.pollFirst();
          if (message != null) {
            if (message == FORCE_EXIT)
              break;
            executeTask(message);
          } else if (shutdown)
            break;

        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        } catch (final Throwable e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on executing asynchronous operation (asyncThread=%s)",
              e, getName());
        }
      }

      try {
        if (database.isOpen() && database.isTransactionActive())
          database.commit();
        onOk();
      } catch (final Exception e) {
        onError(e);
      }

      // #4954: whatever ended the loop (FORCE_EXIT, shutdown flag, interrupt), tasks may still sit in
      // the queue and will never execute. Notify completed() on each so threads blocked in scanType()
      // or waitCompletion() are released instead of hanging forever (the previous code queue.clear()-ed
      // on interrupt, silently discarding them).
      drainQueueNotifyingWaiters();
    }

    /**
     * Executes a single task applying the shared transaction-batching contract (begin on demand,
     * commit every {@code commitEvery} tasks, rollback + onError on failure, always notify
     * {@code completed()}). Called from the run loop only: the help-while-waiting path of #4953
     * defers polled tasks back to the run loop instead of nesting executions, so that no
     * transaction boundary can fall inside a suspended task's execution (#5062 review, point 1).
     */
    private void executeTask(final DatabaseAsyncTask message) {
      final boolean nested = executingTask.getAndSet(true);
      try {
        LogManager.instance()
            .log(this, Level.FINE, "Received async message %s (threadId=%d)", message,
                Thread.currentThread().getId());

        if (message.requiresActiveTx() && !database.isTransactionActive())
          database.begin();

        message.execute(this, database);

        // #5062 review (point 1): the commit-every-N boundary must never fire from a nested
        // execution while an enclosing task is suspended mid-execute(), or it would commit that
        // task's partial writes. The helping path defers tasks instead of nesting (see
        // offerHelping), so today `nested` is always false here; the guard is defense-in-depth
        // should a re-entrant call path ever be reintroduced.
        if (!nested) {
          count++;

          if (database.isTransactionActive() && count % commitEvery == 0) {
            database.commit();
            database.begin();
          }
        }
      } catch (final Throwable e) {
        onError(e);
        // SAME GUARD AS ABOVE: A NESTED ROLLBACK WOULD DESTROY THE SUSPENDED TASK'S WRITES
        if (!nested && database.isTransactionActive())
          database.rollback();
      } finally {
        try {
          message.completed();
        } finally {
          completedTaskCount++;
          executingTask.set(nested);
        }
      }
    }

    private void drainQueueNotifyingWaiters() {
      DatabaseAsyncTask leftover;
      // TASKS PARKED BY THE HELPING PATH ARE DROPPED TASKS TOO: NOTIFY THEM FIRST, THEN THE QUEUE
      while ((leftover = helpDeferredTasks.isEmpty() ? queue.poll() : helpDeferredTasks.pollFirst()) != null)
        if (leftover != FORCE_EXIT)
          try {
            leftover.completed();
          } catch (final Throwable e) {
            LogManager.instance()
                .log(this, Level.SEVERE, "Error on notifying completion of dropped asynchronous task %s", e, leftover);
          }
    }

    DatabaseAsyncExecutorImpl getOwner() {
      return DatabaseAsyncExecutorImpl.this;
    }

    public void onError(final Throwable e) {
      DatabaseAsyncExecutorImpl.this.onError(e);
    }

    public void onOk() {
      DatabaseAsyncExecutorImpl.this.onOk();
    }

    public boolean isExecutingTask() {
      return executingTask.get();
    }
  }

  public DatabaseAsyncExecutorImpl(final DatabaseInternal database, final ContextConfiguration configuration) {
    this.database = database;
    this.configuration = configuration;
    // #4961: a non-positive batch size would make every task fail on count % commitEvery.
    this.commitEvery = Math.max(1, database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_TX_BATCH_SIZE));
    createThreads(database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS));
  }

  /**
   * <b>Must stay lock-free, and must stay callable on a shut-down executor (#5636.)</b> {@code Profiler.toJSON()}
   * reads this while holding its own monitor, which a closing database can be waiting on, so a lock taken here would
   * sit on the other side of that wait. The volatile read of {@code executorThreads} plus the queue sizes below is
   * deliberately all it does - and the null check on that read is what keeps it answering during the window between
   * a database being torn down and it reaching {@code Profiler.unregisterDatabase}, where a scrape still sees it
   * registered. Zeros are the right answer there; throwing would take the whole snapshot down.
   */
  public DBAsyncStats getStats() {
    final DBAsyncStats stats = new DBAsyncStats();

    stats.queueSize = 0;
    final AsyncThread[] threads = executorThreads;
    if (threads != null)
      for (int i = 0; i < threads.length; ++i)
        stats.queueSize += threads[i].queue.size();

    stats.scheduledTasks = counterScheduledTasks.get();

    return stats;
  }

  @Override
  public void setTransactionUseWAL(final boolean transactionUseWAL) {
    this.transactionUseWAL = transactionUseWAL;
    createThreads(parallelLevel);
  }

  @Override
  public boolean isTransactionUseWAL() {
    return transactionUseWAL;
  }

  @Override
  public WALFile.FlushType getTransactionSync() {
    return transactionSync;
  }

  @Override
  public void setTransactionSync(final WALFile.FlushType transactionSync) {
    this.transactionSync = transactionSync;
    createThreads(parallelLevel);
  }

  public long getCheckForStalledQueuesMaxDelay() {
    return checkForStalledQueuesMaxDelay;
  }

  /**
   * Sets the duration of one stall-detection window (default 5s). Producers blocked on a full queue
   * throw after {@code STALLED_CROSS_SLOT_NO_PROGRESS_WINDOWS} (3) flat windows when the target
   * worker is parked handing a task cross-slot, or after {@code STALLED_NO_PROGRESS_WINDOWS} (12)
   * flat windows when it is not (wedged in user code). Only the window DURATION is tunable, the
   * window counts are fixed. #5062 review r4 (point 2): scheduling chains deeper than two workers
   * whose tail runs individual tasks longer than 3 windows (15s by default) can still trip the
   * cross-slot detector even though the chain would resolve; raise this delay for such workloads.
   */
  public void setCheckForStalledQueuesMaxDelay(final long checkForStalledQueuesMaxDelay) {
    this.checkForStalledQueuesMaxDelay = checkForStalledQueuesMaxDelay;
  }

  @Override
  public void onOk(final OkCallback callback) {
    onOkCallback = callback;
  }

  @Override
  public void onError(final ErrorCallback callback) {
    onErrorCallback = callback;
  }

  public void compact(final IndexInternal index) {
    if (!index.scheduleCompaction())
      return;

    // scheduleCompaction() has reserved the index (AVAILABLE -> COMPACTION_SCHEDULED) and only the compaction
    // itself gives that back. So when nothing is going to run it, hand it back here: a full worker queue answers
    // false without throwing, and a shut-down executor throws out of getBestSlot()/scheduleTask. Leaving it
    // reserved would be permanent - every later attempt, an explicit COMPACT INDEX included, needs the same
    // AVAILABLE -> SCHEDULED move - so the index would silently stop compacting until the database is reopened.
    boolean scheduled = false;
    try {
      // No back-pressure (0) on purpose, unlike the user-facing async entry points. Both callers of this method are
      // index onAfterCommit hooks, so the thread that would be slowed down is a committer whose work is already
      // durable: sleeping it throttles nothing that is filling the queue, it only adds latency to a commit that is
      // finished. A full queue instead makes the offer below give up, the finally hands the slot back, and the next
      // commit past the threshold schedules again - both gates are level-triggered, so no compaction is lost.
      scheduled = scheduleTask(getBestSlot(), new DatabaseAsyncIndexCompaction(index), false, 0);
    } finally {
      if (!scheduled)
        index.setStatus(new IndexInternal.INDEX_STATUS[] { IndexInternal.INDEX_STATUS.COMPACTION_SCHEDULED },
            IndexInternal.INDEX_STATUS.AVAILABLE);
    }
  }

  /**
   * Looks for an empty queue or the queue with less messages.
   */
  // Package-private (instead of private) so the shutdown-race regression test for #4955 can call it directly.
  int getBestSlot() {
    final AsyncThread[] threads = executorThreads;
    if (threads == null || threads.length == 0)
      // #4955: close()/kill() nulls the array under the lifecycle lock; a caller racing shutdown must get
      // the same intended error as scheduleTask, not an NPE on the snapshot below.
      throw new DatabaseOperationException("Async executor has been shut down");
    int minQueueSize = 0;
    int minQueueIndex = -1;
    for (int i = 0; i < threads.length; ++i) {
      final int qSize = threads[i].queue.size() + (threads[i].isExecutingTask() ? 1 : 0);

      if (qSize == 0)
        // EMPTY QUEUE, USE THIS
        return i;

      if (minQueueIndex == -1 || qSize < minQueueSize) {
        minQueueSize = qSize;
        minQueueIndex = i;
      }
    }

    return minQueueIndex;
  }

  /**
   * Returns a random slot.
   */
  int getRandomSlot() {
    final AsyncThread[] threads = executorThreads;
    if (threads == null || threads.length == 0)
      // #4955: same shutdown race as getBestSlot. The length check also covers a zero-length array
      // (nextInt(0) would throw IllegalArgumentException), unreachable today but cheap to guard.
      throw new DatabaseOperationException("Async executor has been shut down");
    return ThreadLocalRandom.current().nextInt(threads.length);
  }

  @Override
  public void waitCompletion() {
    waitCompletion(0L);
  }

  @Override
  public boolean waitCompletion(long timeout) {
    if (timeout <= 0)
      timeout = Long.MAX_VALUE;
    final long beginTime = System.currentTimeMillis();

    // FIRST, because an async command can submit async record tasks of its own and those must end up behind the
    // markers below rather than in front of them (issue #6303, item 3). Costs nothing when nothing was dispatched.
    if (!awaitCommands(beginTime, timeout))
      return false;

    final AsyncThread[] threads = executorThreads;
    if (threads == null)
      return true;

    final DatabaseAsyncAbstractCallbackTask[] semaphores =
        new DatabaseAsyncAbstractCallbackTask[threads.length];

    for (int i = 0; i < threads.length; ++i)
      try {
        semaphores[i] = new DatabaseAsyncCompletion();
        // #4954: bounded offer with a liveness check instead of an untimed put(): a worker that
        // exited (shutdown) will never drain a full queue, so the old code hung here forever.
        // #5062 review (point 3): the timeout is a single budget spanning enqueue AND await, so a
        // persistently full queue on a live worker cannot block the caller past the timeout.
        while (true) {
          final long remaining = timeout - (System.currentTimeMillis() - beginTime);
          if (remaining <= 0)
            // #5062 review r6 (point 3): NOT A LEAK - markers already enqueued on earlier workers
            // simply execute and count down a latch nobody awaits anymore.
            return false;
          if (threads[i].queue.offer(semaphores[i], Math.min(500, remaining), TimeUnit.MILLISECONDS))
            break;
          if (!threads[i].isAlive()) {
            // NOTHING WILL EVER RUN ON THIS QUEUE ANYMORE: TREAT IT AS FLUSHED
            semaphores[i].completed();
            break;
          }
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }

    long currentTimeout = timeout - (System.currentTimeMillis() - beginTime);
    if (currentTimeout < 1)
      return false;

    for (int i = 0; i < semaphores.length; ++i)
      try {
        if (!semaphores[i].waitCompletion(currentTimeout))
          return false;

        // UPDATE THE TIMEOUT
        currentTimeout = timeout - (System.currentTimeMillis() - beginTime);
        if (currentTimeout < 1)
          return false;

      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }

    return true;
  }

  @Override
  public void query(final String language, final String query, final AsyncResultsetCallback callback,
                    final Object... args) {
    // A query is idempotent by definition, so it is never the DDL that has to leave the workers: straight to a slot,
    // exactly as before #6303.
    scheduleOnWorker(new DatabaseAsyncCommand(configuration, true, language, query, args, callback, captureCurrentUser()));
  }

  @Override
  public void query(final String language, final String query, final AsyncResultsetCallback callback,
                    final Map<String, Object> args) {
    scheduleOnWorker(new DatabaseAsyncCommand(configuration, true, language, query, args, callback, captureCurrentUser()));
  }

  @Override
  public void command(final String language, final String query, final AsyncResultsetCallback callback,
                      final Object... args) {
    dispatch(new DatabaseAsyncCommand(configuration, false, language, query, args, callback, captureCurrentUser()));
  }

  @Override
  public void command(final String language, final String query, final AsyncResultsetCallback callback,
                      final Map<String, Object> args) {
    dispatch(new DatabaseAsyncCommand(configuration, false, language, query, args, callback, captureCurrentUser()));
  }

  /**
   * Sends a dispatched command where it can actually run: {@link AsyncCommandPool} when it is DDL, one of this
   * executor's own workers otherwise (issue #6303, item 3).
   * <p>
   * <b>DDL is the part that could not run on a worker.</b> The DDL that has to SCAN the data - {@code CREATE INDEX},
   * a manual index create, {@code REBUILD INDEX} - must first make this very executor quiesce, and a worker cannot:
   * the quiescence enqueues a task on every worker, its own included, and only that worker drains its queue. Before
   * #6281 such a command parked for ever; #6281 made it a clear refusal with a workaround; this gives the operation
   * back. The pool's threads are deliberately not workers of any executor, so the barrier is satisfiable there.
   * <p>
   * <b>And everything else stays exactly where it was</b>, which is not timidity but the point. A worker is not just
   * a thread: it owns a batch transaction across up to {@link GlobalConfiguration#ASYNC_TX_BATCH_SIZE} tasks and it
   * is the unit {@code ThreadBucketSelectionStrategy} pins a bucket to, so "as many workers as buckets" is a
   * documented way to make concurrent async writers never contend for the same pages ({@code AsyncInsertTest}).
   * Routing ordinary write commands to a JVM-wide pool sized for the machine breaks that arithmetic - measured, as
   * conflicts and duplicated rows in that very test. The move is therefore as narrow as the problem: the statements
   * that could not run on a worker at all, and nothing else.
   */
  private void dispatch(final DatabaseAsyncCommand task) {
    if (requiresOffWorkerExecution(task))
      submitCommand(task);
    else
      scheduleOnWorker(task);
  }

  private void scheduleOnWorker(final DatabaseAsyncCommand task) {
    scheduleTask(getSlot((int) commandRoundRobinIndex.getAndIncrement()), task, true, backPressurePercentage);
  }

  /**
   * Whether a dispatched command is DDL, and therefore has to run off the workers.
   * <p>
   * The DECISION is always a parse, never a text match - a keyword can appear in a string literal, and routing a
   * write command off the workers costs the bucket pinning that keeps concurrent writers apart. The parse belongs to
   * the LANGUAGE, so it is asked of the language: {@link QueryEngine#classifyDDL(String)} (issue #6324, item 5).
   * Until then this method knew about SQL and nothing else, so {@code CREATE INDEX} sent with
   * {@code awaitResponse=false} worked in SQL and was refused in Cypher - an asymmetry a user met without warning.
   * <p>
   * An engine that cannot classify cheaply answers {@link QueryEngine.DDLClassification#UNKNOWN} and its statements
   * keep the behaviour they have always had, including the #6281 refusal if one turns out to need the barrier, which
   * is an honest refusal rather than a guess made from a keyword.
   * <p>
   * A statement that does not parse is NOT routed anywhere new: it goes to a worker and fails there, reported through
   * the caller's {@code onError} exactly as before. Classification must never be the thing that changes how an error
   * is delivered.
   * <p>
   * <b>What it costs.</b> For {@code sql} and {@code cypher} the parse is a statement-cache lookup that execution is
   * about to repeat with the same key, so classification is free. For {@code sqlscript} there is no such cache -
   * {@code SQLScriptQueryEngine.parseScript} parses every time and execution parses again - so classifying by parsing
   * alone would make every dispatched script, DDL or not, pay two full parses on the submitting thread. Hence
   * {@link #mayContainDDL}: a statement that does not so much as mention a DDL verb cannot be DDL, and is answered
   * without asking any engine anything.
   */
  private boolean requiresOffWorkerExecution(final DatabaseAsyncCommand task) {
    try {
      if (!mayContainDDL(task.command))
        // NO LANGUAGE HERE CAN BE DDL WITHOUT ONE OF THE VERBS, so the common case - an ordinary write or read
        // command - is classified without parsing anything on the submitting thread. It matters for `sql` too, not
        // just for scripts: the statement cache makes the parse free only once it is WARM, and a first dispatch of a
        // fresh statement would otherwise pay a full parse before the task is even queued, which is the one thing a
        // caller passing awaitResponse=false asked not to wait for.
        return false;

      // database.getQueryEngine and not QueryEngineManager.getEngine: the former caches the reusable engines per
      // database, so classifying costs a map lookup rather than a fresh engine object per dispatched command, on the
      // submitting thread of a caller that asked not to wait for anything.
      return database.getQueryEngine(task.language).classifyDDL(task.command) == QueryEngine.DDLClassification.DDL;

    } catch (final Exception e) {
      LogManager.instance()
          .log(this, Level.FINE, "Cannot classify asynchronous command %s: scheduling it on a worker", e, task);
    }
    return false;
  }

  /**
   * The cheap half of the classification: {@code false} means the text cannot be a DDL statement, so the parse that
   * would prove it is skipped and no engine is asked.
   * <p>
   * Sound because it is used ONLY as a negative filter. Every {@code DDLStatement} in the SQL grammar begins with one
   * of these verbs, and so do all four Cypher DDL statements ({@code CREATE}/{@code DROP INDEX},
   * {@code CREATE}/{@code DROP CONSTRAINT}), so a statement containing none of them has no DDL in it and the answer
   * is exact. A false positive - the word inside a string literal or an identifier - costs nothing but the parse that
   * was being paid unconditionally before, and the parse then gives the right answer anyway.
   * <p>
   * <b>The list has to keep up with the grammars</b>, and {@code AsyncDDLKeywordCoverageTest} is what makes that
   * automatic: it walks every {@code DDLStatement} subclass on the classpath, and every Cypher DDL kind, and fails if
   * one begins with a verb that is not here. A miss would not corrupt anything - the statement would go to a worker
   * and be refused there by #6281's guard - but it would quietly take back the operation this routing exists to give.
   */
  static boolean mayContainDDL(final String script) {
    for (final String keyword : DDL_LEADING_KEYWORDS)
      if (StringUtils.containsIgnoreCase(script, keyword))
        return true;
    return false;
  }

  /**
   * Runs an asynchronously dispatched command or query on {@link AsyncCommandPool} rather than on one of this
   * executor's own workers (issue #6303, item 3).
   * <p>
   * A command can be DDL, and the DDL that has to SCAN the data - {@code CREATE INDEX}, a manual index create,
   * {@code REBUILD INDEX} - must first make this very executor quiesce. A worker cannot do that: the quiescence
   * enqueues a task on every worker, its own included, and only that worker drains its queue. Before #6281 such a
   * command parked for ever; #6281 made it a clear refusal with a workaround; this gives the operation back, once,
   * for every operation that needs the barrier rather than by teaching each of them a special case.
   * <p>
   * The round-robin over the workers goes with the dispatch it was choosing between, and little is lost with it:
   * consecutive commands already landed on different workers, so it never ordered anything.
   * <p>
   * <b>The transaction contract is unchanged</b>, deliberately: a command used to run inside the worker's batch
   * transaction (every task declares {@code requiresActiveTx()} and the worker opens one on demand), so it runs
   * inside one here too - its own, begun before and committed after, rather than shared with unrelated record
   * writes. That is strictly closer to what the same command does synchronously.
   * <p>
   * <b>And it stays visible to {@link #waitCompletion(long)} and {@link #isProcessing()}</b>, which is the property
   * a caller would otherwise silently lose: {@code async().command(...)} followed by {@code async().waitCompletion()}
   * has to keep meaning what it meant.
   */
  private void submitCommand(final DatabaseAsyncCommand task) {
    // The same liveness refusal every other async task gets, which only scheduleTask used to give: this branch does
    // not go through it, so without this a DDL statement dispatched while the database is closing would be handed to
    // a JVM-wide pool and run against a half-closed database instead of failing synchronously here.
    if (executorThreads == null)
      throw new DatabaseOperationException(
          "Async executor has been shut down; cannot schedule asynchronous task " + task);

    commandsInFlight.incrementAndGet();
    // Counted at SUBMISSION, as scheduleTask counts a task the moment it reaches a queue, so the stat keeps meaning
    // "how many were accepted" rather than "how many have finished".
    counterScheduledTasks.incrementAndGet();

    boolean handedOver = false;
    try {
      AsyncCommandPool.getInstance().getExecutorService().execute(() -> runCommand(task));
      handedOver = true;
    } finally {
      // ONLY when the task provably never ran, which is why this is a flag and not a catch around the release.
      // Today's rejection policy runs the command on the submitter rather than throwing, so `execute` returning
      // normally covers the inline case too - and there runCommand has ALREADY released the count in its own
      // finally, so releasing again here would drive it below zero and let every later waitCompletion() return
      // while a command is still running. The flag is set after `execute` returns for exactly that reason: it
      // distinguishes "the policy refused outright" (nothing ran, release) from "the policy ran it here" (it
      // released itself). Unreachable while the policy is caller-runs; kept because the alternative to a correct
      // backstop is a silent leak the day somebody makes it abort.
      if (!handedOver)
        commandCompleted();
    }
  }

  private void runCommand(final DatabaseAsyncCommand task) {
    // EVERYTHING HERE HAS TO SURVIVE RUNNING ON A THREAD THAT IS NOT ONE OF THE POOL'S. The pool's rejection policy
    // is caller-runs, so a saturated pool executes this on the submitter - which for the transport this dispatch
    // exists for is an HTTP worker in the middle of its own request, with its own DatabaseContext and very possibly
    // its own open transaction. Adopting them and then committing and removing them would end that request's
    // transaction from underneath it. So the rule is: touch nothing this method did not create.
    final DatabaseContext.DatabaseContextTL existing = DatabaseContext.INSTANCE.getContextIfExists(
        database.getDatabasePath());
    final boolean contextCreated = existing == null;
    final DatabaseContext.DatabaseContextTL dbContext = contextCreated ?
        DatabaseContext.INSTANCE.init(database) :
        existing;

    // perThreadBucketSelection is set exactly as a worker sets it, because that flag is NOT "am I on an async
    // worker": it is what makes the default round-robin bucket strategy pick a bucket per THREAD, so writers
    // stop competing for the same pages. It matters here even though only DDL is routed to this pool, because a
    // sqlscript carrying one DDL statement brings whatever else it contains with it. What the flag is NOT is a
    // usable answer to "can this thread drain its own queue" - see note b of #6303 and the comment in
    // RebuildIndexStatement, which is precisely the confusion that used to make it refuse DDL dispatched here.
    //
    // Set and RESTORED, rather than set only on a context we created: on the caller-runs path the context belongs to
    // the submitter's own request, so leaving the flag raised would change the bucket selection of everything that
    // request does afterwards - and leaving it alone instead would run this statement under a different rule from
    // the identical one that was not rejected. Same shape as the principal binding in DatabaseAsyncCommand.
    final boolean previousBucketSelection = dbContext.perThreadBucketSelection;
    dbContext.perThreadBucketSelection = true;
    try {
      // Whether the transaction is OURS. On the caller-runs path it may well not be, and then there is nothing to
      // commit and nothing to roll back: the command is part of the submitter's transaction and its owner decides
      // its fate.
      //
      // Note the rollback below keys on THIS and not on task.idempotent, unlike the one inside the task - which
      // would be wrong for a query sharing a worker's batch, and cannot arise here: only command() reaches this
      // method (query() goes straight to a worker), so a task that gets this far is never idempotent.
      final boolean ownTransaction = task.requiresActiveTx() && !database.isTransactionActive();
      try {
        try {
          if (ownTransaction)
            database.begin();

          // No AsyncThread to hand it: the task never used it, and error reporting is this method's job now.
          task.execute(null, database);

          if (ownTransaction && database.isTransactionActive())
            database.commit();

        } catch (final Throwable e) {
          if (ownTransaction)
            rollbackQuietly(task);

          // Delivered exactly once, to the submitter's own callback when it left one - which is where it went
          // before, and the reason there is no retry here: a retry re-runs the statement, so it would notify
          // onComplete twice and, for a statement that writes, apply it twice. The executor-wide onError stays out
          // of it too: it is the record-task channel, and routing a command failure there as well both duplicates
          // the report and lets a callback that throws (a test's assertion, say) escape onto the pool thread ahead
          // of the notification the submitter is actually waiting for. Logged only when it has nowhere else to go.
          if (!task.notifyError(e))
            LogManager.instance().log(this, Level.SEVERE, "Error on executing asynchronous command %s", e, task);
        }
      } finally {
        if (contextCreated)
          DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
        else
          dbContext.perThreadBucketSelection = previousBucketSelection;
      }
    } finally {
      try {
        task.completed();
      } catch (final Throwable e) {
        // A completion notification that fails must not become the executor's problem: letting it out of here would
        // propagate into the pool's rejection policy on the inline path, where submitCommand would then have to tell
        // "the task never ran" from "the task ran and threw on its way out". Contained, so it cannot.
        LogManager.instance()
            .log(this, Level.WARNING, "Error on notifying the completion of asynchronous command %s", e, task);
      } finally {
        commandCompleted();
      }
    }
  }

  /** Discards the transaction of a failed command attempt so the next attempt - or the next task - starts clean. */
  private void rollbackQuietly(final DatabaseAsyncCommand task) {
    try {
      if (database.isTransactionActive())
        database.rollback();
    } catch (final Exception rollbackError) {
      LogManager.instance()
          .log(this, Level.WARNING, "Error on rolling back the transaction of asynchronous command %s", rollbackError,
              task);
    }
  }

  /**
   * Whether any dispatched command of this database is still running - <b>as seen from the calling thread</b>.
   * <p>
   * Always {@code false} on an {@link AsyncCommandPool} thread, and that exemption is the whole reason this is a
   * method rather than a field read. The caller IS one of the commands it would be asking about, and every
   * scan-based index build reaches {@link #waitCompletion(long)}: a dispatched {@code CREATE INDEX} would wait for a
   * set it is a member of, and {@code LocalDatabase.waitForAsyncCompletion}'s
   * {@code do { ... } while (isAsyncProcessing())} loop would never exit. That is the self-deadlock #6281 fixed on
   * the workers, rebuilt one pool over - measured as exactly that before this exemption existed.
   * <p>
   * Waiting for a PEER command would not be safe either, which is why the exemption is "on a pool thread" and not
   * "all but my own": the pool's queue is bounded, so a peer can be sitting behind this task in it with no thread
   * left to run it. What the barrier is actually for - the workers' open batches - is served by the per-worker
   * markers, which run either way. The pool is JVM-wide, so this also exempts a thread running ANOTHER database's
   * command; that is intended and rests on the same argument - see {@link AsyncCommandPool}'s POOL_THREAD javadoc.
   */
  private boolean hasPendingCommands() {
    return commandsInFlight.get() > 0 && !AsyncCommandPool.isPoolThread();
  }

  /** Drops one in-flight command and releases {@link #waitCompletion(long)} when the last one lands. */
  private void commandCompleted() {
    if (commandsInFlight.decrementAndGet() <= 0)
      synchronized (commandsLock) {
        commandsLock.notifyAll();
      }
  }

  /**
   * Waits for the commands dispatched to {@link AsyncCommandPool} within whatever is left of the caller's budget.
   * Runs BEFORE the per-worker markers of {@link #waitCompletion(long)}, because a command can itself submit async
   * record tasks and those have to end up behind the markers rather than in front of them.
   *
   * @return false when the budget ran out first, matching {@link #waitCompletion(long)}'s own contract.
   */
  private boolean awaitCommands(final long beginTime, final long timeout) {
    while (hasPendingCommands()) {
      final long remaining = timeout - (System.currentTimeMillis() - beginTime);
      if (remaining <= 0)
        return false;
      synchronized (commandsLock) {
        if (!hasPendingCommands())
          return true;
        try {
          commandsLock.wait(Math.min(remaining, 500));
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Captures the principal bound to the calling thread's {@link DatabaseContext} so it can be re-bound on the worker
   * thread before the command/query runs (GHSA-5j4x-3jfw-8xv3). Returns null in embedded/internal use where no user is
   * bound; the engine permission gates are then a no-op, preserving the prior behaviour for non-HTTP callers.
   */
  private SecurityDatabaseUser captureCurrentUser() {
    final DatabaseContext.DatabaseContextTL ctx = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
    return ctx != null ? ctx.getCurrentUser() : null;
  }

  @Override
  public void scanType(final String typeName, final boolean polymorphic, final DocumentCallback callback) {
    scanType(typeName, polymorphic, callback, null);
  }

  @Override
  public void scanType(final String typeName, final boolean polymorphic, final DocumentCallback callback,
                       final ErrorRecordCallback errorRecordCallback) {
    try {
      final DocumentType type = database.getSchema().getType(typeName);

      final List<Bucket> buckets = type.getBuckets(polymorphic);
      final CountDownLatch semaphore = new CountDownLatch(buckets.size());

      for (final Bucket b : buckets) {
        final int slot = getSlot(b.getFileId());
        scheduleTask(slot, new DatabaseAsyncScanBucket(semaphore, callback, errorRecordCallback, b), true,
            backPressurePercentage);
      }

      semaphore.await();

    } catch (final Exception e) {
      throw new DatabaseOperationException(
          "Error on executing parallel scan of type '" + database.getSchema().getType(typeName) + "'", e);
    }
  }

  @Override
  public void transaction(final Database.TransactionScope txBlock) {
    transaction(txBlock, database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES));
  }

  @Override
  public void transaction(final Database.TransactionScope txBlock, final int retries) {
    transaction(txBlock, retries, null, null);
  }

  @Override
  public void transaction(final Database.TransactionScope txBlock, final int retries, final OkCallback ok,
                          final ErrorCallback error) {
    transaction(txBlock, retries, ok, error, getSlot((int) transactionCounter.getAndIncrement()));
  }

  @Override
  public void transaction(final Database.TransactionScope txBlock, final int retries, final OkCallback ok,
                          final ErrorCallback error, final int slot) {
    scheduleTask(slot, new DatabaseAsyncTransaction(txBlock, retries, ok, error), true, backPressurePercentage);
  }

  @Override
  public void createRecord(final MutableDocument record, final NewRecordCallback newRecordCallback) {
    createRecord(record, newRecordCallback, null);
  }

  @Override
  public void createRecord(final MutableDocument record, final NewRecordCallback newRecordCallback,
                           final ErrorCallback errorCallback) {
    final DocumentType type = record.getType();

    if (record.getIdentity() == null) {
      // NEW
      final Bucket bucket = type.getBucketIdByRecord(record, false);
      final int slot = getSlot(bucket.getFileId());

      scheduleTask(slot, new DatabaseAsyncCreateRecord(record, bucket, newRecordCallback, errorCallback), true,
          backPressurePercentage);

    } else
      throw new IllegalArgumentException("Cannot create a new record because it is already persistent");
  }

  @Override
  public void createRecord(final Record record, final String bucketName, final NewRecordCallback newRecordCallback) {
    createRecord(record, bucketName, newRecordCallback, null);
  }

  @Override
  public void createRecord(final Record record, final String bucketName, final NewRecordCallback newRecordCallback,
                           final ErrorCallback errorCallback) {
    final Bucket bucket = database.getSchema().getBucketByName(bucketName);
    final int slot = getSlot(bucket.getFileId());

    if (record.getIdentity() == null)
      // NEW
      scheduleTask(slot, new DatabaseAsyncCreateRecord(record, bucket, newRecordCallback, errorCallback), true,
          backPressurePercentage);
    else
      throw new IllegalArgumentException("Cannot create a new record because it is already persistent");
  }

  @Override
  public void updateRecord(final MutableDocument record, final UpdatedRecordCallback updateRecordCallback) {
    updateRecord(record, updateRecordCallback, null);
  }

  @Override
  public void updateRecord(final MutableDocument record, final UpdatedRecordCallback updateRecordCallback,
                           final ErrorCallback errorCallback) {
    if (record.getIdentity() != null) {
      // UPDATE
      final int slot = getSlot(record.getIdentity().getBucketId());
      scheduleTask(slot, new DatabaseAsyncUpdateRecord(record, updateRecordCallback, errorCallback), true,
          backPressurePercentage);

    } else
      throw new IllegalArgumentException("Cannot updated a not persistent record");
  }

  @Override
  public void deleteRecord(final Record record, final DeletedRecordCallback deleteRecordCallback) {
    deleteRecord(record, deleteRecordCallback, null);
  }

  @Override
  public void deleteRecord(final Record record, final DeletedRecordCallback deleteRecordCallback,
                           final ErrorCallback errorCallback) {
    if (record.getIdentity() != null) {
      // DELETE
      final int slot = getSlot(record.getIdentity().getBucketId());
      scheduleTask(slot, new DatabaseAsyncDeleteRecord(record, deleteRecordCallback, errorCallback), true,
          backPressurePercentage);

    } else
      throw new IllegalArgumentException("Cannot delete a not persistent record");
  }

  @Override
  @Deprecated
  public void newEdge(final Vertex sourceVertex, final String edgeType, final RID destinationVertexRID,
                      final boolean bidirectional,
                      final boolean light, final NewEdgeCallback callback, final Object... properties) {
    if (!bidirectional && database.getSchema().getType(edgeType) instanceof EdgeType type && type.isBidirectional())
      throw new IllegalArgumentException("Edge type '" + edgeType + "' is not bidirectional");

    newEdge(sourceVertex, edgeType, destinationVertexRID, light, callback, properties);
  }

  @Override
  public void newEdge(final Vertex sourceVertex, final String edgeType, final RID destinationVertexRID,
                      final boolean light,
                      final NewEdgeCallback callback, final Object... properties) {
    if (sourceVertex == null)
      throw new IllegalArgumentException("Source vertex is null");

    if (destinationVertexRID == null)
      throw new IllegalArgumentException("Destination vertex is null");

    final int sourceSlot = getSlot(sourceVertex.getIdentity().getBucketId());
    final int destinationSlot = getSlot(destinationVertexRID.getBucketId());

    // Validate the type kind before scheduling any task: a vertex or document type used as edge type
    // must fail with a clean schema error, not an internal ClassCastException (issue #5194)
    if (!(database.getSchema().getType(edgeType) instanceof EdgeType resolvedEdgeType))
      throw new SchemaException("Type '" + edgeType + "' is not an edge type");
    final boolean bidirectional = resolvedEdgeType.isBidirectional();

    if (sourceSlot == destinationSlot)
      // BOTH VERTICES HAVE THE SAME SLOT, CREATE THE EDGE USING IT
      scheduleTask(sourceSlot,
          new CreateEdgeAsyncTask(sourceVertex, destinationVertexRID, edgeType, properties, light, callback), true,
          backPressurePercentage);
    else {
      // CREATE THE EDGE IN THE SOURCE VERTEX'S SLOT AND A CASCADE TASK TO ADD THE INCOMING EDGE FROM DESTINATION
      // VERTEX (THIS IS THE MOST EXPENSIVE CASE WHERE 2 TASKS ARE EXECUTED)
      scheduleTask(sourceSlot, new CreateEdgeAsyncTask(sourceVertex, destinationVertexRID, edgeType, properties, light,
          (newEdge, createdSourceVertex, createdDestinationVertex) -> {
            if (bidirectional) {
              scheduleTask(destinationSlot,
                  new CreateIncomingEdgeAsyncTask(sourceVertex.getIdentity(), destinationVertexRID, newEdge,
                      (newEdge1, createdSourceVertex1, createdDestinationVertex1) -> {
                        if (callback != null)
                          callback.call(newEdge1, createdSourceVertex1, createdDestinationVertex1);
                      }), true, 0);
            } else if (callback != null)
              callback.call(newEdge, createdSourceVertex, createdDestinationVertex);

          }), true, backPressurePercentage);
    }
  }

  @Override
  public void newEdgeByKeys(final String sourceVertexType, final String sourceVertexKeyName,
                            final Object sourceVertexKeyValue,
                            final String destinationVertexType, final String destinationVertexKeyName,
                            final Object destinationVertexKeyValue,
                            final boolean createVertexIfNotExist, final String edgeType, final boolean bidirectional,
                            final boolean lightWeight,
                            final NewEdgeCallback callback, final Object... properties) {
    newEdgeByKeys(sourceVertexType, new String[]{sourceVertexKeyName}, new Object[]{sourceVertexKeyValue},
        destinationVertexType, new String[]{destinationVertexKeyName}, new Object[]{destinationVertexKeyValue},
        createVertexIfNotExist, edgeType, bidirectional, lightWeight, callback, properties);
  }

  @Override
  public void newEdgeByKeys(final String sourceVertexType, final String[] sourceVertexKeyNames,
                            final Object[] sourceVertexKeyValues, final String destinationVertexType,
                            final String[] destinationVertexKeyNames,
                            final Object[] destinationVertexKeyValues, final boolean createVertexIfNotExist,
                            final String edgeType,
                            final boolean bidirectional, final boolean lightWeight, final NewEdgeCallback callback,
                            final Object... properties) {

    if (sourceVertexKeyNames == null)
      throw new IllegalArgumentException("Source vertex key is null");

    if (sourceVertexKeyNames.length != sourceVertexKeyValues.length)
      throw new IllegalArgumentException("Source vertex key and value arrays have different sizes");

    if (destinationVertexKeyNames == null)
      throw new IllegalArgumentException("Destination vertex key is null");

    if (destinationVertexKeyNames.length != destinationVertexKeyValues.length)
      throw new IllegalArgumentException("Destination vertex key and value arrays have different sizes");

    final Iterator<Identifiable> sourceResult = database.lookupByKey(sourceVertexType, sourceVertexKeyNames,
        sourceVertexKeyValues);
    final Iterator<Identifiable> destinationResult = database.lookupByKey(destinationVertexType,
        destinationVertexKeyNames,
        destinationVertexKeyValues);

    final RID sourceRID = sourceResult.hasNext() ? sourceResult.next().getIdentity() : null;
    final RID destinationRID = destinationResult.hasNext() ? destinationResult.next().getIdentity() : null;

    if (sourceRID == null && destinationRID == null) {

      if (!createVertexIfNotExist)
        throw new IllegalArgumentException(
            "Cannot find source and destination vertices with respectively key " + Arrays.toString(sourceVertexKeyNames) + "="
                + Arrays.toString(sourceVertexKeyValues) + " and " + Arrays.toString(destinationVertexKeyNames) + "="
                + Arrays.toString(destinationVertexKeyValues));

      // SOURCE AND DESTINATION VERTICES BOTH DON'T EXIST: CREATE 2 VERTICES + EDGE IN THE SAME TASK PICKING THE BEST
      // SLOT
      scheduleTask(getRandomSlot(),
          new CreateBothVerticesAndEdgeAsyncTask(sourceVertexType, sourceVertexKeyNames, sourceVertexKeyValues,
              destinationVertexType, destinationVertexKeyNames, destinationVertexKeyValues, edgeType, properties,
              lightWeight,
              callback), true, backPressurePercentage);

    } else if (sourceRID != null && destinationRID == null) {

      if (!createVertexIfNotExist)
        throw new IllegalArgumentException(
            "Cannot find destination vertex with key " + Arrays.toString(destinationVertexKeyNames) + "=" + Arrays.toString(
                destinationVertexKeyValues));

      // ONLY SOURCE VERTEX EXISTS, CREATE DESTINATION VERTEX + EDGE IN SOURCE'S SLOT
      scheduleTask(getSlot(sourceRID.getBucketId()),
          new CreateDestinationVertexAndEdgeAsyncTask(sourceRID, destinationVertexType, destinationVertexKeyNames,
              destinationVertexKeyValues, edgeType, properties, lightWeight, callback), true,
          backPressurePercentage);

    } else if (sourceRID == null && destinationRID != null) {

      if (!createVertexIfNotExist)
        throw new IllegalArgumentException(
            "Cannot find source vertex with key " + Arrays.toString(sourceVertexKeyNames) + "=" + Arrays.toString(
                sourceVertexKeyValues));

      // ONLY DESTINATION VERTEX EXISTS
      scheduleTask(getSlot(destinationRID.getBucketId()),
          new CreateSourceVertexAndEdgeAsyncTask(sourceVertexType, sourceVertexKeyNames, sourceVertexKeyValues,
              destinationRID,
              edgeType, properties, lightWeight, callback), true, backPressurePercentage);

    } else
      // BOTH VERTICES EXIST
      newEdge(sourceRID.asVertex(true), edgeType, destinationRID, lightWeight, callback, properties);
  }

  @Override
  public void appendSamples(final String typeName, final long[] timestamps, final Object[]... columnValues) {
    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(typeName);
    final TimeSeriesEngine engine = tsType.getEngine();
    final int shardIdx = (int) (tsAppendCounter.getAndIncrement() % engine.getShardCount());
    final int slot = getSlot(shardIdx);
    scheduleTask(slot, new DatabaseAsyncAppendSamples(engine, shardIdx, timestamps, columnValues), true,
        backPressurePercentage);
  }

  @Override
  public void appendSamples(final String typeName, final TimeSeriesRowSource source) {
    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(typeName);
    final TimeSeriesEngine engine = tsType.getEngine();
    final int shardIdx = (int) (tsAppendCounter.getAndIncrement() % engine.getShardCount());
    final int slot = getSlot(shardIdx);
    scheduleTask(slot, new DatabaseAsyncAppendSamples(engine, shardIdx, source), true, backPressurePercentage);
  }

  /**
   * Test only API.
   */
  @Override
  public void kill() {
    final AsyncThread[] threads;
    synchronized (lifecycleLock) {
      threads = executorThreads;
      if (threads == null)
        return;
      // Unpublish first so concurrent callers stop targeting the about-to-die threads.
      executorThreads = null;
      for (int i = 0; i < threads.length; ++i)
        threads[i].forceShutdown = true;
    }
    // WAIT FOR SHUTDOWN, MAX 1S EACH - interrupt to wake threads from blocking queue poll
    for (int i = 0; i < threads.length; ++i)
      threads[i].interrupt();
    // Defer re-asserting the caller's interrupt status until every thread has been joined: setting
    // it mid-loop makes the next join() throw immediately, skipping the remaining threads.
    boolean interrupted = false;
    for (int i = 0; i < threads.length; ++i) {
      try {
        threads[i].join(1000);
      } catch (final InterruptedException e) {
        interrupted = true;
      }
      if (threads[i].isAlive())
        LogManager.instance()
            .log(this, Level.WARNING, "AsyncThread %s did not stop within 1s after kill()", threads[i].getName());
    }
    if (interrupted)
      Thread.currentThread().interrupt();
  }

  public void close() {
    // Commands dispatched to AsyncCommandPool hold this database and are not in any worker queue, so the worker
    // shutdown below does not reach them (issue #6303, item 3). Give them a bounded chance to finish first: a
    // command still running against a database that has just closed fails on every operation it attempts, which is
    // noise the caller cannot act on. Bounded by the same budget the worker join uses, and NOT fatal when it
    // expires - close() must never be the thing that hangs.
    if (!awaitCommands(System.currentTimeMillis(), shutdownJoinTimeoutMs))
      LogManager.instance().log(this, Level.WARNING,
          "%d asynchronous command(s) of database '%s' were still running after %d ms: closing anyway",
          null, commandsInFlight.get(), database.getName(), shutdownJoinTimeoutMs);

    shutdownThreads();
  }

  @Override
  public int getParallelLevel() {
    return parallelLevel;
  }

  @Override
  public void setParallelLevel(final int parallelLevel) {
    if (parallelLevel != this.parallelLevel)
      createThreads(parallelLevel);
  }

  @Override
  public int getBackPressure() {
    return backPressurePercentage;
  }

  @Override
  public void setBackPressure(final int percentage) {
    this.backPressurePercentage = percentage;
  }

  @Override
  public int getCommitEvery() {
    return commitEvery;
  }

  @Override
  public void setCommitEvery(final int commitEvery) {
    // #4961: 0 would make every task fail with an ArithmeticException on count % commitEvery.
    if (commitEvery < 1)
      throw new IllegalArgumentException("commitEvery must be >= 1 (was " + commitEvery + ")");
    this.commitEvery = commitEvery;
  }

  @Override
  public int getThreadCount() {
    final AsyncThread[] threads = executorThreads;
    return threads != null ? threads.length : 0;
  }

  public static class DBAsyncStats {
    public long queueSize;
    public long scheduledTasks;
  }

  // Package-private test hook (#5072 review, point 4): rebuilds the worker pool so configuration
  // changes (e.g. ASYNC_OPERATIONS_QUEUE_SIZE) are picked up deterministically, instead of the
  // tests relying on setTransactionUseWAL()'s createThreads() side effect.
  void recreateThreadsForTests() {
    createThreads(parallelLevel);
  }

  private void createThreads(int parallelLevel) {
    if (parallelLevel < 1)
      parallelLevel = 1;

    // Build the new pool fully before publishing, so readers never observe a half-initialized
    // array. Synchronization on lifecycleLock serializes concurrent createThreads/shutdownThreads
    // calls (the prior version could NPE under concurrent GraphBatch users; see issue: heavy
    // parallel inserts triggered "Cannot store to object array because executorThreads is null").
    synchronized (lifecycleLock) {
      shutdownThreadsLocked();

      final AsyncThread[] newThreads = new AsyncThread[parallelLevel];
      this.parallelLevel = parallelLevel;
      for (int i = 0; i < parallelLevel; ++i) {
        newThreads[i] = new AsyncThread(database, i);
        newThreads[i].start();
      }

      this.executorThreads = newThreads;
    }
  }

  private void shutdownThreads() {
    synchronized (lifecycleLock) {
      shutdownThreadsLocked();
    }
  }

  // Caller must hold lifecycleLock.
  private void shutdownThreadsLocked() {
    final AsyncThread[] toClose = executorThreads;
    if (toClose == null)
      return;

    // Unpublish first so concurrent readers stop seeing the about-to-die threads.
    executorThreads = null;

    // SET SHUTDOWN STATUS TO ALL THE THREADS
    for (int i = 0; i < toClose.length; ++i)
      toClose[i].shutdown = true;

    // #4954: the old untimed queue.put(FORCE_EXIT) under lifecycleLock hung database.close() (and
    // any later createThreads()/kill()) for as long as a busy worker with a full queue kept running
    // its current task. Bounded offer + interrupt on failure: the woken worker exits its loop and
    // notifies completed() on whatever it could not execute (see drainQueueNotifyingWaiters).
    boolean interrupted = false;
    for (int i = 0; i < toClose.length; ++i) {
      try {
        if (!toClose[i].queue.offer(FORCE_EXIT, 1, TimeUnit.SECONDS))
          toClose[i].interrupt();

        // WAIT FOR SHUTDOWN, MAX shutdownJoinTimeoutMs EACH (10S BY DEFAULT)
        toClose[i].join(shutdownJoinTimeoutMs);

        if (toClose[i].isAlive()) {
          // #5062 review r4 (point 1): a successful FORCE_EXIT offer sends no interrupt, but the
          // marker may have been consumed inside offerHelping, which only sets forceShutdown and
          // keeps looping to hand off the current follow-up: on a wedged peer's full queue the flag
          // is not re-checked until the stall backstop fires (up to ~60s). Escalate after the grace
          // period: the interrupt wakes the offer park, unwinds the task loudly (onError) and the
          // run loop exits on the flag.
          toClose[i].interrupt();
          toClose[i].join(shutdownJoinTimeoutMs);
        }
      } catch (final InterruptedException e) {
        interrupted = true;
      }
      if (toClose[i].isAlive())
        LogManager.instance()
            .log(this, Level.WARNING, "AsyncThread %s did not stop within %dms after shutdown (escalated to interrupt)",
                toClose[i].getName(), shutdownJoinTimeoutMs);
    }
    if (interrupted)
      Thread.currentThread().interrupt();
  }

  @Override
  public void onOk() {
    if (onOkCallback != null) {
      try {
        onOkCallback.call();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, """
            Error on invoking onOk() callback for asynchronous operation \
            %s""", e, this);
      }
    }
  }

  @Override
  public void onError(final Throwable e) {
    if (onErrorCallback != null) {
      try {
        onErrorCallback.call(e);
      } catch (final Exception e1) {
        LogManager.instance()
            .log(this, Level.SEVERE, "Error on invoking onError() callback for asynchronous operation %s", e1, this);
      }
    }
  }

  /**
   * Schedule a task to be executed by parallel executors.
   *
   * @param slot              slot id
   * @param task              task to schedule
   * @param waitIfQueueIsFull true to wait in case the queue is full, otherwise false
   * @return true if the task has been scheduled, otherwise false
   */
  public boolean scheduleTask(int slot, final DatabaseAsyncTask task, final boolean waitIfQueueIsFull,
                              final int applyBackPressureOnPercentage) {
    try {
      if (slot == -1)
        slot = getBestSlot();

      final AsyncThread[] threads = executorThreads;
      if (threads == null)
        throw new DatabaseOperationException(
            "Async executor has been shut down; cannot schedule asynchronous task " + task);

      final AsyncThread target = threads[slot];
      final BlockingQueue<DatabaseAsyncTask> queue = target.queue;

      if (applyBackPressureOnPercentage > 0) {
        final int queueFullAt = queueFullPercentage(queue);

        if (queueFullAt >= applyBackPressureOnPercentage)
          // TODO: VARIABLE SLEEP TIME BASED ON HOW MUCH THE QUEUE IS FULL
          Thread.sleep(queueFullAt);
      }

      final boolean scheduled;
      if (queue.offer(task))
        scheduled = true;
      else if (waitIfQueueIsFull) {
        offerWaiting(target, slot, task, applyBackPressureOnPercentage);
        scheduled = true;
      } else
        scheduled = false;

      if (scheduled) {
        // NOTE (#5081 review): remove(Object) on the Disruptor queue is an O(n) whole-queue-locking scan.
        // Acceptable ONLY because this undo runs on the rare dead-worker post-shutdown race - it must never
        // migrate onto the steady-state scheduling path.
        if (!target.isAlive() && removeQuietly(queue, task))
          // The worker exited (shutdown) after its final queue drain but before this offer landed:
          // the task would sit unexecuted forever. Undo the offer and fail like any post-shutdown
          // scheduling attempt. #5062 review r3 (point 2): this recheck is best-effort, not total -
          // an offer landing after the final drain poll but before isAlive() flips to false passes
          // the guard and is orphaned; closing it would need a lock on this hot path.
          // #5062 review r4 (point 4): completed() is deliberately NOT invoked on the removed task -
          // unlike the shutdown drain, the scheduling caller is still on the stack and this
          // exception informs it directly, so no waiter can be parked on the task yet.
          throw new DatabaseOperationException(
              "Async executor has been shut down; cannot schedule asynchronous task " + task);
        counterScheduledTasks.incrementAndGet();
      }
      return scheduled;

    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DatabaseOperationException("Error on executing asynchronous task " + task);
    }
  }

  private static boolean removeQuietly(final BlockingQueue<DatabaseAsyncTask> queue, final DatabaseAsyncTask task) {
    try {
      return queue.remove(task);
    } catch (final UnsupportedOperationException e) {
      // #5066: DEFENSIVE ONLY - both shipped queue impls ('standard' ArrayBlockingQueue and 'fast'
      // DisruptorBlockingQueue) support remove(Object) now that 'fast' no longer maps to
      // PushPullBlockingQueue. If a future impl lacks remove, the offer cannot be undone: a task
      // that landed right after a dead worker's final drain stays queued with completed() never
      // fired, and the WARNING below gives operators the reason should a waiter hang.
      LogManager.instance()
          .log(DatabaseAsyncExecutorImpl.class, Level.WARNING,
              "Asynchronous task %s was scheduled on a worker that already shut down and cannot be removed from its "
                  + "'fast' queue: its completion will never be notified. Use the 'standard' async queue implementation "
                  + "to close this window", task);
      return false;
    }
  }

  /**
   * Back-pressure gauge: how full the queue is, as a 0-100 percentage.
   * <p>
   * #5081 review: {@code remainingCapacity()} and {@code size()} are read ONCE each into locals so the
   * numerator and denominator are computed from the same pair - the previous inline form called each twice,
   * and on the 'fast' {@link com.conversantmedia.util.concurrent.DisruptorBlockingQueue} the two are
   * weakly-consistent estimates, so a full-then-drained race between the calls could yield a zero
   * denominator ({@code ArithmeticException}). {@code Math.max(1, ...)} guards the divide regardless.
   * <p>
   * On the guarded {@code remaining=0, size=0} snapshot this returns 100 ("full"), not 0: an ambiguous
   * estimate biases toward MORE back-pressure for that one iteration, self-correcting on the next re-read.
   */
  // Package-visible for AsyncFastQueueShutdownUndoTest, which feeds a fake queue reporting the racy 0/0
  // snapshot the real TOCTOU cannot be forced to produce deterministically.
  static int queueFullPercentage(final BlockingQueue<DatabaseAsyncTask> queue) {
    final int remaining = queue.remainingCapacity();
    final int size = queue.size();
    return 100 - (remaining * 100 / Math.max(1, remaining + size));
  }

  /**
   * Blocks until the task is enqueued on the target worker's currently full queue (#4953).
   * <p>
   * Two behaviors depending on the calling thread:
   * <ul>
   *   <li><b>Worker of this executor (e.g. the cross-slot incoming-edge follow-up in newEdge):</b>
   *   the worker drains tasks from its OWN queue into a parked list while it waits (see
   *   {@code offerHelping}). Two workers cross-scheduling into each other's full queues
   *   (bidirectional edge load) formed a wait cycle the old head-identity stall detector could only
   *   break by throwing, which rolled back the worker's whole in-flight commit batch and silently
   *   dropped the follow-up. Draining frees the slot the peer is parked on, so the cycle resolves
   *   without throwing; if the deferral budget runs out the worker falls through to the bounded
   *   wait below.</li>
   *   <li><b>Any other producer:</b> waits in windows of {@code checkForStalledQueuesMaxDelay} and
   *   throws if the target worker completed no task over
   *   {@code STALLED_CROSS_SLOT_NO_PROGRESS_WINDOWS} consecutive windows while itself being
   *   parked handing a task to another queue (a genuine scheduling cycle beyond the helping budget),
   *   after the worker died, or - the backstop for a worker wedged inside user code - after
   *   {@code STALLED_NO_PROGRESS_WINDOWS} consecutive windows with zero completed tasks. A worker
   *   merely busy on a single slow task no longer trips the detector: the old code compared the
   *   identity of the queue head across two windows, so any head task running longer than 2x the
   *   delay made innocent producers throw.</li>
   * </ul>
   */
  private void offerWaiting(final AsyncThread target, final int slot, final DatabaseAsyncTask task,
                            final int applyBackPressureOnPercentage) throws InterruptedException {
    final AsyncThread self =
        Thread.currentThread() instanceof AsyncThread worker && worker.getOwner() == this ? worker : null;

    if (self != null && offerHelping(self, target, task))
      return;

    final BlockingQueue<DatabaseAsyncTask> queue = target.queue;
    final boolean prevWaiting = self != null && self.waitingCrossSlotOffer;
    if (self != null)
      self.waitingCrossSlotOffer = true;
    try {
      long observedCompleted = target.completedTaskCount;
      int windowsWithoutProgress = 0;
      while (!queue.offer(task, checkForStalledQueuesMaxDelay, TimeUnit.MILLISECONDS)) {
        if (!target.isAlive())
          throw new DatabaseOperationException(
              "Async executor has been shut down; cannot schedule asynchronous task " + task);

        final long nowCompleted = target.completedTaskCount;
        if (nowCompleted == observedCompleted) {
          ++windowsWithoutProgress;

          if (target.waitingCrossSlotOffer) {
            // NO TASK COMPLETED OVER N CONSECUTIVE WINDOWS WHILE THE WORKER IS ITSELF PARKED HANDING
            // A TASK TO ANOTHER QUEUE. #5062 review r3 (point 1): a single flat window used to throw
            // here, but a budget-exhausted worker whose peer is merely busy on one slow task shows
            // exactly that signature for a moment: require enough windows to outlive a slow peer.
            if (windowsWithoutProgress >= STALLED_CROSS_SLOT_NO_PROGRESS_WINDOWS)
              throw new DatabaseOperationException("Asynchronous queue " + slot
                  + " is stalled. This could happen when an asynchronous task schedules more asynchronous tasks");

            // #5062 review r2 (point 1): a worker wedged inside user code (infinite loop or blocking
            // call in a task's execute()/callback) never sets waitingCrossSlotOffer, so without this
            // backstop the producer would hang here forever. Much longer than the old 2-window
            // head-identity detector and progress-gated, so a single slow task does not trip it.
          } else if (windowsWithoutProgress >= STALLED_NO_PROGRESS_WINDOWS)
            throw new DatabaseOperationException(
                "Asynchronous queue " + slot + " is stalled: no task completed in the last " + (
                    STALLED_NO_PROGRESS_WINDOWS * checkForStalledQueuesMaxDelay)
                    + " ms while its queue stayed full. The worker may be blocked inside a user task or callback");
        } else {
          windowsWithoutProgress = 0;
          observedCompleted = nowCompleted;
        }

        if (applyBackPressureOnPercentage > 0) {
          final int queueFullAt = queueFullPercentage(queue);
          Thread.sleep(100 + (4L * queueFullAt));
        }
      }
    } finally {
      if (self != null)
        self.waitingCrossSlotOffer = prevWaiting;
    }
  }

  /**
   * Help-while-waiting loop (#4953): the calling worker keeps draining its own queue while it waits
   * for space on the target queue, so two workers cross-scheduling into each other's full queues
   * free the slot the peer is parked on and the cycle resolves on its own. Polled tasks are NOT
   * executed here: this worker is suspended mid-execute() of its current task, and a nested
   * execution could commit or roll back that task's partial writes, breaking per-task atomicity
   * (#5062 review, point 1). They are parked in {@code helpDeferredTasks} and run by the run loop,
   * in poll order, once the current task unwinds; that is also after the hand-off, so a parked
   * waitCompletion() marker cannot fire before the current task's follow-up has been scheduled.
   * <p>
   * The parked backlog is capped at the queue capacity: past it the method gives up (returns false)
   * and the caller falls back to the bounded wait with stall detection, restoring queue-full
   * backpressure on producers. A cheap-trigger liveness detector is deliberately absent: over short
   * horizons a target busy on a legitimately slow task is indistinguishable from a wedged one, and
   * throwing out of a worker mid-task rolls back its commit batch and drops the follow-up, which is
   * the exact #4953 defect. Two bounded escapes exist nevertheless (#5062 review r6): with an EMPTY
   * own queue the deferral budget can never grow, so a wedged-alive peer is covered by the same
   * progress-gated {@code STALLED_NO_PROGRESS_WINDOWS} backstop as the producer path, failing the
   * hand-off loudly (onError + batch rollback; the worker itself survives and keeps serving its
   * queue) instead of spinning forever; and once {@code forceShutdown} is observed the hand-off is
   * abandoned immediately, so close() does not sit out the grace period waiting for the interrupt
   * escalation.
   *
   * @return true if the task was handed to the target queue, false if the deferral budget is
   * exhausted and the caller must fall back to the bounded wait of {@code offerWaiting}.
   */
  private boolean offerHelping(final AsyncThread self, final AsyncThread target, final DatabaseAsyncTask task)
      throws InterruptedException {
    final boolean prevWaiting = self.waitingCrossSlotOffer;
    self.waitingCrossSlotOffer = true;
    try {
      long observedCompleted = target.completedTaskCount;
      long windowStart = System.currentTimeMillis();
      int windowsWithoutProgress = 0;
      long lastProgressAt = System.currentTimeMillis();
      while (true) {
        // #5062 review r6 (point 2): consuming FORCE_EXIT below only sets the flag; keeping the
        // hand-off alive on a possibly wedged peer would make close() pay the whole grace period
        // before the interrupt escalation. Dropping the follow-up loudly at shutdown is fine - the
        // workers are dying - and matches the target-dead branch below.
        if (self.forceShutdown)
          throw new DatabaseOperationException(
              "Async executor has been shut down; cannot schedule asynchronous task " + task);

        // SHORT WINDOW WHEN THERE IS OWN WORK TO DRAIN, LONGER WHEN IDLE TO AVOID SPINNING. THE 1MS
        // WINDOW IS INTENTIONALLY AGGRESSIVE: EVERY ITERATION FREES A SLOT THE PARKED PEER MAY BE
        // WAITING ON, AND THE LOOP IS BOUNDED BY THE DEFERRAL BUDGET BELOW
        final long window = self.queue.isEmpty() ? 100 : 1;
        if (target.queue.offer(task, window, TimeUnit.MILLISECONDS))
          return true;

        if (!target.isAlive())
          throw new DatabaseOperationException(
              "Async executor has been shut down; cannot schedule asynchronous task " + task);

        // #5062 review r6 (point 1): with an empty own queue the deferral budget below never grows,
        // so a wedged-alive peer would spin this loop forever, silently losing both workers in
        // normal operation. Same progress-gated bound as the producer-side wedged backstop; only the
        // long, conservative count is used because aborting the hand-off costs the current task's
        // batch (onError + rollback) - a peer parked on a slow chain is outlived as long as its
        // individual tasks stay under STALLED_NO_PROGRESS_WINDOWS windows.
        final long now = System.currentTimeMillis();
        if (now - windowStart >= checkForStalledQueuesMaxDelay) {
          final long nowCompleted = target.completedTaskCount;
          if (nowCompleted == observedCompleted) {
            if (++windowsWithoutProgress >= STALLED_NO_PROGRESS_WINDOWS)
              // Report the MEASURED span since the last observed progress (#5072 review): at tiny
              // configured delays each iteration is floored by the offer window, so the nominal
              // windows-x-delay product would understate it - and measuring from the last progress (not
              // the first no-progress window) covers the full span, matching the 12-window trip.
              throw new DatabaseOperationException(
                  "Asynchronous queue of " + target.getName() + " is stalled: no task completed in the last " + (
                      now - lastProgressAt)
                      + " ms while handing off a cross-slot task. The worker may be blocked inside a user task or callback");
          } else {
            windowsWithoutProgress = 0;
            observedCompleted = nowCompleted;
            lastProgressAt = now;
          }
          windowStart = now;
        }

        if (self.helpDeferredTasks.size() >= self.queueCapacity)
          // DEFERRAL BUDGET EXHAUSTED: STOP EXTENDING THE OWN QUEUE AND FALL BACK TO THE BOUNDED WAIT
          return false;

        final DatabaseAsyncTask own = self.queue.poll();
        if (own == null)
          continue;

        if (own == FORCE_EXIT)
          // GRACEFUL SHUTDOWN REQUESTED WHILE HELPING: THE MARKER IS CONSUMED HERE AND IT IS THE
          // forceShutdown FLAG (NOT THE MARKER) THAT DRIVES THE EXIT - THE NEXT LOOP ITERATION BAILS
          // OUT AT THE TOP, THE CURRENT TASK UNWINDS LOUDLY AND THE RUN LOOP STOPS ON THE FLAG, SO
          // shutdownThreadsLocked'S BOUNDED join() COMPLETES WITHOUT THE INTERRUPT ESCALATION
          self.forceShutdown = true;
        else
          self.helpDeferredTasks.addLast(own);
      }
    } finally {
      self.waitingCrossSlotOffer = prevWaiting;
    }
  }

  public int getSlot(final int value) {
    final AsyncThread[] threads = executorThreads;
    if (threads == null)
      throw new DatabaseOperationException("Async executor has been shut down");
    return (value & 0x7fffffff) % threads.length;
  }

  /**
   * Whether the calling thread is one of THIS executor's own workers.
   * <p>
   * The question {@link #waitCompletion()} cannot survive being asked from inside the executor: it enqueues a marker
   * on every worker - the caller's own included - and then blocks until each one has run, and the only consumer of a
   * worker's queue is that worker. A worker that calls it parks on a marker nobody can ever dequeue, and is lost for
   * the life of the process (issue #6281 review). Same test as {@code offerWaiting}'s: the owner check matters
   * because a second database's workers are threads of the same class but not of this executor.
   */
  public boolean isCurrentThreadOneOfMyWorkers() {
    return Thread.currentThread() instanceof AsyncThread worker && worker.getOwner() == this;
  }

  /**
   * Parks every worker of this executor with its transaction batch committed, and does not return until each one has
   * CONFIRMED it (issue #6303, item 2).
   * <p>
   * This is what an index build needs and what {@link com.arcadedb.database.DatabaseInternal#waitForAsyncCompletion()}
   * alone cannot give it. The barrier answers about the PAST - everything submitted before it has run and been
   * committed - which is the half that makes the scan see what the async side already wrote. It says nothing about
   * the future: the instant it returns, a worker is free to take the next task and write records the in-progress scan
   * will not see, and which staged no entry for an index that did not exist when they were saved.
   * <p>
   * {@code BucketIndexBuilder} used to reach for the second half with a pause task per worker, scheduled and
   * forgotten - the return value of {@code scheduleTask} discarded, nothing awaited. A task already queued ahead of
   * that pause therefore ran DURING the build, and the pause tasks did not commit, so a worker could also park
   * holding records nobody could see. Both halves are closed here: the park task commits first (see
   * {@link DatabaseAsyncParkWorker}), and this waits for every worker to say it is parked before returning.
   * <p>
   * <b>Reentrant, and serialized between threads.</b> A nested quiescence on the same thread - {@code REBUILD INDEX}
   * quiescing and then calling a builder that quiesces again - rides on the outer one rather than scheduling park
   * tasks behind workers that are already parked and could never run them. A second THREAD waits its turn, which
   * makes concurrent index builds on one database serial instead of deadlocked.
   * <p>
   * <b>Refused from one of this executor's own workers</b>, for the same reason the barrier is: the park task would be
   * enqueued on the caller's own queue, and the only consumer of that queue is the caller. Since #6303 no engine path
   * gets here that way - an async-dispatched command runs on {@link AsyncCommandPool}, not on a worker - so this is
   * the backstop for anything that reaches it anyway.
   *
   * @return a handle that releases the workers when closed. Never null; a database whose executor has no workers gets
   *     a handle that does nothing, which is the correct answer rather than a special case at every call site.
   *
   * @throws NeedRetryException when called from one of this executor's own workers, or when a worker did not park
   *     within {@link #quiesceTimeoutMillis()}. Retryable rather than fatal: what it reports is a worker still busy,
   *     and the alternative - building the index anyway - is the silently incomplete index of #6281.
   */
  public AsyncQuiesce quiesceWorkers() {
    if (isCurrentThreadOneOfMyWorkers())
      throw new NeedRetryException(
          "Cannot quiesce the asynchronous executor of database '" + database.getName()
              + "' from one of its own worker threads: the park it schedules on every worker would include this one, "
              + "and only this thread drains its queue. Run the operation outside the asynchronous executor");

    if (executorThreads == null || executorThreads.length == 0)
      // NOTHING TO PARK, and the lock is deliberately NOT taken for it. Holding it here would make a nested
      // quiescence short-circuit on hold count and hand back another no-op - so if workers came into existence in
      // between (a concurrent setParallelLevel on a shut-down executor is the only way), the nested call would leave
      // them running under a scan. Not locking makes that nested call a fresh outer one that parks them properly.
      //
      // The residual, named rather than papered over: workers created after this check are not parked by THIS
      // quiescence either. Closing that needs the executor's creation gated on the quiescence, which is a lock on
      // the async lifecycle path for a window an index build has to race a pool resize to reach - out of scope here,
      // and the same window the barrier of #6281 has always had.
      return new HeldQuiesce(null, false);

    quiesceLock.lock();
    if (quiesceLock.getHoldCount() > 1)
      // ALREADY QUIESCED BY THIS THREAD: the workers are parked, and re-parking them is what would hang.
      return new HeldQuiesce(null, true);

    boolean handedOver = false;
    // Declared out here so the finally can release whatever DID park when the quiescence fails halfway - a worker
    // parked on a latch nobody will ever count down is lost until the database closes, which is a worse outcome than
    // the failure that got us there.
    CountDownLatch release = null;
    try {
      final AsyncThread[] threads = executorThreads;
      if (threads == null || threads.length == 0) {
        // Shut down between the check above and here: nothing to park, and the lock this holds is given back by the
        // handle like any other.
        handedOver = true;
        return new HeldQuiesce(null, true);
      }

      final CountDownLatch parked = new CountDownLatch(threads.length);
      release = new CountDownLatch(1);

      for (int i = 0; i < threads.length; i++)
        // waitIfQueueIsFull, so a busy worker is queued behind rather than skipped: an unparked worker is exactly the
        // hole this method exists to close, and a `false` here would be one - which is why the old call site's
        // discarded boolean mattered. Any refusal (a worker that has shut down) throws, and the finally below
        // releases whatever was already parked.
        if (!scheduleTask(i, new DatabaseAsyncParkWorker(parked, release), true, 0))
          throw new NeedRetryException(
              "Cannot quiesce the asynchronous executor of database '" + database.getName() + "': worker " + i
                  + " refused the park task");

      final long timeout = quiesceTimeoutMillis();
      try {
        if (!parked.await(timeout, TimeUnit.MILLISECONDS))
          throw new NeedRetryException("The asynchronous executor of database '" + database.getName()
              + "' did not quiesce within " + timeout
              + " ms: a worker is still busy inside a task. The operation would otherwise scan data that the "
              + "asynchronous side is still writing");
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new NeedRetryException(
            "Interrupted while quiescing the asynchronous executor of database '" + database.getName() + "'", e);
      }

      final AsyncQuiesce held = new HeldQuiesce(release, true);
      handedOver = true;
      return held;

    } finally {
      if (!handedOver) {
        // Nothing owns the release, so it happens here: any worker that reached its park task before the failure is
        // let go, and the lock goes back so the next caller can try.
        if (release != null)
          release.countDown();
        quiesceLock.unlock();
      }
    }
  }

  /**
   * How long {@link #quiesceWorkers()} waits for the workers to confirm. Deliberately the same budget the producer
   * side already allows a worker wedged inside user code ({@code STALLED_NO_PROGRESS_WINDOWS} windows of
   * {@code checkForStalledQueuesMaxDelay}, 60 s with the defaults), so a single number governs "how long may a worker
   * be busy before we stop believing in it" wherever that question is asked.
   */
  long quiesceTimeoutMillis() {
    return STALLED_NO_PROGRESS_WINDOWS * checkForStalledQueuesMaxDelay;
  }

  /** The handle {@link #quiesceWorkers()} hands out: releases the parked workers and gives the lock back, once. */
  private final class HeldQuiesce implements AsyncQuiesce {
    private final CountDownLatch release;
    /** Whether this handle owns a hold of {@link #quiesceLock} - false only for the "there was nothing to park" one. */
    private final boolean        locked;
    private final AtomicBoolean  closed = new AtomicBoolean();

    private HeldQuiesce(final CountDownLatch release, final boolean locked) {
      this.release = release;
      this.locked = locked;
    }

    @Override
    public void close() {
      if (!closed.compareAndSet(false, true))
        return;
      try {
        if (release != null)
          release.countDown();
      } finally {
        if (locked)
          quiesceLock.unlock();
      }
    }
  }

  /**
   * Whether any worker still has a TASK queued or in execution.
   * <p>
   * <b>Not a durability predicate, and never usable as one</b> (issue #6281): a worker opens a transaction when it
   * starts and keeps it open across up to {@link GlobalConfiguration#ASYNC_TX_BATCH_SIZE} tasks, so {@code false} here
   * means only that the tasks have RUN - their records can still be sitting uncommitted in that batch, invisible to
   * every reader. A caller that needs to read what the async side wrote must use
   * {@link com.arcadedb.database.DatabaseInternal#waitForAsyncCompletion()}, which enqueues a marker behind everything
   * already submitted and commits the batch. Deliberately left answering about tasks only: broadening it to "a
   * transaction is open" would make it permanently true for as long as the workers are alive, which is no answer at
   * all.
   */
  @Override
  public boolean isProcessing() {
    if (hasPendingCommands())
      // Dispatched commands run on AsyncCommandPool since #6303 and are not in any worker's queue, so walking the
      // workers alone would report an executor with a command in flight as idle.
      return true;

    final AsyncThread[] threads = executorThreads;
    if (threads != null)
      for (int i = 0; i < threads.length; ++i) {
        if (threads[i].isExecutingTask())
          return true;

        if (threads[i].queue.size() > 0)
          return true;
      }
    return false;
  }
}
