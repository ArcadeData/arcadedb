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
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.log.LogManager;
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
import java.util.concurrent.atomic.AtomicLong;
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

      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).asyncMode = true;
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
                Thread.currentThread().threadId());

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
    if (index.scheduleCompaction())
      scheduleTask(getBestSlot(), new DatabaseAsyncIndexCompaction(index), false, backPressurePercentage);
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
    final AsyncThread[] threads = executorThreads;
    if (threads == null)
      return true;

    if (timeout <= 0)
      timeout = Long.MAX_VALUE;
    final long beginTime = System.currentTimeMillis();

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
    final int slot = getSlot((int) commandRoundRobinIndex.getAndIncrement());
    scheduleTask(slot, new DatabaseAsyncCommand(configuration, true, language, query, args, callback), true,
        backPressurePercentage);
  }

  @Override
  public void query(final String language, final String query, final AsyncResultsetCallback callback,
                    final Map<String, Object> args) {
    final int slot = getSlot((int) commandRoundRobinIndex.getAndIncrement());
    scheduleTask(slot, new DatabaseAsyncCommand(configuration, true, language, query, args, callback), true,
        backPressurePercentage);
  }

  @Override
  public void command(final String language, final String query, final AsyncResultsetCallback callback,
                      final Object... args) {
    final int slot = getSlot((int) commandRoundRobinIndex.getAndIncrement());
    scheduleTask(slot, new DatabaseAsyncCommand(configuration, false, language, query, args, callback), true,
        backPressurePercentage);
  }

  @Override
  public void command(final String language, final String query, final AsyncResultsetCallback callback,
                      final Map<String, Object> args) {
    final int slot = getSlot((int) commandRoundRobinIndex.getAndIncrement());
    scheduleTask(slot, new DatabaseAsyncCommand(configuration, false, language, query, args, callback), true,
        backPressurePercentage);
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
    if (!bidirectional && ((EdgeType) database.getSchema().getType(edgeType)).isBidirectional())
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

    final boolean bidirectional = ((EdgeType) database.getSchema().getType(edgeType)).isBidirectional();

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
              """
              Asynchronous task %s was scheduled on a worker that already shut down and cannot be removed from its \
              'fast' queue: its completion will never be notified. Use the 'standard' async queue implementation \
              to close this window""", task);
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

  @Override
  public boolean isProcessing() {
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
