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
 */
package com.arcadedb.database.async;

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
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.conversantmedia.util.concurrent.PushPullBlockingQueue;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

public class DatabaseAsyncExecutorImpl implements DatabaseAsyncExecutor {
  private final DatabaseInternal   database;
  private final Random             random                        = new Random();
  private       AsyncThread[]      executorThreads;
  private       int                parallelLevel                 = 1;
  private       int                commitEvery;
  private       int                backPressurePercentage        = 0;
  private       boolean            transactionUseWAL             = true;
  private       WALFile.FLUSH_TYPE transactionSync               = WALFile.FLUSH_TYPE.NO;
  private       long               checkForStalledQueuesMaxDelay = 5_000;
  private final AtomicLong         transactionCounter            = new AtomicLong();
  private final AtomicLong         commandRoundRobinIndex        = new AtomicLong();

  // SPECIAL TASKS
  public final static DatabaseAsyncTask FORCE_EXIT = new DatabaseAsyncTask() {
    @Override
    public void execute(AsyncThread async, DatabaseInternal database) {
    }

    @Override
    public String toString() {
      return "FORCE_EXIT";
    }
  };

  private OkCallback    onOkCallback;
  private ErrorCallback onErrorCallback;

  public class AsyncThread extends Thread {
    public final    BlockingQueue<DatabaseAsyncTask> queue;
    public final    DatabaseInternal                 database;
    public volatile boolean                          shutdown      = false;
    public volatile boolean                          forceShutdown = false;
    public          long                             count         = 0;

    private AsyncThread(final DatabaseInternal database, final int id) {
      super("AsyncExecutor-" + id);
      this.database = database;

      final int queueSize = database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_SIZE) / parallelLevel;

      final String cfgQueueImpl = database.getConfiguration().getValueAsString(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_IMPL);
      if ("fast".equalsIgnoreCase(cfgQueueImpl))
        this.queue = new PushPullBlockingQueue<>(queueSize);
      else if ("standard".equalsIgnoreCase(cfgQueueImpl))
        this.queue = new ArrayBlockingQueue<>(queueSize);
      else {
        // WARNING AND THEN USE THE DEFAULT
        LogManager.instance().log(this, Level.WARNING, "Error on async operation queue implementation setting: %s is not supported", cfgQueueImpl);
        this.queue = new ArrayBlockingQueue<>(queueSize);
      }
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
      database.getTransaction().begin();

      while (!forceShutdown) {
        try {
          final DatabaseAsyncTask message = queue.poll(500, TimeUnit.MILLISECONDS);
          if (message != null) {
            LogManager.instance().log(this, Level.FINE, "Received async message %s (threadId=%d)", message, Thread.currentThread().getId());

            if (message == FORCE_EXIT) {
              break;
            } else {
              try {
                if (message.requiresActiveTx() && !database.isTransactionActive())
                  database.begin();

                message.execute(this, database);

                count++;

                if (database.isTransactionActive() && count % commitEvery == 0) {
                  database.commit();
                  database.begin();
                }

              } catch (Throwable e) {
                onError(e);
              } finally {
                message.completed();
              }
            }

          } else if (shutdown)
            break;

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          queue.clear();
          break;
        } catch (Throwable e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on executing asynchronous operation (asyncThread=%s)", e, getName());
        }
      }

      try {
        if (database.isTransactionActive())
          database.commit();
        onOk();
      } catch (Exception e) {
        onError(e);
      }
    }

    public void onError(final Throwable e) {
      DatabaseAsyncExecutorImpl.this.onError(e);
    }

    public void onOk() {
      DatabaseAsyncExecutorImpl.this.onOk();
    }
  }

  public DatabaseAsyncExecutorImpl(final DatabaseInternal database) {
    this.database = database;
    this.commitEvery = database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_TX_BATCH_SIZE);
    createThreads(database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS));
  }

  public DBAsyncStats getStats() {
    final DBAsyncStats stats = new DBAsyncStats();
    stats.queueSize = 0;

    if (executorThreads != null)
      for (int i = 0; i < executorThreads.length; ++i)
        stats.queueSize += executorThreads[i].queue.size();

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
  public void setTransactionSync(final WALFile.FLUSH_TYPE transactionSync) {
    this.transactionSync = transactionSync;
    createThreads(parallelLevel);
  }

  public long getCheckForStalledQueuesMaxDelay() {
    return checkForStalledQueuesMaxDelay;
  }

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
  private int getBestSlot() {
    int minQueueSize = 0;
    int minQueueIndex = -1;
    for (int i = 0; i < executorThreads.length; ++i) {
      final int qSize = executorThreads[i].queue.size();
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
  private int getRandomSlot() {
    return random.nextInt(executorThreads.length);
  }

  @Override
  public void waitCompletion() {
    waitCompletion(0L);
  }

  public boolean waitCompletion(long timeout) {
    if (executorThreads == null)
      return true;

    final DatabaseAsyncCompletion[] semaphores = new DatabaseAsyncCompletion[executorThreads.length];

    for (int i = 0; i < executorThreads.length; ++i)
      try {
        semaphores[i] = new DatabaseAsyncCompletion();
        executorThreads[i].queue.put(semaphores[i]);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }

    if (timeout <= 0)
      timeout = Long.MAX_VALUE;

    long currentTimeout = timeout;
    final long beginTime = System.currentTimeMillis();

    for (int i = 0; i < semaphores.length; ++i)
      try {
        semaphores[i].waitForCompetition(currentTimeout);

        // UPDATE THE TIMEOUT
        currentTimeout = timeout - (System.currentTimeMillis() - beginTime);
        if (currentTimeout < 1)
          return false;

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }

    return true;
  }

  @Override
  public void query(final String language, final String query, final AsyncResultsetCallback callback, final Object... args) {
    final int slot = getSlot((int) commandRoundRobinIndex.getAndIncrement());
    scheduleTask(slot, new DatabaseAsyncCommand(true, language, query, args, callback), true, backPressurePercentage);
  }

  @Override
  public void query(final String language, final String query, final AsyncResultsetCallback callback, final Map<String, Object> args) {
    final int slot = getSlot((int) commandRoundRobinIndex.getAndIncrement());
    scheduleTask(slot, new DatabaseAsyncCommand(true, language, query, args, callback), true, backPressurePercentage);
  }

  @Override
  public void command(final String language, final String query, final AsyncResultsetCallback callback, final Object... args) {
    final int slot = getSlot((int) commandRoundRobinIndex.getAndIncrement());
    scheduleTask(slot, new DatabaseAsyncCommand(false, language, query, args, callback), true, backPressurePercentage);
  }

  @Override
  public void command(final String language, final String query, final AsyncResultsetCallback callback, final Map<String, Object> args) {
    final int slot = getSlot((int) commandRoundRobinIndex.getAndIncrement());
    scheduleTask(slot, new DatabaseAsyncCommand(false, language, query, args, callback), true, backPressurePercentage);
  }

  @Override
  public void scanType(final String typeName, final boolean polymorphic, final DocumentCallback callback) {
    scanType(typeName, polymorphic, callback, null);
  }

  @Override
  public void scanType(final String typeName, final boolean polymorphic, final DocumentCallback callback, final ErrorRecordCallback errorRecordCallback) {
    try {
      final DocumentType type = database.getSchema().getType(typeName);

      final List<Bucket> buckets = type.getBuckets(polymorphic);
      final CountDownLatch semaphore = new CountDownLatch(buckets.size());

      for (Bucket b : buckets) {
        final int slot = getSlot(b.getId());
        scheduleTask(slot, new DatabaseAsyncScanBucket(semaphore, callback, errorRecordCallback, b), true, backPressurePercentage);
      }

      semaphore.await();

    } catch (Exception e) {
      throw new DatabaseOperationException("Error on executing parallel scan of type '" + database.getSchema().getType(typeName) + "'", e);
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
  public void transaction(final Database.TransactionScope txBlock, final int retries, final OkCallback ok, final ErrorCallback error) {
    scheduleTask(getSlot((int) transactionCounter.getAndIncrement()), new DatabaseAsyncTransaction(txBlock, retries, ok, error), true, backPressurePercentage);
  }

  @Override
  public void createRecord(final MutableDocument record, final NewRecordCallback newRecordCallback) {
    final DocumentType type = record.getType();

    if (record.getIdentity() == null) {
      // NEW
      final Bucket bucket = type.getBucketIdByRecord(record, false);
      final int slot = getSlot(bucket.getId());

      scheduleTask(slot, new DatabaseAsyncCreateRecord(record, bucket, newRecordCallback), true, backPressurePercentage);

    } else
      throw new IllegalArgumentException("Cannot create a new record because it is already persistent");
  }

  @Override
  public void createRecord(final Record record, final String bucketName, final NewRecordCallback newRecordCallback) {
    final Bucket bucket = database.getSchema().getBucketByName(bucketName);
    final int slot = getSlot(bucket.getId());

    if (record.getIdentity() == null)
      // NEW
      scheduleTask(slot, new DatabaseAsyncCreateRecord(record, bucket, newRecordCallback), true, backPressurePercentage);
    else
      throw new IllegalArgumentException("Cannot create a new record because it is already persistent");
  }

  @Override
  public void updateRecord(final MutableDocument record, final UpdatedRecordCallback updateRecordCallback) {
    if (record.getIdentity() != null) {
      // UPDATE
      final DocumentType type = record.getType();
      final Bucket bucket = type.getBucketIdByRecord(record, false);
      final int slot = getSlot(bucket.getId());

      scheduleTask(slot, new DatabaseAsyncUpdateRecord(record, updateRecordCallback), true, backPressurePercentage);

    } else
      throw new IllegalArgumentException("Cannot updated a not persistent record");
  }

  @Override
  public void newEdge(final Vertex sourceVertex, final String edgeType, final RID destinationVertexRID, final boolean bidirectional, final boolean light,
      final NewEdgeCallback callback, final Object... properties) {
    if (sourceVertex == null)
      throw new IllegalArgumentException("Source vertex is null");

    if (destinationVertexRID == null)
      throw new IllegalArgumentException("Destination vertex is null");

    final int sourceSlot = getSlot(sourceVertex.getIdentity().getBucketId());
    final int destinationSlot = getSlot(destinationVertexRID.getBucketId());

    if (sourceSlot == destinationSlot)
      // BOTH VERTICES HAVE THE SAME SLOT, CREATE THE EDGE USING IT
      scheduleTask(sourceSlot, new CreateEdgeAsyncTask(sourceVertex, destinationVertexRID, edgeType, properties, bidirectional, light, callback), true,
          backPressurePercentage);
    else {
      // CREATE THE EDGE IN THE SOURCE VERTEX'S SLOT AND A CASCADE TASK TO ADD THE INCOMING EDGE FROM DESTINATION VERTEX (THIS IS THE MOST EXPENSIVE CASE WHERE 2 TASKS ARE EXECUTED)
      scheduleTask(sourceSlot, new CreateEdgeAsyncTask(sourceVertex, destinationVertexRID, edgeType, properties, false, light,
          (newEdge, createdSourceVertex, createdDestinationVertex) -> {
            if (bidirectional) {
              scheduleTask(destinationSlot, new CreateIncomingEdgeAsyncTask(sourceVertex.getIdentity(), destinationVertexRID, newEdge,
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
  public void newEdgeByKeys(final String sourceVertexType, final String sourceVertexKeyName, final Object sourceVertexKeyValue,
      final String destinationVertexType, final String destinationVertexKeyName, final Object destinationVertexKeyValue, final boolean createVertexIfNotExist,
      final String edgeType, final boolean bidirectional, final boolean lightWeight, final NewEdgeCallback callback, final Object... properties) {
    newEdgeByKeys(sourceVertexType, new String[] { sourceVertexKeyName }, new Object[] { sourceVertexKeyValue }, destinationVertexType,
        new String[] { destinationVertexKeyName }, new Object[] { destinationVertexKeyValue }, createVertexIfNotExist, edgeType, bidirectional, lightWeight,
        callback, properties);
  }

  @Override
  public void newEdgeByKeys(final String sourceVertexType, final String[] sourceVertexKeyNames, final Object[] sourceVertexKeyValues,
      final String destinationVertexType, final String[] destinationVertexKeyNames, final Object[] destinationVertexKeyValues,
      final boolean createVertexIfNotExist, final String edgeType, final boolean bidirectional, final boolean lightWeight, final NewEdgeCallback callback,
      final Object... properties) {

    if (sourceVertexKeyNames == null)
      throw new IllegalArgumentException("Source vertex key is null");

    if (sourceVertexKeyNames.length != sourceVertexKeyValues.length)
      throw new IllegalArgumentException("Source vertex key and value arrays have different sizes");

    if (destinationVertexKeyNames == null)
      throw new IllegalArgumentException("Destination vertex key is null");

    if (destinationVertexKeyNames.length != destinationVertexKeyValues.length)
      throw new IllegalArgumentException("Destination vertex key and value arrays have different sizes");

    final Iterator<Identifiable> sourceResult = database.lookupByKey(sourceVertexType, sourceVertexKeyNames, sourceVertexKeyValues);
    final Iterator<Identifiable> destinationResult = database.lookupByKey(destinationVertexType, destinationVertexKeyNames, destinationVertexKeyValues);

    final RID sourceRID = sourceResult.hasNext() ? sourceResult.next().getIdentity() : null;
    final RID destinationRID = destinationResult.hasNext() ? destinationResult.next().getIdentity() : null;

    if (sourceRID == null && destinationRID == null) {

      if (!createVertexIfNotExist)
        throw new IllegalArgumentException(
            "Cannot find source and destination vertices with respectively key " + Arrays.toString(sourceVertexKeyNames) + "=" + Arrays.toString(
                sourceVertexKeyValues) + " and " + Arrays.toString(destinationVertexKeyNames) + "=" + Arrays.toString(destinationVertexKeyValues));

      // SOURCE AND DESTINATION VERTICES BOTH DON'T EXIST: CREATE 2 VERTICES + EDGE IN THE SAME TASK PICKING THE BEST SLOT
      scheduleTask(getRandomSlot(), new CreateBothVerticesAndEdgeAsyncTask(sourceVertexType, sourceVertexKeyNames, sourceVertexKeyValues, destinationVertexType,
          destinationVertexKeyNames, destinationVertexKeyValues, edgeType, properties, bidirectional, lightWeight, callback), true, backPressurePercentage);

    } else if (sourceRID != null && destinationRID == null) {

      if (!createVertexIfNotExist)
        throw new IllegalArgumentException(
            "Cannot find destination vertex with key " + Arrays.toString(destinationVertexKeyNames) + "=" + Arrays.toString(destinationVertexKeyValues));

      // ONLY SOURCE VERTEX EXISTS, CREATE DESTINATION VERTEX + EDGE IN SOURCE'S SLOT
      scheduleTask(getSlot(sourceRID.getBucketId()),
          new CreateDestinationVertexAndEdgeAsyncTask(sourceRID, destinationVertexType, destinationVertexKeyNames, destinationVertexKeyValues, edgeType,
              properties, bidirectional, lightWeight, callback), true, backPressurePercentage);

    } else if (sourceRID == null && destinationRID != null) {

      if (!createVertexIfNotExist)
        throw new IllegalArgumentException(
            "Cannot find source vertex with key " + Arrays.toString(sourceVertexKeyNames) + "=" + Arrays.toString(sourceVertexKeyValues));

      // ONLY DESTINATION VERTEX EXISTS
      scheduleTask(getSlot(destinationRID.getBucketId()),
          new CreateSourceVertexAndEdgeAsyncTask(sourceVertexType, sourceVertexKeyNames, sourceVertexKeyValues, destinationRID, edgeType, properties,
              bidirectional, lightWeight, callback), true, backPressurePercentage);

    } else
      // BOTH VERTICES EXIST
      newEdge(sourceRID.asVertex(true), edgeType, destinationRID, bidirectional, lightWeight, callback, properties);
  }

  /**
   * Test only API.
   */
  @Override
  public void kill() {
    if (executorThreads != null) {
      // WAIT FOR SHUTDOWN, MAX 1S EACH
      for (int i = 0; i < executorThreads.length; ++i)
        executorThreads[i].forceShutdown = true;
      executorThreads = null;
    }
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
    this.commitEvery = commitEvery;
  }

  public static class DBAsyncStats {
    public long queueSize;
  }

  private void createThreads(int parallelLevel) {
    if (parallelLevel < 1)
      parallelLevel = 1;

    shutdownThreads();

    executorThreads = new AsyncThread[parallelLevel];
    for (int i = 0; i < parallelLevel; ++i) {
      executorThreads[i] = new AsyncThread(database, i);
      executorThreads[i].start();
    }

    this.parallelLevel = parallelLevel;
  }

  private void shutdownThreads() {
    if (executorThreads != null) {
      try {
        // WAIT FOR SHUTDOWN, MAX 1S EACH
        for (int i = 0; i < executorThreads.length; ++i) {
          executorThreads[i].shutdown = true;
          executorThreads[i].queue.put(FORCE_EXIT);
          executorThreads[i].join(10000);
        }
      } catch (InterruptedException e) {
        // IGNORE IT
        Thread.currentThread().interrupt();
      } finally {
        executorThreads = null;
      }
    }
  }

  @Override
  public void onOk() {
    if (onOkCallback != null) {
      try {
        onOkCallback.call();
      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on invoking onOk() callback for asynchronous operation %s", e, this);
      }
    }
  }

  @Override
  public void onError(final Throwable e) {
    if (onErrorCallback != null) {
      try {
        onErrorCallback.call(e);
      } catch (Exception e1) {
        LogManager.instance().log(this, Level.SEVERE, "Error on invoking onError() callback for asynchronous operation %s", e, this);
      }
    }
  }

  /**
   * Schedule a task to be executed by parallel executors.
   *
   * @param slot              slot id
   * @param task              task to schedule
   * @param waitIfQueueIsFull true to wait in case the queue is full, otherwise false
   *
   * @return true if the task has been scheduled, otherwise false
   */
  public boolean scheduleTask(final int slot, final DatabaseAsyncTask task, final boolean waitIfQueueIsFull, final int applyBackPressureOnPercentage) {
    try {
      final BlockingQueue<DatabaseAsyncTask> queue = executorThreads[slot].queue;

      if (applyBackPressureOnPercentage > 0) {
        final int queueFullAt = 100 - (queue.remainingCapacity() * 100 / (queue.remainingCapacity() + queue.size()));

        if (queueFullAt >= applyBackPressureOnPercentage)
          // TODO: VARIABLE SLEEP TIME BASED ON HOW MUCH THE QUEUE IS FULL
          Thread.sleep(queueFullAt);
      }

      if (waitIfQueueIsFull) {
        if (!queue.offer(task, checkForStalledQueuesMaxDelay, TimeUnit.MILLISECONDS)) {
          // QUEUE FULL, RETRY WITH CHECK FOR QUEUE STALLED

          final DatabaseAsyncTask firstInQueueAtBeginning = queue.peek();

          while (!queue.offer(task, checkForStalledQueuesMaxDelay, TimeUnit.MILLISECONDS)) {
            final DatabaseAsyncTask firstInQueue = queue.peek();
            if (firstInQueue != null && firstInQueue == firstInQueueAtBeginning) {
              // QUEUE STALLED
              throw new DatabaseOperationException(
                  "Asynchronous queue " + slot + " is stalled. This could happen when an asynchronous task schedules more asynchronous tasks");
            }

            if (applyBackPressureOnPercentage > 0) {
              final int queueFullAt = 100 - (queue.remainingCapacity() * 100 / (queue.remainingCapacity() + queue.size()));
              Thread.sleep(100 + (4L * queueFullAt));
            }
          }
        }
        return true;
      }

      return queue.offer(task);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DatabaseOperationException("Error on executing asynchronous task " + task);
    }
  }

  public int getSlot(final int value) {
    return (value & 0x7fffffff) % executorThreads.length;
  }
}
