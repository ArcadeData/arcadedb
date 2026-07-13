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
package com.arcadedb.index.lsm;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionIndexContext;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexException;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

/** Builds empty LSM indexes from a bounded, globally sorted logical entry stream. */
public final class LSMTreeIndexBulkLoader implements AutoCloseable {
  private static final ThreadLocal<BuildTestHook> BUILD_TEST_HOOK = new ThreadLocal<>();

  private static final long ESTIMATED_BYTES_PER_ENTRY    = 3_072L;
  private static final long MIN_MEMORY_BUDGET_BYTES      = 1L << 20;
  private static final long WRITER_WORKER_OVERHEAD_BYTES = 8L << 20;
  private static final int  MAX_BUFFERED_RIDS_PER_GROUP  = 256;
  private static final int  WRITER_BATCH_GROUPS          = 256;
  private static final int  WRITER_QUEUE_CAPACITY        = 8;

  private final DatabaseInternal           database;
  private final String                     indexName;
  private final long                       memoryBudgetBytes;
  private final Path                       spillDirectory;
  private final Path                       spillWorkspace;
  private final int                        mergeFanIn;
  private final int                        requestedBuildParallelism;
  private final int                        maxEntriesPerRun;
  private final List<Entry>                entries;
  private final BuildTestHook               buildTestHook;
  private final Map<Integer, LSMTreeIndex> indexesByBucket = new LinkedHashMap<>();
  private       LSMTreeIndexExternalSorter externalSorter;
  private       byte[]                     binaryKeyTypes;
  private       Boolean                    unique;
  private       long                       totalEntries;
  private       long                       inMemorySortNanos;
  private       long                       finalStreamAndWriteNanos;
  private       long                       attachmentNanos;
  private       int                        admittedWriterParallelism = 1;
  private       int                        maxConcurrentWriters;
  private final AtomicInteger              completedWriterGroups = new AtomicInteger();

  public LSMTreeIndexBulkLoader(final DatabaseInternal database, final String indexName,
      final long configuredMemoryBudgetBytes, final Path spillDirectory, final int mergeFanIn) {
    this(database, indexName, configuredMemoryBudgetBytes, spillDirectory, mergeFanIn, null, 1);
  }

  public LSMTreeIndexBulkLoader(final DatabaseInternal database, final String indexName,
      final long configuredMemoryBudgetBytes, final Path spillDirectory, final int mergeFanIn,
      final Path spillWorkspace) {
    this(database, indexName, configuredMemoryBudgetBytes, spillDirectory, mergeFanIn, spillWorkspace, 1);
  }

  public LSMTreeIndexBulkLoader(final DatabaseInternal database, final String indexName,
      final long configuredMemoryBudgetBytes, final Path spillDirectory, final int mergeFanIn,
      final Path spillWorkspace, final int buildParallelism) {
    if (mergeFanIn < 2)
      throw new IllegalArgumentException("mergeFanIn must be at least 2");
    if (buildParallelism < 1)
      throw new IllegalArgumentException("buildParallelism must be at least 1");

    this.database = database;
    this.indexName = indexName;
    this.memoryBudgetBytes = resolveMemoryBudget(configuredMemoryBudgetBytes);
    this.spillDirectory = spillDirectory;
    this.spillWorkspace = spillWorkspace;
    this.mergeFanIn = mergeFanIn;
    this.requestedBuildParallelism = buildParallelism;
    this.maxEntriesPerRun = (int) Math.max(1L,
        Math.min(Integer.MAX_VALUE, memoryBudgetBytes / ESTIMATED_BYTES_PER_ENTRY));
    this.entries = new ArrayList<>(Math.min(maxEntriesPerRun, 65_536));
    this.buildTestHook = BUILD_TEST_HOOK.get();

    LogManager.instance().log(this, Level.INFO,
        "Sorted index build '%s': memoryBudget=%s maxEntriesPerRun=%,d spillParent=%s mergeFanIn=%d buildParallelism=%d",
        indexName, FileUtils.getSizeAsString(memoryBudgetBytes), maxEntriesPerRun,
        spillDirectory != null ? spillDirectory : Path.of(database.getDatabasePath()), mergeFanIn,
        requestedBuildParallelism);
  }

  public void add(final LSMTreeIndex index, final Document record) {
    final RID rid = record.getIdentity();
    if (rid == null)
      throw new IndexException("Cannot bulk-index a non-persistent record in '" + indexName + "'");

    database.getIndexer().forEachIndexKey(index, record, rawKeys -> {
      final Object[] normalizedKeys = index.normalizeKeysForBulkBuild(rawKeys);
      index.getMutableIndex().checkForNulls(normalizedKeys);
      final boolean containsNull = LSMTreeIndexAbstract.isKeyNull(normalizedKeys);
      if (containsNull && index.getNullStrategy() == LSMTreeIndexAbstract.NULL_STRATEGY.SKIP)
        return;

      registerIndex(index);
      entries.add(new Entry(index, new TransactionIndexContext.ComparableKey(normalizedKeys), rid));
      totalEntries++;
      if (entries.size() >= maxEntriesPerRun)
        spillCurrentRun();
    });
  }

  public BuildOutcome writeCompacted() {
    if (database.isTransactionActive())
      throw new IndexException("Sorted build for '" + indexName + "' requires no active transaction");

    final Map<LSMTreeIndex, BucketWriter> writers = new LinkedHashMap<>();
    boolean success = false;
    try {
      prepareSortedEntries();
      invokeBuildTestHook(BuildPhase.AFTER_SORT, null, 0);
      final long started = System.nanoTime();
      final long mergeNanosBeforeStream = getMaterializedMergeNanos();
      final long streamStarted = System.nanoTime();
      admittedWriterParallelism = selectWriterParallelism(requestedBuildParallelism, indexesByBucket.size(),
          memoryBudgetBytes, Runtime.getRuntime().availableProcessors(),
          LSMTreeIndexExternalSorter.getAvailableFileDescriptors());

      final long written;
      if (admittedWriterParallelism > 1) {
        try (BucketWriteDispatcher dispatcher = new BucketWriteDispatcher(writers, admittedWriterParallelism)) {
          written = forEachSortedGroup(dispatcher::append);
          dispatcher.complete();
          maxConcurrentWriters = dispatcher.getMaxConcurrentWriters();
        }
      } else {
        written = forEachSortedGroup((first, rids) -> appendDirect(writers, first, rids));
        maxConcurrentWriters = writers.isEmpty() ? 0 : 1;
      }
      final long streamNanos = System.nanoTime() - streamStarted;
      finalStreamAndWriteNanos += Math.max(0L,
          streamNanos - (getMaterializedMergeNanos() - mergeNanosBeforeStream));
      invokeBuildTestHook(BuildPhase.AFTER_ENTRY_WRITES, null, 0);

      final long attachmentStarted = System.nanoTime();
      int attachedBuckets = 0;
      for (final BucketWriter writer : writers.values()) {
        writer.finishAndAttach();
        invokeBuildTestHook(BuildPhase.AFTER_BUCKET_ATTACHMENT, writer.mainIndex, ++attachedBuckets);
      }
      invokeBuildTestHook(BuildPhase.AFTER_ALL_ATTACHMENTS, null, attachedBuckets);
      attachmentNanos += System.nanoTime() - attachmentStarted;

      success = true;
      final int runCount = externalSorter != null ? externalSorter.getRunCount() : 0;
      final long spillBytes = externalSorter != null ? externalSorter.getSpilledBytes() : 0L;
      final BuildOutcome outcome = new BuildOutcome(written, writers.size(), runCount, spillBytes,
          (System.nanoTime() - started) / 1_000_000L, memoryBudgetBytes);
      LogManager.instance().log(this, Level.INFO,
          "Completed sorted index build '%s': entries=%,d bucketIndexes=%d finalRuns=%d spillBytes=%s writeMillis=%,d",
          indexName, outcome.entries(), outcome.bucketIndexes(), outcome.finalRuns(),
          FileUtils.getSizeAsString(outcome.spillBytes()), outcome.writeMillis());
      return outcome;
    } catch (final InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new IndexException("Sorted build for '" + indexName + "' was interrupted", error);
    } catch (final IndexException error) {
      throw error;
    } catch (final Exception error) {
      throw new IndexException("Cannot build sorted index '" + indexName + "'", error);
    } finally {
      if (!success)
        for (final BucketWriter writer : writers.values())
          writer.abort();
    }
  }

  public long size() {
    return totalEntries;
  }

  private void appendDirect(final Map<LSMTreeIndex, BucketWriter> writers, final Entry first, final RID[] rids)
      throws IOException, InterruptedException {
    final BucketWriter writer = getOrCreateWriter(writers, first.index());
    writer.append(first.key().values, rids);
    invokeBuildTestHook(BuildPhase.AFTER_BUCKET_APPEND, first.index(), completedWriterGroups.incrementAndGet());
  }

  private BucketWriter getOrCreateWriter(final Map<LSMTreeIndex, BucketWriter> writers, final LSMTreeIndex index)
      throws IOException {
    BucketWriter writer = writers.get(index);
    if (writer == null) {
      writer = new BucketWriter(index);
      writers.put(index, writer);
    }
    return writer;
  }

  private long forEachSortedGroup(final SortedGroupConsumer consumer) throws Exception {
    long processed = 0L;
    try (LSMTreeIndexExternalSorter.EntryCursor cursor = openSortedEntries()) {
      final RID[] ridBuffer = new RID[MAX_BUFFERED_RIDS_PER_GROUP];
      Entry groupFirst = null;
      Entry previousEntry = null;
      RID previousRid = null;
      int bufferedRids = 0;

      while (cursor.hasNext()) {
        final Entry current = cursor.next();
        if (Boolean.TRUE.equals(unique) && previousEntry != null
            && current.key().compareTo(previousEntry.key()) == 0
            && !LSMTreeIndexAbstract.isKeyNull(current.key().values)
            && !current.rid().equals(previousEntry.rid()))
          throw new DuplicatedKeyException(indexName, Arrays.toString(current.key().values), previousEntry.rid());
        previousEntry = current;

        if (groupFirst == null || current.index() != groupFirst.index()
            || current.key().compareTo(groupFirst.key()) != 0) {
          if (groupFirst != null)
            processed += flushRidBuffer(consumer, groupFirst, ridBuffer, bufferedRids);
          groupFirst = current;
          previousRid = null;
          bufferedRids = 0;
        }

        if (current.rid().equals(previousRid))
          continue;

        ridBuffer[bufferedRids++] = current.rid();
        previousRid = current.rid();
        if (bufferedRids == ridBuffer.length) {
          processed += flushRidBuffer(consumer, groupFirst, ridBuffer, bufferedRids);
          bufferedRids = 0;
        }
      }

      if (groupFirst != null)
        processed += flushRidBuffer(consumer, groupFirst, ridBuffer, bufferedRids);
    }
    return processed;
  }

  private static int flushRidBuffer(final SortedGroupConsumer consumer, final Entry first, final RID[] buffer,
      final int size) throws Exception {
    if (size == 0)
      return 0;
    consumer.accept(first, Arrays.copyOf(buffer, size));
    return size;
  }

  private void prepareSortedEntries() {
    if (externalSorter != null)
      spillCurrentRun();
    else {
      final long started = System.nanoTime();
      entries.sort(LSMTreeIndexBulkLoader::compareEntries);
      inMemorySortNanos += System.nanoTime() - started;
    }
  }

  private long getMaterializedMergeNanos() {
    return externalSorter != null ? externalSorter.getMaterializedMergeNanos() : 0L;
  }

  public StageMetrics getStageMetrics() {
    return new StageMetrics(mergeFanIn, externalSorter != null ? externalSorter.getMergeFanIn() : mergeFanIn,
        requestedBuildParallelism,
        externalSorter != null ? externalSorter.getAdmittedMergeParallelism() : 1,
        externalSorter != null ? externalSorter.getMaxConcurrentMerges() : 0,
        admittedWriterParallelism, maxConcurrentWriters,
        externalSorter != null ? externalSorter.getInitialRunCount() : 0,
        externalSorter != null ? externalSorter.getRunCount() : 0,
        externalSorter != null ? externalSorter.getInitialRunEntries() : 0L,
        externalSorter != null ? externalSorter.getInitialRunBytes() : 0L,
        externalSorter != null ? externalSorter.getInitialRunNanos() : 0L,
        inMemorySortNanos,
        externalSorter != null ? externalSorter.getMaterializedMergeGenerationCount() : 0,
        externalSorter != null ? externalSorter.getMaterializedMergeEntries() : 0L,
        externalSorter != null ? externalSorter.getMaterializedMergeBytes() : 0L,
        getMaterializedMergeNanos(), finalStreamAndWriteNanos, attachmentNanos);
  }

  static int selectWriterParallelism(final int configured, final int bucketIndexes, final long memoryBudgetBytes,
      final int availableProcessors, final long availableFileDescriptors) {
    if (configured < 1)
      throw new IllegalArgumentException("writer parallelism must be at least 1");
    if (bucketIndexes < 1)
      return 1;

    final long cpuLimit = Math.max(1L, (long) availableProcessors - 1L);
    final long memoryLimit = Math.max(1L, memoryBudgetBytes / WRITER_WORKER_OVERHEAD_BYTES);
    final long descriptorLimit = availableFileDescriptors == Long.MAX_VALUE ? Integer.MAX_VALUE
        : Math.max(1L, availableFileDescriptors / 2L);
    return (int) Math.min(configured,
        Math.min(bucketIndexes, Math.min(cpuLimit, Math.min(memoryLimit, descriptorLimit))));
  }

  private LSMTreeIndexExternalSorter.EntryCursor openSortedEntries() throws IOException {
    if (externalSorter != null)
      return externalSorter.openCursor();

    return new LSMTreeIndexExternalSorter.EntryCursor() {
      private int position;

      @Override
      public boolean hasNext() {
        return position < entries.size();
      }

      @Override
      public Entry next() {
        return entries.get(position++);
      }

      @Override
      public void close() {
        // The in-memory cursor owns no closeable resources.
      }
    };
  }

  private void registerIndex(final LSMTreeIndex index) {
    if (unique == null)
      unique = index.isUnique();
    else if (unique != index.isUnique())
      throw new IndexException("Bucket index '" + index.getName() + "' has incompatible uniqueness for sorted build '"
          + indexName + "'");

    final byte[] indexKeyTypes = index.getBinaryKeyTypes();
    if (binaryKeyTypes == null)
      binaryKeyTypes = Arrays.copyOf(indexKeyTypes, indexKeyTypes.length);
    else if (!Arrays.equals(binaryKeyTypes, indexKeyTypes))
      throw new IndexException("Bucket index '" + index.getName() + "' has incompatible key types for sorted build '"
          + indexName + "'");

    final LSMTreeIndex previous = indexesByBucket.putIfAbsent(index.getAssociatedBucketId(), index);
    if (previous != null && previous != index)
      throw new IndexException("Multiple indexes are associated with bucket " + index.getAssociatedBucketId());
  }

  private void spillCurrentRun() {
    if (entries.isEmpty())
      return;

    try {
      if (externalSorter == null)
        externalSorter = new LSMTreeIndexExternalSorter(database, binaryKeyTypes, indexesByBucket, spillDirectory,
            mergeFanIn, memoryBudgetBytes, spillWorkspace, requestedBuildParallelism);
      externalSorter.addRun(entries);
      entries.clear();
    } catch (final IOException error) {
      throw new IndexException("Cannot spill external sort run for index '" + indexName + "'", error);
    }
  }

  private long resolveMemoryBudget(final long configuredMemoryBudgetBytes) {
    final Runtime runtime = Runtime.getRuntime();
    return resolveMemoryBudget(configuredMemoryBudgetBytes, runtime.maxMemory(), runtime.totalMemory(),
        runtime.freeMemory());
  }

  static long resolveMemoryBudget(final long configuredMemoryBudgetBytes, final long maxMemoryBytes,
      final long totalMemoryBytes, final long freeMemoryBytes) {
    if (configuredMemoryBudgetBytes < 0L)
      throw new IllegalArgumentException("memory budget cannot be negative");
    if (configuredMemoryBudgetBytes > 0L) {
      if (configuredMemoryBudgetBytes < MIN_MEMORY_BUDGET_BYTES)
        throw new IllegalArgumentException("memory budget must be at least " + MIN_MEMORY_BUDGET_BYTES + " bytes");
      return configuredMemoryBudgetBytes;
    }

    final long usedHeap = Math.max(0L, totalMemoryBytes - freeMemoryBytes);
    final long effectiveMaxMemory = maxMemoryBytes == Long.MAX_VALUE ? totalMemoryBytes : maxMemoryBytes;
    final long availableHeap = Math.max(0L, effectiveMaxMemory - usedHeap);
    return Math.max(MIN_MEMORY_BUDGET_BYTES, availableHeap / 4L);
  }

  @Override
  public void close() {
    entries.clear();
    if (externalSorter != null)
      try {
        externalSorter.close();
      } catch (final IOException error) {
        throw new IndexException("Cannot remove external sort files for index '" + indexName + "'", error);
      } finally {
        externalSorter = null;
      }
  }

  static int compareEntries(final Entry left, final Entry right) {
    final int keyComparison = left.key().compareTo(right.key());
    return keyComparison != 0 ? keyComparison : left.rid().compareTo(right.rid());
  }

  static record Entry(LSMTreeIndex index, TransactionIndexContext.ComparableKey key, RID rid) {
  }

  static void setBuildTestHook(final BuildTestHook hook) {
    if (hook == null)
      BUILD_TEST_HOOK.remove();
    else
      BUILD_TEST_HOOK.set(hook);
  }

  private void invokeBuildTestHook(final BuildPhase phase, final LSMTreeIndex index,
      final int completedBuckets) {
    if (buildTestHook != null)
      buildTestHook.onPhase(phase, index, completedBuckets);
  }

  enum BuildPhase {
    AFTER_SORT,
    AFTER_BUCKET_APPEND,
    AFTER_ENTRY_WRITES,
    AFTER_BUCKET_ATTACHMENT,
    AFTER_ALL_ATTACHMENTS
  }

  @FunctionalInterface
  interface BuildTestHook {
    void onPhase(BuildPhase phase, LSMTreeIndex index, int completedBuckets);
  }

  public record BuildOutcome(long entries, int bucketIndexes, int finalRuns, long spillBytes, long writeMillis,
                             long memoryBudgetBytes) {
  }

  public record StageMetrics(int requestedMergeFanIn, int admittedMergeFanIn,
                             int requestedBuildParallelism, int admittedMergeParallelism,
                             int maxConcurrentMerges, int admittedWriterParallelism,
                             int maxConcurrentWriters, int initialRuns, int finalRuns,
                             long initialRunEntries, long initialRunBytes, long initialRunNanos,
                             long inMemorySortNanos, int materializedMergeGenerations,
                             long materializedMergeEntries, long materializedMergeBytes,
                             long materializedMergeNanos, long finalStreamAndWriteNanos, long attachmentNanos) {
  }

  @FunctionalInterface
  private interface SortedGroupConsumer {
    void accept(Entry first, RID[] rids) throws Exception;
  }

  private final class BucketWriteDispatcher implements AutoCloseable {
    private final Map<LSMTreeIndex, BucketWriter> writers;
    private final List<ArrayBlockingQueue<WriteBatch>> queues;
    private final List<WriteBatch> pendingBatches;
    private final Map<Integer, Integer> workerByBucket = new LinkedHashMap<>();
    private final ExecutorService executor;
    private final List<Future<?>> futures = new ArrayList<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final AtomicInteger activeWriters = new AtomicInteger();
    private final AtomicInteger maxActiveWriters = new AtomicInteger();
    private final WriteBatch poison = new WriteBatch(true);
    private boolean completed;

    private BucketWriteDispatcher(final Map<LSMTreeIndex, BucketWriter> writers, final int workerCount) {
      this.writers = writers;
      this.queues = new ArrayList<>(workerCount);
      this.pendingBatches = new ArrayList<>(workerCount);

      final List<Integer> bucketIds = new ArrayList<>(indexesByBucket.keySet());
      bucketIds.sort(Integer::compareTo);
      for (int i = 0; i < bucketIds.size(); i++)
        workerByBucket.put(bucketIds.get(i), i % workerCount);

      final AtomicInteger threadNumber = new AtomicInteger();
      executor = Executors.newFixedThreadPool(workerCount, task -> {
        final Thread thread = new Thread(task, "arcadedb-index-bucket-writer-" + threadNumber.incrementAndGet());
        thread.setDaemon(true);
        return thread;
      });
      for (int i = 0; i < workerCount; i++) {
        final ArrayBlockingQueue<WriteBatch> queue = new ArrayBlockingQueue<>(WRITER_QUEUE_CAPACITY);
        queues.add(queue);
        pendingBatches.add(new WriteBatch(false));
        futures.add(executor.submit(() -> runWorker(queue)));
      }
    }

    private void append(final Entry first, final RID[] rids) throws Exception {
      checkFailure();
      final BucketWriter writer = getOrCreateWriter(writers, first.index());
      final Integer worker = workerByBucket.get(first.index().getAssociatedBucketId());
      if (worker == null)
        throw new IndexException("No writer worker assigned to bucket " + first.index().getAssociatedBucketId());

      WriteBatch batch = pendingBatches.get(worker);
      batch.add(writer, first.key().values, rids);
      if (batch.isFull()) {
        enqueue(queues.get(worker), batch);
        pendingBatches.set(worker, new WriteBatch(false));
      }
    }

    private void complete() throws Exception {
      for (int i = 0; i < queues.size(); i++) {
        final WriteBatch pending = pendingBatches.get(i);
        if (!pending.isEmpty())
          enqueue(queues.get(i), pending);
        enqueue(queues.get(i), poison);
      }

      executor.shutdown();
      for (final Future<?> future : futures)
        future.get();
      checkFailure();
      if (!executor.awaitTermination(30L, TimeUnit.SECONDS))
        throw new IndexException("Timed out waiting for sorted index bucket writers");
      completed = true;
    }

    private void runWorker(final ArrayBlockingQueue<WriteBatch> queue) {
      try {
        while (true) {
          if (failure.get() != null)
            return;
          final WriteBatch batch = queue.take();
          if (batch.isPoison())
            return;

          final int active = activeWriters.incrementAndGet();
          maxActiveWriters.accumulateAndGet(active, Math::max);
          try {
            for (int i = 0; i < batch.size(); i++) {
              if (failure.get() != null)
                return;
              final BucketWriter writer = batch.writer(i);
              writer.append(batch.keys(i), batch.rids(i));
              invokeBuildTestHook(BuildPhase.AFTER_BUCKET_APPEND, writer.mainIndex,
                  completedWriterGroups.incrementAndGet());
            }
          } finally {
            activeWriters.decrementAndGet();
          }
        }
      } catch (final Throwable error) {
        failure.compareAndSet(null, error);
        if (error instanceof InterruptedException)
          Thread.currentThread().interrupt();
      }
    }

    private void enqueue(final ArrayBlockingQueue<WriteBatch> queue, final WriteBatch batch) throws Exception {
      while (!queue.offer(batch, 100L, TimeUnit.MILLISECONDS))
        checkFailure();
    }

    private void checkFailure() throws Exception {
      final Throwable error = failure.get();
      if (error == null)
        return;
      if (error instanceof Exception exception)
        throw exception;
      if (error instanceof Error fatal)
        throw fatal;
      throw new IndexException("Sorted index bucket writer failed", error);
    }

    private int getMaxConcurrentWriters() {
      return maxActiveWriters.get();
    }

    @Override
    public void close() {
      if (!completed) {
        for (final ArrayBlockingQueue<WriteBatch> queue : queues)
          queue.clear();
        executor.shutdownNow();
      }
      try {
        if (!executor.awaitTermination(30L, TimeUnit.SECONDS))
          LogManager.instance().log(LSMTreeIndexBulkLoader.this, Level.WARNING,
              "Timed out stopping sorted index bucket writers for '%s'", indexName);
      } catch (final InterruptedException error) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private final class WriteBatch {
    private final BucketWriter[] writers;
    private final Object[][] keys;
    private final RID[][] rids;
    private final boolean poison;
    private int size;

    private WriteBatch(final boolean poison) {
      this.poison = poison;
      this.writers = poison ? null : new BucketWriter[WRITER_BATCH_GROUPS];
      this.keys = poison ? null : new Object[WRITER_BATCH_GROUPS][];
      this.rids = poison ? null : new RID[WRITER_BATCH_GROUPS][];
    }

    private void add(final BucketWriter writer, final Object[] key, final RID[] ridValues) {
      writers[size] = writer;
      keys[size] = key;
      rids[size] = ridValues;
      size++;
    }

    private BucketWriter writer(final int index) {
      return writers[index];
    }

    private Object[] keys(final int index) {
      return keys[index];
    }

    private RID[] rids(final int index) {
      return rids[index];
    }

    private int size() {
      return size;
    }

    private boolean isEmpty() {
      return size == 0;
    }

    private boolean isFull() {
      return size == WRITER_BATCH_GROUPS;
    }

    private boolean isPoison() {
      return poison;
    }
  }

  private final class BucketWriter {
    private final LSMTreeIndex                       mainIndex;
    private final LSMTreeIndexCompacted              compactedIndex;
    private final LSMTreeIndexCompactedStreamWriter  streamWriter;
    private       boolean                            attached;

    private BucketWriter(final LSMTreeIndex mainIndex) throws IOException {
      this.mainIndex = mainIndex;
      this.compactedIndex = mainIndex.getMutableIndex().createNewForCompaction();
      try {
        database.getSchema().getEmbedded().registerFile(compactedIndex);
        this.streamWriter = new LSMTreeIndexCompactedStreamWriter(mainIndex, compactedIndex);
      } catch (final RuntimeException error) {
        try {
          database.getFileManager().dropFile(compactedIndex.getFileId());
        } catch (final Throwable cleanupError) {
          error.addSuppressed(cleanupError);
        }
        throw error;
      }
    }

    private void append(final Object[] keys, final RID[] rids) throws IOException, InterruptedException {
      streamWriter.appendBounded(keys, rids,
          LSMTreeIndexCompactedStreamWriter.DEFAULT_MAX_DATA_PAGES_PER_SERIES);
    }

    private void finishAndAttach() throws IOException, InterruptedException {
      streamWriter.finishIfActive();
      database.getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
      mainIndex.splitIndex(mainIndex.getMutableIndex().getTotalPages(), compactedIndex, false);
      attached = true;
    }

    private void abort() {
      if (attached)
        return;
      try {
        database.getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
        database.transaction(() -> {
          try {
            database.getPageManager().deleteFile(database, compactedIndex.getFileId());
            database.getFileManager().dropFile(compactedIndex.getFileId());
            database.getSchema().getEmbedded().removeFile(compactedIndex.getFileId());
          } catch (final IOException error) {
            throw new IndexException("Cannot remove failed sorted index file '" + compactedIndex.getName() + "'", error);
          }
        }, false, 1, null, null);
      } catch (final Throwable error) {
        LogManager.instance().log(this, Level.WARNING, "Cannot clean failed sorted index file '%s'", error,
            compactedIndex.getName());
      }
    }
  }
}
