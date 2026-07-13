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

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionIndexContext;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.BinaryTypes;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/** Creates bounded sorted spill runs and exposes a k-way merged entry stream. */
final class LSMTreeIndexExternalSorter implements AutoCloseable {
  static final String DIRECTORY_PREFIX = ".arcadedb-index-sort-";

  private static final ThreadLocal<SpillWriteTestHook> SPILL_WRITE_TEST_HOOK = new ThreadLocal<>();
  private static final ThreadLocal<MaterializedMergeTestHook> MATERIALIZED_MERGE_TEST_HOOK = new ThreadLocal<>();

  private static final int RUN_MAGIC       = 0x41584953;
  private static final int RUN_VERSION     = 1;
  private static final int BUFFER_SIZE     = 256 << 10;
  private static final int MAX_ENTRY_BYTES = 64 << 20;
  private static final long MERGE_WORKER_OVERHEAD_BYTES = 8L << 20;
  private static final long FILE_DESCRIPTOR_RESERVE = 64L;

  private final DatabaseInternal                  database;
  private final BinarySerializer                  serializer;
  private final byte[]                            binaryKeyTypes;
  private final Map<Integer, LSMTreeIndex>         indexesByBucket;
  private final Path                              directory;
  private final int                               mergeFanIn;
  private final int                               admittedMergeParallelism;
  private final List<RunFile>                     runs = new ArrayList<>();
  private final Binary                            writePayload = new Binary();
  private final SpillWriteTestHook                spillWriteTestHook;
  private final MaterializedMergeTestHook         materializedMergeTestHook;
  private final AtomicInteger                     activeMerges = new AtomicInteger();
  private final AtomicInteger                     maxConcurrentMerges = new AtomicInteger();
  private       int                               initialRunCount;
  private       long                              initialRunEntries;
  private       long                              initialRunBytes;
  private       long                              initialRunNanos;
  private       long                              materializedMergeEntries;
  private       long                              materializedMergeBytes;
  private       long                              materializedMergeNanos;
  private       long                              spilledBytes;
  private       int                               mergeGeneration;

  LSMTreeIndexExternalSorter(final DatabaseInternal database, final byte[] binaryKeyTypes,
      final Map<Integer, LSMTreeIndex> indexesByBucket, final Path spillParent, final int configuredMergeFanIn,
      final long memoryBudgetBytes, final Path spillWorkspace, final int configuredMergeParallelism) throws IOException {
    if (configuredMergeFanIn < 2)
      throw new IllegalArgumentException("mergeFanIn must be at least 2");
    if (configuredMergeParallelism < 1)
      throw new IllegalArgumentException("mergeParallelism must be at least 1");

    this.database = database;
    this.serializer = database.getSerializer();
    this.binaryKeyTypes = Arrays.copyOf(binaryKeyTypes, binaryKeyTypes.length);
    this.indexesByBucket = indexesByBucket;
    final long availableFileDescriptors = getAvailableFileDescriptors();
    this.mergeFanIn = selectMergeFanIn(configuredMergeFanIn, memoryBudgetBytes, availableFileDescriptors);
    this.admittedMergeParallelism = selectMergeParallelism(configuredMergeParallelism, mergeFanIn,
        memoryBudgetBytes, Runtime.getRuntime().availableProcessors(), availableFileDescriptors);
    this.spillWriteTestHook = SPILL_WRITE_TEST_HOOK.get();
    this.materializedMergeTestHook = MATERIALIZED_MERGE_TEST_HOOK.get();

    if (spillWorkspace != null) {
      directory = spillWorkspace.toAbsolutePath().normalize();
      Files.createDirectories(directory.getParent());
      Files.createDirectory(directory);
    } else {
      final Path parent = spillParent != null ? spillParent : Path.of(database.getDatabasePath());
      Files.createDirectories(parent);
      directory = Files.createTempDirectory(parent, DIRECTORY_PREFIX);
    }
  }

  static int selectMergeFanIn(final int configured, final long memoryBudgetBytes,
      final long availableFileDescriptors) throws IOException {
    final long memoryLimit = Math.max(2L, memoryBudgetBytes / (2L * BUFFER_SIZE));
    final long descriptorLimit = availableFileDescriptors == Long.MAX_VALUE ? Integer.MAX_VALUE
        : availableFileDescriptors - 1L;
    if (descriptorLimit < 2L)
      throw new IOException("Insufficient file descriptors for external sort merging");
    return (int) Math.min(configured, Math.min(memoryLimit, Math.min(descriptorLimit, Integer.MAX_VALUE)));
  }

  static int selectMergeParallelism(final int configured, final int mergeFanIn, final long memoryBudgetBytes,
      final int availableProcessors, final long availableFileDescriptors) {
    if (configured < 1)
      throw new IllegalArgumentException("merge parallelism must be at least 1");
    if (mergeFanIn < 2)
      throw new IllegalArgumentException("merge fan-in must be at least 2");

    final long cpuLimit = Math.max(1L, (long) availableProcessors - 1L);
    final long bytesPerWorker = ((long) mergeFanIn + 1L) * BUFFER_SIZE + MERGE_WORKER_OVERHEAD_BYTES;
    final long memoryLimit = Math.max(1L, memoryBudgetBytes / bytesPerWorker);
    final long descriptorLimit = availableFileDescriptors == Long.MAX_VALUE ? Integer.MAX_VALUE
        : Math.max(1L, availableFileDescriptors / (mergeFanIn + 1L));
    return (int) Math.min(configured, Math.min(cpuLimit, Math.min(memoryLimit, descriptorLimit)));
  }

  static long getAvailableFileDescriptors() {
    try {
      final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      final ObjectName operatingSystem = ObjectName.getInstance(ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME);
      final Object maximumValue = server.getAttribute(operatingSystem, "MaxFileDescriptorCount");
      final Object openValue = server.getAttribute(operatingSystem, "OpenFileDescriptorCount");
      if (maximumValue instanceof Number maximumNumber && openValue instanceof Number openNumber) {
        final long maximum = maximumNumber.longValue();
        final long open = openNumber.longValue();
        if (maximum > 0L && open >= 0L)
          return Math.max(0L, maximum - open - FILE_DESCRIPTOR_RESERVE);
      }
    } catch (final Exception | LinkageError ignored) {
      // Descriptor metrics are optional on non-Unix or non-HotSpot runtimes.
    }
    return Long.MAX_VALUE;
  }

  void addRun(final List<LSMTreeIndexBulkLoader.Entry> entries) throws IOException {
    if (entries.isEmpty())
      return;

    final long started = System.nanoTime();
    entries.sort(LSMTreeIndexBulkLoader::compareEntries);
    final Path run = directory.resolve("run-%06d.bin".formatted(runs.size()));
    boolean complete = false;
    try (DataOutputStream output = openRunOutput(run, entries.size())) {
      for (final LSMTreeIndexBulkLoader.Entry entry : entries)
        writeEntry(output, entry, writePayload);
      complete = true;
    } finally {
      if (!complete)
        Files.deleteIfExists(run);
    }

    final long runBytes = Files.size(run);
    runs.add(new RunFile(run, entries.size()));
    initialRunCount++;
    initialRunEntries += entries.size();
    initialRunBytes += runBytes;
    initialRunNanos += System.nanoTime() - started;
    spilledBytes += runBytes;
  }

  EntryCursor openCursor() throws IOException {
    consolidateRuns();
    return new MergedCursor(List.copyOf(runs));
  }

  int getRunCount() {
    return runs.size();
  }

  long getSpilledBytes() {
    return spilledBytes;
  }

  int getMergeFanIn() {
    return mergeFanIn;
  }

  int getAdmittedMergeParallelism() {
    return admittedMergeParallelism;
  }

  int getMaxConcurrentMerges() {
    return maxConcurrentMerges.get();
  }

  int getInitialRunCount() {
    return initialRunCount;
  }

  long getInitialRunEntries() {
    return initialRunEntries;
  }

  long getInitialRunBytes() {
    return initialRunBytes;
  }

  long getInitialRunNanos() {
    return initialRunNanos;
  }

  int getMaterializedMergeGenerationCount() {
    return mergeGeneration;
  }

  long getMaterializedMergeEntries() {
    return materializedMergeEntries;
  }

  long getMaterializedMergeBytes() {
    return materializedMergeBytes;
  }

  long getMaterializedMergeNanos() {
    return materializedMergeNanos;
  }

  private void consolidateRuns() throws IOException {
    while (runs.size() > mergeFanIn) {
      final List<RunFile> sourceRuns = new ArrayList<>(runs);
      final RunFile[] mergedRuns = new RunFile[(sourceRuns.size() + mergeFanIn - 1) / mergeFanIn];
      final List<MergePlan> mergePlans = new ArrayList<>(mergedRuns.length);

      int groupIndex = 0;
      for (int from = 0; from < sourceRuns.size(); from += mergeFanIn, groupIndex++) {
        final int to = Math.min(sourceRuns.size(), from + mergeFanIn);
        final List<RunFile> group = sourceRuns.subList(from, to);
        if (group.size() == 1) {
          mergedRuns[groupIndex] = group.getFirst();
          continue;
        }

        final Path mergedPath = directory.resolve(
            "merge-%03d-%06d.bin".formatted(mergeGeneration, groupIndex));
        final long entryCount = group.stream().mapToLong(RunFile::entries).sum();
        mergePlans.add(new MergePlan(groupIndex, List.copyOf(group), mergedPath, entryCount));
      }

      final long mergeStarted = System.nanoTime();
      final List<MergeResult> results = executeMergePlans(mergePlans);
      materializedMergeNanos += System.nanoTime() - mergeStarted;

      for (final MergeResult result : results) {
        mergedRuns[result.groupIndex()] = result.run();
        materializedMergeEntries += result.run().entries();
        materializedMergeBytes += result.bytes();
        spilledBytes += result.bytes();
      }

      for (final MergeResult result : results)
        for (final RunFile source : result.sources())
          Files.delete(source.path());

      runs.clear();
      runs.addAll(Arrays.asList(mergedRuns));
      mergeGeneration++;
    }
  }

  private List<MergeResult> executeMergePlans(final List<MergePlan> plans) throws IOException {
    if (plans.isEmpty())
      return List.of();
    if (admittedMergeParallelism == 1 || plans.size() == 1) {
      final List<MergeResult> results = new ArrayList<>(plans.size());
      for (final MergePlan plan : plans)
        results.add(mergeRuns(plan));
      return results;
    }

    final int workers = Math.min(admittedMergeParallelism, plans.size());
    final AtomicInteger threadNumber = new AtomicInteger();
    final ExecutorService executor = Executors.newFixedThreadPool(workers, task -> {
      final Thread thread = new Thread(task, "arcadedb-index-merge-" + threadNumber.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    });
    final ExecutorCompletionService<MergeResult> completion = new ExecutorCompletionService<>(executor);
    final List<Future<MergeResult>> futures = new ArrayList<>(plans.size());
    final List<MergeResult> results = new ArrayList<>(plans.size());
    Throwable failure = null;
    boolean interrupted = false;
    try {
      for (final MergePlan plan : plans)
        futures.add(completion.submit(() -> mergeRuns(plan)));
      for (int i = 0; i < plans.size(); i++)
        results.add(completion.take().get());
    } catch (final ExecutionException error) {
      failure = error.getCause();
    } catch (final InterruptedException error) {
      failure = new InterruptedIOException("Interrupted while merging external sort runs");
      failure.initCause(error);
      interrupted = true;
    } catch (final RuntimeException | Error error) {
      failure = error;
    } finally {
      if (failure != null) {
        for (final Future<MergeResult> future : futures)
          future.cancel(true);
        executor.shutdownNow();
      } else
        executor.shutdown();

      try {
        if (!executor.awaitTermination(30L, TimeUnit.SECONDS)) {
          executor.shutdownNow();
          if (!executor.awaitTermination(30L, TimeUnit.SECONDS)) {
            final IOException timeout = new IOException("Timed out stopping external sort merge workers");
            if (failure == null)
              failure = timeout;
            else
              failure.addSuppressed(timeout);
          }
        }
      } catch (final InterruptedException error) {
        final InterruptedIOException waitFailure = new InterruptedIOException(
            "Interrupted while stopping external sort merge workers");
        waitFailure.initCause(error);
        if (failure == null)
          failure = waitFailure;
        else
          failure.addSuppressed(waitFailure);
        interrupted = true;
        executor.shutdownNow();
      }
    }

    if (interrupted)
      Thread.currentThread().interrupt();
    if (failure instanceof IOException ioError)
      throw ioError;
    if (failure instanceof RuntimeException runtimeError)
      throw runtimeError;
    if (failure instanceof Error fatalError)
      throw fatalError;
    if (failure != null)
      throw new IOException("Cannot merge external sort runs", failure);

    results.sort(Comparator.comparingInt(MergeResult::groupIndex));
    return results;
  }

  private MergeResult mergeRuns(final MergePlan plan) throws IOException {
    final int active = activeMerges.incrementAndGet();
    maxConcurrentMerges.accumulateAndGet(active, Math::max);
    final Binary payload = new Binary();
    long entriesWritten = 0L;
    boolean complete = false;
    try (DataOutputStream output = openRunOutput(plan.outputPath(), plan.entryCount());
        EntryCursor cursor = new MergedCursor(plan.sources())) {
      while (cursor.hasNext()) {
        if (Thread.currentThread().isInterrupted())
          throw new InterruptedIOException("External sort merge worker was cancelled");
        writeEntry(output, cursor.next(), payload);
        invokeMaterializedMergeTestHook(plan.groupIndex(), ++entriesWritten);
      }
      complete = true;
    } finally {
      activeMerges.decrementAndGet();
      if (!complete)
        Files.deleteIfExists(plan.outputPath());
    }
    final RunFile merged = new RunFile(plan.outputPath(), plan.entryCount());
    return new MergeResult(plan.groupIndex(), merged, Files.size(plan.outputPath()), plan.sources());
  }

  private DataOutputStream openRunOutput(final Path path, final long entryCount) throws IOException {
    final DataOutputStream output = new DataOutputStream(new BufferedOutputStream(
        Files.newOutputStream(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE), BUFFER_SIZE));
    try {
      output.writeInt(RUN_MAGIC);
      output.writeInt(RUN_VERSION);
      output.writeInt(binaryKeyTypes.length);
      output.write(binaryKeyTypes);
      output.writeLong(entryCount);
      return output;
    } catch (final IOException | RuntimeException error) {
      try {
        output.close();
      } catch (final IOException closeError) {
        error.addSuppressed(closeError);
      }
      throw error;
    }
  }

  private void writeEntry(final DataOutputStream output, final LSMTreeIndexBulkLoader.Entry entry,
      final Binary payload) throws IOException {
    payload.clear();
    for (int i = 0; i < binaryKeyTypes.length; i++) {
      final Object key = entry.key().values[i];
      payload.putByte((byte) (key == null ? 0 : 1));
      if (key != null)
        serializer.serializeValue(database, payload, binaryKeyTypes[i], key);
    }
    serializer.serializeValue(database, payload, BinaryTypes.TYPE_COMPRESSED_RID, entry.rid());

    output.writeInt(entry.index().getAssociatedBucketId());
    output.writeInt(payload.size());
    output.write(payload.getContent(), payload.getContentBeginOffset(), payload.size());

    if (spillWriteTestHook != null)
      spillWriteTestHook.afterEntryWritten();
  }

  private void invokeMaterializedMergeTestHook(final int groupIndex, final long entriesWritten) throws IOException {
    if (materializedMergeTestHook == null)
      return;
    try {
      materializedMergeTestHook.afterEntryWritten(mergeGeneration, groupIndex, entriesWritten);
    } catch (final InterruptedException error) {
      Thread.currentThread().interrupt();
      final InterruptedIOException interrupted = new InterruptedIOException("Materialized merge test hook interrupted");
      interrupted.initCause(error);
      throw interrupted;
    } catch (final IOException | RuntimeException error) {
      throw error;
    } catch (final Exception error) {
      throw new IOException("Materialized merge test hook failed", error);
    }
  }

  static void setSpillWriteTestHook(final SpillWriteTestHook hook) {
    if (hook == null)
      SPILL_WRITE_TEST_HOOK.remove();
    else
      SPILL_WRITE_TEST_HOOK.set(hook);
  }

  static void setMaterializedMergeTestHook(final MaterializedMergeTestHook hook) {
    if (hook == null)
      MATERIALIZED_MERGE_TEST_HOOK.remove();
    else
      MATERIALIZED_MERGE_TEST_HOOK.set(hook);
  }

  @Override
  public void close() throws IOException {
    if (!Files.exists(directory))
      return;

    final List<Path> paths;
    try (Stream<Path> walk = Files.walk(directory)) {
      paths = walk.sorted(Comparator.reverseOrder()).toList();
    }

    IOException failure = null;
    for (final Path path : paths)
      try {
        Files.deleteIfExists(path);
      } catch (final IOException error) {
        if (failure == null)
          failure = error;
        else
          failure.addSuppressed(error);
      }
    if (failure != null)
      throw failure;
  }

  interface EntryCursor extends AutoCloseable {
    boolean hasNext();

    LSMTreeIndexBulkLoader.Entry next() throws IOException;

    @Override
    void close() throws IOException;
  }

  private final class MergedCursor implements EntryCursor {
    private final PriorityQueue<RunReader> readers = new PriorityQueue<>((left, right) ->
        LSMTreeIndexBulkLoader.compareEntries(left.current, right.current));
    private boolean closed;

    private MergedCursor(final List<RunFile> sourceRuns) throws IOException {
      try {
        for (final RunFile run : sourceRuns) {
          final RunReader reader = new RunReader(run);
          if (reader.current != null)
            readers.add(reader);
          else
            reader.close();
        }
      } catch (final IOException | RuntimeException error) {
        try {
          close();
        } catch (final IOException closeError) {
          error.addSuppressed(closeError);
        }
        throw error;
      }
    }

    @Override
    public boolean hasNext() {
      return !readers.isEmpty();
    }

    @Override
    public LSMTreeIndexBulkLoader.Entry next() throws IOException {
      final RunReader reader = readers.remove();
      final LSMTreeIndexBulkLoader.Entry result = reader.current;
      try {
        reader.advance();
      } catch (final IOException | RuntimeException error) {
        try {
          reader.close();
        } catch (final IOException closeError) {
          error.addSuppressed(closeError);
        }
        throw error;
      }

      if (reader.current != null)
        readers.add(reader);
      else
        reader.close();
      return result;
    }

    @Override
    public void close() throws IOException {
      if (closed)
        return;
      closed = true;

      IOException failure = null;
      for (final RunReader reader : readers)
        try {
          reader.close();
        } catch (final IOException error) {
          if (failure == null)
            failure = error;
          else
            failure.addSuppressed(error);
        }
      readers.clear();
      if (failure != null)
        throw failure;
    }
  }

  private final class RunReader implements AutoCloseable {
    private final DataInputStream input;
    private       long            remaining;
    private       LSMTreeIndexBulkLoader.Entry current;

    private RunReader(final RunFile run) throws IOException {
      input = new DataInputStream(new BufferedInputStream(Files.newInputStream(run.path()), BUFFER_SIZE));
      try {
        if (input.readInt() != RUN_MAGIC)
          throw new IOException("Invalid external sort run header in " + run.path());
        if (input.readInt() != RUN_VERSION)
          throw new IOException("Unsupported external sort run version in " + run.path());

        final int keyCount = input.readInt();
        if (keyCount != binaryKeyTypes.length)
          throw new IOException("External sort run key count " + keyCount + " does not match " + binaryKeyTypes.length);
        final byte[] runKeyTypes = input.readNBytes(keyCount);
        if (!Arrays.equals(runKeyTypes, binaryKeyTypes))
          throw new IOException("External sort run key types do not match the target index");

        remaining = input.readLong();
        if (remaining < 0 || remaining != run.entries())
          throw new IOException("Invalid external sort run entry count " + remaining + " in " + run.path());
        advance();
      } catch (final IOException | RuntimeException error) {
        try {
          input.close();
        } catch (final IOException closeError) {
          error.addSuppressed(closeError);
        }
        throw error;
      }
    }

    private void advance() throws IOException {
      if (remaining == 0) {
        current = null;
        return;
      }

      try {
        final int bucketId = input.readInt();
        final int length = input.readInt();
        if (length < 1 || length > MAX_ENTRY_BYTES)
          throw new IOException("Invalid external sort entry length " + length);

        final byte[] bytes = input.readNBytes(length);
        if (bytes.length != length)
          throw new EOFException("Truncated external sort entry: expected " + length + " bytes, found " + bytes.length);

        final LSMTreeIndex index = indexesByBucket.get(bucketId);
        if (index == null)
          throw new IOException("External sort run references unknown bucket " + bucketId);

        final Binary payload = new Binary(bytes);
        final Object[] keys = new Object[binaryKeyTypes.length];
        for (int i = 0; i < binaryKeyTypes.length; i++)
          if (payload.getByte() != 0)
            keys[i] = serializer.deserializeValue(database, payload, binaryKeyTypes[i], null);
        final RID rid = (RID) serializer.deserializeValue(database, payload, BinaryTypes.TYPE_COMPRESSED_RID, null);
        current = new LSMTreeIndexBulkLoader.Entry(index, new TransactionIndexContext.ComparableKey(keys), rid);
        remaining--;
      } catch (final EOFException error) {
        throw new EOFException("Truncated external sort run with " + remaining + " entries remaining: " + error.getMessage());
      }
    }

    @Override
    public void close() throws IOException {
      input.close();
    }
  }

  private record RunFile(Path path, long entries) {
  }

  private record MergePlan(int groupIndex, List<RunFile> sources, Path outputPath, long entryCount) {
  }

  private record MergeResult(int groupIndex, RunFile run, long bytes, List<RunFile> sources) {
  }

  @FunctionalInterface
  interface SpillWriteTestHook {
    void afterEntryWritten() throws IOException;
  }

  @FunctionalInterface
  interface MaterializedMergeTestHook {
    void afterEntryWritten(int generation, int groupIndex, long entriesWritten) throws Exception;
  }
}
