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
import java.util.stream.Stream;

/** Creates bounded sorted spill runs and exposes a k-way merged entry stream. */
final class LSMTreeIndexExternalSorter implements AutoCloseable {
  static final String DIRECTORY_PREFIX = ".arcadedb-index-sort-";

  private static final ThreadLocal<SpillWriteTestHook> SPILL_WRITE_TEST_HOOK = new ThreadLocal<>();

  private static final int RUN_MAGIC       = 0x41584953;
  private static final int RUN_VERSION     = 1;
  private static final int BUFFER_SIZE     = 256 << 10;
  private static final int MAX_ENTRY_BYTES = 64 << 20;
  private static final long FILE_DESCRIPTOR_RESERVE = 64L;

  private final DatabaseInternal                  database;
  private final BinarySerializer                  serializer;
  private final byte[]                            binaryKeyTypes;
  private final Map<Integer, LSMTreeIndex>         indexesByBucket;
  private final Path                              directory;
  private final int                               mergeFanIn;
  private final List<RunFile>                     runs = new ArrayList<>();
  private final Binary                            writePayload = new Binary();
  private       long                              spilledBytes;
  private       int                               mergeGeneration;

  LSMTreeIndexExternalSorter(final DatabaseInternal database, final byte[] binaryKeyTypes,
      final Map<Integer, LSMTreeIndex> indexesByBucket, final Path spillParent, final int configuredMergeFanIn,
      final long memoryBudgetBytes, final Path spillWorkspace) throws IOException {
    if (configuredMergeFanIn < 2)
      throw new IllegalArgumentException("mergeFanIn must be at least 2");

    this.database = database;
    this.serializer = database.getSerializer();
    this.binaryKeyTypes = Arrays.copyOf(binaryKeyTypes, binaryKeyTypes.length);
    this.indexesByBucket = indexesByBucket;
    this.mergeFanIn = selectMergeFanIn(configuredMergeFanIn, memoryBudgetBytes, getAvailableFileDescriptors());

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

  private static long getAvailableFileDescriptors() {
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

    entries.sort(LSMTreeIndexBulkLoader::compareEntries);
    final Path run = directory.resolve("run-%06d.bin".formatted(runs.size()));
    boolean complete = false;
    try (DataOutputStream output = openRunOutput(run, entries.size())) {
      for (final LSMTreeIndexBulkLoader.Entry entry : entries)
        writeEntry(output, entry);
      complete = true;
    } finally {
      if (!complete)
        Files.deleteIfExists(run);
    }

    runs.add(new RunFile(run, entries.size()));
    spilledBytes += Files.size(run);
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

  private void consolidateRuns() throws IOException {
    while (runs.size() > mergeFanIn) {
      final List<RunFile> mergedRuns = new ArrayList<>((runs.size() + mergeFanIn - 1) / mergeFanIn);
      final List<RunFile> sourceRuns = new ArrayList<>(runs);

      for (int from = 0; from < sourceRuns.size(); from += mergeFanIn) {
        final int to = Math.min(sourceRuns.size(), from + mergeFanIn);
        final List<RunFile> group = sourceRuns.subList(from, to);
        if (group.size() == 1) {
          mergedRuns.add(group.getFirst());
          continue;
        }

        final Path mergedPath = directory.resolve(
            "merge-%03d-%06d.bin".formatted(mergeGeneration, mergedRuns.size()));
        final long entryCount = group.stream().mapToLong(RunFile::entries).sum();
        final RunFile merged = mergeRuns(group, mergedPath, entryCount);
        mergedRuns.add(merged);
        spilledBytes += Files.size(mergedPath);

        for (final RunFile source : group)
          Files.delete(source.path());
      }

      runs.clear();
      runs.addAll(mergedRuns);
      mergeGeneration++;
    }
  }

  private RunFile mergeRuns(final List<RunFile> sources, final Path outputPath, final long entryCount) throws IOException {
    boolean complete = false;
    try (DataOutputStream output = openRunOutput(outputPath, entryCount);
        EntryCursor cursor = new MergedCursor(sources)) {
      while (cursor.hasNext())
        writeEntry(output, cursor.next());
      complete = true;
    } finally {
      if (!complete)
        Files.deleteIfExists(outputPath);
    }
    return new RunFile(outputPath, entryCount);
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

  private void writeEntry(final DataOutputStream output, final LSMTreeIndexBulkLoader.Entry entry) throws IOException {
    writePayload.clear();
    for (int i = 0; i < binaryKeyTypes.length; i++) {
      final Object key = entry.key().values[i];
      writePayload.putByte((byte) (key == null ? 0 : 1));
      if (key != null)
        serializer.serializeValue(database, writePayload, binaryKeyTypes[i], key);
    }
    serializer.serializeValue(database, writePayload, BinaryTypes.TYPE_COMPRESSED_RID, entry.rid());

    output.writeInt(entry.index().getAssociatedBucketId());
    output.writeInt(writePayload.size());
    output.write(writePayload.getContent(), writePayload.getContentBeginOffset(), writePayload.size());

    final SpillWriteTestHook hook = SPILL_WRITE_TEST_HOOK.get();
    if (hook != null)
      hook.afterEntryWritten();
  }

  static void setSpillWriteTestHook(final SpillWriteTestHook hook) {
    if (hook == null)
      SPILL_WRITE_TEST_HOOK.remove();
    else
      SPILL_WRITE_TEST_HOOK.set(hook);
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

  @FunctionalInterface
  interface SpillWriteTestHook {
    void afterEntryWritten() throws IOException;
  }
}
