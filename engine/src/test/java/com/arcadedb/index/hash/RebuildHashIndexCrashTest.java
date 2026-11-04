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
package com.arcadedb.index.hash;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexBuildTestHooks;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A process killed in the middle of {@code REBUILD INDEX} on a UNIQUE HASH type index reopened without the index, and without its
 * constraint: the rebuild dropped it first. A hash index is rebuilt in place now, one bucket index at a time into a new file swapped
 * in by a schema save, so a crash at any point must reopen with a complete index that still refuses a duplicate key and with no file
 * of the interrupted rebuild left. Each case kills a child JVM ({@code Runtime.halt}, as a SIGKILL would): in the middle of the fill
 * of the first new file, once it is filled and before the swap, after the swap was saved and before the previous file was dropped,
 * and after the first bucket index was rebuilt.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RebuildHashIndexCrashTest {
  private static final int    CRASH_EXIT_CODE = 73;
  private static final String TYPE            = "CrashAccount";
  private static final String INDEX           = TYPE + "[uid]";
  private static final int    BUCKETS         = 4;
  private static final int    RECORDS         = 4_000;

  @TempDir
  Path tempDirectory;

  @ParameterizedTest
  @ValueSource(strings = { "mid-shadow", "before-swap", "after-swap-before-old-drop", "after-first-bucket" })
  void aCrashDuringTheRebuildLeavesACompleteUniqueIndex(final String crashPoint) throws Exception {
    final Path databasePath = tempDirectory.resolve(crashPoint);
    final ChildResult prepared = runChild("prepare", databasePath);
    assertThat(prepared.exitCode()).as(prepared.output()).isZero();

    final ChildResult crashed = runChild(crashPoint, databasePath);
    assertThat(crashed.exitCode()).as(crashed.output()).isEqualTo(CRASH_EXIT_CODE);

    final List<String> names = new ArrayList<>();
    try (final DatabaseFactory factory = new DatabaseFactory(databasePath.toString()); final Database database = factory.open()) {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
      assertThat(index).as("the index survives the crash").isNotNull();
      assertThat(index.isUnique()).isTrue();
      assertThat(index.isValid()).isTrue();
      assertComplete(database, index);
      for (final Index candidate : database.getSchema().getIndexes())
        assertThat(candidate.getTypeName()).as("index %s belongs to a type", candidate.getName()).isNotNull();
      for (final IndexInternal onBucket : index.getIndexesOnBuckets()) {
        names.add(onBucket.getName());
        assertThat(database.getSchema().getIndexByName(onBucket.getName())).isSameAs(onBucket);
      }
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument(TYPE).set("uid", "key-42").save()))
          .isInstanceOf(DuplicatedKeyException.class);
    }
    assertThat(hashFilesOnDisk(databasePath)).as("one file per bucket index").isEqualTo(BUCKETS);

    try (final DatabaseFactory factory = new DatabaseFactory(databasePath.toString()); final Database database = factory.open()) {
      // the bucket indexes answer to the same names after one more open, and the rebuild runs again to the end
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
      final List<String> reopened = new ArrayList<>();
      for (final IndexInternal onBucket : index.getIndexesOnBuckets())
        reopened.add(onBucket.getName());
      assertThat(reopened).containsExactlyInAnyOrderElementsOf(names);
      database.command("sql", "REBUILD INDEX `" + INDEX + "`");
      assertComplete(database, (TypeIndex) database.getSchema().getIndexByName(INDEX));
    }
    assertThat(hashFilesOnDisk(databasePath)).isEqualTo(BUCKETS);
  }

  private static void assertComplete(final Database database, final TypeIndex index) {
    final DocumentType type = database.getSchema().getType(TYPE);
    assertThat(index.getIndexesOnBuckets()).hasSize(BUCKETS);
    final Map<Integer, Long> recordsPerBucket = new HashMap<>();
    database.iterateType(TYPE, false).forEachRemaining(r -> recordsPerBucket.merge(r.getIdentity().getBucketId(), 1L, Long::sum));
    for (final Bucket bucket : type.getBuckets(false)) {
      IndexInternal onBucket = null;
      for (final IndexInternal candidate : index.getIndexesOnBuckets())
        if (candidate.getAssociatedBucketId() == bucket.getFileId())
          onBucket = candidate;
      assertThat(onBucket).as("bucket index of %s", bucket.getName()).isNotNull();
      assertThat(onBucket.countEntries()).as("entries of %s", bucket.getName())
          .isEqualTo(recordsPerBucket.getOrDefault(bucket.getFileId(), 0L));
    }
    for (int i = 0; i < RECORDS; i += 97)
      assertThat(count(database.lookupByKey(TYPE, "uid", "key-" + i))).isEqualTo(1);
  }

  private static long hashFilesOnDisk(final Path databasePath) throws IOException {
    try (final Stream<Path> paths = Files.list(databasePath)) {
      return paths.filter(path -> path.getFileName().toString().endsWith("hashidx")).count();
    }
  }

  public static void main(final String[] args) {
    if (args.length != 2)
      throw new IllegalArgumentException("Expected mode and database path");
    if ("prepare".equals(args[0]))
      prepare(args[1]);
    else
      crash(args[0], args[1]);
  }

  private static void prepare(final String databasePath) {
    try (final DatabaseFactory factory = new DatabaseFactory(databasePath); final Database database = factory.create()) {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(BUCKETS).create();
      type.createProperty("uid", Type.STRING);
      database.getSchema().buildTypeIndex(TYPE, new String[] { "uid" }).withType(Schema.INDEX_TYPE.HASH).withUnique(true).create();
      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          database.newDocument(TYPE).set("uid", "key-" + i).save();
      });
    }
  }

  private static void crash(final String mode, final String databasePath) {
    final AtomicInteger swaps = new AtomicInteger();
    switch (mode) {
    case "mid-shadow" -> {
      final AtomicInteger records = new AtomicInteger();
      IndexBuildTestHooks.onRecordIndexed = (index, rid) -> {
        if (records.incrementAndGet() == 300)
          Runtime.getRuntime().halt(CRASH_EXIT_CODE);
      };
    }
    case "before-swap" -> HashIndex.setSwapTestHook(phase -> {
      if (phase == HashIndex.SwapPhase.BEFORE_SWAP)
        Runtime.getRuntime().halt(CRASH_EXIT_CODE);
    });
    case "after-swap-before-old-drop" -> HashIndex.setSwapTestHook(phase -> {
      if (phase == HashIndex.SwapPhase.AFTER_SWAP)
        Runtime.getRuntime().halt(CRASH_EXIT_CODE);
    });
    case "after-first-bucket" -> HashIndex.setSwapTestHook(phase -> {
      if (phase == HashIndex.SwapPhase.BEFORE_SWAP && swaps.incrementAndGet() == 2)
        Runtime.getRuntime().halt(CRASH_EXIT_CODE);
    });
    default -> throw new IllegalArgumentException("Unknown mode " + mode);
    }

    try (final DatabaseFactory factory = new DatabaseFactory(databasePath); final Database database = factory.open()) {
      database.command("sql", "REBUILD INDEX `" + INDEX + "`");
    }
    throw new AssertionError("The rebuild completed without reaching the crash point " + mode);
  }

  private ChildResult runChild(final String mode, final Path databasePath) throws IOException, InterruptedException {
    final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    final String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    final List<String> command = new ArrayList<>(List.of(java, "-Xms128m", "-Xmx512m", "--add-exports",
        "java.management/sun.management=ALL-UNNAMED", "--add-opens", "java.base/java.util.concurrent.atomic=ALL-UNNAMED", "--add-opens",
        "java.base/java.nio.channels.spi=ALL-UNNAMED", "--add-modules", "jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED",
        "-cp", classpath, RebuildHashIndexCrashTest.class.getName(), mode, databasePath.toString()));
    // to a file, not a pipe: reading a pipe blocks until the child closes it, so a hung child would never reach the timeout
    final Path outputFile = Files.createTempFile(tempDirectory, "child-", ".log");
    final Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(outputFile.toFile()).start();
    final boolean finished = process.waitFor(60, TimeUnit.SECONDS);
    if (!finished)
      process.destroyForcibly().waitFor();
    final String output = Files.readString(outputFile, StandardCharsets.UTF_8);
    assertThat(finished).as(output).isTrue();
    return new ChildResult(process.exitValue(), output);
  }

  private static int count(final IndexCursor cursor) {
    int count = 0;
    while (cursor.hasNext())
      if (cursor.next() != null)
        count++;
    return count;
  }

  private record ChildResult(int exitCode, String output) {
  }
}
