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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;

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
 * A process killed in the middle of {@code REBUILD INDEX} on a UNIQUE LSM_TREE type index reopened with
 * NO index on the property: the rebuild dropped the index first, and the startup removed the half-built one as "left by an
 * interrupted sorted index build". The unique constraint went with it, so later upserts created duplicates. A crash at any point
 * of the rebuild must now reopen with a complete index that still refuses a duplicate key.
 * <p>
 * Each case kills a child JVM ({@code Runtime.halt}, as a SIGKILL would) at one point of the rebuild: after the sort of the first
 * bucket, while the entries of the first bucket are written, and after the first bucket's new content was swapped in.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RebuildIndexCrashTest {
  private static final int    CRASH_EXIT_CODE = 73;
  private static final String TYPE            = "CrashForm";
  private static final String INDEX           = TYPE + "[uid]";
  private static final int    BUCKETS         = 4;
  private static final int    RECORDS         = 4_000;

  @TempDir
  Path tempDirectory;

  @ParameterizedTest
  @ValueSource(strings = { "after-sort", "mid-write", "after-first-swap", "after-second-swap" })
  void aCrashDuringTheRebuildLeavesACompleteUniqueIndex(final String crashPoint) throws Exception {
    final Path databasePath = tempDirectory.resolve(crashPoint);
    final ChildResult prepared = runChild("prepare", databasePath);
    assertThat(prepared.exitCode()).as(prepared.output()).isZero();

    final ChildResult crashed = runChild(crashPoint, databasePath);
    assertThat(crashed.exitCode()).as(crashed.output()).isEqualTo(CRASH_EXIT_CODE);

    try (final DatabaseFactory factory = new DatabaseFactory(databasePath.toString()); final Database database = factory.open()) {
      final DocumentType type = database.getSchema().getType(TYPE);
      final TypeIndex index = type.getIndexByProperties("uid");
      assertThat(index).as("the index survives the crash").isNotNull();
      assertThat(index.isUnique()).isTrue();
      assertThat(index.isValid()).isTrue();
      assertComplete(database, index);

      assertThatThrownBy(() -> database.transaction(() -> database.newDocument(TYPE).set("uid", "key-42").save()))
          .isInstanceOf(DuplicatedKeyException.class);

      // and the rebuild can run again to the end
      database.command("sql", "REBUILD INDEX `" + INDEX + "`");
      assertComplete(database, (TypeIndex) database.getSchema().getIndexByName(INDEX));
    }
    assertThat(buildArtifacts(databasePath)).isEmpty();
  }

  /**
   * The repair of a type index left incomplete by a rebuild of an earlier version (two buckets without a bucket index): a crash after
   * the missing bucket indexes were created EMPTY, or after the first of them was filled but before it was counted, reopens in the
   * incomplete state, reported as not valid, never with an empty bucket index counted as complete. The next REBUILD repairs it, and
   * nothing of the interrupted repair is left behind.
   */
  @ParameterizedTest
  @ValueSource(strings = { "repair-after-create", "repair-after-first-fill" })
  void aCrashDuringTheRepairOfAnIncompleteIndexLeavesItReportedIncomplete(final String crashPoint) throws Exception {
    final Path databasePath = tempDirectory.resolve(crashPoint);
    final ChildResult prepared = runChild("prepare-incomplete", databasePath);
    assertThat(prepared.exitCode()).as(prepared.output()).isZero();

    final ChildResult crashed = runChild(crashPoint, databasePath);
    assertThat(crashed.exitCode()).as(crashed.output()).isEqualTo(CRASH_EXIT_CODE);

    try (final DatabaseFactory factory = new DatabaseFactory(databasePath.toString()); final Database database = factory.open()) {
      final TypeIndex index = database.getSchema().getType(TYPE).getIndexByProperties("uid");
      assertThat(index).isNotNull();
      assertThat(index.isValid()).as("an incomplete index is not reported as valid").isFalse();
      assertThat(index.getBucketsWithoutIndex()).hasSize(2);
      // every bucket index it does have is complete for its bucket
      final Map<Integer, Long> recordsPerBucket = new HashMap<>();
      database.iterateType(TYPE, false).forEachRemaining(r -> recordsPerBucket.merge(r.getIdentity().getBucketId(), 1L, Long::sum));
      for (final IndexInternal onBucket : index.getIndexesOnBuckets())
        assertThat(onBucket.countEntries()).isEqualTo(recordsPerBucket.getOrDefault(onBucket.getAssociatedBucketId(), 0L));
      assertNoUnreferencedBucketIndex(database);

      database.command("sql", "REBUILD INDEX `" + INDEX + "`");
      final TypeIndex repaired = (TypeIndex) database.getSchema().getIndexByName(INDEX);
      assertThat(repaired.isValid()).isTrue();
      assertComplete(database, repaired);
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument(TYPE).set("uid", "key-42").save()))
          .isInstanceOf(DuplicatedKeyException.class);
    }
    try (final DatabaseFactory factory = new DatabaseFactory(databasePath.toString()); final Database database = factory.open()) {
      assertThat(((TypeIndex) database.getSchema().getIndexByName(INDEX)).isValid()).isTrue();
      assertNoUnreferencedBucketIndex(database);
    }
    assertThat(buildArtifacts(databasePath)).isEmpty();
  }

  /**
   * A crash after a bucket index's swap was saved and before its previous mutable file was dropped, on an index that never
   * compacted: the previous mutable file is still named after the bucket index, which is the logical name the schema entry of the
   * new file carries.
   */
  @Test
  void aCrashBeforeThePreviousFileIsDroppedLeavesACompleteUniqueIndex() throws Exception {
    final Path databasePath = tempDirectory.resolve("after-swap-before-old-drop");
    final ChildResult prepared = runChild("prepare", databasePath);
    assertThat(prepared.exitCode()).as(prepared.output()).isZero();
    final ChildResult crashed = runChild("after-swap-before-old-drop", databasePath);
    assertThat(crashed.exitCode()).as(crashed.output()).isEqualTo(CRASH_EXIT_CODE);
    assertReopensCompleteAfterSwapCrash(databasePath);
  }

  /**
   * The same crash on an index rebuilt in place a second time: its bucket indexes were given new files once already, so their
   * files are no longer named after the indexes.
   */
  @Test
  void aCrashBeforeThePreviousFileOfACompactedIndexIsDroppedLeavesACompleteUniqueIndex() throws Exception {
    final Path databasePath = tempDirectory.resolve("compacted-after-swap-before-old-drop");
    final ChildResult prepared = runChild("prepare-compacted", databasePath);
    assertThat(prepared.exitCode()).as(prepared.output()).isZero();
    final ChildResult crashed = runChild("after-swap-before-old-drop", databasePath);
    assertThat(crashed.exitCode()).as(crashed.output()).isEqualTo(CRASH_EXIT_CODE);
    assertReopensCompleteAfterSwapCrash(databasePath);
  }

  private void assertReopensCompleteAfterSwapCrash(final Path databasePath) throws IOException {
    try (final DatabaseFactory factory = new DatabaseFactory(databasePath.toString()); final Database database = factory.open()) {
      final TypeIndex index = database.getSchema().getType(TYPE).getIndexByProperties("uid");
      assertThat(index).isNotNull();
      assertThat(index.isValid()).isTrue();
      assertComplete(database, index);
      // every bucket index the schema serves by name is the one of the type index
      for (final IndexInternal onBucket : index.getIndexesOnBuckets())
        assertThat(database.getSchema().getIndexByName(onBucket.getName())).isSameAs(onBucket);
      assertNoUnreferencedBucketIndex(database);
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument(TYPE).set("uid", "key-42").save()))
          .isInstanceOf(DuplicatedKeyException.class);
    }
    assertThat(indexFilesOnDisk(databasePath)).as("one mutable file per bucket index").isEqualTo(BUCKETS);
    try (final DatabaseFactory factory = new DatabaseFactory(databasePath.toString()); final Database database = factory.open()) {
      assertComplete(database, (TypeIndex) database.getSchema().getIndexByName(INDEX));
    }
  }

  private static long indexFilesOnDisk(final Path databasePath) throws IOException {
    try (final Stream<Path> paths = Files.list(databasePath)) {
      return paths.filter(path -> path.getFileName().toString().endsWith("mtidx")).count();
    }
  }

  private static void assertNoUnreferencedBucketIndex(final Database database) {
    for (final Index index : database.getSchema().getIndexes())
      assertThat(index.getTypeName()).as("index %s belongs to a type", index.getName()).isNotNull();
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

  public static void main(final String[] args) {
    if (args.length != 2)
      throw new IllegalArgumentException("Expected mode and database path");
    if ("prepare".equals(args[0]))
      prepare(args[1]);
    else if ("prepare-compacted".equals(args[0])) {
      prepare(args[1]);
      try (final DatabaseFactory factory = new DatabaseFactory(args[1]); final Database database = factory.open()) {
        database.command("sql", "REBUILD INDEX `" + INDEX + "`");
      }
    } else if ("prepare-incomplete".equals(args[0])) {
      prepare(args[1]);
      try (final DatabaseFactory factory = new DatabaseFactory(args[1]); final Database database = factory.open()) {
        final IndexInternal[] onBuckets = ((TypeIndex) database.getSchema().getIndexByName(INDEX)).getIndexesOnBuckets();
        database.getSchema().dropIndex(onBuckets[1].getName());
        database.getSchema().dropIndex(onBuckets[3].getName());
      }
    } else
      crash(args[0], args[1]);
  }

  private static void prepare(final String databasePath) {
    try (final DatabaseFactory factory = new DatabaseFactory(databasePath); final Database database = factory.create()) {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(BUCKETS).create();
      type.createProperty("uid", Type.STRING);
      database.getSchema().buildTypeIndex(TYPE, new String[] { "uid" }).withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(true).create();
      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          database.newDocument(TYPE).set("uid", "key-" + i).save();
      });
    }
  }

  private static void crash(final String mode, final String databasePath) {
    // so a drop-and-create rebuild takes the sorted build too, and reaches the same points
    GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS.setValue(1L);

    if ("after-swap-before-old-drop".equals(mode))
      LSMTreeIndex.setAfterSwapTestHook(() -> Runtime.getRuntime().halt(CRASH_EXIT_CODE));

    final AtomicInteger swaps = new AtomicInteger();
    LSMTreeIndexBulkLoader.setBuildTestHook((phase, index, completedBuckets) -> {
      final boolean halt = switch (mode) {
        case "after-sort" -> phase == LSMTreeIndexBulkLoader.BuildPhase.AFTER_SORT;
        case "mid-write" -> phase == LSMTreeIndexBulkLoader.BuildPhase.AFTER_BUCKET_APPEND;
        case "after-first-swap" -> phase == LSMTreeIndexBulkLoader.BuildPhase.AFTER_BUCKET_ATTACHMENT && swaps.incrementAndGet() == 1;
        case "after-second-swap" -> phase == LSMTreeIndexBulkLoader.BuildPhase.AFTER_BUCKET_ATTACHMENT && swaps.incrementAndGet() == 2;
        case "repair-after-create" -> phase == LSMTreeIndexBulkLoader.BuildPhase.AFTER_SORT;
        case "repair-after-first-fill" -> phase == LSMTreeIndexBulkLoader.BuildPhase.AFTER_BUCKET_ATTACHMENT;
        case "after-swap-before-old-drop" -> false;
        default -> throw new IllegalArgumentException("Unknown mode " + mode);
      };
      if (halt)
        Runtime.getRuntime().halt(CRASH_EXIT_CODE);
    });

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
        "-cp", classpath, RebuildIndexCrashTest.class.getName(), mode, databasePath.toString()));
    final Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    final String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertThat(process.waitFor(60, TimeUnit.SECONDS)).as(output).isTrue();
    return new ChildResult(process.exitValue(), output);
  }

  private static int count(final IndexCursor cursor) {
    int count = 0;
    while (cursor.hasNext())
      if (cursor.next() != null)
        count++;
    return count;
  }

  private static List<Path> buildArtifacts(final Path databasePath) throws IOException {
    try (final Stream<Path> paths = Files.list(databasePath)) {
      return paths.filter(path -> {
        final String name = path.getFileName().toString();
        return name.startsWith(".arcadedb-sorted-index-build-") || name.startsWith(".arcadedb-index-sort-");
      }).toList();
    }
  }

  private record ChildResult(int exitCode, String output) {
  }
}
