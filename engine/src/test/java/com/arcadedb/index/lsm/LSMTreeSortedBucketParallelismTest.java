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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.IndexBuildMode;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class LSMTreeSortedBucketParallelismTest extends TestHelper {
  @Test
  void admitsWriterWorkersAgainstBucketsCpuHeapAndFileDescriptors() {
    assertThat(LSMTreeIndexBulkLoader.selectWriterParallelism(8, 4, 64L << 20, 16, 100)).isEqualTo(4);
    assertThat(LSMTreeIndexBulkLoader.selectWriterParallelism(8, 4, 8L << 20, 16, 100)).isEqualTo(1);
    assertThat(LSMTreeIndexBulkLoader.selectWriterParallelism(8, 4, 64L << 20, 2, 100)).isEqualTo(1);
    assertThat(LSMTreeIndexBulkLoader.selectWriterParallelism(8, 4, 64L << 20, 16, 2)).isEqualTo(1);
    assertThat(LSMTreeIndexBulkLoader.selectWriterParallelism(8, 1, 64L << 20, 16, 100)).isEqualTo(1);
    assertThat(LSMTreeIndexBulkLoader.selectWriterParallelism(8, 0, 64L << 20, 16, 100)).isEqualTo(1);
    assertThatThrownBy(() -> LSMTreeIndexBulkLoader.selectWriterParallelism(0, 4, 64L << 20, 16, 100))
        .isInstanceOf(IllegalArgumentException.class);

    final DocumentType type = database.getSchema().createDocumentType("ParallelismValidation");
    type.createProperty("lookupKey", Type.STRING);
    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("ParallelismValidation",
        new String[] { "lookupKey" }).withBuildParallelism(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at least 1");
  }

  @Test
  void writesIndependentBucketsConcurrentlyAndReopens() throws Exception {
    final int admitted = LSMTreeIndexBulkLoader.selectWriterParallelism(4, 4, 64L << 20,
        Runtime.getRuntime().availableProcessors(), LSMTreeIndexExternalSorter.getAvailableFileDescriptors());
    assumeTrue(admitted > 1, "test host does not admit multiple bucket writers");

    createRecords("ParallelBuckets", 4, 4_000);
    final CountDownLatch firstTwoWriters = new CountDownLatch(2);
    final AtomicInteger gatedCalls = new AtomicInteger();
    final AtomicInteger active = new AtomicInteger();
    final AtomicInteger maxActive = new AtomicInteger();

    LSMTreeIndexBulkLoader.setBuildTestHook((phase, index, completed) -> {
      if (phase != LSMTreeIndexBulkLoader.BuildPhase.AFTER_BUCKET_APPEND || gatedCalls.incrementAndGet() > 2)
        return;
      final int current = active.incrementAndGet();
      maxActive.accumulateAndGet(current, Math::max);
      firstTwoWriters.countDown();
      try {
        if (!firstTwoWriters.await(10L, TimeUnit.SECONDS))
          throw new IllegalStateException("bucket writers did not overlap");
      } catch (final InterruptedException error) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted while observing bucket writer overlap", error);
      } finally {
        active.decrementAndGet();
      }
    });

    TypeIndex index;
    try {
      index = build("ParallelBuckets", null, 4);
    } finally {
      LSMTreeIndexBulkLoader.setBuildTestHook(null);
    }

    assertThat(maxActive).hasValueGreaterThan(1);
    assertThat(count(index.iterator(true))).isEqualTo(4_000);
    assertThat(count(index.range(true, new Object[] { key(1_000) }, true,
        new Object[] { key(1_099) }, true))).isEqualTo(100);

    reopenDatabase();
    index = database.getSchema().getType("ParallelBuckets").getIndexByProperties("lookupKey");
    assertThat(count(index.iterator(false))).isEqualTo(4_000);
    assertNoBuildArtifacts(Path.of(getDatabasePath()));
  }

  @Test
  void preservesHighMultiplicityNonUniqueGroupsAcrossRidChunksAndBuckets() throws Exception {
    final DocumentType type = database.getSchema().buildDocumentType().withName("ParallelNonUnique")
        .withTotalBuckets(4).create();
    type.createProperty("lookupKey", Type.STRING);
    database.transaction(() -> {
      for (int i = 0; i < 12_000; i++)
        database.newDocument("ParallelNonUnique").set("lookupKey", "shared-" + i % 3).save();
    });

    TypeIndex index = build("ParallelNonUnique", null, 4, false, 1_024);
    assertThat(count(index.get(new Object[] { "shared-0" }))).isEqualTo(4_000);
    assertThat(count(index.get(new Object[] { "shared-1" }))).isEqualTo(4_000);
    assertThat(count(index.get(new Object[] { "shared-2" }))).isEqualTo(4_000);
    assertThat(count(index.iterator(true))).isEqualTo(12_000);

    reopenDatabase();
    index = database.getSchema().getType("ParallelNonUnique").getIndexByProperties("lookupKey");
    assertThat(count(index.get(new Object[] { "shared-0" }))).isEqualTo(4_000);
    assertThat(count(index.iterator(false))).isEqualTo(12_000);
  }

  @Test
  void retainsGlobalUniqueCheckBeforeParallelBucketWrites() throws Exception {
    final Path spillParent = Path.of(getDatabasePath()).resolve("parallel-duplicate");
    final DocumentType type = createRecords("ParallelDuplicate", 4, 4_000);
    database.transaction(() -> {
      database.newDocument("ParallelDuplicate").set("lookupKey", "duplicate-key").save();
      database.newDocument("ParallelDuplicate").set("lookupKey", "duplicate-key").save();
    });

    assertThatThrownBy(() -> build("ParallelDuplicate", spillParent, 4))
        .hasRootCauseInstanceOf(DuplicatedKeyException.class);
    assertThat(type.getIndexByProperties("lookupKey")).isNull();
    assertNoBuildArtifacts(spillParent);
    assertNoBuildArtifacts(Path.of(getDatabasePath()));

    reopenDatabase();
    assertThat(database.getSchema().getType("ParallelDuplicate").getIndexByProperties("lookupKey")).isNull();
  }

  @Test
  void requestedParallelismFallsBackForOneBucket() throws Exception {
    createRecords("ParallelOneBucket", 1, 1_000);
    TypeIndex index = build("ParallelOneBucket", null, 4);
    assertThat(count(index.iterator(true))).isEqualTo(1_000);

    reopenDatabase();
    index = database.getSchema().getType("ParallelOneBucket").getIndexByProperties("lookupKey");
    assertThat(count(index.iterator(false))).isEqualTo(1_000);
  }

  @Test
  void cancelsPeerWritersAndPublishesNothingAfterWorkerFailure() throws Exception {
    final int admitted = LSMTreeIndexBulkLoader.selectWriterParallelism(4, 4, 64L << 20,
        Runtime.getRuntime().availableProcessors(), LSMTreeIndexExternalSorter.getAvailableFileDescriptors());
    assumeTrue(admitted > 1, "test host does not admit multiple bucket writers");

    final Path spillParent = Path.of(getDatabasePath()).resolve("parallel-writer-failure");
    final DocumentType type = createRecords("ParallelWriterFailure", 4, 4_000);
    final AtomicInteger completed = new AtomicInteger();
    LSMTreeIndexBulkLoader.setBuildTestHook((phase, index, batches) -> {
      if (phase == LSMTreeIndexBulkLoader.BuildPhase.AFTER_BUCKET_APPEND && completed.incrementAndGet() == 10)
        throw new IllegalStateException("injected bucket writer failure");
    });
    try {
      assertThatThrownBy(() -> build("ParallelWriterFailure", spillParent, 4))
          .isInstanceOf(IndexException.class)
          .hasStackTraceContaining("injected bucket writer failure");
    } finally {
      LSMTreeIndexBulkLoader.setBuildTestHook(null);
    }

    assertThat(type.getIndexByProperties("lookupKey")).isNull();
    assertNoBuildArtifacts(spillParent);
    assertNoBuildArtifacts(Path.of(getDatabasePath()));
    reopenDatabase();
    assertThat(database.getSchema().getType("ParallelWriterFailure").getIndexByProperties("lookupKey")).isNull();
  }

  private DocumentType createRecords(final String typeName, final int buckets, final int records) {
    final DocumentType type = database.getSchema().buildDocumentType().withName(typeName)
        .withTotalBuckets(buckets).create();
    type.createProperty("lookupKey", Type.STRING);
    database.transaction(() -> {
      for (int i = 0; i < records; i++)
        database.newDocument(typeName).set("lookupKey", key(i)).save();
    });
    return type;
  }

  private TypeIndex build(final String typeName, final Path spillParent, final int parallelism) {
    return build(typeName, spillParent, parallelism, true, 0);
  }

  private TypeIndex build(final String typeName, final Path spillParent, final int parallelism,
      final boolean unique, final int pageSize) {
    final var builder = database.getSchema().buildTypeIndex(typeName, new String[] { "lookupKey" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withBuildMemoryBudget(64L << 20)
        .withBuildMergeFanIn(2)
        .withBuildParallelism(parallelism);
    builder.withUnique(unique);
    if (pageSize > 0)
      builder.withPageSize(pageSize);
    if (spillParent != null)
      builder.withBuildSpillDirectory(spillParent);
    return builder.create();
  }

  private static long count(final IndexCursor cursor) {
    long count = 0L;
    try {
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      return count;
    } finally {
      cursor.close();
    }
  }

  private static void assertNoBuildArtifacts(final Path parent) throws Exception {
    if (!Files.exists(parent))
      return;
    try (Stream<Path> paths = Files.walk(parent)) {
      assertThat(paths.map(path -> path.getFileName().toString())
          .filter(name -> name.startsWith(".arcadedb-index-sort-")
              || name.startsWith(".arcadedb-sorted-index-build-")
              || name.endsWith(".temp_numtidx") || name.endsWith(".temp_nuctidx"))
          .toList()).isEmpty();
    }
  }

  private static String key(final int value) {
    return "key-%08d-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".formatted(value);
  }
}
