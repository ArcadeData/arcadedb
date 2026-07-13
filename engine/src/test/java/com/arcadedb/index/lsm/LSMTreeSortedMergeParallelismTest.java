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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class LSMTreeSortedMergeParallelismTest extends TestHelper {
  private static final int RECORDS = 25_000;
  private static final long MEMORY_BUDGET = 24L << 20;

  @Test
  void admitsMergeWorkersAgainstCpuHeapAndFileDescriptors() {
    assertThat(LSMTreeIndexExternalSorter.selectMergeParallelism(8, 2, 128L << 20, 16, 100)).isEqualTo(8);
    assertThat(LSMTreeIndexExternalSorter.selectMergeParallelism(8, 32, 32L << 20, 16, 1_000)).isEqualTo(1);
    assertThat(LSMTreeIndexExternalSorter.selectMergeParallelism(8, 2, 128L << 20, 2, 100)).isEqualTo(1);
    assertThat(LSMTreeIndexExternalSorter.selectMergeParallelism(8, 8, 128L << 20, 16, 27)).isEqualTo(3);
    assertThatThrownBy(() -> LSMTreeIndexExternalSorter.selectMergeParallelism(0, 2, 128L << 20, 16, 100))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> LSMTreeIndexExternalSorter.selectMergeParallelism(2, 1, 128L << 20, 16, 100))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void mergesIndependentGroupsConcurrentlyAndReopens() throws Exception {
    assumeParallelMergeAdmission();
    createUniqueRecords("ParallelMerge", RECORDS);
    final CountDownLatch firstTwoGroups = new CountDownLatch(2);
    final AtomicInteger active = new AtomicInteger();
    final AtomicInteger maxActive = new AtomicInteger();

    LSMTreeIndexExternalSorter.setMaterializedMergeTestHook((generation, group, written) -> {
      if (generation != 0 || group > 1 || written != 1L)
        return;
      final int current = active.incrementAndGet();
      maxActive.accumulateAndGet(current, Math::max);
      firstTwoGroups.countDown();
      try {
        if (!firstTwoGroups.await(10L, TimeUnit.SECONDS))
          throw new IOException("materialized merge groups did not overlap");
      } finally {
        active.decrementAndGet();
      }
    });

    TypeIndex index;
    try {
      index = build("ParallelMerge", true, null);
    } finally {
      LSMTreeIndexExternalSorter.setMaterializedMergeTestHook(null);
    }

    assertThat(maxActive).hasValueGreaterThan(1);
    assertUniqueOrder(index.iterator(true), true);
    assertUniqueOrder(index.iterator(false), false);
    assertThat(count(index.range(true, new Object[] { key(1_000) }, true,
        new Object[] { key(1_099) }, true))).isEqualTo(100);
    assertThat(count(index.get(new Object[] { key(RECORDS / 2) }))).isEqualTo(1);

    reopenDatabase();
    index = database.getSchema().getType("ParallelMerge").getIndexByProperties("lookupKey");
    assertUniqueOrder(index.iterator(true), true);
    assertUniqueOrder(index.iterator(false), false);
    assertThat(count(index.get(new Object[] { key(RECORDS / 2) }))).isEqualTo(1);
    assertNoBuildArtifacts(Path.of(getDatabasePath()));
  }

  @Test
  void preservesNonUniqueGroupsAcrossParallelMergeOutputs() throws Exception {
    assumeParallelMergeAdmission();
    final DocumentType type = database.getSchema().createDocumentType("ParallelMergeNonUnique");
    type.createProperty("lookupKey", Type.STRING);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument("ParallelMergeNonUnique").set("lookupKey", "shared-" + i % 5).save();
    });

    TypeIndex index = build("ParallelMergeNonUnique", false, null);
    for (int i = 0; i < 5; i++)
      assertThat(count(index.get(new Object[] { "shared-" + i }))).isEqualTo(RECORDS / 5);
    assertThat(count(index.iterator(true))).isEqualTo(RECORDS);

    reopenDatabase();
    index = database.getSchema().getType("ParallelMergeNonUnique").getIndexByProperties("lookupKey");
    assertThat(count(index.iterator(false))).isEqualTo(RECORDS);
    assertThat(count(index.get(new Object[] { "shared-3" }))).isEqualTo(RECORDS / 5);
  }

  @Test
  void retainsFinalGlobalDuplicateBoundaryAfterParallelMerges() throws Exception {
    assumeParallelMergeAdmission();
    final Path spillParent = Path.of(getDatabasePath()).resolve("parallel-merge-duplicate");
    final DocumentType type = createUniqueRecords("ParallelMergeDuplicate", RECORDS);
    database.transaction(() -> {
      database.newDocument("ParallelMergeDuplicate").set("lookupKey", "duplicate-key").save();
      database.newDocument("ParallelMergeDuplicate").set("lookupKey", "duplicate-key").save();
    });

    assertThatThrownBy(() -> build("ParallelMergeDuplicate", true, spillParent))
        .hasRootCauseInstanceOf(DuplicatedKeyException.class);
    assertThat(type.getIndexByProperties("lookupKey")).isNull();
    assertNoBuildArtifacts(spillParent);
    assertNoBuildArtifacts(Path.of(getDatabasePath()));
  }

  @Test
  void cancelsPeerMergesAndPublishesNothingAfterWorkerFailure() throws Exception {
    assumeParallelMergeAdmission();
    final Path spillParent = Path.of(getDatabasePath()).resolve("parallel-merge-failure");
    final DocumentType type = createUniqueRecords("ParallelMergeFailure", RECORDS);
    final CountDownLatch peerStarted = new CountDownLatch(1);
    final CountDownLatch neverRelease = new CountDownLatch(1);
    final AtomicBoolean peerInterrupted = new AtomicBoolean();

    LSMTreeIndexExternalSorter.setMaterializedMergeTestHook((generation, group, written) -> {
      if (generation != 0 || written != 1L)
        return;
      if (group == 0) {
        if (!peerStarted.await(10L, TimeUnit.SECONDS))
          throw new IOException("peer merge did not start");
        throw new IOException("injected parallel merge failure");
      }
      if (group == 1) {
        peerStarted.countDown();
        try {
          neverRelease.await();
        } catch (final InterruptedException error) {
          peerInterrupted.set(true);
          throw error;
        }
      }
    });

    try {
      assertThatThrownBy(() -> build("ParallelMergeFailure", false, spillParent))
          .isInstanceOf(IndexException.class)
          .hasStackTraceContaining("injected parallel merge failure");
    } finally {
      LSMTreeIndexExternalSorter.setMaterializedMergeTestHook(null);
    }

    assertThat(peerInterrupted).isTrue();
    assertThat(type.getIndexByProperties("lookupKey")).isNull();
    assertNoBuildArtifacts(spillParent);
    assertNoBuildArtifacts(Path.of(getDatabasePath()));
    reopenDatabase();
    assertThat(database.getSchema().getType("ParallelMergeFailure").getIndexByProperties("lookupKey")).isNull();
  }

  private void assumeParallelMergeAdmission() {
    final int admitted = LSMTreeIndexExternalSorter.selectMergeParallelism(4, 2, MEMORY_BUDGET,
        Runtime.getRuntime().availableProcessors(), LSMTreeIndexExternalSorter.getAvailableFileDescriptors());
    assumeTrue(admitted > 1, "test host does not admit multiple merge workers");
  }

  private DocumentType createUniqueRecords(final String typeName, final int records) {
    final DocumentType type = database.getSchema().createDocumentType(typeName);
    type.createProperty("lookupKey", Type.STRING);
    database.transaction(() -> {
      for (int i = 0; i < records; i++) {
        final int permuted = Math.floorMod(i * 12_371, records);
        database.newDocument(typeName).set("lookupKey", key(permuted)).save();
      }
    });
    return type;
  }

  private TypeIndex build(final String typeName, final boolean unique, final Path spillParent) {
    final var builder = database.getSchema().buildTypeIndex(typeName, new String[] { "lookupKey" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED)
        .withBuildMemoryBudget(MEMORY_BUDGET)
        .withBuildMergeFanIn(2)
        .withBuildParallelism(4);
    builder.withUnique(unique);
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

  private static void assertUniqueOrder(final IndexCursor cursor, final boolean ascending) {
    int position = 0;
    try {
      while (cursor.hasNext()) {
        cursor.next();
        final int expected = ascending ? position : RECORDS - position - 1;
        assertThat(cursor.getKeys()[0]).isEqualTo(key(expected));
        position++;
      }
    } finally {
      cursor.close();
    }
    assertThat(position).isEqualTo(RECORDS);
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
