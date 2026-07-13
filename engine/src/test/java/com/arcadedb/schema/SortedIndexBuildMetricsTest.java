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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class SortedIndexBuildMetricsTest extends TestHelper {
  @Test
  void capturesExternalRunsMaterializedMergesAndPublication() {
    final AtomicReference<SortedIndexBuildMetrics> captured = new AtomicReference<>();
    TypeIndexBuilder.setSortedBuildMetricsTestHook(captured::set);
    try {
      final DocumentType type = database.getSchema().buildDocumentType().withName("MetricsExternal")
          .withTotalBuckets(3).create();
      type.createProperty("lookupKey", Type.STRING);
      database.transaction(() -> {
        for (int i = 0; i < 2_000; i++) {
          final int permuted = Math.floorMod(i * 1_237, 2_000);
          database.newDocument("MetricsExternal").set("lookupKey", "key-%08d".formatted(permuted)).save();
        }
      });

      final TypeIndex index = database.getSchema().buildTypeIndex("MetricsExternal", new String[] { "lookupKey" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withBuildMode(IndexBuildMode.SORTED)
          .withBuildMemoryBudget(1L << 20)
          .withBuildMergeFanIn(2)
          .withUnique(true)
          .create();
      assertThat(count(index.iterator(true))).isEqualTo(2_000);
    } finally {
      TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
    }

    final SortedIndexBuildMetrics metrics = captured.get();
    assertThat(metrics).isNotNull();
    assertThat(metrics.unique()).isTrue();
    assertThat(metrics.scannedRecords()).isEqualTo(2_000);
    assertThat(metrics.logicalEntries()).isEqualTo(2_000);
    assertThat(metrics.writtenEntries()).isEqualTo(2_000);
    assertThat(metrics.bucketIndexes()).isEqualTo(3);
    assertThat(metrics.memoryBudgetBytes()).isEqualTo(1L << 20);
    assertThat(metrics.requestedMergeFanIn()).isEqualTo(2);
    assertThat(metrics.admittedMergeFanIn()).isEqualTo(2);
    assertThat(metrics.requestedBuildParallelism()).isEqualTo(1);
    assertThat(metrics.admittedMergeParallelism()).isEqualTo(1);
    assertThat(metrics.maxConcurrentMerges()).isEqualTo(1);
    assertThat(metrics.admittedWriterParallelism()).isEqualTo(1);
    assertThat(metrics.maxConcurrentWriters()).isEqualTo(1);
    assertThat(metrics.initialRuns()).isGreaterThan(2);
    assertThat(metrics.finalRuns()).isBetween(1, 2);
    assertThat(metrics.materializedMergeGenerations()).isGreaterThan(0);
    assertThat(metrics.initialRunEntries()).isEqualTo(2_000);
    assertThat(metrics.initialRunBytes()).isGreaterThan(0);
    assertThat(metrics.materializedMergeEntries()).isGreaterThan(0);
    assertThat(metrics.materializedMergeBytes()).isGreaterThan(0);
    assertThat(metrics.totalSpillBytes()).isGreaterThan(metrics.initialRunBytes());
    assertThat(metrics.bucketIndexCreationNanos()).isGreaterThan(0);
    assertThat(metrics.sourceScanNanos()).isGreaterThan(0);
    assertThat(metrics.initialRunGenerationNanos()).isGreaterThan(0);
    assertThat(metrics.inMemorySortNanos()).isZero();
    assertThat(metrics.materializedMergeNanos()).isGreaterThan(0);
    assertThat(metrics.finalStreamAndWriteNanos()).isGreaterThan(0);
    assertThat(metrics.attachmentNanos()).isGreaterThan(0);
    assertThat(metrics.pipelineNanos()).isGreaterThan(0);
    assertThat(metrics.schemaRecordAndPublicationNanos()).isGreaterThan(0);
    assertThat(metrics.cleanupNanos()).isGreaterThan(0);
    assertThat(metrics.totalNanos()).isGreaterThanOrEqualTo(metrics.pipelineNanos());

    final JSONObject json = metrics.toJSON();
    assertThat(json.getInt("format_version")).isEqualTo(SortedIndexBuildMetrics.FORMAT_VERSION);
    assertThat(json.getJSONObject("resources").getInt("requested_build_parallelism")).isEqualTo(1);
    assertThat(json.getJSONObject("resources").getInt("admitted_merge_parallelism")).isEqualTo(1);
    assertThat(json.getJSONObject("resources").getInt("max_concurrent_merges")).isEqualTo(1);
    assertThat(json.getJSONObject("resources").getInt("admitted_writer_parallelism")).isEqualTo(1);
    assertThat(json.getJSONObject("resources").getInt("max_concurrent_writers")).isEqualTo(1);
    assertThat(json.getJSONObject("external_sort").getBoolean("enabled")).isTrue();
    assertThat(json.getJSONObject("external_sort").getInt("materialized_merge_generations")).isGreaterThan(0);
    assertThat(json.getJSONObject("timings").getDouble("materialized_merge_millis")).isGreaterThan(0D);
    assertThat(json.getJSONObject("timings").getDouble("schema_record_and_publication_millis")).isGreaterThan(0D);
  }

  @Test
  void distinguishesAnInMemoryBuildFromExternalSorting() {
    final AtomicReference<SortedIndexBuildMetrics> captured = new AtomicReference<>();
    TypeIndexBuilder.setSortedBuildMetricsTestHook(captured::set);
    try {
      final DocumentType type = database.getSchema().createDocumentType("MetricsInMemory");
      type.createProperty("lookupKey", Type.INTEGER);
      database.transaction(() -> {
        for (int i = 0; i < 100; i++)
          database.newDocument("MetricsInMemory").set("lookupKey", Math.floorMod(i * 37, 100)).save();
      });

      final TypeIndex index = database.getSchema().buildTypeIndex("MetricsInMemory", new String[] { "lookupKey" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withBuildMode(IndexBuildMode.SORTED)
          .withBuildMemoryBudget(1L << 20)
          .withUnique(true)
          .create();
      assertThat(count(index.iterator(true))).isEqualTo(100);
    } finally {
      TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
    }

    final SortedIndexBuildMetrics metrics = captured.get();
    assertThat(metrics).isNotNull();
    assertThat(metrics.initialRuns()).isZero();
    assertThat(metrics.finalRuns()).isZero();
    assertThat(metrics.materializedMergeGenerations()).isZero();
    assertThat(metrics.initialRunBytes()).isZero();
    assertThat(metrics.materializedMergeBytes()).isZero();
    assertThat(metrics.totalSpillBytes()).isZero();
    assertThat(metrics.initialRunGenerationNanos()).isZero();
    assertThat(metrics.inMemorySortNanos()).isGreaterThan(0);
    assertThat(metrics.materializedMergeNanos()).isZero();
    assertThat(metrics.maxConcurrentMerges()).isZero();
    assertThat(metrics.finalStreamAndWriteNanos()).isGreaterThan(0);
    assertThat(metrics.toJSON().getJSONObject("external_sort").getBoolean("enabled")).isFalse();
  }

  @Test
  void capturesBoundedWriterParallelism() {
    final AtomicReference<SortedIndexBuildMetrics> captured = new AtomicReference<>();
    TypeIndexBuilder.setSortedBuildMetricsTestHook(captured::set);
    try {
      final DocumentType type = database.getSchema().buildDocumentType().withName("MetricsParallel")
          .withTotalBuckets(4).create();
      type.createProperty("lookupKey", Type.STRING);
      database.transaction(() -> {
        for (int i = 0; i < 4_000; i++)
          database.newDocument("MetricsParallel").set("lookupKey", "key-%08d".formatted(i)).save();
      });

      database.getSchema().buildTypeIndex("MetricsParallel", new String[] { "lookupKey" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withBuildMode(IndexBuildMode.SORTED)
          .withBuildMemoryBudget(64L << 20)
          .withBuildParallelism(4)
          .withUnique(true)
          .create();
    } finally {
      TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
    }

    final SortedIndexBuildMetrics metrics = captured.get();
    assertThat(metrics).isNotNull();
    assertThat(metrics.requestedBuildParallelism()).isEqualTo(4);
    assertThat(metrics.admittedWriterParallelism()).isBetween(1, 4);
    assertThat(metrics.maxConcurrentWriters()).isBetween(1, metrics.admittedWriterParallelism());
    assertThat(metrics.toJSON().getJSONObject("resources").getInt("requested_build_parallelism")).isEqualTo(4);
  }

  @Test
  void capturesBoundedMergeParallelism() {
    final AtomicReference<SortedIndexBuildMetrics> captured = new AtomicReference<>();
    TypeIndexBuilder.setSortedBuildMetricsTestHook(captured::set);
    try {
      final DocumentType type = database.getSchema().createDocumentType("MetricsParallelMerge");
      type.createProperty("lookupKey", Type.STRING);
      database.transaction(() -> {
        for (int i = 0; i < 25_000; i++)
          database.newDocument("MetricsParallelMerge").set("lookupKey", "key-%08d".formatted(i)).save();
      });

      database.getSchema().buildTypeIndex("MetricsParallelMerge", new String[] { "lookupKey" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withBuildMode(IndexBuildMode.SORTED)
          .withBuildMemoryBudget(24L << 20)
          .withBuildMergeFanIn(2)
          .withBuildParallelism(4)
          .withUnique(true)
          .create();
    } finally {
      TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
    }

    final SortedIndexBuildMetrics metrics = captured.get();
    assertThat(metrics).isNotNull();
    assertThat(metrics.requestedBuildParallelism()).isEqualTo(4);
    assertThat(metrics.admittedMergeParallelism()).isBetween(1, 2);
    assertThat(metrics.maxConcurrentMerges()).isBetween(1, metrics.admittedMergeParallelism());
    assertThat(metrics.materializedMergeGenerations()).isGreaterThan(0);
    assertThat(metrics.admittedWriterParallelism()).isEqualTo(1);
    final JSONObject resources = metrics.toJSON().getJSONObject("resources");
    assertThat(resources.getInt("requested_build_parallelism")).isEqualTo(4);
    assertThat(resources.getInt("admitted_merge_parallelism")).isEqualTo(metrics.admittedMergeParallelism());
    assertThat(resources.getInt("max_concurrent_merges")).isEqualTo(metrics.maxConcurrentMerges());
  }

  private static long count(final IndexCursor cursor) {
    long total = 0L;
    try {
      while (cursor.hasNext()) {
        cursor.next();
        total++;
      }
      return total;
    } finally {
      cursor.close();
    }
  }
}
