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

import com.arcadedb.serializer.json.JSONObject;

/** Versioned, coarse-grained diagnostics for one successful sorted LSM index build. */
record SortedIndexBuildMetrics(String logicalIndexName, boolean unique, long scannedRecords, long logicalEntries,
    long writtenEntries, int bucketIndexes, long memoryBudgetBytes, int requestedMergeFanIn, int admittedMergeFanIn,
    int requestedWriterParallelism, int admittedWriterParallelism, int maxConcurrentWriters,
    int initialRuns, int finalRuns, int materializedMergeGenerations,
    long initialRunEntries, long initialRunBytes, long materializedMergeEntries, long materializedMergeBytes,
    long totalSpillBytes, long bucketIndexCreationNanos, long sourceScanNanos, long initialRunGenerationNanos,
    long inMemorySortNanos, long materializedMergeNanos, long finalStreamAndWriteNanos, long attachmentNanos,
    long pipelineNanos, long schemaRecordAndPublicationNanos, long cleanupNanos, long totalNanos) {

  static final int    FORMAT_VERSION = 1;
  static final String LOG_MARKER     = "SORTED_INDEX_BUILD_METRICS ";

  SortedIndexBuildMetrics completed(final long schemaRecordAndPublicationNanos, final long cleanupNanos,
      final long totalNanos) {
    return new SortedIndexBuildMetrics(logicalIndexName, unique, scannedRecords, logicalEntries, writtenEntries,
        bucketIndexes, memoryBudgetBytes, requestedMergeFanIn, admittedMergeFanIn,
        requestedWriterParallelism, admittedWriterParallelism, maxConcurrentWriters, initialRuns, finalRuns,
        materializedMergeGenerations, initialRunEntries, initialRunBytes, materializedMergeEntries,
        materializedMergeBytes, totalSpillBytes, bucketIndexCreationNanos, sourceScanNanos,
        initialRunGenerationNanos, inMemorySortNanos, materializedMergeNanos, finalStreamAndWriteNanos,
        attachmentNanos, pipelineNanos, schemaRecordAndPublicationNanos, cleanupNanos, totalNanos);
  }

  JSONObject toJSON() {
    final JSONObject counts = new JSONObject()
        .put("scanned_records", scannedRecords)
        .put("logical_entries", logicalEntries)
        .put("written_entries", writtenEntries)
        .put("bucket_indexes", bucketIndexes);

    final JSONObject resources = new JSONObject()
        .put("memory_budget_bytes", memoryBudgetBytes)
        .put("requested_merge_fan_in", requestedMergeFanIn)
        .put("admitted_merge_fan_in", admittedMergeFanIn)
        .put("requested_writer_parallelism", requestedWriterParallelism)
        .put("admitted_writer_parallelism", admittedWriterParallelism)
        .put("max_concurrent_writers", maxConcurrentWriters);

    final JSONObject externalSort = new JSONObject()
        .put("enabled", initialRuns > 0)
        .put("initial_runs", initialRuns)
        .put("final_runs", finalRuns)
        .put("materialized_merge_generations", materializedMergeGenerations)
        .put("initial_run_entries", initialRunEntries)
        .put("initial_run_bytes", initialRunBytes)
        .put("materialized_merge_entries", materializedMergeEntries)
        .put("materialized_merge_bytes", materializedMergeBytes)
        .put("total_spill_bytes", totalSpillBytes)
        .put("spill_write_amplification", initialRunBytes > 0L ? (double) totalSpillBytes / initialRunBytes : 0D);

    final JSONObject timings = new JSONObject()
        .put("bucket_index_creation_millis", millis(bucketIndexCreationNanos))
        .put("source_scan_and_inline_run_generation_millis", millis(sourceScanNanos))
        .put("initial_run_generation_millis", millis(initialRunGenerationNanos))
        .put("in_memory_sort_millis", millis(inMemorySortNanos))
        .put("materialized_merge_millis", millis(materializedMergeNanos))
        .put("final_stream_and_compacted_write_millis", millis(finalStreamAndWriteNanos))
        .put("attachment_and_flush_millis", millis(attachmentNanos))
        .put("pipeline_millis", millis(pipelineNanos))
        .put("schema_record_and_publication_millis", millis(schemaRecordAndPublicationNanos))
        .put("cleanup_millis", millis(cleanupNanos))
        .put("total_millis", millis(totalNanos));

    return new JSONObject()
        .put("format_version", FORMAT_VERSION)
        .put("logical_index_name", logicalIndexName)
        .put("unique", unique)
        .put("counts", counts)
        .put("resources", resources)
        .put("external_sort", externalSort)
        .put("timings", timings);
  }

  private static double millis(final long nanos) {
    return nanos / 1_000_000D;
  }
}
