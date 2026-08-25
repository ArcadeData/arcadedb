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
package com.arcadedb.index.vector;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds all metrics and counters for LSMVectorIndex operations.
 * Provides thread-safe tracking of:
 * - Operation counts (search, insert, rebuild, compaction)
 * - Vector fetch sources (quantized, documents, graph)
 * - Latency tracking (search, insert)
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexMetrics {

  // Operation counters
  private final AtomicLong searchOperations = new AtomicLong(0);
  private final AtomicLong insertOperations = new AtomicLong(0);
  private final AtomicLong graphRebuildCount = new AtomicLong(0);
  private final AtomicLong compactionCount = new AtomicLong(0);
  // Queries the graph search could not fill, which then walked every ordinal. A full scan per query is the most
  // expensive thing this index does, and until now it was only a log line (issue #5558). Counts the plain k-NN path
  // only, which is the only one with a fallback to count: the grouped and PQ paths deliberately have none.
  private final AtomicLong bruteForceScans = new AtomicLong(0);
  // Queries answered by the pre-filter plan (issue #6502, extended to the groupBy and PQ-approximate paths by issue
  // #6514): the RID allow-list was narrow enough that scoring it directly was chosen up front, in place of the HNSW
  // graph walk. Unlike bruteForceScans this is not a fallback - it is the cheaper of the two plans available for a
  // given search path (exact on the plain and groupBy paths, PQ-approximate on the PQ path), picked before any
  // graph traversal ran. Shared across all three paths; a query on any of them can bump this counter.
  private final AtomicLong preFilterSearches = new AtomicLong(0);
  // Grouped searches (vector.neighbors with groupBy) that ran out of candidate budget before they could open `limit`
  // distinct groups, so the caller got a correct but short answer. Finding the limit-th nearest group costs however
  // many candidates the data puts between it and the query, which no fixed budget can guarantee (issue #5761), so
  // this is the signal to raise efSearch on the index or the query.
  private final AtomicLong groupedSearchesShortOfLimit = new AtomicLong(0);
  // Grouped searches that merged at least one row out of the delta buffer into their answer (issue #6501). Before
  // that issue the grouped path skipped the buffer outright, so a groupBy query silently answered from the corpus as
  // of the last graph rebuild while the same query without groupBy returned the newer rows; the merge is what closed
  // that, and this counter is what makes it visible. A number that tracks the query rate means the graph is
  // persistently behind the write rate and every grouped query is paying a linear scan of the buffer for it -
  // deltaVectorsCount says how long that scan is, and a lower vectorIndex.mutationsBeforeRebuild or
  // rebuildGraphRatio is what shortens it.
  private final AtomicLong groupedSearchesMergingDelta = new AtomicLong(0);
  // Times a persisted graph was reused on the strength of its node count alone, because it carries no manifest
  // saying which records it was built over (issue #6106) - a graph written by a version older than the manifest, or
  // one restored from a backup, which does not carry the sidecar. Non-zero means this index is still being judged by
  // the weaker comparison; REBUILD INDEX, or any graph persist, writes a manifest and takes it off that path.
  private final AtomicLong unverifiedGraphReuses = new AtomicLong(0);
  // Online rebuild cycles declined because the estimated peak footprint did not fit the available heap (issue
  // #6503). Deferring is the intended outcome - the alternative is an OutOfMemoryError - but it is not free: the
  // graph stays as stale as the delta buffer is long, so every query pays a longer brute-force scan over it. A
  // number that keeps climbing is the signal to give the JVM more heap, lower graphBuildCacheMaxHeapPercent, or
  // split the index; one that climbed once and stopped is a transient the next trigger already recovered from.
  private final AtomicLong rebuildsDeferredForMemory = new AtomicLong(0);
  // Times a stale persisted graph (more live vectors than it covers, no deletions) was reused as a prefix instead
  // of being discarded for a synchronous full rebuild on the calling search thread (issue #6655). Each one traded
  // a blocking rebuild sized to the whole index for an immediate answer plus a background rebuild; the gap
  // vectors stay searchable meanwhile through the delta buffer they were queued into.
  private final AtomicLong stalePrefixGraphReuses = new AtomicLong(0);

  // Vector fetch source tracking
  private final AtomicLong vectorFetchFromQuantized = new AtomicLong(0);
  private final AtomicLong vectorFetchFromDocuments = new AtomicLong(0);
  private final AtomicLong vectorFetchFromGraph = new AtomicLong(0);

  // Latency tracking (cumulative)
  private final AtomicLong searchLatencyMs = new AtomicLong(0);
  private final AtomicLong insertLatencyMs = new AtomicLong(0);

  // Operation tracking methods

  void incrementSearchOperations() {
    searchOperations.incrementAndGet();
  }

  void incrementInsertOperations() {
    insertOperations.incrementAndGet();
  }

  void incrementInsertOperations(final int count) {
    insertOperations.addAndGet(count);
  }

  void incrementGraphRebuildCount() {
    graphRebuildCount.incrementAndGet();
  }

  void incrementCompactionCount() {
    compactionCount.incrementAndGet();
  }

  void incrementBruteForceScans() {
    bruteForceScans.incrementAndGet();
  }

  void incrementPreFilterSearches() {
    preFilterSearches.incrementAndGet();
  }

  void incrementGroupedSearchesShortOfLimit() {
    groupedSearchesShortOfLimit.incrementAndGet();
  }

  void incrementGroupedSearchesMergingDelta() {
    groupedSearchesMergingDelta.incrementAndGet();
  }

  void incrementUnverifiedGraphReuses() {
    unverifiedGraphReuses.incrementAndGet();
  }

  void incrementRebuildsDeferredForMemory() {
    rebuildsDeferredForMemory.incrementAndGet();
  }

  void incrementStalePrefixGraphReuses() {
    stalePrefixGraphReuses.incrementAndGet();
  }

  // Vector fetch source tracking methods

  void incrementVectorFetchFromQuantized() {
    vectorFetchFromQuantized.incrementAndGet();
  }

  void incrementVectorFetchFromDocuments() {
    vectorFetchFromDocuments.incrementAndGet();
  }

  void incrementVectorFetchFromGraph() {
    vectorFetchFromGraph.incrementAndGet();
  }

  // Latency tracking methods

  void addSearchLatency(final long latencyMs) {
    searchLatencyMs.addAndGet(latencyMs);
  }

  void addInsertLatency(final long latencyMs) {
    insertLatencyMs.addAndGet(latencyMs);
  }

  // Getters for statistics

  long getSearchOperations() {
    return searchOperations.get();
  }

  long getInsertOperations() {
    return insertOperations.get();
  }

  long getGraphRebuildCount() {
    return graphRebuildCount.get();
  }

  long getCompactionCount() {
    return compactionCount.get();
  }

  long getBruteForceScans() {
    return bruteForceScans.get();
  }

  long getPreFilterSearches() {
    return preFilterSearches.get();
  }

  long getGroupedSearchesShortOfLimit() {
    return groupedSearchesShortOfLimit.get();
  }

  long getGroupedSearchesMergingDelta() {
    return groupedSearchesMergingDelta.get();
  }

  long getVectorFetchFromQuantized() {
    return vectorFetchFromQuantized.get();
  }

  long getVectorFetchFromDocuments() {
    return vectorFetchFromDocuments.get();
  }

  long getVectorFetchFromGraph() {
    return vectorFetchFromGraph.get();
  }

  long getSearchLatencyMs() {
    return searchLatencyMs.get();
  }

  long getInsertLatencyMs() {
    return insertLatencyMs.get();
  }

  /**
   * Get average search latency in milliseconds.
   */
  long getAvgSearchLatencyMs() {
    final long ops = searchOperations.get();
    return ops > 0 ? searchLatencyMs.get() / ops : 0L;
  }

  /**
   * Get average insert latency in milliseconds.
   */
  long getAvgInsertLatencyMs() {
    final long ops = insertOperations.get();
    return ops > 0 ? insertLatencyMs.get() / ops : 0L;
  }

  /**
   * Populate a map with all metrics for getStats().
   */
  void populateStats(final Map<String, Long> stats) {
    stats.put("searchOperations", searchOperations.get());
    stats.put("insertOperations", insertOperations.get());
    stats.put("graphRebuildCount", graphRebuildCount.get());
    stats.put("bruteForceScans", bruteForceScans.get());
    stats.put("preFilterSearches", preFilterSearches.get());
    stats.put("groupedSearchesShortOfLimit", groupedSearchesShortOfLimit.get());
    stats.put("groupedSearchesMergingDelta", groupedSearchesMergingDelta.get());
    stats.put("unverifiedGraphReuses", unverifiedGraphReuses.get());
    stats.put("rebuildsDeferredForMemory", rebuildsDeferredForMemory.get());
    stats.put("stalePrefixGraphReuses", stalePrefixGraphReuses.get());
    stats.put("compactionCount", compactionCount.get());

    stats.put("vectorFetchFromQuantized", vectorFetchFromQuantized.get());
    stats.put("vectorFetchFromDocuments", vectorFetchFromDocuments.get());
    stats.put("vectorFetchFromGraph", vectorFetchFromGraph.get());

    stats.put("avgSearchLatencyMs", getAvgSearchLatencyMs());
    stats.put("avgInsertLatencyMs", getAvgInsertLatencyMs());
  }

  /**
   * Reset all metrics to zero.
   */
  void reset() {
    searchOperations.set(0);
    insertOperations.set(0);
    graphRebuildCount.set(0);
    bruteForceScans.set(0);
    preFilterSearches.set(0);
    groupedSearchesShortOfLimit.set(0);
    groupedSearchesMergingDelta.set(0);
    unverifiedGraphReuses.set(0);
    rebuildsDeferredForMemory.set(0);
    stalePrefixGraphReuses.set(0);
    compactionCount.set(0);
    vectorFetchFromQuantized.set(0);
    vectorFetchFromDocuments.set(0);
    vectorFetchFromGraph.set(0);
    searchLatencyMs.set(0);
    insertLatencyMs.set(0);
  }
}
