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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds all metrics and counters for LSMVectorIndex operations.
 * Provides thread-safe tracking of:
 * - Operation counts (search, insert, rebuild, compaction)
 * - Cache statistics (hits, misses)
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

  // Cache statistics
  private final AtomicLong vectorCacheHits = new AtomicLong(0);
  private final AtomicLong vectorCacheMisses = new AtomicLong(0);

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

  void incrementGraphRebuildCount() {
    graphRebuildCount.incrementAndGet();
  }

  void incrementCompactionCount() {
    compactionCount.incrementAndGet();
  }

  // Cache tracking methods

  void incrementVectorCacheHits() {
    vectorCacheHits.incrementAndGet();
  }

  void incrementVectorCacheMisses() {
    vectorCacheMisses.incrementAndGet();
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

  long getVectorCacheHits() {
    return vectorCacheHits.get();
  }

  long getVectorCacheMisses() {
    return vectorCacheMisses.get();
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
    stats.put("compactionCount", compactionCount.get());

    stats.put("vectorCacheHits", vectorCacheHits.get());
    stats.put("vectorCacheMisses", vectorCacheMisses.get());

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
    compactionCount.set(0);
    vectorCacheHits.set(0);
    vectorCacheMisses.set(0);
    vectorFetchFromQuantized.set(0);
    vectorFetchFromDocuments.set(0);
    vectorFetchFromGraph.set(0);
    searchLatencyMs.set(0);
    insertLatencyMs.set(0);
  }
}
