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
package com.arcadedb.query.opencypher.optimizer.statistics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Database-scoped cache for {@link StatisticsProvider}'s edge-multiplicity statistics
 * ({@code getMeanEdgesPerConnectedPair}, {@code getAverageDegree}), shared across queries.
 * <p>
 * {@link StatisticsProvider} is instantiated fresh per {@code CypherOptimizer}, so its own caches only
 * survive one query's planning - a hot query pattern re-samples the same edge type on every plan. This
 * cache lives one level up, on the database, so repeated planning of the same edge type reuses the
 * previously sampled (or CSR-exact) value.
 * <p>
 * Each entry is stamped with the edge type's record count at the time it was computed. A read is a hit
 * only if the caller's current count still matches - any insert or delete on that edge type since the
 * entry was cached is treated as a miss, forcing a fresh sample. A count that returns to its original
 * value after a balanced insert+delete is not detected and can serve a stale entry; this is an accepted
 * heuristic-cache tradeoff, since a stale entry degrades plan quality rather than query correctness, and
 * it reuses the O(1) cached bucket counter that already backs {@code count(*)} rather than adding new
 * event-listener plumbing.
 */
public class GraphStatisticsCache {
  private record CachedStat(double value, long generation) {
  }

  private final Map<String, CachedStat> meanEdgesPerConnectedPairCache = new ConcurrentHashMap<>();
  private final Map<String, CachedStat> averageDegreeCache             = new ConcurrentHashMap<>();

  /**
   * @param edgeType         the edge type name
   * @param currentEdgeCount the edge type's current record count
   * @return the cached mean, or {@code null} if absent or stale (the edge count changed since it was cached)
   */
  public Double getMeanEdgesPerConnectedPair(final String edgeType, final long currentEdgeCount) {
    return getIfValid(meanEdgesPerConnectedPairCache, edgeType, currentEdgeCount);
  }

  public void putMeanEdgesPerConnectedPair(final String edgeType, final double value, final long currentEdgeCount) {
    meanEdgesPerConnectedPairCache.put(edgeType, new CachedStat(value, currentEdgeCount));
  }

  /**
   * @param cacheKey         {@code relationshipType:sourceLabel:targetLabel}, matching {@link StatisticsProvider}'s key
   * @param currentEdgeCount the relationship type's current record count
   * @return the cached average degree, or {@code null} if absent or stale
   */
  public Double getAverageDegree(final String cacheKey, final long currentEdgeCount) {
    return getIfValid(averageDegreeCache, cacheKey, currentEdgeCount);
  }

  public void putAverageDegree(final String cacheKey, final double value, final long currentEdgeCount) {
    averageDegreeCache.put(cacheKey, new CachedStat(value, currentEdgeCount));
  }

  private static Double getIfValid(final Map<String, CachedStat> cache, final String key, final long currentGeneration) {
    final CachedStat entry = cache.get(key);
    return entry != null && entry.generation() == currentGeneration ? entry.value() : null;
  }

  /**
   * Clears all cached statistics. Useful for testing.
   */
  public void clear() {
    meanEdgesPerConnectedPairCache.clear();
    averageDegreeCache.clear();
  }

  public int size() {
    return meanEdgesPerConnectedPairCache.size() + averageDegreeCache.size();
  }
}
