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
package com.arcadedb.query.opencypher.query;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.opencypher.optimizer.plan.PhysicalPlan;
import com.arcadedb.utility.MostUsedCache;

import java.util.Collections;
import java.util.Map;

/**
 * Frequency-based cache for OpenCypher execution plans. Caches optimized physical plans to avoid
 * expensive query optimization (statistics collection, cost estimation, plan selection) on every execution.
 * <p>
 * Uses {@link MostUsedCache} which keeps the 75% most frequently accessed plans when eviction occurs.
 * This is ideal for query plans since frequently-executed queries benefit most from caching, while
 * one-off queries naturally get evicted.
 * <p>
 * Caching plans provides dramatic performance improvements:
 * - Avoids statistics collection (100-200ms per query)
 * - Skips optimization rules and cost calculations (50-200ms per query)
 * - Eliminates plan building overhead (50-100ms per query)
 * <p>
 * Total savings: 200-500ms per cached query execution.
 * <p>
 * {@link #put} and {@link #invalidate()} synchronize on {@code this}, so the {@code lastInvalidation} check and the
 * insert into {@code cache} happen atomically with respect to a concurrent invalidation (issue #6671). {@link #get}
 * does not need that lock - it never reads {@link #lastInvalidation} - and instead relies on {@code cache} being a
 * {@link Collections#synchronizedMap} for its own thread safety, so the frequent read path stays uncontended by the
 * much rarer {@code put}/{@code invalidate}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CypherPlanCache {
  private final DatabaseInternal database;
  private final Map<String, PhysicalPlan> cache;
  private long lastInvalidation = -1;

  /**
   * Creates a new plan cache.
   *
   * @param database the database instance
   * @param size     maximum number of plans to cache (frequency-based eviction when exceeded)
   */
  public CypherPlanCache(final DatabaseInternal database, final int size) {
    this.database = database;
    // Use MostUsedCache wrapped in synchronizedMap for thread-safety
    // MostUsedCache is better than LRU for plans since frequently-used queries should stay cached
    this.cache = Collections.synchronizedMap(new MostUsedCache<>(size));
  }

  /**
   * Gets a cached physical plan for a query.
   *
   * @param query the OpenCypher query string (used as cache key)
   * @return the cached PhysicalPlan, or null if not in cache
   */
  public PhysicalPlan get(final String query) {
    return cache.get(query);
  }

  /**
   * Puts a physical plan into the cache, unless a DDL has invalidated the cache since {@code planningStart} - the
   * moment the caller began building this plan. Without this guard a plan built against a schema that a concurrent
   * {@code DROP INDEX}/{@code ALTER TYPE}/create-index already made stale could be cached right after the
   * invalidation that was supposed to keep it out, leaving future executions running against a dropped/renamed
   * bucket or index (or missing a new one) until the next invalidation happens to clear it (issue #6671). The check
   * and the insert run under the same lock as {@link #invalidate()}, so a concurrent invalidation is guaranteed to
   * either be observed here (the put is skipped) or to run after this put returns (and clear it right back out).
   *
   * @param query         the OpenCypher query string (cache key)
   * @param plan          the optimized PhysicalPlan to cache
   * @param planningStart the timestamp (as returned by {@link System#currentTimeMillis()}) taken before planning
   *                      began
   */
  public synchronized void put(final String query, final PhysicalPlan plan, final long planningStart) {
    if (lastInvalidation >= planningStart)
      return;
    cache.put(query, plan);
  }

  /**
   * Checks if a plan is in the cache.
   *
   * @param query the query string
   * @return true if a plan for this query is cached
   */
  public boolean contains(final String query) {
    return cache.containsKey(query);
  }

  /**
   * Invalidates all cached plans.
   * Should be called when schema changes make cached plans invalid (e.g., index additions/deletions,
   * type definitions changes).
   */
  public void invalidate() {
    synchronized (this) {
      cache.clear();
      lastInvalidation = System.currentTimeMillis();
    }
  }

  /**
   * Gets the timestamp of the last cache invalidation.
   *
   * @return timestamp in milliseconds, or -1 if never invalidated
   */
  public long getLastInvalidation() {
    synchronized (this) {
      return lastInvalidation;
    }
  }

  /**
   * Returns the current cache size.
   *
   * @return number of cached plans
   */
  public int size() {
    return cache.size();
  }

  /**
   * Clears the cache (alias for invalidate).
   */
  public void clear() {
    invalidate();
  }
}
