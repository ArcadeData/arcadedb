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
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.InternalExecutionPlan;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class is an LRU cache for already prepared SQL execution plans. It stores itself in the storage as a resource. It also acts
 * an an entry point for the SQL executor.
 * <p>
 * Every accessor synchronizes on {@code this}, deliberately trading the finer-grained locking a previous version had
 * (a separate lock for the map versus {@link #lastInvalidation}) for the atomicity {@link #put} needs to check-and-
 * insert without a race against a concurrent {@link #invalidate()} (issue #6671). {@link #get} - called on every
 * cached query execution - now contends with the much rarer {@link #put}/{@link #invalidate}, rather than with
 * nothing; deliberate, since correctness here outweighs the lock-contention cost on a path this infrequent relative
 * to query execution itself.
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class ExecutionPlanCache {
  private final DatabaseInternal                   db;
  private final Map<String, InternalExecutionPlan> map;
  private final int                                mapSize;
  protected     long                               lastInvalidation = -1;

  /**
   * @param size the size of the cache
   */
  public ExecutionPlanCache(final DatabaseInternal db, final int size) {
    this.db = db;
    this.mapSize = size;
    this.map = new LinkedHashMap<>(size) {
      protected boolean removeEldestEntry(final Map.Entry<String, InternalExecutionPlan> eldest) {
        return super.size() > mapSize;
      }
    };
  }

  public synchronized long getLastInvalidation() {
    return lastInvalidation;
  }

  /**
   * @param statement an SQL statement
   *
   * @return true if the corresponding executor is present in the cache
   */
  public synchronized boolean contains(final String statement) {
    return map.containsKey(statement);
  }

  /**
   * returns an already prepared SQL execution plan, taking it from the cache if it exists or creating a new one if it doesn't
   *
   * @param statement the SQL statement
   * @param context
   *
   * @return a statement executor from the cache
   */
  public synchronized ExecutionPlan get(final String statement, final CommandContext context) {
    //LRU
    InternalExecutionPlan result = map.remove(statement);
    if (result != null) {
      map.put(statement, result);
      result = result.copy(context);
    }

    return result;
  }

  /**
   * Stores {@code plan} in the cache, unless a DDL has invalidated the cache since {@code planningStart} - the moment
   * the caller began building this plan. Checking {@code planningStart} against {@link #lastInvalidation} and
   * inserting into the map happen under the same lock this class uses for {@link #invalidate()}, so a DDL that
   * invalidates the cache concurrently with this call is guaranteed to either be observed here (the put is skipped)
   * or to run after this put returns (and clear it right back out) - it can never land in the gap between the check
   * and the insert the way two separately-locked calls could (issue #6671).
   *
   * @param statement     the SQL statement, used as the cache key
   * @param plan          the execution plan to cache
   * @param planningStart the timestamp (as returned by {@link System#currentTimeMillis()}) taken before planning
   *                      began; the plan is discarded instead of cached if a concurrent DDL invalidated the cache
   *                      at or after that moment, since the plan may have been built against stale schema/index state
   */
  public synchronized void put(final String statement, final ExecutionPlan plan, final long planningStart) {
    if (lastInvalidation >= planningStart)
      // a DDL invalidated the cache after planning started: the plan may reference a dropped/renamed bucket or
      // index (or be missing a new one), so it must not be cached
      return;
    InternalExecutionPlan internal = (InternalExecutionPlan) plan;
    internal = internal.copy(null);
    map.put(statement, internal);
  }

  public synchronized void invalidate() {
    map.clear();
    lastInvalidation = System.currentTimeMillis();
  }

  public static ExecutionPlanCache instance(final DatabaseInternal db) {
    if (db == null)
      throw new IllegalArgumentException("DB cannot be null");

    return db.getExecutionPlanCache();
  }

}
