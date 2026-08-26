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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6671: {@code ExecutionPlanCache.getLastInvalidation() < planningStart} used to be checked in a separate
 * lock scope ({@code synchronized(this)}) from the {@code put()} that followed it ({@code synchronized(map)}),
 * leaving a window in which a concurrent {@code invalidate()} (triggered by a DDL such as {@code DROP INDEX}) could
 * run between the check and the insert: the check would see "not yet invalidated", the DDL would invalidate and
 * clear the map, and then the stale plan would be inserted into the freshly-cleared map anyway.
 * <p>
 * This test reproduces the race deterministically (no real threads, hence no flakiness) by driving the two
 * operations - "plan a query" and "invalidate the cache" - in the exact interleaving that used to defeat the
 * check: capture {@code planningStart}, invalidate the cache (standing in for a concurrent DDL), then attempt to
 * cache a plan built at that {@code planningStart}. The single-lock, single-call {@code put(statement, plan,
 * planningStart)} introduced by the fix must refuse to cache it.
 */
class Issue6671ExecutionPlanCacheRaceTest {
  private DatabaseInternal database;

  @BeforeEach
  void setUp() {
    database = (DatabaseInternal) new DatabaseFactory("./target/databases/issue6671-execplan-race").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void planBuiltBeforeAConcurrentInvalidationIsNeverCached() throws Exception {
    database.getSchema().createDocumentType("Race6671");
    final String stm = "SELECT FROM Race6671";

    final ExecutionPlanCache cache = database.getExecutionPlanCache();
    final CommandContext context = new BasicCommandContext().setDatabase(database);

    // put()'s invalidation guard (issue #6671) is millisecond-timestamped: a plan built in the very same
    // millisecond as a preceding invalidation (here, createDocumentType above) is correctly not cached. Sleeping
    // past the millisecond boundary before the warm-up query below keeps this assertion deterministic, exactly
    // like ExecutionPlanCacheTest.cacheInvalidation1 already does for the same reason.
    Thread.sleep(2);

    // Warm the cache once to obtain a real, valid ExecutionPlan instance to fight over.
    database.query("sql", stm).close();
    assertThat(cache.contains(stm)).isTrue();
    final ExecutionPlan plan = cache.get(stm, context);
    assertThat(plan).isNotNull();

    // Planning "began" here, before the concurrent DDL below invalidates the cache - exactly the interleaving
    // that used to slip through the two separately-locked calls.
    final long planningStart = System.currentTimeMillis();

    // Stand in for a concurrent DDL (e.g. DROP INDEX) landing on another thread while planning was in flight.
    cache.invalidate();
    assertThat(cache.contains(stm)).isFalse();

    // The plan was built against planningStart, which is now older than the invalidation: it must be rejected.
    cache.put(stm, plan, planningStart);
    assertThat(cache.contains(stm)).as("a plan built before a concurrent invalidation must never be cached").isFalse();

    // A plan whose planning genuinely started after the invalidation must still be cached normally.
    final long freshPlanningStart = System.currentTimeMillis() + 1;
    cache.put(stm, plan, freshPlanningStart);
    assertThat(cache.contains(stm)).as("a plan planned after the invalidation must be cached").isTrue();
  }
}
