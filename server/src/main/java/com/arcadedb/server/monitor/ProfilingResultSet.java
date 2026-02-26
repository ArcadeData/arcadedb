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
package com.arcadedb.server.monitor;

import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;

import java.util.Optional;
import java.util.logging.Level;

public class ProfilingResultSet implements ResultSet {
  private final ResultSet            delegate;
  private final ServerQueryProfiler  profiler;
  private final String               database;
  private final String               language;
  private final String               queryText;
  private final long                 startNanos;
  private volatile boolean           recorded;

  public ProfilingResultSet(final ResultSet delegate, final ServerQueryProfiler profiler, final String database,
      final String language, final String queryText) {
    this.delegate = delegate;
    this.profiler = profiler;
    this.database = database;
    this.language = language;
    this.queryText = queryText;
    this.startNanos = System.nanoTime();
  }

  @Override
  public boolean hasNext() {
    final boolean hasNext = delegate.hasNext();
    if (!hasNext)
      recordEntry();
    return hasNext;
  }

  @Override
  public Result next() {
    return delegate.next();
  }

  @Override
  public void close() {
    recordEntry();
    delegate.close();
  }

  @Override
  public Optional<ExecutionPlan> getExecutionPlan() {
    return delegate.getExecutionPlan();
  }

  @Override
  public void reset() {
    delegate.reset();
  }

  private void recordEntry() {
    if (recorded)
      return;
    recorded = true;

    final long elapsed = System.nanoTime() - startNanos;

    JSONObject planJson = null;
    try {
      final Optional<ExecutionPlan> plan = delegate.getExecutionPlan();
      if (plan.isPresent())
        planJson = plan.get().toResult().toJSON();
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Could not extract execution plan for profiling", e);
    }

    profiler.recordQuery(database, language, queryText, elapsed, planJson);
  }
}
