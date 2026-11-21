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

import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The rows of a top-level openCypher statement run by a user whose security groups set a {@code resultSetLimit}, capped at it
 * (issue #12888). The quota caps the rows a statement answers, as SQL applies it as the statement's LIMIT, and never the data the
 * statement reads: it used to cut every bucket scan short, so a WHERE missed records and an aggregate or an ORDER BY ran over part
 * of them. Applied once to the whole statement, so a {@code UNION} answers at most the quota rather than the quota per branch, and
 * a LIMIT of the statement below the quota stays in force on its own.
 * <p>
 * A statement the capped one runs while it executes or while its rows are read - a {@code COUNT}, {@code COLLECT} or
 * {@code EXISTS} subquery, a procedure or a user-defined function that runs a query - reads on the thread that reads the rows,
 * and is a part of the capped statement rather than an answer to the user: {@link #isRunning()} tells the engine not to cap it.
 * A subquery is never evaluated in the workers of a parallel scan, which keep to predicates that read the row alone.
 */
final class ProfiledLimitResultSet implements ResultSet {
  // HOW MANY CAPPED STATEMENTS THIS THREAD IS RUNNING OR READING A ROW OF; A COUNTER, NOT A FLAG, SO A STATEMENT THAT READS THE
  // ROWS OF ANOTHER ONE IT HOLDS DOES NOT CLEAR IT FOR THE OUTER ONE
  private static final ThreadLocal<int[]> RUNNING = ThreadLocal.withInitial(() -> new int[1]);

  private final ResultSet delegate;
  private final long      limit;
  private       long      returned;

  private ProfiledLimitResultSet(final ResultSet delegate, final long limit) {
    this.delegate = delegate;
    this.limit = limit;
  }

  /** Whether this thread is running a capped statement, or reading one of its rows: a statement run now is a part of it. */
  static boolean isRunning() {
    return RUNNING.get()[0] > 0;
  }

  /**
   * Runs the statement and caps the rows it answers at {@code limit}. A write runs to its end inside {@code execution}, so the
   * cap reaches none of what it writes.
   */
  static ResultSet run(final long limit, final Supplier<ResultSet> execution) {
    final int[] running = RUNNING.get();
    ++running[0];
    try {
      return new ProfiledLimitResultSet(execution.get(), limit);
    } finally {
      --running[0];
    }
  }

  @Override
  public boolean hasNext() {
    if (returned >= limit)
      return false;
    final int[] running = RUNNING.get();
    ++running[0];
    try {
      return delegate.hasNext();
    } finally {
      --running[0];
    }
  }

  @Override
  public Result next() {
    if (returned >= limit)
      throw new NoSuchElementException();
    final int[] running = RUNNING.get();
    ++running[0];
    try {
      final Result row = delegate.next();
      ++returned;
      return row;
    } finally {
      --running[0];
    }
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public void reset() {
    delegate.reset();
    returned = 0;
  }

  @Override
  public Optional<ExecutionPlan> getExecutionPlan() {
    return delegate.getExecutionPlan();
  }

  @Override
  public Optional<QueryStatistics> getStatistics() {
    return delegate.getStatistics();
  }

  @Override
  public FacetResults getFacets() {
    return delegate.getFacets();
  }

  @Override
  public int getStatementLimit() {
    return delegate.getStatementLimit();
  }
}
