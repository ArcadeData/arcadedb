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
package com.arcadedb.query;

/**
 * The most rows the caller of the next statement reads, for a caller that serializes a window of the answer and stops pulling (the
 * HTTP endpoints, whose response is capped, issue #12016). It is what the caller pushes down in place of a LIMIT it used to append to
 * the statement text: a LIMIT written into the text cannot be told from one the user wrote, so every planner decision that must
 * respect a real LIMIT was taken against the response cap too (a DISTINCT stayed serial, a GROUP BY lost its columnar path, the plan
 * was cached under the rewritten text).
 * <p>
 * The cap changes nothing about the plan. It reaches only the steps that hold rows (the sort of an ORDER BY, the groups of a GROUP
 * BY) or read a source that materializes its rows (a time series scan, an indexed function), and only as an upper bound they stop
 * at, so the plan is the one of the statement without it and is cached and shared with every caller of the same text.
 * <p>
 * It is handed over through the thread, because the query entry points of a database carry nothing but the text and the parameters of
 * the statement. The first SELECT or MATCH statement (or SQL script) a query engine starts on the thread {@link #take() takes} it, so
 * it is never seen by a statement that runs meanwhile - a subquery, a function in any language, a trigger -, which is a part of the
 * one it was set for and not an answer to the caller. A script takes it for its last statement only. Whoever {@link #open opens} the
 * scope opens it only around a command it knows to be a query, and closes it in a {@code finally}, which clears what no engine took
 * (a command that failed to parse) so it cannot reach the next request served by the thread.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public final class ResultRowCapScope implements AutoCloseable {
  private static final ThreadLocal<int[]> PENDING = ThreadLocal.withInitial(() -> new int[1]);
  /** One instance for all: the state is the thread's, so opening a scope allocates nothing. */
  private static final ResultRowCapScope  INSTANCE = new ResultRowCapScope();

  private ResultRowCapScope() {
  }

  /**
   * Sets the cap the next statement started on this thread reads at most.
   *
   * @param rowCap the rows the caller reads at most, {@code 0} or less for no cap
   *
   * @return the scope to close, in a try-with-resources
   */
  public static ResultRowCapScope open(final int rowCap) {
    PENDING.get()[0] = Math.max(rowCap, 0);
    return INSTANCE;
  }

  /**
   * Hands the cap over to the statement starting now, and clears it: a statement it was not set for must not find it. Called by the
   * query engines at the entry of a statement they run for a caller.
   *
   * @return the rows the caller reads at most, or {@code 0} when it set no cap
   */
  public static int take() {
    final int[] pending = PENDING.get();
    final int rowCap = pending[0];
    pending[0] = 0;
    return rowCap;
  }

  /** Clears the cap no statement took. */
  @Override
  public void close() {
    PENDING.get()[0] = 0;
  }
}
