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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandExecutionException;

/**
 * The cap {@link GlobalConfiguration#QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP} puts on the elements a single operation
 * of a query holds in heap at once: the rows a sort or a join buffers, the keys a DISTINCT remembers, the groups an
 * aggregation keeps, the values a collect() gathers. Past it the query fails with a {@link CommandExecutionException}
 * naming the setting, instead of the server running out of memory (issue #8585).
 * <p>
 * The setting is read once, when the operation starts, so the per-element check is a comparison.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class HeapElementsLimit {
  private final long   maxElements;
  private final String elementsName;
  private final String operation;

  private HeapElementsLimit(final long maxElements, final String elementsName, final String operation) {
    this.maxElements = maxElements;
    this.elementsName = elementsName;
    this.operation = operation;
  }

  /**
   * @param context   the command context, whose database may override the global setting (null reads the global one)
   * @param operation what holds the elements, as the error message names it (e.g. "ORDER BY", "Cartesian product")
   */
  public static HeapElementsLimit of(final CommandContext context, final String operation) {
    return of(context, "elements", operation);
  }

  /**
   * @param elementsName what the operation holds, as the error message names it (e.g. "groups")
   */
  public static HeapElementsLimit of(final CommandContext context, final String elementsName, final String operation) {
    final Database database = context == null ? null : context.getDatabase();
    final long maxElements = database == null ?
        GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong() :
        database.getConfiguration().getValueAsLong(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP);
    return new HeapElementsLimit(maxElements, elementsName, operation);
  }

  /**
   * Fails the query when the operation holds more elements than allowed.
   *
   * @param elements the number of elements the operation holds, the one being added included
   */
  public void check(final long elements) {
    if (isExceededBy(elements))
      throw new CommandExecutionException(
          "Limit of allowed " + elementsName + " for in-heap " + operation + " in a single query exceeded (" + maxElements
              + "). You can set " + GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey() + " to increase this limit");
  }

  /**
   * Fails the query when the operation holds more elements than allowed, after {@code release} lets go of what it holds:
   * the query fails, but the heap it took is given back at once rather than when the plan is collected.
   */
  public void check(final long elements, final Runnable release) {
    if (isExceededBy(elements)) {
      release.run();
      check(elements);
    }
  }

  /** Whether holding {@code elements} is past the limit. */
  public boolean isExceededBy(final long elements) {
    return maxElements > 0 && elements > maxElements;
  }

  /** The maximum number of elements, or a non-positive number when there is no limit. */
  public long getMaxElements() {
    return maxElements;
  }
}
