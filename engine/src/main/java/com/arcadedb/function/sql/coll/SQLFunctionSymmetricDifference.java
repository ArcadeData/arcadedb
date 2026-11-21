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
package com.arcadedb.function.sql.coll;

import com.arcadedb.database.Identifiable;
import com.arcadedb.function.DistinctValueSet;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.function.sql.SQLAggregatedCollectionFunction;

import java.util.Collection;
import java.util.Set;

/**
 * This operator can work as aggregate or inline. If only one argument is passed than aggregates, otherwise executes, and returns,
 * the SYMMETRIC DIFFERENCE between the collections received as parameters. Works also with no collection values.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionSymmetricDifference extends SQLAggregatedCollectionFunction<Set<Object>> {
  public static final String NAME = "symmetricDifference";

  private Set<Object> rejected;

  public SQLFunctionSymmetricDifference() {
    super(NAME);
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  private static void addItemToResult(final Object o, final Set<Object> accepted, final Set<Object> rejected) {
    if (!accepted.contains(o) && !rejected.contains(o)) {
      accepted.add(o);
    } else {
      accepted.remove(o);
      rejected.add(o);
    }
  }

  private static void addItemsToResult(final Collection<Object> co, final Set<Object> accepted, final Set<Object> rejected) {
    for (final Object o : co) {
      addItemToResult(o, accepted, rejected);
    }
  }

  @SuppressWarnings("unchecked")
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext ctx) {
    if (params[0] == null)
      return null;

    // a list stored as ARRAY_OF_LONGS / ARRAY_OF_DOUBLES is a primitive array: it is the list of its items, not one item (issue
    // #12896). A byte[] is a BINARY value and stays one item
    final Object value = asItems(params[0]);

    if (params.length == 1) {
      // AGGREGATION MODE (STATEFUL)
      if (context == null) {
        context = new DistinctValueSet();
        rejected = new DistinctValueSet();
      }
      if (value instanceof Collection<?>) {
        addItemsToResult((Collection<Object>) value, context, rejected);
      } else {
        addItemToResult(value, context, rejected);
      }

      return null;
    } else {
      // IN-LINE MODE (STATELESS)
      final Set<Object> result = new DistinctValueSet();
      final Set<Object> rejected = new DistinctValueSet();

      for (final Object param : params) {
        final Object iParameter = asItems(param);
        if (iParameter instanceof Collection<?>) {
          addItemsToResult((Collection<Object>) iParameter, result, rejected);
        } else {
          addItemToResult(iParameter, result, rejected);
        }
      }

      return result;
    }
  }

  private static Object asItems(final Object value) {
    return MultiValue.isSequenceArray(value) ? MultiValue.getMultiValueAsList(value) : value;
  }

  public String getSyntax() {
    return "difference(<field>*)";
  }

  /** The values seen once so far and the ones seen more than once: both sets grow with the input (issue #9793). */
  @Override
  public long getHeldValues() {
    return context == null ? 0L : context.size() + rejected.size();
  }
}
