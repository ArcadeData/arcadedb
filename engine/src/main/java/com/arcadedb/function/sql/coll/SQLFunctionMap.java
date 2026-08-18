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
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.function.sql.SQLAggregatedCollectionFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * This operator add an entry in a map. The entry is composed by a key and a value.
 * <p>
 * A key must be a STRING and anything else is refused, which is deliberately stricter than the
 * {@code [...].asMap()} method: this function takes key/value pairs the caller wrote out one by one, so a
 * non-string among them is a mistake in the query worth reporting, whereas {@code asMap()} is documented as
 * turning an arbitrary list INTO a map and converts what it is handed. Both used to answer a ClassCastException
 * instead (issue #6389).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionMap extends SQLAggregatedCollectionFunction<Map<String, Object>> {
  public static final String NAME = "map";

  public SQLFunctionMap() {
    super(NAME);
  }

  @SuppressWarnings("unchecked")
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext ctx) {

    if (params.length > 2)
      // IN LINE MODE
      context = new HashMap<>();

    if (params.length == 1) {
      if (params[0] == null)
        return null;

      if (params[0] instanceof Map<?, ?>) {
        if (context == null)
          // AGGREGATION MODE (STATEFUL)
          context = new HashMap<>();

        // INSERT EVERY SINGLE COLLECTION ITEM
        context.putAll((Map<String, Object>) params[0]);
      } else
        throw new IllegalArgumentException("Map function: expected a map or pairs of parameters as key, value");
    } else if (params.length % 2 != 0)
      throw new IllegalArgumentException("Map function: expected a map or pairs of parameters as key, value");
    else
      for (int i = 0; i < params.length; i += 2) {
        // A MAP KEY IS A STRING; ANYTHING ELSE USED TO REACH THIS CAST AND THROW ClassCastException (ISSUE #6389).
        if (!(params[i] instanceof String))
          throw new IllegalArgumentException(
              "Map function: expected a STRING key, but received " + (params[i] == null ?
                  "null" :
                  "a value of type " + params[i].getClass().getSimpleName()));

        final String key = (String) params[i];
        final Object value = params[i + 1];

        if (value != null) {
          if (params.length <= 2 && context == null)
            // AGGREGATION MODE (STATEFUL)
            context = new HashMap<>();

          context.put(key, value);
        }
      }

    return prepareResult(context);
  }

  public String getSyntax() {
    return "map(<map>|[<key>,<value>]*)";
  }

  @Override
  public Map<String, Object> getResult() {
    final Map<String, Object> res = context;
    context = null;
    return prepareResult(res);
  }

  protected Map<String, Object> prepareResult(final Map<String, Object> res) {
    return res;
  }
}
