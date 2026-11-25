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
package com.arcadedb.function.map;

import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * map.clean(map, keysToRemove, valuesToRemove) - Remove the listed keys, the null values and the listed values from a map.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class MapClean extends AbstractMapFunction {
  @Override
  protected String getSimpleName() {
    return "clean";
  }

  @Override
  public int getMinArgs() {
    return 3;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Return a new map without the specified keys, the null values and the entries with specified values";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Map<String, Object> map = asMap(args[0]);

    // A list stored as ARRAY_OF_LONGS / ARRAY_OF_DOUBLES is a primitive array, and is the same LIST to every list function (issue #11582)
    final List<Object> keysToRemove = CypherFunctionHelper.asListOrNull(args[1]);
    if (keysToRemove == null) {
      throw new IllegalArgumentException("map.clean() second argument must be a list of keys to remove");
    }
    final List<Object> valuesToRemove = CypherFunctionHelper.asListOrNull(args[2]);
    if (valuesToRemove == null) {
      throw new IllegalArgumentException("map.clean() third argument must be a list of values to remove");
    }

    if (map == null)
      return new LinkedHashMap<>();

    // Build sets for efficient lookup
    final Set<String> keysSet = new HashSet<>();
    for (final Object key : keysToRemove) {
      if (key != null)
        keysSet.add(key.toString());
    }

    final Set<Object> valuesSet = new HashSet<>(valuesToRemove);
    // The string form of a value can only match a listed String: skip rendering it (one allocation per entry, a full render for
    // a nested list or map) when no String is listed
    boolean matchStringForm = false;
    for (final Object value : valuesSet)
      if (value instanceof String) {
        matchStringForm = true;
        break;
      }

    // As in APOC, an entry goes when its key is listed, its value is null, or its value or its value's string form is listed
    final Map<String, Object> result = new LinkedHashMap<>();
    for (final Map.Entry<String, Object> entry : map.entrySet()) {
      final Object value = entry.getValue();
      if (!keysSet.contains(entry.getKey()) && value != null && !valuesSet.contains(value) && !(matchStringForm && valuesSet.contains(
          value.toString())))
        result.put(entry.getKey(), value);
    }

    return result;
  }
}
