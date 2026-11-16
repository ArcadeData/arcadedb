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
import java.util.List;
import java.util.Map;

/**
 * map.submap(map, keys, [values], [fail]) - Extract a submap containing only specified keys, as APOC's
 * {@code apoc.map.submap}: a key the map lacks takes the non-null value at the same position of {@code values}, and without one it is
 * an error unless {@code fail} (default true) is false, in which case it maps to null (issue #12105).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class MapSubmap extends AbstractMapFunction {
  @Override
  protected String getSimpleName() {
    return "submap";
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getDescription() {
    return "Extract a submap containing only the specified keys";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Map<String, Object> map = asMap(args[0]);

    // A list stored as ARRAY_OF_LONGS / ARRAY_OF_DOUBLES is a primitive array, and is the same LIST to every list function (issue #11582)
    final List<Object> keys = CypherFunctionHelper.asListOrNull(args[1]);
    if (keys == null && args[1] != null) {
      throw new IllegalArgumentException("map.submap() second argument must be a list of keys");
    }

    final Object valuesArgument = args.length > 2 ? args[2] : null;
    final List<Object> values = valuesArgument == null ? List.of() : CypherFunctionHelper.asListOrNull(valuesArgument);
    if (values == null)
      throw CypherFunctionHelper.typeMismatch(getName(), "a LIST of default values", valuesArgument);
    final Object failArgument = args.length > 3 ? args[3] : null;
    if (failArgument != null && !(failArgument instanceof Boolean))
      throw CypherFunctionHelper.typeMismatch(getName(), "a BOOLEAN fail flag", failArgument);
    final boolean fail = failArgument == null || (Boolean) failArgument;

    // A null map or null keys answer null, as in APOC (issue #12105)
    if (map == null || keys == null)
      return null;

    final Map<String, Object> result = new LinkedHashMap<>();
    for (int i = 0; i < keys.size(); i++) {
      final Object key = keys.get(i);
      if (key == null)
        continue;
      final String keyStr = key.toString();
      if (map.containsKey(keyStr))
        result.put(keyStr, map.get(keyStr));
      else if (i < values.size() && values.get(i) != null)
        // A null default is no default, as in APOC: the key still fails when fail is true
        result.put(keyStr, values.get(i));
      else if (fail)
        throw new IllegalArgumentException(
            getName() + "(): key '" + keyStr + "' is not one of the map's keys " + map.keySet() + " and has no default value");
      else
        result.put(keyStr, null);
    }

    return result;
  }
}
