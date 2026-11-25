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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * map.values(map, keys = [], addNullsForMissing = false) - Get the values for the keys in order, as APOC does: a key the
 * map does not hold is skipped unless addNullsForMissing is true, which adds null for it.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class MapValues extends AbstractMapFunction {
  @Override
  protected String getSimpleName() {
    return "values";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Get the values for the specified keys in order, skipping missing keys unless the third argument is true";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Map<String, Object> map = asMap(args[0]);
    final Object keysArg = args.length > 1 ? args[1] : null;
    if (keysArg == null)
      return new ArrayList<>();

    // A list stored as ARRAY_OF_LONGS / ARRAY_OF_DOUBLES is a primitive array, and is the same LIST to every list function (issue #11582)
    final List<Object> keys = CypherFunctionHelper.asListOrNull(keysArg);
    if (keys == null)
      throw new IllegalArgumentException("map.values() second argument must be a list of keys");

    final Object addNulls = args.length > 2 ? args[2] : null;
    if (addNulls != null && !(addNulls instanceof Boolean))
      throw new IllegalArgumentException("map.values() third argument must be a boolean");

    final boolean addNullsForMissing = Boolean.TRUE.equals(addNulls);
    final List<Object> result = new ArrayList<>(keys.size());
    for (final Object key : keys) {
      final String keyStr = key != null ? key.toString() : null;
      if (keyStr != null && map != null && map.containsKey(keyStr))
        result.add(map.get(keyStr));
      else if (addNullsForMissing)
        result.add(null);
    }

    return result;
  }
}
