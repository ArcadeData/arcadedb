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
 * map.fromLists(keys, values) - Create map from key and value lists.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class MapFromLists extends AbstractMapFunction {
  @Override
  protected String getSimpleName() {
    return "fromLists";
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Create a map from parallel lists of keys and values";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    // A list stored as ARRAY_OF_LONGS / ARRAY_OF_DOUBLES is a primitive array, and is the same LIST to every list function (issue #11582)
    final List<Object> keys = CypherFunctionHelper.asListOrNull(args[0]);
    if (keys == null) {
      throw new IllegalArgumentException("map.fromLists() first argument must be a list of keys");
    }
    final List<Object> values = CypherFunctionHelper.asListOrNull(args[1]);
    if (values == null) {
      throw new IllegalArgumentException("map.fromLists() second argument must be a list of values");
    }

    final Map<String, Object> result = new LinkedHashMap<>();
    final int size = Math.min(keys.size(), values.size());

    for (int i = 0; i < size; i++) {
      final Object key = keys.get(i);
      if (key != null) {
        result.put(key.toString(), values.get(i));
      }
    }

    return result;
  }
}
