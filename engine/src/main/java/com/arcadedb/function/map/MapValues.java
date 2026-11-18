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
 * map.values(map, keys) - Get values for specified keys in order.
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
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Get values for the specified keys in order";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Map<String, Object> map = asMap(args[0]);

    // A list stored as ARRAY_OF_LONGS / ARRAY_OF_DOUBLES is a primitive array, and is the same LIST to every list function (issue #11582)
    final List<Object> keys = CypherFunctionHelper.asListOrNull(args[1]);
    if (keys == null) {
      throw new IllegalArgumentException("map.values() second argument must be a list of keys");
    }
    final List<Object> result = new ArrayList<>(keys.size());

    if (map == null) {
      // Return list of nulls if map is null
      for (int i = 0; i < keys.size(); i++) {
        result.add(null);
      }
      return result;
    }

    for (final Object key : keys) {
      if (key != null) {
        result.add(map.get(key.toString()));
      } else {
        result.add(null);
      }
    }

    return result;
  }
}
