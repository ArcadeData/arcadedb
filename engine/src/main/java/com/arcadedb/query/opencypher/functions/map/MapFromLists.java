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
package com.arcadedb.query.opencypher.functions.map;

import com.arcadedb.query.sql.executor.CommandContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * map.fromLists(keys, values) - Create map from key and value lists.
 *
 * @author ArcadeDB Team
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
    if (!(args[0] instanceof List)) {
      throw new IllegalArgumentException("map.fromLists() first argument must be a list of keys");
    }
    if (!(args[1] instanceof List)) {
      throw new IllegalArgumentException("map.fromLists() second argument must be a list of values");
    }

    final List<?> keys = (List<?>) args[0];
    final List<?> values = (List<?>) args[1];

    final Map<String, Object> result = new HashMap<>();
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
