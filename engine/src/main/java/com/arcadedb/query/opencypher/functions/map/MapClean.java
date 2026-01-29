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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * map.clean(map, keysToRemove, valuesToRemove) - Remove keys and values from map.
 *
 * @author ArcadeDB Team
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
    return "Return a new map with specified keys removed and entries with specified values removed";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Map<String, Object> map = asMap(args[0]);

    if (!(args[1] instanceof List)) {
      throw new IllegalArgumentException("map.clean() second argument must be a list of keys to remove");
    }
    if (!(args[2] instanceof List)) {
      throw new IllegalArgumentException("map.clean() third argument must be a list of values to remove");
    }

    final List<?> keysToRemove = (List<?>) args[1];
    final List<?> valuesToRemove = (List<?>) args[2];

    if (map == null)
      return new HashMap<>();

    // Build sets for efficient lookup
    final Set<String> keysSet = new HashSet<>();
    for (final Object key : keysToRemove) {
      if (key != null)
        keysSet.add(key.toString());
    }

    final Set<Object> valuesSet = new HashSet<>(valuesToRemove);

    // Build result, excluding specified keys and values
    final Map<String, Object> result = new HashMap<>();
    for (final Map.Entry<String, Object> entry : map.entrySet()) {
      if (!keysSet.contains(entry.getKey()) && !valuesSet.contains(entry.getValue())) {
        result.put(entry.getKey(), entry.getValue());
      }
    }

    return result;
  }
}
