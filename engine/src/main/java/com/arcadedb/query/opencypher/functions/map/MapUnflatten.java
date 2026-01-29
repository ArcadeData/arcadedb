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
import java.util.Map;
import java.util.regex.Pattern;

/**
 * map.unflatten(map, delimiter) - Unflatten a map with key paths into nested maps.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class MapUnflatten extends AbstractMapFunction {
  @Override
  protected String getSimpleName() {
    return "unflatten";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Unflatten a map with dotted key paths into nested maps";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Map<String, Object> map = asMap(args[0]);
    final String delimiter = args.length > 1 && args[1] != null ? args[1].toString() : ".";

    if (map == null)
      return new HashMap<>();

    final Map<String, Object> result = new HashMap<>();
    final String delimiterRegex = Pattern.quote(delimiter);

    for (final Map.Entry<String, Object> entry : map.entrySet()) {
      final String[] parts = entry.getKey().split(delimiterRegex);
      setNestedValue(result, parts, entry.getValue());
    }

    return result;
  }

  @SuppressWarnings("unchecked")
  private void setNestedValue(final Map<String, Object> map, final String[] path, final Object value) {
    Map<String, Object> current = map;

    for (int i = 0; i < path.length - 1; i++) {
      final String key = path[i];
      Object next = current.get(key);

      if (next == null) {
        next = new HashMap<String, Object>();
        current.put(key, next);
      } else if (!(next instanceof Map)) {
        // Key already exists with non-map value, create a map anyway
        next = new HashMap<String, Object>();
        current.put(key, next);
      }

      current = (Map<String, Object>) next;
    }

    current.put(path[path.length - 1], value);
  }
}
