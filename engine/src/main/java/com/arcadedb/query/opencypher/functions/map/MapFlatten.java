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

/**
 * map.flatten(map, delimiter) - Flatten nested map with key paths.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class MapFlatten extends AbstractMapFunction {
  @Override
  protected String getSimpleName() {
    return "flatten";
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
    return "Flatten a nested map into a single-level map with dotted key paths";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Map<String, Object> map = asMap(args[0]);
    final String delimiter = args.length > 1 && args[1] != null ? args[1].toString() : ".";

    if (map == null)
      return new HashMap<>();

    final Map<String, Object> result = new HashMap<>();
    flatten("", map, delimiter, result);
    return result;
  }

  @SuppressWarnings("unchecked")
  private void flatten(final String prefix, final Map<String, Object> map,
                        final String delimiter, final Map<String, Object> result) {
    for (final Map.Entry<String, Object> entry : map.entrySet()) {
      final String key = prefix.isEmpty() ? entry.getKey() : prefix + delimiter + entry.getKey();
      final Object value = entry.getValue();

      if (value instanceof Map) {
        flatten(key, (Map<String, Object>) value, delimiter, result);
      } else {
        result.put(key, value);
      }
    }
  }
}
