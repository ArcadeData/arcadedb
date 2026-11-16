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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * map.removeKeys(map, keys, [config]) - Remove multiple keys from the map. With {@code {recursive: true}} in the config,
 * as in APOC's {@code apoc.map.removeKeys}, the keys are removed from the nested maps too, including the maps directly
 * inside a list, and a nested map or list left empty is removed with them (issue #12105).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class MapRemoveKeys extends AbstractMapFunction {
  @Override
  protected String getSimpleName() {
    return "removeKeys";
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Return a new map with the specified keys removed";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Map<String, Object> map = asMap(args[0]);

    // A list stored as ARRAY_OF_LONGS / ARRAY_OF_DOUBLES is a primitive array, and is the same LIST to every list function (issue #11582)
    final List<Object> keys = CypherFunctionHelper.asListOrNull(args[1]);
    if (keys == null) {
      throw new IllegalArgumentException("map.removeKeys() second argument must be a list of keys");
    }
    final boolean recursive = recursive(getName(), args.length > 2 ? args[2] : null);

    if (map == null)
      return new LinkedHashMap<>();

    final Set<String> names = new HashSet<>(keys.size());
    for (final Object key : keys)
      if (key != null)
        names.add(key.toString());
    return removeKeys(map, names, recursive);
  }

  /**
   * The {@code recursive} entry of the config map APOC's {@code removeKey}/{@code removeKeys} take, read as APOC reads a
   * flag: anything but null, {@code false}, a number whose whole part is 0 and the strings "", "false", "no" and "0" (in
   * any case) is true. A null config is the default, no recursion.
   */
  static boolean recursive(final String functionName, final Object config) {
    if (config == null)
      return false;
    if (!(config instanceof Map<?, ?> map))
      throw CypherFunctionHelper.typeMismatch(functionName, "a MAP config", config);
    final Object value = map.get("recursive");
    if (value == null || Boolean.FALSE.equals(value))
      return false;
    if (value instanceof Number number)
      return number.longValue() != 0L;
    if (value instanceof String text)
      return switch (text.toLowerCase(Locale.ROOT)) {
        case "", "false", "no", "0" -> false;
        default -> true;
      };
    return true;
  }

  /**
   * A copy of the map without the keys. When recursive, as APOC: a nested map loses them too and is dropped once empty;
   * a list loses them from the maps it holds directly, drops those left empty and is itself dropped once empty.
   */
  @SuppressWarnings("unchecked")
  static Map<String, Object> removeKeys(final Map<String, Object> map, final Collection<String> keys,
      final boolean recursive) {
    final Map<String, Object> result = new LinkedHashMap<>(map);
    result.keySet().removeAll(keys);
    if (!recursive)
      return result;

    for (final Iterator<Map.Entry<String, Object>> it = result.entrySet().iterator(); it.hasNext(); ) {
      final Map.Entry<String, Object> entry = it.next();
      final Object value = entry.getValue();
      if (value instanceof Map<?, ?> nested) {
        final Map<String, Object> cleaned = removeKeys((Map<String, Object>) nested, keys, true);
        if (cleaned.isEmpty())
          it.remove();
        else
          entry.setValue(cleaned);
      } else if (value instanceof Collection<?> collection) {
        final List<Object> cleaned = new ArrayList<>(collection.size());
        for (final Object element : collection) {
          if (element instanceof Map<?, ?> nested) {
            final Map<String, Object> cleanedElement = removeKeys((Map<String, Object>) nested, keys, true);
            if (!cleanedElement.isEmpty())
              cleaned.add(cleanedElement);
          } else
            cleaned.add(element);
        }
        if (cleaned.isEmpty())
          it.remove();
        else
          entry.setValue(cleaned);
      }
    }
    return result;
  }
}
