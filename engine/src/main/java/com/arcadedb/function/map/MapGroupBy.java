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

import com.arcadedb.database.Document;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * map.groupBy(list, key) - Map each value of the key to the last element (map, node or relationship) that holds it,
 * as APOC does. Elements without the key are skipped. {@code map.groupByMulti} keeps every element per key.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class MapGroupBy extends AbstractMapFunction {
  @Override
  protected String getSimpleName() {
    return "groupBy";
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
    return "Group a list of maps, nodes or relationships by a key property into a map of the last element per key value";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final List<Object> list = elements(args, getName());
    final Map<String, Object> result = new LinkedHashMap<>();
    if (args[1] == null)
      return result;

    final String key = args[1].toString();
    for (final Object element : list) {
      final Object groupKey = groupKey(element, key);
      if (groupKey != null)
        result.put(groupKey.toString(), element);
    }
    return result;
  }

  /**
   * The list argument of map.groupBy and map.groupByMulti. A list stored as a primitive array is a list too (issue #11582).
   */
  static List<Object> elements(final Object[] args, final String functionName) {
    final List<Object> list = CypherFunctionHelper.asListOrNull(args[0]);
    if (list == null)
      throw new IllegalArgumentException(functionName + "() first argument must be a list");
    return list;
  }

  /**
   * The value an element is grouped by: the map entry or the node/relationship property, null for anything else.
   */
  @SuppressWarnings("unchecked")
  static Object groupKey(final Object element, final String key) {
    if (element instanceof Map)
      return ((Map<String, Object>) element).get(key);
    if (element instanceof Document document)
      return document.get(key);
    return null;
  }
}
