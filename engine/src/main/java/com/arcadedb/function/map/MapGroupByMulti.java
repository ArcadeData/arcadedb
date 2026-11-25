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

import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * map.groupByMulti(list, key) - Map each value of the key to the list of elements (maps, nodes or relationships) that
 * hold it, as APOC does. Elements without the key are skipped.
 */
public class MapGroupByMulti extends AbstractMapFunction {
  @Override
  protected String getSimpleName() {
    return "groupByMulti";
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
    return "Group a list of maps, nodes or relationships by a key property into a map of lists";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final List<Object> list = MapGroupBy.elements(args, getName());
    final Map<String, List<Object>> result = new LinkedHashMap<>();
    if (args[1] == null)
      return result;

    final String key = args[1].toString();
    for (final Object element : list) {
      final Object groupKey = MapGroupBy.groupKey(element, key);
      if (groupKey != null)
        result.computeIfAbsent(groupKey.toString(), k -> new ArrayList<>()).add(element);
    }
    return result;
  }
}
