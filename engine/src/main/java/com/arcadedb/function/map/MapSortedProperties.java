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
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * map.sortedProperties(map) - Get sorted list of [key, value] pairs.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class MapSortedProperties extends AbstractMapFunction {
  @Override
  protected String getSimpleName() {
    return "sortedProperties";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public String getDescription() {
    return "Get a list of [key, value] pairs sorted by key";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Map<String, Object> map = asMap(args[0]);

    if (map == null || map.isEmpty())
      return new ArrayList<>();

    final List<String> keys = new ArrayList<>(map.keySet());
    Collections.sort(keys);

    final List<List<Object>> result = new ArrayList<>(keys.size());
    for (final String key : keys) {
      result.add(List.of(key, map.get(key) == null ? "null" : map.get(key)));
    }

    return result;
  }
}
