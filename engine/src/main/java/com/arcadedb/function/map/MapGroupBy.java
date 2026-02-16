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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * map.groupBy(list, key) - Group list of maps by a key value.
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
    return "Group a list of maps by a key property into a map of lists";
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object execute(final Object[] args, final CommandContext context) {
    if (!(args[0] instanceof List)) {
      throw new IllegalArgumentException("map.groupBy() first argument must be a list");
    }

    final String key = args[1] != null ? args[1].toString() : null;
    if (key == null) {
      throw new IllegalArgumentException("map.groupBy() key cannot be null");
    }

    final List<?> list = (List<?>) args[0];
    final Map<Object, List<Object>> result = new HashMap<>();

    for (final Object item : list) {
      if (item instanceof Map) {
        final Map<String, Object> map = (Map<String, Object>) item;
        final Object groupKey = map.get(key);
        final String keyStr = groupKey != null ? groupKey.toString() : "null";

        result.computeIfAbsent(keyStr, k -> new ArrayList<>()).add(item);
      }
    }

    return result;
  }
}
