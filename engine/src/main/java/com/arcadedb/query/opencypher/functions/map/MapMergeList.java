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
 * map.mergeList(listOfMaps) - Merge a list of maps into one.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class MapMergeList extends AbstractMapFunction {
  @Override
  protected String getSimpleName() {
    return "mergeList";
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
    return "Merge a list of maps into a single map";
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return new HashMap<>();

    if (!(args[0] instanceof List)) {
      throw new IllegalArgumentException("map.mergeList() requires a list argument");
    }

    final List<?> list = (List<?>) args[0];
    final Map<String, Object> result = new HashMap<>();

    for (final Object item : list) {
      if (item instanceof Map) {
        result.putAll((Map<String, Object>) item);
      }
    }

    return result;
  }
}
