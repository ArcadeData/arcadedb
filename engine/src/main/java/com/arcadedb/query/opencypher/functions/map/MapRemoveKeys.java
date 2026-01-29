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
 * map.removeKeys(map, keys) - Remove multiple keys from the map.
 *
 * @author ArcadeDB Team
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
    return 2;
  }

  @Override
  public String getDescription() {
    return "Return a new map with the specified keys removed";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Map<String, Object> map = asMap(args[0]);

    if (!(args[1] instanceof List)) {
      throw new IllegalArgumentException("map.removeKeys() second argument must be a list of keys");
    }

    final List<?> keys = (List<?>) args[1];

    if (map == null)
      return new HashMap<>();

    final Map<String, Object> result = new HashMap<>(map);
    for (final Object key : keys) {
      if (key != null) {
        result.remove(key.toString());
      }
    }
    return result;
  }
}
