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

import java.util.HashMap;
import java.util.Map;

/**
 * map.setKey(map, key, value) - Set or add a key in the map.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class MapSetKey extends AbstractMapFunction {
  @Override
  protected String getSimpleName() {
    return "setKey";
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
    return "Return a new map with the key set to the value";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Map<String, Object> map = asMap(args[0]);
    final String key = args[1] != null ? args[1].toString() : null;
    final Object value = args[2];

    if (key == null) {
      throw new IllegalArgumentException("map.setKey() key cannot be null");
    }

    final Map<String, Object> result = new HashMap<>();
    if (map != null)
      result.putAll(map);

    result.put(key, value);
    return result;
  }
}
