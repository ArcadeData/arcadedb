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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * map.removeKey(map, key, [config]) - Remove a key from the map. The config takes {@code recursive}, as
 * {@link MapRemoveKeys} does, with APOC's quirk kept: a map without the key at its top level is answered unchanged, the
 * nested maps included (issue #12105).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class MapRemoveKey extends AbstractMapFunction {
  @Override
  protected String getSimpleName() {
    return "removeKey";
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
    return "Return a new map with the specified key removed";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Map<String, Object> map = asMap(args[0]);
    final String key = args[1] != null ? args[1].toString() : null;
    final boolean recursive = MapRemoveKeys.recursive(getName(), args.length > 2 ? args[2] : null);

    if (map == null)
      return new LinkedHashMap<>();

    // As APOC's apoc.map.removeKey: only a key present at the top level starts a (recursive) removal
    if (key == null || !map.containsKey(key))
      return new LinkedHashMap<>(map);
    return MapRemoveKeys.removeKeys(map, Set.of(key), recursive);
  }
}
