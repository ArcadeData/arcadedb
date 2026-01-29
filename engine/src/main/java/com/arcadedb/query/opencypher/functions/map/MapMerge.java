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
 * map.merge(map1, map2) - Merge two maps into one. Values from map2 override map1.
 *
 * @author ArcadeDB Team
 */
public class MapMerge extends AbstractMapFunction {
  @Override
  protected String getSimpleName() {
    return "merge";
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
    return "Merge two maps; values from second map override the first";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Map<String, Object> map1 = asMap(args[0]);
    final Map<String, Object> map2 = asMap(args[1]);

    final Map<String, Object> result = new HashMap<>();

    if (map1 != null)
      result.putAll(map1);

    if (map2 != null)
      result.putAll(map2);

    return result;
  }
}
