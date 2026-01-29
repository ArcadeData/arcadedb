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
package com.arcadedb.query.opencypher.functions.agg;

import com.arcadedb.query.sql.executor.CommandContext;

import java.util.List;

/**
 * agg.nth(list, n) - Return the nth element in the list (0-indexed).
 *
 * @author ArcadeDB Team
 */
public class AggNth extends AbstractAggFunction {
  @Override
  protected String getSimpleName() {
    return "nth";
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
    return "Return the nth element in the list (0-indexed)";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null || args[1] == null)
      return null;

    final List<Object> list = toObjectList(args[0]);
    final int index = ((Number) args[1]).intValue();

    if (index < 0 || index >= list.size())
      return null;

    return list.get(index);
  }
}
