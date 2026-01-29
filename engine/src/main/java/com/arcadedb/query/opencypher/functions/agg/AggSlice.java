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

import java.util.Collections;
import java.util.List;

/**
 * agg.slice(list, from, to) - Return a sublist from index 'from' to 'to' (exclusive).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class AggSlice extends AbstractAggFunction {
  @Override
  protected String getSimpleName() {
    return "slice";
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
    return "Return a sublist from index 'from' to 'to' (exclusive)";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    final List<Object> list = toObjectList(args[0]);
    final int from = args[1] != null ? ((Number) args[1]).intValue() : 0;
    final int to = args.length > 2 && args[2] != null ? ((Number) args[2]).intValue() : list.size();

    if (from >= list.size() || from >= to)
      return Collections.emptyList();

    final int actualFrom = Math.max(0, from);
    final int actualTo = Math.min(list.size(), to);

    return list.subList(actualFrom, actualTo);
  }
}
