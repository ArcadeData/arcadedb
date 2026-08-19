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
package com.arcadedb.function.coll;

import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.utility.LongRangeList;

import java.util.List;

/**
 * coll.max(list) - Returns the maximum value in the list.
 * <p>
 * The largest element of an arithmetic progression is one of its two endpoints, so a range is answered without
 * walking it (issue #6403). See {@link CollMin} for why the constant-space walk left by issue #6353 is not enough.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CollMax extends AbstractCollFunction {
  @Override
  protected String getSimpleName() {
    return "max";
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
    return "Returns the maximum value in the list";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    final List<Object> list = asList(args[0]);
    if (list == null || list.isEmpty())
      return null;

    final LongRangeList range = asRange(list);
    if (range != null)
      return range.getStep() > 0 ? range.get(range.size() - 1) : range.get(0);

    Object max = null;
    for (final Object item : list) {
      if (item == null)
        continue;
      if (max == null || CypherFunctionHelper.cypherCompare(item, max) > 0)
        max = item;
    }
    return max;
  }
}
