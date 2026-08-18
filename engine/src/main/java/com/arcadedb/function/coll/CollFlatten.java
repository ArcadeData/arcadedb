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

import java.util.ArrayList;
import java.util.List;

/**
 * coll.flatten(list, [depth]) - Flattens nested lists into a single list.
 * The depth parameter controls how many levels of nesting to flatten (default 1).
 * If depth is 0, the list is returned unchanged. If depth is null, returns null.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CollFlatten extends AbstractCollFunction {
  @Override
  protected String getSimpleName() {
    return "flatten";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Flattens nested lists into a single list";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    final List<Object> list = asList(args[0]);
    if (list == null)
      return null;

    int maxDepth = 1; // default: flatten one level
    if (args.length > 1) {
      if (args[1] == null)
        return null;
      if (args[1] instanceof Boolean)
        maxDepth = (Boolean) args[1] ? 1 : -1;
      else
        // A caller mistake here is a client error, exactly like every other argument in the family (issue
        // #6403) - args[1] used to fall through every branch silently and leave maxDepth at its default instead
        // of raising, so coll.flatten([[1]], 'x') answered as though depth had been omitted (code review).
        maxDepth = CypherFunctionHelper.requireNumberArgument(args[1], getName()).intValue();
    }

    if (asRange(list) != null)
      // A range holds longs and nothing else, so it is already flat at every depth - including depth 0, where the
      // answer is the input. Copying it materialised a range that costs no heap while it stays lazy (issue #6353).
      return list;

    if (maxDepth == 0)
      return new ArrayList<>(list);

    final List<Object> result = new ArrayList<>();
    flatten(list, result, maxDepth, 0);
    return result;
  }

  @SuppressWarnings("unchecked")
  private void flatten(final List<Object> source, final List<Object> result, final int maxDepth, final int currentDepth) {
    for (final Object item : source) {
      if (item instanceof List && (maxDepth == -1 || currentDepth < maxDepth))
        flatten((List<Object>) item, result, maxDepth, currentDepth + 1);
      else
        result.add(item);
    }
  }
}
