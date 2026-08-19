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

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.utility.LongRangeList;

import java.util.ArrayList;
import java.util.List;

/**
 * coll.remove(list, index, [count]) - Returns a new list with element(s) removed starting at the given index.
 * If count is not provided, removes one element.
 * <p>
 * Removing a prefix or a suffix of a range leaves an arithmetic progression, so those two cases are answered in
 * constant space rather than by copying a list that costs no heap while it stays lazy (issue #6353). Cutting out
 * of the middle does not: the result is two progressions, which a Cypher LIST cannot be, so it is materialised.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CollRemove extends AbstractCollFunction {
  @Override
  protected String getSimpleName() {
    return "remove";
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
    return "Returns a new list with element(s) removed at the given index";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    final List<Object> list = asList(args[0]);
    if (list == null)
      return null;
    final Number indexArg = CypherFunctionHelper.requireNumberArgument(args[1], getName());
    if (indexArg == null)
      return null;

    final int index = indexArg.intValue();
    if (index < 0)
      throw new CommandSemanticException(getName() + "() does not support negative index: " + index);
    if (index >= list.size())
      throw new CommandSemanticException(getName() + "() index " + index + " is out of range for list of size " + list.size());
    final Number countArg = args.length > 2 ? CypherFunctionHelper.requireNumberArgument(args[2], getName()) : null;
    if (args.length > 2 && countArg == null)
      return null;
    final int count = countArg != null ? countArg.intValue() : 1;
    final LongRangeList range = asRange(list);
    if (range != null) {
      // The loop below stops at the end of the list, so it removes min(count, size - index) elements.
      final int removed = Math.min(Math.max(count, 0), range.size() - index);
      if (index == 0)
        return range.subList(removed, range.size());
      if (index + removed >= range.size())
        return range.subList(0, index);
    }
    final List<Object> result = new ArrayList<>(list);
    for (int i = 0; i < count && index < result.size(); i++)
      result.remove(index);
    return result;
  }
}
