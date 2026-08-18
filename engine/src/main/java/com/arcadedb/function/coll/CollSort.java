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
 * coll.sort(list) - Returns a sorted copy of the list using Cypher comparison semantics.
 * <p>
 * Sorting a range needs no copy: an ascending arithmetic progression is already sorted and a descending one is its
 * own reverse, both answered in constant space. Materialising it instead reinstated the heap exhaustion the lazy
 * range removed (issue #6353, advisory GHSA-xmjm-8q85-g778).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CollSort extends AbstractCollFunction {
  @Override
  protected String getSimpleName() {
    return "sort";
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
    return "Returns a sorted copy of the list";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    final List<Object> list = asList(args[0]);
    if (list == null)
      return null;
    final LongRangeList range = asRange(list);
    if (range != null)
      return range.getStep() > 0 ? range : range.reversed();
    final List<Object> result = new ArrayList<>(list);
    result.sort(CypherFunctionHelper::cypherCompare);
    return result;
  }
}
