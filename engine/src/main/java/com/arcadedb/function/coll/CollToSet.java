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

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.utility.LongRangeList;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * coll.toSet(list) - Returns a unique list backed by a set, i.e. the given list with duplicates removed.
 * Order is not significant to the caller, but the order of first occurrence is preserved so the result is
 * deterministic. Like the rest of the {@code coll.*} namespace, duplicates are recognized by object equality,
 * so {@code coll.toSet([1, 1.0])} keeps both elements.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CollToSet extends AbstractCollFunction {
  @Override
  protected String getSimpleName() {
    return "toSet";
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
    return "Returns a unique list from the given list, preserving the order of first occurrence";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    final List<Object> list = asList(args[0]);
    if (list == null)
      return null;
    if (args[0] instanceof LongRangeList range)
      // The step of a range is never zero, so no element repeats: the set IS the range (issue #6353).
      return range;
    return new ArrayList<>(new LinkedHashSet<>(list));
  }
}
