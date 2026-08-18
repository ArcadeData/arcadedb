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

import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.utility.LongRangeList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * tail() function - returns all elements except the first.
 * <p>
 * Signature: {@code tail(list :: LIST<ANY>) :: LIST<ANY>}. A non-list argument is a type error, not an
 * empty list (issue #5476); {@code null} propagates to {@code null} (issue #3920).
 * <p>
 * A range beheaded is still an arithmetic progression, so a {@link LongRangeList} argument is answered in
 * constant space. Copying it instead reinstated the heap exhaustion the lazy range removed (issue #6353,
 * advisory GHSA-xmjm-8q85-g778): {@code tail(range(0, 999999999))} allocated a billion boxed longs.
 */
public class TailFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "tail";
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
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    final List<Object> list = CypherFunctionHelper.requireListArgument(args[0], "tail");
    if (list == null)
      return null;
    if (list.size() <= 1)
      return Collections.emptyList();
    if (args[0] instanceof LongRangeList range)
      // LongRangeList.subList() returns a range, not a view that walks this one, so nothing is materialised.
      return range.subList(1, range.size());
    return new ArrayList<>(list.subList(1, list.size()));
  }
}
