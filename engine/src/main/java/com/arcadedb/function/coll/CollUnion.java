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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * coll.union(list1, list2) - Returns the distinct union of two lists, preserving order of first occurrence.
 */
public class CollUnion extends AbstractCollFunction {
  @Override
  protected String getSimpleName() {
    return "union";
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
    return "Returns the distinct union of two lists, preserving order of first occurrence";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    final List<Object> list1 = asList(args[0]);
    final List<Object> list2 = asList(args[1]);

    final LinkedHashSet<Object> union = new LinkedHashSet<>();
    if (list1 != null)
      union.addAll(list1);
    if (list2 != null)
      union.addAll(list2);

    return new ArrayList<>(union);
  }
}
