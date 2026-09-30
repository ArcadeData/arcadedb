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

import com.arcadedb.function.DistinctNumericKey;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.List;

/**
 * coll.indexOf(list, value) - Returns the index of the first occurrence of value in the list, or -1 if not found.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CollIndexOf extends AbstractCollFunction {
  @Override
  protected String getSimpleName() {
    return "indexOf";
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
    return "Returns the index of the first occurrence of value in the list, or -1 if not found";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    final List<Object> list = asList(args[0]);
    if (list == null || args[1] == null)
      return null;
    // Elements are compared the way Cypher's = does, so 1 and 1.0 are the same element (issue #8561)
    final Object wanted = DistinctNumericKey.canonicalize(args[1]);
    // Nothing canonicalizes to a different value than itself for a string or the like, so the plain search is exact and
    // allocation-free: only a number or a container can equal an element of another representation
    if (args[1] instanceof CharSequence)
      return (long) list.indexOf(args[1]);
    if (asRange(list) != null)
      return wanted instanceof Long ? (long) list.indexOf(wanted) : -1L;
    for (int i = 0; i < list.size(); i++)
      if (wanted.equals(DistinctNumericKey.canonicalize(list.get(i))))
        return (long) i;
    return -1L;
  }
}
