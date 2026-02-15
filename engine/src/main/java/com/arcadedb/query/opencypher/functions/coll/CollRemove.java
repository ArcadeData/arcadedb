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
package com.arcadedb.query.opencypher.functions.coll;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.List;

/**
 * coll.remove(list, index, [count]) - Returns a new list with element(s) removed starting at the given index.
 * If count is not provided, removes one element.
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
    if (args.length < 2)
      throw new CommandExecutionException("coll.remove() requires at least 2 arguments (list, index)");
    final List<Object> list = asList(args[0]);
    if (list == null)
      return null;

    final int index = ((Number) args[1]).intValue();
    final int count = args.length > 2 ? ((Number) args[2]).intValue() : 1;
    final List<Object> result = new ArrayList<>(list);
    for (int i = 0; i < count && index < result.size(); i++)
      result.remove(index);
    return result;
  }
}
