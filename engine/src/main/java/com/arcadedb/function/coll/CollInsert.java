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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.List;

/**
 * coll.insert(list, index, value) - Returns a new list with value inserted at the given index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CollInsert extends AbstractCollFunction {
  @Override
  protected String getSimpleName() {
    return "insert";
  }

  @Override
  public int getMinArgs() {
    return 3;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Returns a new list with value inserted at the given index";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 3)
      throw new CommandExecutionException("coll.insert() requires exactly 3 arguments (list, index, value)");
    final List<Object> list = asList(args[0]);
    if (list == null)
      return null;
    final int index = ((Number) args[1]).intValue();
    final List<Object> result = new ArrayList<>(list);
    result.add(index, args[2]);
    return result;
  }
}
