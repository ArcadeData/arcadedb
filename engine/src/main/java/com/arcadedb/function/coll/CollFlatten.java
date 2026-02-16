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
 * coll.flatten(list, [shallow]) - Flattens nested lists into a single list.
 * If shallow is true (default false), only flattens one level deep.
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
    if (args.length < 1)
      throw new CommandExecutionException("coll.flatten() requires at least 1 argument");
    final List<Object> list = asList(args[0]);
    if (list == null)
      return null;

    final boolean shallow = args.length > 1 && args[1] instanceof Boolean && (Boolean) args[1];
    final List<Object> result = new ArrayList<>();
    flatten(list, result, shallow ? 1 : -1, 0);
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
