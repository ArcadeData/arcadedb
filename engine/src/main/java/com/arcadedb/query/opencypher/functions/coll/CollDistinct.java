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
import java.util.LinkedHashSet;
import java.util.List;

/**
 * coll.distinct(list) - Returns a list with duplicate values removed.
 * Preserves the order of first occurrence.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CollDistinct extends AbstractCollFunction {
  @Override
  protected String getSimpleName() {
    return "distinct";
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
    return "Returns a list with duplicate values removed, preserving order of first occurrence";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 1)
      throw new CommandExecutionException("coll.distinct() requires exactly 1 argument");
    final List<Object> list = asList(args[0]);
    if (list == null)
      return null;
    return new ArrayList<>(new LinkedHashSet<>(list));
  }
}
