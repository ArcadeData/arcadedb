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
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.List;

/**
 * head() function - returns the first element of a list.
 * <p>
 * Signature: {@code head(list :: LIST<ANY>) :: ANY}. A non-list argument is a type error, not a null
 * result (issue #5476); {@code null} propagates to {@code null} and an empty list answers {@code null}.
 */
public class HeadFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "head";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 1)
      throw new CommandExecutionException("head() requires exactly one argument");
    final List<Object> list = CypherFunctionHelper.requireListArgument(args[0], "head");
    if (list == null)
      return null;
    return list.isEmpty() ? null : list.get(0);
  }
}
