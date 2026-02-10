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
package com.arcadedb.query.opencypher.function;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * min() aggregation function with Cypher mixed-type support.
 * Skips nulls. For mixed types uses Cypher type ordering.
 */
public class CypherMinFunction implements StatelessFunction {
  private Object minValue = null;
  private boolean hasValue = false;

  @Override
  public String getName() {
    return "min";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 1)
      throw new CommandExecutionException("min() requires exactly one argument");
    if (args[0] == null)
      return null;
    if (!hasValue || CypherFunctionHelper.cypherCompare(args[0], minValue) < 0) {
      minValue = args[0];
      hasValue = true;
    }
    return null;
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getAggregatedResult() {
    return hasValue ? minValue : null;
  }
}
