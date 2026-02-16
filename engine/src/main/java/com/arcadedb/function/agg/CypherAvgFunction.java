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
package com.arcadedb.function.agg;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Cypher avg() aggregation function.
 * Always returns a Double result matching Neo4j/OpenCypher semantics,
 * unlike the SQL avg() which preserves the input numeric type.
 * Skips nulls.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CypherAvgFunction implements StatelessFunction {
  private double sum = 0.0;
  private int    count = 0;

  @Override
  public String getName() {
    return "avg";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 1)
      throw new CommandExecutionException("avg() requires exactly one argument");
    if (args[0] == null)
      return null;
    if (args[0] instanceof Number number) {
      sum += number.doubleValue();
      count++;
    }
    return null;
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getAggregatedResult() {
    if (count == 0)
      return null;
    return sum / count;
  }
}
