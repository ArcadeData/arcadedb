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
package com.arcadedb.function.temporal;

import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * timestamp() function - returns the current time in milliseconds since the Unix epoch.
 * Compatible with Neo4j's timestamp() behavior: the value comes from the statement clock, frozen once per
 * statement, so every occurrence in one statement answers the same number rather than re-reading the wall clock
 * and drifting by a millisecond (issue #7052).
 * <p>
 * The function stays non-foldable in {@code ExpressionOptimizer}: the value is per execution, not per plan, and
 * the plan cache outlives a single execution.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TimestampFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "timestamp";
  }

  @Override
  public int getMinArgs() {
    return 0;
  }

  @Override
  public int getMaxArgs() {
    return 0;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    return CypherFunctionHelper.getStatementTime(context).get(CypherFunctionHelper.TIMESTAMP);
  }
}
