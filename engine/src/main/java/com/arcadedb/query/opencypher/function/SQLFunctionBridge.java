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

import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.SQLFunction;

/**
 * Bridge from Cypher function to SQL function.
 * For aggregation functions, we configure them to enable proper state accumulation.
 */
public class SQLFunctionBridge implements StatelessFunction {
  private final SQLFunction sqlFunction;
  private final String cypherFunctionName;
  private final boolean isAggregation;

  public SQLFunctionBridge(final SQLFunction sqlFunction, final String cypherFunctionName) {
    this.sqlFunction = sqlFunction;
    this.cypherFunctionName = cypherFunctionName;

    // Configure the function to enable aggregation mode
    // SQL aggregation functions check configuredParameters in aggregateResults()
    // We configure with a dummy parameter since the actual values come through execute() args
    sqlFunction.config(new Object[]{"dummy"});

    // Cache whether this is an aggregation function
    this.isAggregation = sqlFunction.aggregateResults();
  }

  @Override
  public String getName() {
    return cypherFunctionName;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    // SQL functions expect (self, currentRecord, currentResult, params, context)
    // For now, we pass nulls for self, currentRecord, currentResult
    return sqlFunction.execute(null, null, null, args, context);
  }

  @Override
  public boolean aggregateResults() {
    return isAggregation;
  }

  @Override
  public Object getAggregatedResult() {
    return sqlFunction.getResult();
  }
}
