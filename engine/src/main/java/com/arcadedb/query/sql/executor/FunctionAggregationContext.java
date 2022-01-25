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
package com.arcadedb.query.sql.executor;

import com.arcadedb.query.sql.parser.Expression;

import java.util.ArrayList;
import java.util.List;

/**
 * Delegates to an aggregate function for aggregation calculation
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class FunctionAggregationContext implements AggregationContext {
  private final SQLFunction      aggregateFunction;
  private       List<Expression> params;

  public FunctionAggregationContext(SQLFunction function, List<Expression> params) {
    this.aggregateFunction = function;
    this.params = params;
    if (this.params == null) {
      this.params = new ArrayList<>();
    }
  }

  @Override
  public Object getFinalValue() {
    return aggregateFunction.getResult();
  }

  @Override
  public void apply(Result next, CommandContext ctx) {
    List<Object> paramValues = new ArrayList<>();
    for (Expression expr : params) {
      paramValues.add(expr.execute(next, ctx));
    }
    aggregateFunction.execute(next, null, null, paramValues.toArray(), ctx);
  }
}
