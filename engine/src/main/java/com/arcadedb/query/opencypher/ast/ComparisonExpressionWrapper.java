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
package com.arcadedb.query.opencypher.ast;

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

/**
 * Wrapper that allows a ComparisonExpression to be used as a regular Expression.
 * This is needed when comparisons appear in contexts that expect Expression (like RETURN).
 * The evaluate method returns Boolean (true/false) as the comparison result.
 */
public class ComparisonExpressionWrapper implements Expression {
  private final ComparisonExpression comparison;

  public ComparisonExpressionWrapper(final Expression left, final ComparisonExpression.Operator operator,
      final Expression right) {
    this.comparison = new ComparisonExpression(left, operator, right);
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    return comparison.evaluate(result, context);
  }

  @Override
  public boolean isAggregation() {
    return false;
  }

  @Override
  public String getText() {
    return comparison.getText();
  }

  /**
   * Returns the underlying ComparisonExpression for use in boolean contexts.
   */
  public ComparisonExpression getComparison() {
    return comparison;
  }
}
