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
 * Adapter that wraps a generic {@link Expression} so it can be used as a
 * {@link BooleanExpression}. Used when the WHERE clause body evaluates to a
 * boolean-typed value but is not a comparison/IS NULL/IN/pattern/EXISTS form -
 * for example a bare boolean literal ({@code WHERE true}, {@code WHERE false}),
 * a parameter, or a property access on a boolean-typed property.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BooleanCoercionExpression implements BooleanExpression {
  private final Expression expression;

  public BooleanCoercionExpression(final Expression expression) {
    this.expression = expression;
  }

  @Override
  public boolean evaluate(final Result result, final CommandContext context) {
    final Object value = expression.evaluate(result, context);
    return Boolean.TRUE.equals(value);
  }

  @Override
  public Object evaluateTernary(final Result result, final CommandContext context) {
    final Object value = expression.evaluate(result, context);
    if (value == null)
      return null;
    if (value instanceof Boolean)
      return value;
    return Boolean.TRUE;
  }

  @Override
  public String getText() {
    return expression.getText();
  }

  public Expression getExpression() {
    return expression;
  }
}
