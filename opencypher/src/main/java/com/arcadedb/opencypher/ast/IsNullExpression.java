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
package com.arcadedb.opencypher.ast;

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

/**
 * IS NULL / IS NOT NULL expression for WHERE clauses.
 * Example: n.email IS NULL, n.age IS NOT NULL
 */
public class IsNullExpression implements BooleanExpression {
  private final Expression expression;
  private final boolean isNot;

  public IsNullExpression(final Expression expression, final boolean isNot) {
    this.expression = expression;
    this.isNot = isNot;
  }

  @Override
  public boolean evaluate(final Result result, final CommandContext context) {
    final Object value = expression.evaluate(result, context);
    final boolean isNull = value == null;
    return isNot ? !isNull : isNull;
  }

  @Override
  public String getText() {
    return expression.getText() + (isNot ? " IS NOT NULL" : " IS NULL");
  }

  public Expression getExpression() {
    return expression;
  }

  public boolean isNot() {
    return isNot;
  }
}
