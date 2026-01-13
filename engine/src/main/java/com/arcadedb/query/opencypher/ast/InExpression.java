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

import java.util.List;

/**
 * IN expression for WHERE clauses.
 * Example: n.name IN ['Alice', 'Bob', 'Charlie']
 */
public class InExpression implements BooleanExpression {
  private final Expression expression;
  private final List<Expression> list;
  private final boolean isNot;

  public InExpression(final Expression expression, final List<Expression> list, final boolean isNot) {
    this.expression = expression;
    this.list = list;
    this.isNot = isNot;
  }

  @Override
  public boolean evaluate(final Result result, final CommandContext context) {
    final Object value = expression.evaluate(result, context);

    // Check if value is in the list
    boolean found = false;
    for (final Expression listItem : list) {
      final Object listValue = listItem.evaluate(result, context);
      if (valuesEqual(value, listValue)) {
        found = true;
        break;
      }
    }

    return isNot ? !found : found;
  }

  private boolean valuesEqual(final Object a, final Object b) {
    if (a == null && b == null) {
      return true;
    }
    if (a == null || b == null) {
      return false;
    }

    // Numeric comparison
    if (a instanceof Number && b instanceof Number) {
      return ((Number) a).doubleValue() == ((Number) b).doubleValue();
    }

    // Default to string comparison
    return a.toString().equals(b.toString());
  }

  @Override
  public String getText() {
    final StringBuilder sb = new StringBuilder();
    sb.append(expression.getText());
    sb.append(isNot ? " NOT IN [" : " IN [");
    for (int i = 0; i < list.size(); i++) {
      if (i > 0) sb.append(", ");
      sb.append(list.get(i).getText());
    }
    sb.append("]");
    return sb.toString();
  }

  public Expression getExpression() {
    return expression;
  }

  public List<Expression> getList() {
    return list;
  }

  public boolean isNot() {
    return isNot;
  }
}
