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
 * Logical expression for combining boolean conditions.
 * Supports: AND, OR, NOT
 * Example: n.age > 30 AND n.city = 'NYC', NOT n.active
 */
public class LogicalExpression implements BooleanExpression {
  public enum Operator {
    AND,
    OR,
    NOT
  }

  private final Operator operator;
  private final BooleanExpression left;
  private final BooleanExpression right; // null for NOT

  public LogicalExpression(final Operator operator, final BooleanExpression left, final BooleanExpression right) {
    this.operator = operator;
    this.left = left;
    this.right = right;
  }

  public LogicalExpression(final Operator operator, final BooleanExpression operand) {
    this(operator, operand, null);
  }

  @Override
  public boolean evaluate(final Result result, final CommandContext context) {
    return switch (operator) {
      case AND -> left.evaluate(result, context) && right.evaluate(result, context);
      case OR -> left.evaluate(result, context) || right.evaluate(result, context);
      case NOT -> !left.evaluate(result, context);
    };
  }

  @Override
  public String getText() {
    return switch (operator) {
      case NOT -> "NOT " + left.getText();
      case AND -> "(" + left.getText() + " AND " + right.getText() + ")";
      case OR -> "(" + left.getText() + " OR " + right.getText() + ")";
    };
  }

  public Operator getOperator() {
    return operator;
  }

  public BooleanExpression getLeft() {
    return left;
  }

  public BooleanExpression getRight() {
    return right;
  }
}
