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
package com.arcadedb.query.opencypher.optimizer;

import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.sql.executor.QueryHelper;

import java.util.List;

/**
 * Represents a range predicate on a property that can be used for index range scans.
 * Supports operators: <, >, <=, >=
 *
 * Examples:
 * - age > 18 and age < 65 → two RangePredicates can be combined into a range scan
 * - date >= $start → single-bound range scan
 */
public class RangePredicate {
  private final String propertyName;
  private final ComparisonExpression.Operator operator;
  private final Object value;
  private final boolean isParameter;
  private final boolean prefixSuccessor;

  public RangePredicate(final String propertyName, final ComparisonExpression.Operator operator,
                       final Object value, final boolean isParameter) {
    this(propertyName, operator, value, isParameter, false);
  }

  private RangePredicate(final String propertyName, final ComparisonExpression.Operator operator,
                        final Object value, final boolean isParameter, final boolean prefixSuccessor) {
    this.propertyName = propertyName;
    this.operator = operator;
    this.value = value;
    this.isParameter = isParameter;
    this.prefixSuccessor = prefixSuccessor;
  }

  /**
   * The two bounds of {@code prop STARTS WITH prefix} (issue #8666): {@code prop >= prefix}, and {@code prop <} the
   * smallest string after every string with that prefix, which the scan works out when it resolves the parameter. The
   * predicate itself stays in the WHERE as a filter.
   */
  public static List<RangePredicate> forPrefix(final String propertyName, final Object prefix, final boolean isParameter) {
    return List.of(new RangePredicate(propertyName, ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, prefix, isParameter),
        new RangePredicate(propertyName, ComparisonExpression.Operator.LESS_THAN, prefix, isParameter, true));
  }

  /**
   * Whether the bound is the string that follows every string starting with {@link #getValue()} rather than the value
   * itself, see {@link QueryHelper#prefixSuccessor(String)}.
   */
  public boolean isPrefixSuccessor() {
    return prefixSuccessor;
  }

  public String getPropertyName() {
    return propertyName;
  }

  public ComparisonExpression.Operator getOperator() {
    return operator;
  }

  public Object getValue() {
    return value;
  }

  public boolean isParameter() {
    return isParameter;
  }

  public boolean isLowerBound() {
    return operator == ComparisonExpression.Operator.GREATER_THAN ||
           operator == ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
  }

  public boolean isUpperBound() {
    return operator == ComparisonExpression.Operator.LESS_THAN ||
           operator == ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
  }

  public boolean isInclusive() {
    return operator == ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL ||
           operator == ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
  }

  @Override
  public String toString() {
    return propertyName + " " + operator + " " + (isParameter ? "$param" : value);
  }
}
