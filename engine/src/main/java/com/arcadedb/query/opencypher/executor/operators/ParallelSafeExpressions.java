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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.query.opencypher.ast.ArithmeticExpression;
import com.arcadedb.query.opencypher.ast.BooleanCoercionExpression;
import com.arcadedb.query.opencypher.ast.BooleanWrapperExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpressionWrapper;
import com.arcadedb.query.opencypher.ast.InExpression;
import com.arcadedb.query.opencypher.ast.IsNullExpression;
import com.arcadedb.query.opencypher.ast.ListExpression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.ParameterExpression;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.StringMatchExpression;
import com.arcadedb.query.opencypher.ast.TernaryLogicalExpression;
import com.arcadedb.query.opencypher.ast.VariableExpression;

/**
 * Whether a Cypher predicate can be evaluated by the workers of a parallel scan (issue #8725): built only of the nodes
 * below, each of which reads the row and the command's parameters and nothing else. A function call, a subquery, a
 * pattern or a comprehension may keep state, call user code or need the rest of the pipeline, so a predicate with one
 * of them keeps the scan on the calling thread, as does any node this list does not know.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ParallelSafeExpressions {
  private ParallelSafeExpressions() {
  }

  /**
   * @param variable the only variable the predicate may read: the node the scan binds
   */
  static boolean isParallelSafe(final Object expression, final String variable) {
    return switch (expression) {
      case null -> false;
      case LiteralExpression ignored -> true;
      case ParameterExpression ignored -> true;
      case VariableExpression v -> variable.equals(v.getVariableName());
      case PropertyAccessExpression p -> variable.equals(p.getVariableName());
      case BooleanWrapperExpression w -> isParallelSafe(w.getBooleanExpression(), variable);
      case ComparisonExpressionWrapper w -> isParallelSafe(w.getComparison(), variable);
      case BooleanCoercionExpression c -> isParallelSafe(c.getExpression(), variable);
      case ComparisonExpression c -> isParallelSafe(c.getLeft(), variable) && isParallelSafe(c.getRight(), variable);
      case LogicalExpression l -> isParallelSafe(l.getLeft(), variable) && (l.getRight() == null || isParallelSafe(l.getRight(), variable));
      case TernaryLogicalExpression t -> isParallelSafe(t.getLeft(), variable) && (t.getRight() == null || isParallelSafe(t.getRight(), variable));
      case IsNullExpression n -> isParallelSafe(n.getExpression(), variable);
      case ArithmeticExpression a -> isParallelSafe(a.getLeft(), variable) && isParallelSafe(a.getRight(), variable);
      case StringMatchExpression s -> isParallelSafe(s.getExpression(), variable) && isParallelSafe(s.getPattern(), variable);
      case InExpression in -> isParallelSafe(in.getExpression(), variable) && allSafe(in.getList(), variable);
      case ListExpression list -> allSafe(list.getElements(), variable);
      default -> false;
    };
  }

  private static boolean allSafe(final Iterable<?> expressions, final String variable) {
    if (expressions == null)
      return false;
    for (final Object expression : expressions)
      if (!isParallelSafe(expression, variable))
        return false;
    return true;
  }
}
