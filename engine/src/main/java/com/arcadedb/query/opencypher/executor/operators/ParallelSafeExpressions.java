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
import com.arcadedb.query.opencypher.ast.StarExpression;
import com.arcadedb.query.opencypher.ast.StringMatchExpression;
import com.arcadedb.query.opencypher.ast.TernaryLogicalExpression;
import com.arcadedb.query.opencypher.ast.VariableExpression;

/**
 * Whether a Cypher predicate can be evaluated by the workers of a parallel scan (issue #8725): built only of the nodes
 * below, each of which reads the row and the command's parameters and nothing else. A function call, a subquery, a
 * pattern or a comprehension may keep state, call user code or need the rest of the pipeline, so a predicate with one
 * of them keeps the scan on the calling thread, as does any node this list does not know.
 * <p>
 * The workers share the one predicate instance, where a SQL scan gives each its own copy of the AST. A node joins the list
 * only if evaluating it is thread-safe: no mutable state, or state published through volatile immutable snapshots as
 * {@code ComparisonExpression}'s temporal memos are. Keep that in mind before adding a node or a cache to one of them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ParallelSafeExpressions {
  private ParallelSafeExpressions() {
  }

  /**
   * @param variable the only variable the predicate may read: the node the scan binds
   */
  public static boolean isParallelSafe(final Object expression, final String variable) {
    if (expression == null)
      return false;
    else if (expression instanceof LiteralExpression)
      return true;
    else if (expression instanceof ParameterExpression)
      return true;
    else if (expression instanceof StarExpression)
      return true;
    else if (expression instanceof VariableExpression v)
      return variable.equals(v.getVariableName());
    else if (expression instanceof PropertyAccessExpression p)
      return variable.equals(p.getVariableName());
    else if (expression instanceof BooleanWrapperExpression w)
      return isParallelSafe(w.getBooleanExpression(), variable);
    else if (expression instanceof ComparisonExpressionWrapper w)
      return isParallelSafe(w.getComparison(), variable);
    else if (expression instanceof BooleanCoercionExpression c)
      return isParallelSafe(c.getExpression(), variable);
    else if (expression instanceof ComparisonExpression c)
      return isParallelSafe(c.getLeft(), variable) && isParallelSafe(c.getRight(), variable);
    else if (expression instanceof LogicalExpression l)
      return isParallelSafe(l.getLeft(), variable) && (l.getRight() == null || isParallelSafe(l.getRight(), variable));
    else if (expression instanceof TernaryLogicalExpression t)
      return isParallelSafe(t.getLeft(), variable) && (t.getRight() == null || isParallelSafe(t.getRight(), variable));
    else if (expression instanceof IsNullExpression n)
      return isParallelSafe(n.getExpression(), variable);
    else if (expression instanceof ArithmeticExpression a)
      return isParallelSafe(a.getLeft(), variable) && isParallelSafe(a.getRight(), variable);
    else if (expression instanceof StringMatchExpression s)
      return isParallelSafe(s.getExpression(), variable) && isParallelSafe(s.getPattern(), variable);
    else if (expression instanceof InExpression in)
      return isParallelSafe(in.getExpression(), variable) && allSafe(in.getList(), variable);
    else if (expression instanceof ListExpression list)
      return allSafe(list.getElements(), variable);
    return false;
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
