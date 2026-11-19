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

import com.arcadedb.function.CypherBuiltinFunctions;
import com.arcadedb.function.CypherFunctionRegistry;
import com.arcadedb.query.opencypher.ast.ArithmeticExpression;
import com.arcadedb.query.opencypher.ast.BooleanCoercionExpression;
import com.arcadedb.query.opencypher.ast.BooleanWrapperExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpressionWrapper;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
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

import java.util.Set;

/**
 * Whether a Cypher predicate can be evaluated by the workers of a parallel scan (issue #8725): built only of the nodes
 * below, each of which reads the row and the command's parameters and nothing else. A subquery, a pattern or a
 * comprehension may need the rest of the pipeline, so a predicate with one of them keeps the scan on the calling thread, as
 * does any node this list does not know. A function call joins the list when it calls one of the built-in scalar functions
 * of {@link #PARALLEL_SAFE_FUNCTIONS} (issue #11873): a user-defined function, a SQL function, a function registered over
 * a built-in name, an aggregate, or a function whose answer is not decided by its arguments alone keeps the scan
 * sequential.
 * <p>
 * The workers share the one predicate instance, where a SQL scan gives each its own copy of the AST. A node joins the list
 * only if evaluating it is thread-safe: no mutable state, or state published through volatile immutable snapshots as
 * {@code ComparisonExpression}'s temporal memos are. Keep that in mind before adding a node or a cache to one of them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ParallelSafeExpressions {
  /**
   * The built-in scalar functions the workers of a parallel scan may call (issue #11873): the answer is decided by the
   * arguments alone, they read nothing but the arguments (the record a node argument is, never another record or the graph
   * around it), write nothing, and their executors hold no mutable state, which {@code ParallelSafeExpressionsStateTest}
   * checks for every name here. Each is a name {@link CypherBuiltinFunctions} lists, which the function factory resolves to
   * an executor of its own as long as no function is registered over it ({@link #isParallelSafeFunction}).
   * <p>
   * Left out on purpose: {@code rand()}, {@code randomUUID()}, {@code timestamp()} and the temporal constructors, whose
   * answer depends on when they run; {@code file()} and {@code linenumber()}, which read the LOAD CSV row context;
   * {@code startNode()}, {@code endNode()} and the path functions, which load other records; the aggregates.
   */
  public static final Set<String> PARALLEL_SAFE_FUNCTIONS = Set.of(
      // Math
      "abs", "ceil", "ceiling", "floor", "round", "sign", "sqrt", "exp", "log", "ln", "log10", "isnan", "pi", "e",
      "sin", "cos", "tan", "cot", "asin", "acos", "atan", "atan2", "sinh", "cosh", "tanh", "coth", "degrees", "radians",
      "haversin",
      // String
      "tolower", "toupper", "lower", "upper", "trim", "btrim", "ltrim", "rtrim", "replace", "substring", "left", "right",
      "split", "reverse", "normalize", "isnormalized", "char_length", "character_length", "charlength", "char.length",
      "character.length",
      // Conversion
      "tostring", "tointeger", "tofloat", "toboolean", "tostringornull", "tointegerornull", "tofloatornull",
      "tobooleanornull", "tostringlist", "tointegerlist", "tofloatlist", "tobooleanlist",
      // Lists and predicates
      "size", "head", "last", "tail", "isempty", "exists",
      // General
      "coalesce", "nullif", "valuetype",
      // The node itself
      "id", "elementid", "labels", "keys", "properties",
      // Vectors
      "vector.similarity.cosine", "vector.similarity.euclidean", "vector.distance", "vector.distance.cosine",
      "vector.distance.euclidean", "vector.distance.manhattan", "vector.norm", "vector.dimension.count",
      "vector_dimension_count");

  private ParallelSafeExpressions() {
  }

  /**
   * @param variable the only variable the predicate may read: the node the scan binds
   */
  public static boolean isParallelSafe(final Object expression, final String variable) {
    return switch (expression) {
      case null -> false;
      case LiteralExpression ignored -> true;
      case ParameterExpression ignored -> true;
      case StarExpression ignored -> true;
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
      case FunctionCallExpression call -> isParallelSafeFunction(call) && allSafe(call.getArguments(), variable);
      default -> false;
    };
  }

  /**
   * Whether a call is one of {@link #PARALLEL_SAFE_FUNCTIONS} and resolves to the built-in: the function factory asks the
   * registry of namespaced functions before its own built-ins, so a function registered under the name would answer instead.
   */
  static boolean isParallelSafeFunction(final FunctionCallExpression call) {
    final String name = call.getFunctionName();
    return !call.isDistinct() && PARALLEL_SAFE_FUNCTIONS.contains(name) && CypherBuiltinFunctions.isBuiltin(name)
        && CypherFunctionRegistry.get(name) == null;
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
