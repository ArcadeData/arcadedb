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
package com.arcadedb.query.opencypher;

import com.arcadedb.function.CypherBuiltinFunctions;
import com.arcadedb.function.CypherFunctionRegistry;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CustomFunctionAdapter;
import com.arcadedb.function.cypher.SQLFunctionBridge;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
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
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.operators.ParallelSafeExpressions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The expressions a parallel label scan evaluates on several workers at once share one instance (issue #8725), so every
 * instance field of them must be final or volatile. A cache added to one of them later fails here instead of racing
 * under a parallel scan. The same holds for the executors of the built-in functions the workers may call (issue #11873):
 * a function call caches its executor, which the workers then share.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ParallelSafeExpressionsStateTest {
  @Test
  void theWhitelistedExpressionsHoldNoUnsafeMutableState() {
    final List<String> unsafe = new ArrayList<>();
    for (final Class<?> type : new Class<?>[] { ArithmeticExpression.class, BooleanCoercionExpression.class,
        BooleanWrapperExpression.class, ComparisonExpression.class, ComparisonExpressionWrapper.class,
        FunctionCallExpression.class, InExpression.class, IsNullExpression.class, ListExpression.class,
        LiteralExpression.class, LogicalExpression.class, ParameterExpression.class, PropertyAccessExpression.class,
        StarExpression.class, StringMatchExpression.class, TernaryLogicalExpression.class, VariableExpression.class })
      for (final Field field : type.getDeclaredFields()) {
        final int modifiers = field.getModifiers();
        if (!Modifier.isStatic(modifiers) && !Modifier.isFinal(modifiers) && !Modifier.isVolatile(modifiers))
          unsafe.add(type.getSimpleName() + "." + field.getName());
      }
    assertThat(unsafe).as("instance fields that are neither final nor volatile").isEmpty();
  }

  /**
   * Every function the workers may call is a scalar built-in the factory resolves to an executor of its own, and that
   * executor, with every ArcadeDB object it holds, has only final or volatile instance fields.
   */
  @Test
  void theWhitelistedFunctionsAreStatelessBuiltIns() throws IllegalAccessException {
    final CypherFunctionFactory factory = new CypherFunctionFactory(DefaultSQLFunctionFactory.getInstance());
    final List<String> problems = new ArrayList<>();
    for (final String name : new TreeSet<>(ParallelSafeExpressions.PARALLEL_SAFE_FUNCTIONS)) {
      if (!CypherBuiltinFunctions.isBuiltin(name))
        problems.add(name + ": not a built-in");
      if (CypherFunctionRegistry.get(name) != null)
        problems.add(name + ": answered by the function registry");
      if (!ParallelSafeExpressions.isParallelSafe(new FunctionCallExpression(name, List.of(), false), "n"))
        problems.add(name + ": not admitted");
      final StatelessFunction function = factory.getFunctionExecutor(name);
      if (function instanceof SQLFunctionBridge || function instanceof CustomFunctionAdapter)
        problems.add(name + ": not resolved to a Cypher built-in but to " + function.getClass().getSimpleName());
      if (function.aggregateResults())
        problems.add(name + ": aggregates");
      collectUnsafeState(name, function, problems, new IdentityHashMap<>());
    }
    assertThat(problems).isEmpty();
  }

  @Test
  void otherCallsAreNotAdmitted() {
    for (final String name : new String[] { "rand", "randomuuid", "timestamp", "date", "datetime", "file", "linenumber",
        "startnode", "endnode", "nodes", "count", "min", "collect", "sql.abs", "my.fn", "text.indexof" })
      assertThat(ParallelSafeExpressions.isParallelSafe(new FunctionCallExpression(name, List.of(), false), "n")).as(name)
          .isFalse();
    assertThat(ParallelSafeExpressions.isParallelSafe(new FunctionCallExpression("abs", List.of(new VariableExpression("m")),
        false), "n")).as("an argument reading another variable").isFalse();
  }

  private static void collectUnsafeState(final String path, final Object value, final List<String> problems,
      final Map<Object, Boolean> visited) throws IllegalAccessException {
    if (value == null || visited.put(value, Boolean.TRUE) != null || !value.getClass().getName().startsWith("com.arcadedb."))
      return;
    for (Class<?> type = value.getClass(); type != null && type != Object.class; type = type.getSuperclass())
      for (final Field field : type.getDeclaredFields()) {
        final int modifiers = field.getModifiers();
        if (Modifier.isStatic(modifiers))
          continue;
        if (!Modifier.isFinal(modifiers) && !Modifier.isVolatile(modifiers))
          problems.add(path + ": " + type.getSimpleName() + "." + field.getName() + " is neither final nor volatile");
        if (!field.getType().isPrimitive() && field.trySetAccessible())
          collectUnsafeState(path + "." + field.getName(), field.get(value), problems, visited);
      }
  }
}
