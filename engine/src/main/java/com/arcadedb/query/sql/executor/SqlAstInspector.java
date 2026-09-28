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
package com.arcadedb.query.sql.executor;

import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.query.sql.method.DefaultSQLMethodFactory;
import com.arcadedb.query.sql.parser.FunctionCall;
import com.arcadedb.query.sql.parser.MethodCall;
import com.arcadedb.query.sql.parser.SimpleNode;
import com.arcadedb.query.sql.parser.Statement;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Answers questions about everything a parsed SQL fragment can run - every function and method call, every nested
 * statement - by walking the whole tree reflectively, so a call nested anywhere (a projection, a WHERE, a
 * FROM-subquery, a nested LET) is seen without every node type having to implement a visitor.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class SqlAstInspector {
  /** Read-only graph traversal methods, resolved at run time as the built-in graph functions of the same name. */
  static final Set<String> GRAPH_METHODS = Set.of("out", "in", "both", "oute", "ine", "bothe", "outv", "inv", "bothv");

  private static final ClassValue<Field[]> AST_FIELDS = new ClassValue<>() {
    @Override
    protected Field[] computeValue(final Class<?> type) {
      final List<Field> fields = new ArrayList<>();
      for (Class<?> c = type; c != null && SimpleNode.class.isAssignableFrom(c); c = c.getSuperclass())
        for (final Field f : c.getDeclaredFields()) {
          // TRANSIENT FIELDS ARE CACHES DERIVED FROM THE AST AT RUN TIME, NOT PART OF IT: WHAT THEY CACHE IS WALKED WHERE IT
          // COMES FROM. WALKING THEM WOULD MAKE THE ANSWER DEPEND ON WHETHER THE FRAGMENT RAN BEFORE (A NAMELESS SENTINEL
          // FunctionCall CACHED BY BaseExpression TURNED A CACHED PLAN SEQUENTIAL AFTER ITS FIRST EXECUTION, #8523)
          if (Modifier.isStatic(f.getModifiers()) || Modifier.isTransient(f.getModifiers()) || f.getType().isPrimitive()
              || f.getType() == String.class)
            continue;
          f.setAccessible(true);
          fields.add(f);
        }
      return fields.toArray(new Field[0]);
    }
  };

  private SqlAstInspector() {
  }

  /**
   * Whether every node reachable from {@code root} - through AST fields, collections, maps and arrays - satisfies
   * {@code test}. A {@code null} root trivially does.
   */
  public static boolean allNodesMatch(final Object root, final Predicate<SimpleNode> test) {
    return allNodesMatch(root, test, new IdentityHashMap<>());
  }

  private static boolean allNodesMatch(final Object node, final Predicate<SimpleNode> test, final IdentityHashMap<Object, Boolean> visited) {
    if (node instanceof SimpleNode simpleNode) {
      if (visited.put(simpleNode, Boolean.TRUE) != null)
        return true;
      if (!test.test(simpleNode))
        return false;
      for (final Field f : AST_FIELDS.get(simpleNode.getClass())) {
        final Object value;
        try {
          value = f.get(simpleNode);
        } catch (final IllegalAccessException e) {
          return false;
        }
        if (value != null && !allNodesMatch(value, test, visited))
          return false;
      }
    } else if (node instanceof Collection<?> collection) {
      for (final Object item : collection)
        if (!allNodesMatch(item, test, visited))
          return false;
    } else if (node instanceof Map<?, ?> map) {
      for (final Map.Entry<?, ?> entry : map.entrySet())
        if (!allNodesMatch(entry.getKey(), test, visited) || !allNodesMatch(entry.getValue(), test, visited))
          return false;
    } else if (node instanceof Object[] array) {
      for (final Object item : array)
        if (!allNodesMatch(item, test, visited))
          return false;
    }
    return true;
  }

  /**
   * Whether the fragments can be evaluated on several threads at once, each on its own copy of them (issue #8523):
   * every call in them is one the engine ships - a user-defined function, a function library or a polyglot script
   * promises nothing about concurrent use - and every nested statement is read-only.
   */
  public static boolean isParallelSafe(final Object... fragments) {
    for (final Object fragment : fragments)
      if (!allNodesMatch(fragment, SqlAstInspector::isParallelSafeNode))
        return false;
    return true;
  }

  private static boolean isParallelSafeNode(final SimpleNode node) {
    if (node instanceof Statement statement)
      return statement.isIdempotent();

    if (node instanceof FunctionCall call) {
      if (call.getName() == null)
        return false;
      return DefaultSQLFunctionFactory.getInstance().isBuiltIn(call.getName().getStringValue().toLowerCase(Locale.ENGLISH));
    }

    if (node instanceof MethodCall call)
      return isBuiltInMethod(call);

    return true;
  }

  /** A method the engine ships, or a graph traversal method (resolved as the built-in function of the same name). */
  static boolean isBuiltInMethod(final MethodCall call) {
    if (call.methodName == null)
      return false;
    final String lower = call.methodName.getStringValue().toLowerCase(Locale.ENGLISH);
    return GRAPH_METHODS.contains(lower) || DefaultSQLMethodFactory.getInstance().isBuiltIn(lower);
  }
}
