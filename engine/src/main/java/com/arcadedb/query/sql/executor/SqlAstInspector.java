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
import com.arcadedb.query.sql.parser.AlterTypeStatement;
import com.arcadedb.query.sql.parser.CaseAlternative;
import com.arcadedb.query.sql.parser.CreateIndexStatement;
import com.arcadedb.query.sql.parser.CreateTimeSeriesTypeStatement;
import com.arcadedb.query.sql.parser.FunctionCall;
import com.arcadedb.query.sql.parser.InsertSetExpression;
import com.arcadedb.query.sql.parser.JsonItem;
import com.arcadedb.query.sql.parser.MethodCall;
import com.arcadedb.query.sql.parser.NamedParameter;
import com.arcadedb.query.sql.parser.OrderByItem;
import com.arcadedb.query.sql.parser.Pattern;
import com.arcadedb.query.sql.parser.PositionalParameter;
import com.arcadedb.query.sql.parser.SimpleNode;
import com.arcadedb.query.sql.parser.Statement;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
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
      for (Class<?> c = type; c != null && isAstClass(c); c = c.getSuperclass())
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
   * The plain holders the parser builds between nodes without making them nodes - an {@code INSERT ... SET} pair, a
   * {@code CASE} alternative, an {@code ORDER BY} item, a JSON item, a MATCH pattern, an ALTER TYPE item (whose
   * {@code CUSTOM} value is an expression evaluated by the DDL), a CREATE INDEX property, a time series column. A walk that stopped at them would
   * not see {@code INSERT INTO T SET a = (CREATE TYPE X)} (issue #9628). An explicit list rather than "anything in the
   * parser package", so a helper or cache kept in a node's field is never walked; {@code SqlAstHolderClassesTest} fails
   * when a node gains a field of a parser class that is neither a node nor listed here.
   */
  static final Set<Class<?>> AST_HOLDER_CLASSES = Set.of(InsertSetExpression.class, CaseAlternative.class, OrderByItem.class,
      JsonItem.class, Pattern.class, AlterTypeStatement.Item.class, CreateIndexStatement.Property.class,
      CreateTimeSeriesTypeStatement.ColumnDef.class);

  private static boolean isAstClass(final Class<?> type) {
    return SimpleNode.class.isAssignableFrom(type) || AST_HOLDER_CLASSES.contains(type);
  }

  private static boolean isAstObject(final Object node) {
    return node instanceof SimpleNode || (node != null && isAstClass(node.getClass()));
  }

  /**
   * Whether every node reachable from {@code root} - through AST fields, collections, maps and arrays - satisfies
   * {@code test}. A {@code null} root trivially does.
   */
  public static boolean allNodesMatch(final Object root, final Predicate<SimpleNode> test) {
    return allNodesMatch(root, test, new IdentityHashMap<>());
  }

  private static boolean allNodesMatch(final Object node, final Predicate<SimpleNode> test, final IdentityHashMap<Object, Boolean> visited) {
    if (isAstObject(node)) {
      if (visited.put(node, Boolean.TRUE) != null)
        return true;
      if (node instanceof SimpleNode simpleNode && !test.test(simpleNode))
        return false;
      for (final Field f : AST_FIELDS.get(node.getClass())) {
        final Object value;
        try {
          value = f.get(node);
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
   * Hands {@code visitor} every statement nested in {@code root}'s tree - a parenthesized statement in an expression, a
   * LET's right-hand side, a FROM-subquery, a block's body - together with the statement that directly encloses it
   * (issue #9628). The visitor answers whether the walk continues into that statement: a statement that only stores
   * what it holds (a DDL defining a view or a trigger) does not run it. Each statement's {@code originalStatement} is a
   * cache pointer to itself or to the text it was copied from, not a statement it runs, so it is never reported.
   * <p>
   * Iterative, not recursive: a parsed expression chain can be thousands of nodes deep (issue #9148), and this runs on
   * every classification of a new statement, on whatever stack the caller has.
   *
   * @return false when a field could not be read, so the walk may have missed a statement: the caller must then assume the
   * worst, as {@link #allNodesMatch} does by answering false
   */
  public static boolean forEachNestedStatement(final Statement root, final BiPredicate<Statement, Statement> visitor) {
    final IdentityHashMap<Object, Boolean> visited = new IdentityHashMap<>();
    final ArrayDeque<Object> nodes = new ArrayDeque<>();
    final ArrayDeque<Statement> enclosing = new ArrayDeque<>();
    visited.put(root, Boolean.TRUE);
    if (!pushFields(root, root, nodes, enclosing, visited))
      return false;

    while (!nodes.isEmpty()) {
      final Object node = nodes.pop();
      final Statement parent = enclosing.pop();
      if (node instanceof Collection<?> collection) {
        for (final Object item : collection)
          push(item, parent, nodes, enclosing);
      } else if (node instanceof Map<?, ?> map) {
        for (final Map.Entry<?, ?> entry : map.entrySet()) {
          push(entry.getKey(), parent, nodes, enclosing);
          push(entry.getValue(), parent, nodes, enclosing);
        }
      } else if (node instanceof Object[] array) {
        for (final Object item : array)
          push(item, parent, nodes, enclosing);
      } else if (isAstObject(node) && visited.put(node, Boolean.TRUE) == null) {
        if (node instanceof Statement statement) {
          if (visitor.test(statement, parent) && !pushFields(statement, statement, nodes, enclosing, visited))
            return false;
        } else if (!pushFields(node, parent, nodes, enclosing, visited))
          return false;
      }
    }
    return true;
  }

  private static void push(final Object node, final Statement parent, final ArrayDeque<Object> nodes, final ArrayDeque<Statement> enclosing) {
    if (node == null)
      return;
    nodes.push(node);
    enclosing.push(parent);
  }

  private static boolean pushFields(final Object node, final Statement parent, final ArrayDeque<Object> nodes,
      final ArrayDeque<Statement> enclosing, final IdentityHashMap<Object, Boolean> visited) {
    if (node instanceof Statement statement && statement.originalStatement != null)
      visited.putIfAbsent(statement.originalStatement, Boolean.TRUE);
    for (final Field f : AST_FIELDS.get(node.getClass())) {
      final Object value;
      try {
        value = f.get(node);
      } catch (final IllegalAccessException e) {
        return false;
      }
      push(value, parent, nodes, enclosing);
    }
    return true;
  }

  /**
   * Suffix for a plan-cache key: empty when the parameters are numbered 0, 1, 2... as in a statement parsed alone, else their
   * numbers, because the printed text hides them (issue #9247). Walk order is stable within a JVM only: never persist or
   * compare the key across nodes.
   */
  public static String parameterNumbersSuffix(final Object root) {
    final int[][] holder = { new int[4] };
    final int[] size = { 0 };
    allNodesMatch(root, node -> {
      final int number;
      if (node instanceof PositionalParameter p)
        number = p.paramNumber;
      else if (node instanceof NamedParameter p)
        number = p.paramNumber;
      else
        return true;
      if (size[0] == holder[0].length)
        holder[0] = Arrays.copyOf(holder[0], size[0] * 2);
      holder[0][size[0]++] = number;
      return true;
    });
    final int[] numbers = holder[0];
    final int count = size[0];
    boolean sequential = true;
    for (int i = 0; i < count && sequential; i++)
      sequential = numbers[i] == i;
    if (sequential)
      return "";
    final StringBuilder builder = new StringBuilder(" /*params:");
    for (int i = 0; i < count; i++)
      builder.append(i > 0 ? "," : "").append(numbers[i]);
    return builder.append("*/").toString();
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
