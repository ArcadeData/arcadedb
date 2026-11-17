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
package com.arcadedb.query.opencypher.rewriter;

import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.OrderByClause;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.VariableExpression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rewrites the ORDER BY items of a collapsing projection that repeat one of the projected
 * expressions so they reference that projection's output column instead (issues #5283, #5286).
 * <p>
 * A DISTINCT or aggregating projection collapses its input rows, so the variables that fed it no
 * longer have a single well-defined value per surviving row. ORDER BY is therefore restricted to the
 * projected columns, and {@code RETURN DISTINCT n.active AS active ORDER BY n.name} is correctly an
 * error. But when the ORDER BY expression <em>is</em> one of the projected expressions, it is
 * constant within every group by construction, so it stays well defined:
 * <pre>
 *   MATCH (n:BugA) RETURN DISTINCT n.active AS active ORDER BY n.active
 *   MATCH (n:BugA) RETURN n.age AS a, count(*) AS c ORDER BY n.age
 * </pre>
 * Both must sort exactly as ordering by the alias does. Neo4j, Memgraph and FalkorDB all accept the
 * DISTINCT form; Neo4j resolves both the same way, by rewriting such an ORDER BY expression to the
 * projection's alias before the scope check runs.
 * <p>
 * Rewriting (rather than merely widening the scope check to let the original variable through) is
 * what makes the sort well defined rather than accidentally correct: the sort then reads the
 * already-projected column instead of re-evaluating the expression against a row that the collapse
 * has changed underneath it. For DISTINCT that row is an arbitrary member of the dedup group; for
 * aggregation the feeding variables are gone from it altogether, which is why ordering by an aliased
 * grouping key used to be silently ignored rather than merely non-deterministic (#5286). It is also
 * cheaper, since it replaces a per-comparison property lookup on the record with a map lookup on the
 * projected row.
 * <p>
 * Identity is decided on the canonical parse-tree text of both sides, so it is insensitive to
 * whitespace ({@code n.age+1} matches a projected {@code n.age + 1}) but never conflates two
 * different expressions. Matching is whole-expression only: an ORDER BY expression that merely
 * <em>contains</em> a projected expression ({@code ORDER BY n.age + 1} against a projected
 * {@code n.age}) is not rewritten, because the AST nodes do not render a canonical per-node text
 * that could be compared subexpression by subexpression.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ProjectedOrderByNormalizer {

  private ProjectedOrderByNormalizer() {
  }

  /**
   * Returns an ORDER BY clause whose items that repeat a projected expression have been replaced by
   * references to the corresponding output column.
   *
   * @param orderBy    the ORDER BY clause to normalize, may be null
   * @param items      the projected items of the RETURN/WITH clause
   * @param collapsing whether the projection collapses its input rows (DISTINCT or aggregating). No
   *                   rewrite is applied otherwise, since a plain projection keeps its input scope
   *                   and its ORDER BY resolves against it unchanged
   *
   * @return the original clause when nothing matched, a rewritten copy otherwise
   */
  public static OrderByClause normalize(final OrderByClause orderBy, final List<ReturnClause.ReturnItem> items,
      final boolean collapsing) {
    if (!collapsing || orderBy == null || orderBy.isEmpty() || items == null || items.isEmpty())
      return orderBy;

    List<OrderByClause.OrderByItem> rewritten = null;
    ProjectedExpressionSubstituter substituter = null;

    final List<OrderByClause.OrderByItem> orderByItems = orderBy.getItems();
    for (int i = 0; i < orderByItems.size(); i++) {
      final OrderByClause.OrderByItem orderByItem = orderByItems.get(i);

      // The whole item repeats a projected expression: sort directly on that output column
      final String outputName = findProjectedOutputName(items, orderByItem.getExpression());
      if (outputName != null) {
        if (rewritten == null)
          rewritten = new ArrayList<>(orderByItems);
        rewritten.set(i,
            new OrderByClause.OrderByItem(outputName, orderByItem.isAscending(), new VariableExpression(outputName)));
        continue;
      }

      // Otherwise the item may still be built out of projected expressions (ORDER BY n.age + 1) or
      // read a property of a projected variable (RETURN n AS node ... ORDER BY n.age)
      if (orderByItem.getExpressionAST() == null)
        continue;

      // An item that mixes an aggregation with a non-aggregated part is ambiguous by specification,
      // even when that part is projected (openCypher ReturnOrderBy6/WithOrderBy4:
      // "Fail if more complex expressions, even if returned, are used inside an order by item which
      // contains an aggregation expression"). Resolving the part here would defeat that diagnosis, so
      // such an item is left exactly as written for the validator to reject.
      if (orderByItem.getExpressionAST().containsAggregation())
        continue;

      if (substituter == null)
        substituter = newSubstituter(items);

      final Object substituted = substituter.rewrite(orderByItem.getExpressionAST());
      if (substituted == orderByItem.getExpressionAST() || !(substituted instanceof Expression))
        continue;

      if (rewritten == null)
        rewritten = new ArrayList<>(orderByItems);

      // The item text is left as written: it names no output column, so ORDER BY falls through to
      // evaluating the substituted AST, which is what carries the resolution
      rewritten.set(i,
          new OrderByClause.OrderByItem(orderByItem.getExpression(), orderByItem.isAscending(), (Expression) substituted));
    }

    return rewritten == null ? orderBy : new OrderByClause(rewritten);
  }

  /**
   * Builds a substituter that maps each projected expression, and each bare-variable projection, onto
   * the output column it is projected as.
   */
  private static ProjectedExpressionSubstituter newSubstituter(final List<ReturnClause.ReturnItem> items) {
    final Map<String, String> outputNameByExpressionText = new HashMap<>();
    final Map<String, String> outputNameByVariable = new HashMap<>();

    for (final ReturnClause.ReturnItem item : items) {
      final Expression expression = item.getExpression();
      if (expression == null || item.isStar())
        continue;

      // First projection of an expression wins, matching the leftmost-column reading of a duplicate
      outputNameByExpressionText.putIfAbsent(expression.getText(), item.getOutputName());
      if (expression instanceof VariableExpression)
        outputNameByVariable.putIfAbsent(((VariableExpression) expression).getVariableName(), item.getOutputName());
    }

    return new ProjectedExpressionSubstituter(outputNameByExpressionText, outputNameByVariable);
  }

  /**
   * Builds a substituter that maps each projected expression onto the output column it is projected as, for an ORDER BY that
   * follows a projection keeping its input rows (issue #12368). Such a projection keeps its input scope, so a property read of a
   * projected variable is left alone: it reads the same record either way, and only a repeated expression saves a read. A
   * projected expression that mentions a name another item's output shadows is left out (see {@link #shadowingOutputNames}).
   */
  public static ProjectedExpressionSubstituter newPlainProjectionSubstituter(final List<ReturnClause.ReturnItem> items) {
    final Set<String> shadowing = shadowingOutputNames(items);
    final Map<String, String> outputNameByExpressionText = new HashMap<>();
    for (final ReturnClause.ReturnItem item : items) {
      final Expression expression = item.getExpression();
      if (expression == null || item.isStar())
        continue;
      final String text = expression.getText();
      if (!mentionsAny(text, shadowing))
        outputNameByExpressionText.putIfAbsent(text, item.getOutputName());
    }
    return new ProjectedExpressionSubstituter(outputNameByExpressionText, Map.of());
  }

  /**
   * The output column of a projection keeping its input rows that holds the value of the whole ORDER BY expression, or null
   * (issue #12368). As {@link #findProjectedOutputName}, and null also when the expression mentions a name another item's output
   * shadows: after {@code RETURN n.y AS n, n.x AS x} the variable {@code n} of {@code ORDER BY n.x} is the column {@code n}, so
   * the key is not the value the projection computed from the variable {@code n}.
   */
  public static String findPlainProjectedOutputName(final List<ReturnClause.ReturnItem> items, final String orderByExpressionText) {
    final String outputName = findProjectedOutputName(items, orderByExpressionText);
    return outputName == null || mentionsAny(orderByExpressionText, shadowingOutputNames(items)) ? null : outputName;
  }

  /**
   * The output names of a projection that hide a variable of the same name from the clauses after it: every output name but the
   * one of a variable projected as itself ({@code RETURN n} or {@code n AS n}). An expression that mentions one of them reads
   * the column after the projection and the variable inside it, so it means two different things there.
   */
  private static Set<String> shadowingOutputNames(final List<ReturnClause.ReturnItem> items) {
    Set<String> names = null;
    for (final ReturnClause.ReturnItem item : items) {
      if (item.isStar())
        continue;
      final String outputName = item.getOutputName();
      if (item.getExpression() instanceof VariableExpression variable && variable.getVariableName().equals(outputName))
        continue;
      if (names == null)
        names = new HashSet<>();
      names.add(outputName);
    }
    return names == null ? Set.of() : names;
  }

  /**
   * Whether the text mentions one of the names as an identifier: a coarse tokenization, which can only over-approximate (a name
   * inside a string literal counts too), and the plain substring for a name that is not a simple identifier.
   */
  private static boolean mentionsAny(final String text, final Set<String> names) {
    if (names.isEmpty() || text == null)
      return false;
    for (final String name : names) {
      if (name == null || name.isEmpty())
        continue;
      if (!isSimpleIdentifier(name)) {
        if (text.contains(name))
          return true;
        continue;
      }
      int from = 0;
      while ((from = text.indexOf(name, from)) >= 0) {
        final int end = from + name.length();
        if ((from == 0 || !isIdentifierChar(text.charAt(from - 1))) && (end == text.length() || !isIdentifierChar(text.charAt(end))))
          return true;
        from = end;
      }
    }
    return false;
  }

  private static boolean isSimpleIdentifier(final String name) {
    for (int i = 0; i < name.length(); i++)
      if (!isIdentifierChar(name.charAt(i)))
        return false;
    return true;
  }

  private static boolean isIdentifierChar(final char c) {
    return Character.isLetterOrDigit(c) || c == '_';
  }

  /**
   * Returns the output column name of the projected item whose expression is the same as the given
   * ORDER BY expression text, or null when no item projects it.
   */
  public static String findProjectedOutputName(final List<ReturnClause.ReturnItem> items, final String orderByExpressionText) {
    if (orderByExpressionText == null)
      return null;

    // An item already referenced by its output name needs no rewrite: ORDER BY resolves it directly. The output names are
    // looked at first, all of them: a projected name shadows the variable it repeats, so in RETURN b AS x, a AS b ORDER BY b the
    // key is the column b, not the column x the variable b was projected as
    for (final ReturnClause.ReturnItem item : items)
      if (orderByExpressionText.equals(item.getOutputName()))
        return null;

    for (final ReturnClause.ReturnItem item : items)
      if (!item.isStar() && orderByExpressionText.equals(item.getExpressionText()))
        return item.getOutputName();
    return null;
  }
}
