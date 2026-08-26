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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.DeleteClause;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.ForeachClause;
import com.arcadedb.query.opencypher.ast.MapProjectionExpression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.OrderByClause;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.RemoveClause;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.SetClause;
import com.arcadedb.query.opencypher.ast.SubqueryClause;
import com.arcadedb.query.opencypher.ast.UnionStatement;
import com.arcadedb.query.opencypher.ast.UnwindClause;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.ast.WithClause;
import com.arcadedb.query.opencypher.parser.CypherExpressionWalker;

import java.util.List;

/**
 * Answers one question for both Cypher executors: is a relationship variable used anywhere outside
 * the pattern that binds it?
 * <p>
 * A relationship variable nobody reads is as good as anonymous, and an anonymous hop can be walked
 * through the CSR adjacency view without materializing a single edge object. Both the step-based
 * executor and the cost-based operators need that answer to pick their fast path, and they have to
 * agree on it: the same query must not traverse edges one way and adjacency ids the other.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CypherVariableUsage {
  private CypherVariableUsage() {
  }

  /**
   * Returns true if the relationship variable is read anywhere outside the pattern that binds it, in
   * which case the executor has to materialize the edge instead of walking adjacency ids.
   * <p>
   * Rather than enumerating every clause type, this method counts how many times
   * the variable appears across all relationship patterns in all MATCH clauses.
   * If it appears more than once, it's a bound reference in another pattern.
   * Then it walks the parsed AST of every other clause (RETURN, WHERE,
   * ORDER BY, WITH, UNWIND, SET, DELETE, CREATE, MERGE) looking for the variable name.
   *
   * @param statement the whole statement the pattern belongs to
   * @param variable  the edge variable name to check
   * @return true if the variable is referenced elsewhere in the query
   */
  public static boolean isEdgeVariableReferenced(final CypherStatement statement, final String variable) {
    // A UNION has no clauses of its own (issue #5671): each branch is checked as one, and a reference in ANY
    // branch keeps the edge binding alive - the same conservative policy documented on the CREATE/MERGE case
    // below. Not currently reachable with a union statement (every call site plans a UNION branch-by-branch
    // before reaching here), but this mirrors innerStatementReferencesVariable's guard below for the same
    // defense-in-depth reason: nothing past this point can answer for a union, and answering wrongly (dropping
    // an edge binding) is worse than the redundant check costs.
    if (statement instanceof UnionStatement union) {
      for (final CypherStatement branch : union.getQueries())
        if (isEdgeVariableReferenced(branch, variable))
          return true;
      return false;
    }

    // 0. RETURN * projects every variable in scope by name, so nothing is unreferenced under it.
    //    Reading only the explicit return items said otherwise and dropped the edge binding, which
    //    made the relationship column disappear from the result.
    if (statement.getReturnClause() != null && statement.getReturnClause().isReturnAll())
      return true;
    if (statement.getClausesInOrder() != null)
      for (final ClauseEntry entry : statement.getClausesInOrder())
        if (entry.getType() == ClauseEntry.ClauseType.RETURN) {
          final ReturnClause returnClause = entry.getTypedClause();
          if (returnClause != null && returnClause.isReturnAll())
            return true;
        }

    // 1. Check if the variable appears as a relationship variable in OTHER MATCH patterns
    //    (e.g., MATCH ()-[r:E]-() MATCH p = ()-[r]-() — r is bound in the second MATCH)
    int relVarCount = 0;
    for (final MatchClause match : statement.getMatchClauses()) {
      if (match.hasPathPatterns()) {
        for (final PathPattern path : match.getPathPatterns()) {
          for (final NodePattern node : path.getNodes())
            if (node.hasWhereExpression()
                && expressionReferencesVariable(node.getWhereExpression(), variable))
              return true;
          for (int i = 0; i < path.getRelationshipCount(); i++) {
            final RelationshipPattern rel = path.getRelationship(i);
            if (variable.equals(rel.getVariable()))
              relVarCount++;
            // An inline WHERE inside a pattern reads the edge, e.g. -[r:E WHERE r.tag = 'ok']->.
            // The predicate lives inside the binding pattern, so the scans below never saw it and the
            // binding was dropped whenever nothing else in the query mentioned r. The predicate then
            // evaluated against an unbound variable and silently filtered out every row (issue #5464).
            if (rel.hasWhereExpression()
                && expressionReferencesVariable(rel.getWhereExpression(), variable))
              return true;
          }
        }
      }
    }
    // If the variable appears in more than one relationship pattern, it's referenced elsewhere
    if (relVarCount > 1)
      return true;

    // 2. Check all expressions from non-MATCH clauses, walking their parsed AST rather than scanning
    //    Expression#getText(): ANTLR's default getText() concatenates token text with no separating
    //    whitespace (e.g. "any(item IN r0.k8 WHERE ...)" becomes "any(itemINr0.k8WHERE...)"), which
    //    silently defeats a word-boundary text scan whenever a keyword ends right where the variable
    //    begins - "r0" no longer starts a word once it is glued to the "N" of "IN" (issue #6567).
    //    Use clausesInOrder to cover everything: RETURN, WHERE, WITH, UNWIND, SET, DELETE, etc.
    if (statement.getClausesInOrder() != null) {
      for (final ClauseEntry entry : statement.getClausesInOrder()) {
        switch (entry.getType()) {
        case RETURN: {
          final ReturnClause rc = entry.getTypedClause();
          for (final ReturnClause.ReturnItem item : rc.getReturnItems())
            if (expressionReferencesVariable(item.getExpression(), variable))
              return true;
          break;
        }
        case WITH: {
          final WithClause wc = entry.getTypedClause();
          for (final ReturnClause.ReturnItem item : wc.getItems())
            if (expressionReferencesVariable(item.getExpression(), variable))
              return true;
          if (wc.getWhereClause() != null && wc.getWhereClause().getConditionExpression() != null)
            if (expressionReferencesVariable(wc.getWhereClause().getConditionExpression(), variable))
              return true;
          break;
        }
        case UNWIND: {
          final UnwindClause uc = entry.getTypedClause();
          if (expressionReferencesVariable(uc.getListExpression(), variable))
            return true;
          break;
        }
        case SET: {
          // The edge variable can be the assignment target (SET r.prop = ...) or appear inside the
          // value/target expression (SET u.x = CASE WHEN r IS NOT NULL ...). Checking only the target
          // variable dropped the edge binding when it was used solely on the right-hand side (issue #5137).
          final SetClause sc = entry.getTypedClause();
          for (final SetClause.SetItem item : sc.getItems()) {
            if (variable.equals(item.getVariable()))
              return true;
            if (item.getValueExpression() != null
                && expressionReferencesVariable(item.getValueExpression(), variable))
              return true;
            if (item.getTargetExpression() != null
                && expressionReferencesVariable(item.getTargetExpression(), variable))
              return true;
          }
          break;
        }
        case REMOVE: {
          // REMOVE r.prop / REMOVE r:Label keeps the edge binding alive. Missing this dropped the
          // edge binding, so a top-level REMOVE on a relationship property silently found no edge
          // and became a no-op unless the edge was also projected through WITH (issue #5013).
          final RemoveClause rc = entry.getTypedClause();
          for (final RemoveClause.RemoveItem item : rc.getItems())
            if (variable.equals(item.getVariable()))
              return true;
          break;
        }
        case DELETE: {
          final DeleteClause dc = entry.getTypedClause();
          if (deleteReferencesVariable(dc, variable))
            return true;
          break;
        }
        case FOREACH: {
          // FOREACH can reference the edge variable in its list expression (e.g. FOREACH (x IN [r] | ...))
          // or inside any of its inner write clauses. Missing this dropped the edge binding, so DELETE
          // inside FOREACH silently found no edge (issue #4912).
          final ForeachClause fc = entry.getTypedClause();
          if (foreachReferencesVariable(fc, variable))
            return true;
          break;
        }
        case SUBQUERY: {
          // A scoped CALL (r) { ... } imports the edge variable, and CALL { WITH r ... } references it in
          // the inner statement. Missing this dropped the edge binding, so DELETE inside a CALL subquery
          // silently found no edge (issue #4913).
          final SubqueryClause sq = entry.getTypedClause();
          if (subqueryReferencesVariable(sq, variable))
            return true;
          break;
        }
        case CREATE:
        case MERGE:
          // Inline property expressions inside a top-level CREATE/MERGE pattern may reference the
          // variable (e.g. CREATE (c {prop: r.x})). Enumerating them cheaply is awkward, so stay
          // conservative like foreachReferencesVariable's own CREATE/MERGE case: keep the edge binding.
          // A false positive only forgoes the CSR/GAV fast path; a false negative would silently drop
          // a still-referenced edge (the class of bug this method exists to prevent - issue #6573).
          return true;
        default:
          break;
        }
      }
    }

    // 3. Check statement-level WHERE, ORDER BY (may not be in clausesInOrder)
    if (statement.getWhereClause() != null && statement.getWhereClause().getConditionExpression() != null)
      if (expressionReferencesVariable(statement.getWhereClause().getConditionExpression(), variable))
        return true;

    for (final MatchClause match : statement.getMatchClauses())
      if (match.hasWhereClause() && match.getWhereClause().getConditionExpression() != null)
        if (expressionReferencesVariable(match.getWhereClause().getConditionExpression(), variable))
          return true;

    if (statement.getOrderByClause() != null)
      for (final OrderByClause.OrderByItem item : statement.getOrderByClause().getItems())
        // The AST is preferred over getExpression()'s text for the same reason as everywhere else in
        // this method; fall back to the legacy text scan only for the rare item that kept no AST.
        if (item.getExpressionAST() != null
            ? expressionReferencesVariable(item.getExpressionAST(), variable)
            : expressionReferencesVariable(item.getExpression(), variable))
          return true;

    // 4. Check RETURN clause (may not be in clausesInOrder for simple queries)
    if (statement.getReturnClause() != null)
      for (final ReturnClause.ReturnItem item : statement.getReturnClause().getReturnItems())
        if (expressionReferencesVariable(item.getExpression(), variable))
          return true;

    // 5. Check UNWIND clauses (may not be in clausesInOrder for legacy path)
    for (final UnwindClause uc : statement.getUnwindClauses())
      if (expressionReferencesVariable(uc.getListExpression(), variable))
        return true;

    return false;
  }

  /**
   * Checks whether a FOREACH clause references the given variable, either in its list expression
   * or inside any of its inner write clauses (recursively for nested FOREACH). Used to keep the
   * edge binding alive when a FOREACH consumes it (issue #4912).
   */
  private static boolean foreachReferencesVariable(final ForeachClause foreachClause, final String variable) {
    if (foreachClause == null)
      return false;
    if (foreachClause.getListExpression() != null
        && expressionReferencesVariable(foreachClause.getListExpression(), variable))
      return true;
    if (foreachClause.getInnerClauses() != null) {
      for (final ClauseEntry inner : foreachClause.getInnerClauses()) {
        switch (inner.getType()) {
        case DELETE:
          if (deleteReferencesVariable(inner.getTypedClause(), variable))
            return true;
          break;
        case SET: {
          final SetClause sc = inner.getTypedClause();
          for (final SetClause.SetItem item : sc.getItems()) {
            if (variable.equals(item.getVariable()))
              return true;
            if (item.getValueExpression() != null
                && expressionReferencesVariable(item.getValueExpression(), variable))
              return true;
            if (item.getTargetExpression() != null
                && expressionReferencesVariable(item.getTargetExpression(), variable))
              return true;
          }
          break;
        }
        case REMOVE: {
          final RemoveClause rc = inner.getTypedClause();
          for (final RemoveClause.RemoveItem item : rc.getItems())
            if (variable.equals(item.getVariable()))
              return true;
          break;
        }
        case FOREACH:
          if (foreachReferencesVariable(inner.getTypedClause(), variable))
            return true;
          break;
        case CREATE:
        case MERGE:
          // Inline property expressions inside CREATE/MERGE patterns may reference the variable.
          // Enumerating them cheaply is awkward, so stay conservative: keep the edge binding.
          // A false positive only forgoes the GAV/CSR fast path; a false negative would silently
          // drop a still-referenced edge (the class of bug this method exists to prevent).
          return true;
        default:
          break;
        }
      }
    }
    return false;
  }

  /**
   * Checks whether a scoped CALL subquery references the given variable, either because the variable is
   * imported via the explicit scope list {@code CALL (r) { ... }} or because the inner statement references
   * it (e.g. {@code CALL { WITH r ... DELETE r }}). Used to keep the edge binding alive when a CALL subquery
   * consumes it (issue #4913).
   */
  private static boolean subqueryReferencesVariable(final SubqueryClause subqueryClause, final String variable) {
    if (subqueryClause == null)
      return false;
    // Explicit scope: CALL (r) { ... } imports r from the outer row.
    final List<String> scope = subqueryClause.getScopeVariables();
    if (scope != null && scope.contains(variable))
      return true;
    // Otherwise inspect the inner statement's clauses (e.g. CALL { WITH r ... DELETE r }).
    return innerStatementReferencesVariable(subqueryClause.getInnerStatement(), variable);
  }

  /**
   * A UNION has no clauses of its own (issue #5671): each branch is its own statement, so it is checked as
   * one. A reference in <b>any</b> branch keeps the edge binding alive, matching the conservative,
   * false-positive-only-costs-the-fast-path policy documented on the CREATE/MERGE case above.
   */
  private static boolean innerStatementReferencesVariable(final CypherStatement inner, final String variable) {
    if (inner == null)
      return false;
    if (inner instanceof UnionStatement union) {
      for (final CypherStatement branch : union.getQueries())
        if (innerStatementReferencesVariable(branch, variable))
          return true;
      return false;
    }
    if (inner.getClausesInOrder() == null)
      return false;
    for (final ClauseEntry entry : inner.getClausesInOrder()) {
      switch (entry.getType()) {
      case WITH: {
        final WithClause wc = entry.getTypedClause();
        for (final ReturnClause.ReturnItem item : wc.getItems())
          if (expressionReferencesVariable(item.getExpression(), variable))
            return true;
        if (wc.getWhereClause() != null && wc.getWhereClause().getConditionExpression() != null
            && expressionReferencesVariable(wc.getWhereClause().getConditionExpression(), variable))
          return true;
        break;
      }
      case UNWIND:
        if (expressionReferencesVariable(((UnwindClause) entry.getTypedClause()).getListExpression(), variable))
          return true;
        break;
      case SET: {
        final SetClause sc = entry.getTypedClause();
        for (final SetClause.SetItem item : sc.getItems()) {
          if (variable.equals(item.getVariable()))
            return true;
          if (item.getValueExpression() != null
              && expressionReferencesVariable(item.getValueExpression(), variable))
            return true;
          if (item.getTargetExpression() != null
              && expressionReferencesVariable(item.getTargetExpression(), variable))
            return true;
        }
        break;
      }
      case REMOVE: {
        final RemoveClause rc = entry.getTypedClause();
        for (final RemoveClause.RemoveItem item : rc.getItems())
          if (variable.equals(item.getVariable()))
            return true;
        break;
      }
      case DELETE:
        if (deleteReferencesVariable(entry.getTypedClause(), variable))
          return true;
        break;
      case RETURN: {
        final ReturnClause rc = entry.getTypedClause();
        for (final ReturnClause.ReturnItem item : rc.getReturnItems())
          if (expressionReferencesVariable(item.getExpression(), variable))
            return true;
        break;
      }
      case FOREACH:
        if (foreachReferencesVariable(entry.getTypedClause(), variable))
          return true;
        break;
      case SUBQUERY:
        if (subqueryReferencesVariable(entry.getTypedClause(), variable))
          return true;
        break;
      default:
        break;
      }
    }
    return false;
  }


  /** Checks whether a DELETE clause references the given variable, by name or within a target expression. */
  private static boolean deleteReferencesVariable(final DeleteClause deleteClause, final String variable) {
    if (deleteClause.getVariables().contains(variable))
      return true;
    final List<Expression> expressions = deleteClause.getExpressions();
    if (expressions != null)
      for (final Expression expression : expressions)
        if (expression != null && expressionReferencesVariable(expression, variable))
          return true;
    return false;
  }

  /**
   * Checks if an expression text references a variable as a standalone identifier.
   * Uses word-boundary matching to avoid false positives (e.g., "relation" matching "r").
   * <p>
   * Kept only for text that never had a parsed AST to walk instead. Prefer
   * {@link #expressionReferencesVariable(Expression, String)} or
   * {@link #expressionReferencesVariable(BooleanExpression, String)} wherever an AST is available: this
   * scan runs against {@link Expression#getText()}, which is ANTLR's default {@code getText()} - it
   * concatenates token text with no separating whitespace, so a keyword sitting right before the
   * variable in the source (e.g. {@code any(item IN r0.k8 ...)} becoming {@code any(itemINr0.k8...)})
   * glues onto it and silently defeats the word-boundary check below (issue #6567).
   */
  public static boolean expressionReferencesVariable(final String expressionText, final String variable) {
    if (expressionText == null || variable == null)
      return false;
    // Find the variable as a standalone identifier (not part of a longer word)
    int idx = 0;
    while ((idx = expressionText.indexOf(variable, idx)) >= 0) {
      final boolean startOk = idx == 0 || !Character.isLetterOrDigit(expressionText.charAt(idx - 1))
          && expressionText.charAt(idx - 1) != '_';
      final int end = idx + variable.length();
      final boolean endOk = end >= expressionText.length() || !Character.isLetterOrDigit(expressionText.charAt(end))
          && expressionText.charAt(end) != '_';
      if (startOk && endOk)
        return true;
      idx++;
    }
    return false;
  }

  /**
   * Checks if {@code expression} reads {@code variable} anywhere within it - a plain variable read, a
   * property access, or a map-projection base - by walking the parsed AST with
   * {@link CypherExpressionWalker} instead of scanning {@link Expression#getText()}. See
   * {@link #expressionReferencesVariable(String, String)} for why the text scan is unreliable.
   */
  public static boolean expressionReferencesVariable(final Expression expression, final String variable) {
    if (expression == null || variable == null)
      return false;
    final VariableReferenceFinder finder = new VariableReferenceFinder(variable);
    CypherExpressionWalker.walk(expression, finder);
    return finder.found;
  }

  /** {@link #expressionReferencesVariable(Expression, String)}, for a WHERE-clause predicate. */
  public static boolean expressionReferencesVariable(final BooleanExpression expression, final String variable) {
    if (expression == null || variable == null)
      return false;
    final VariableReferenceFinder finder = new VariableReferenceFinder(variable);
    CypherExpressionWalker.walk(expression, finder);
    return finder.found;
  }

  /**
   * A {@link CypherExpressionWalker.Visitor} that stops at the first read of one target variable name,
   * as a plain variable, a property access, a map-projection base, or a re-mention inside a nested
   * MATCH/CREATE/MERGE pattern. A nested subquery body (a {@code CALL}, or the parsed body of an
   * {@code EXISTS}/{@code COUNT}/{@code COLLECT}) auto-correlates to the names in scope where it was
   * written, so the walk keeps using this same finder inside it (the default
   * {@link CypherExpressionWalker.Visitor#forNestedStatement}) - reading the outer name again in there,
   * whether as an expression or as a pattern variable it re-binds to, is still reading it. Missing the
   * pattern case dropped the edge binding for {@code WHERE EXISTS { MATCH (p)-[r:KNOWS]->(x) ... } } -
   * the body's own MATCH re-mentions the outer "r" only as a relationship-pattern variable, which
   * {@link #visit(Expression)} never sees (issue #6567 review).
   */
  private static final class VariableReferenceFinder implements CypherExpressionWalker.Visitor {
    private final String  variable;
    private       boolean found = false;

    VariableReferenceFinder(final String variable) {
      this.variable = variable;
    }

    @Override
    public void visit(final Expression expression) {
      if (found)
        return;
      if (expression instanceof VariableExpression variableExpression)
        found = variable.equals(variableExpression.getVariableName());
      else if (expression instanceof PropertyAccessExpression property)
        found = variable.equals(property.getVariableName());
      else if (expression instanceof MapProjectionExpression projection)
        found = variable.equals(projection.getVariableName());
      // GENERIC CASE: not a name-carrying node; the walk still descends into its children.
    }

    @Override
    public void visitPattern(final PathPattern pattern) {
      if (found)
        return;
      if (variable.equals(pattern.getPathVariable())) {
        found = true;
        return;
      }
      if (pattern.getNodes() != null)
        for (final NodePattern node : pattern.getNodes())
          if (node != null && variable.equals(node.getVariable())) {
            found = true;
            return;
          }
      if (pattern.getRelationships() != null)
        for (final RelationshipPattern relationship : pattern.getRelationships())
          if (relationship != null && variable.equals(relationship.getVariable())) {
            found = true;
            return;
          }
    }
  }
}
