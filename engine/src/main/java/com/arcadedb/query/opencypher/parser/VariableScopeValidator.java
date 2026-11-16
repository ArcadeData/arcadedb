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
package com.arcadedb.query.opencypher.parser;

import com.arcadedb.query.opencypher.parser.CypherSemanticValidator.VarType;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.opencypher.ast.*;
import java.util.*;

/**
 * The scope phase of the semantic validation: every variable an expression reads must be in scope where it is read.
 * It follows the scope clause by clause - what a WITH forwards, what a subquery imports, what a FOREACH or a list
 * comprehension binds locally, what an ORDER BY can still see after an aggregation or a DISTINCT - and rejects a name
 * that is undefined, or one that shadows a name it may not.
 * <p>
 * Split out of {@link CypherSemanticValidator}, which runs the phases of the validation in order.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class VariableScopeValidator {
  private VariableScopeValidator() {
  }

  static void validateVariableScope(final CypherStatement statement) {
    validateVariableScope(statement, new HashSet<>(), Set.of());
  }

  /**
   * Validates variable references in {@code statement} against {@code scope}, the set of variables
   * visible when the statement begins. Top-level statements start with an empty scope; a CALL
   * subquery body starts with only the variables it imports (explicit {@code CALL (v) { ... }} scope
   * list, {@code CALL (*)}, or an importing {@code WITH}).
   * <p>
   * {@code shadowed} holds the names declared in an enclosing scope that this subquery body did NOT
   * import. Such a name is not a free identifier the body may re-declare: a CREATE/MERGE pattern that
   * binds it would silently mint a fresh anonymous vertex instead of writing against the outer entity,
   * so it is rejected as undefined.
   */
  static void validateVariableScope(final CypherStatement statement, final Set<String> scope,
      final Set<String> shadowed) {
    final List<ClauseEntry> clausesInOrder = statement.getClausesInOrder();
    if (clausesInOrder == null || clausesInOrder.isEmpty())
      return; // Can't validate scope without clause ordering

    for (final ClauseEntry entry : clausesInOrder) {
      switch (entry.getType()) {
        case MATCH:
          final MatchClause matchClause = entry.getTypedClause();
          if (matchClause.hasPathPatterns()) {
            for (final PathPattern path : matchClause.getPathPatterns())
              VariableBindingValidator.addBoundVarsFromPattern(path, scope);
            for (final PathPattern path : matchClause.getPathPatterns())
              checkGroupWhereScope(path, scope);
          }
          // Validate WHERE clause references against scope
          if (matchClause.getWhereClause() != null)
            checkBooleanExpressionScope(matchClause.getWhereClause().getConditionExpression(), scope);
          break;
        case CREATE:
          validateCreateClauseScope(entry.getTypedClause(), scope, shadowed);
          break;
        case MERGE:
          validateMergeClauseScope(entry.getTypedClause(), scope, shadowed);
          break;
        case UNWIND:
          final UnwindClause unwindClause = entry.getTypedClause();
          // The list expression in UNWIND may reference outer scope variables
          // but the unwind variable itself is new
          scope.add(unwindClause.getVariable());
          break;
        case LOAD_CSV:
          final LoadCSVClause loadCSVClause = entry.getTypedClause();
          scope.add(loadCSVClause.getVariable());
          break;
        case WITH:
          // WITH resets scope to only projected aliases
          final WithClause withClause = entry.getTypedClause();
          // Check for WITH * — passes all variables through
          boolean hasWildcard = false;
          for (final ReturnClause.ReturnItem item : withClause.getItems()) {
            if (item.isStar()) {
              hasWildcard = true;
              break;
            }
          }
          if (hasWildcard) {
            // WITH * keeps all variables in scope.
            // First validate all extra item expressions against the incoming scope (no scope
            // mutation during validation — aliases must not be visible to sibling expressions).
            for (final ReturnClause.ReturnItem item : withClause.getItems()) {
              if (item.isStar())
                continue;
              checkExpressionScope(item.getExpression(), scope);
            }
            // WITH * keeps every variable, so its ORDER BY is held to the incoming scope widened with
            // the extra aliases - the same rule as the projecting form below. Nothing narrows here, so
            // the only way to fail it is to name a variable no clause ever bound, which used to sort on
            // nothing instead of reporting (found by the #7426 sweep).
            if (withClause.getOrderByClause() != null)
              validatePlainOrderByScope(withClause.getItems(), withClause.getOrderByClause(), scope);
            // Then add extra aliases to scope for subsequent clauses
            for (final ReturnClause.ReturnItem item : withClause.getItems()) {
              final String alias = item.getAlias();
              if (alias != null)
                scope.add(alias);
            }
            break;
          }
          // First validate that all referenced variables in WITH are in scope
          for (final ReturnClause.ReturnItem item : withClause.getItems())
            checkExpressionScope(item.getExpression(), scope);
          // Build the scope for ORDER BY
          if (withClause.getOrderByClause() != null) {
            if (withClause.hasAggregations() || withClause.isDistinct()) {
              // A collapsing WITH restricts ORDER BY to the projected columns, exactly as the
              // matching RETURN form does (issues #5286, #5287)
              validateCollapsedOrderByScope(withClause.getItems(), withClause.getOrderByClause(),
                  withClause.hasAggregations());
            } else
              // Non-aggregating WITH: ORDER BY can reference both original scope + aliases
              validatePlainOrderByScope(withClause.getItems(), withClause.getOrderByClause(), scope);
          }
          // Reset scope to only projected aliases
          scope.clear();
          for (final ReturnClause.ReturnItem item : withClause.getItems()) {
            final String alias = item.getAlias();
            if (alias != null)
              scope.add(alias);
            else if (item.getExpression() instanceof VariableExpression)
              scope.add(((VariableExpression) item.getExpression()).getVariableName());
          }
          break;
        case SET:
          validateSetClauseScope(entry.getTypedClause(), scope);
          break;
        case REMOVE:
          validateRemoveClauseScope(entry.getTypedClause(), scope);
          break;
        case DELETE:
          validateDeleteClauseScope(entry.getTypedClause(), scope);
          break;
        case RETURN:
          // Validate RETURN references — only check top-level variable references
          // to avoid false positives from complex expression types that the AST builder
          // creates as VariableExpression with raw text
          if (statement.getReturnClause() != null) {
            for (final ReturnClause.ReturnItem item : statement.getReturnClause().getReturnItems())
              checkExpressionScope(item.getExpression(), scope);
            // Validate ORDER BY scope
            if (statement.getOrderByClause() != null) {
              if (hasStar(statement.getReturnClause().getReturnItems())) {
                // RETURN *, ... projects every variable in scope, so even a DISTINCT or an aggregating one keeps them
                // all as columns (grouping keys for the aggregation): its ORDER BY sees the scope plus the extra
                // aliases, the rule WITH *, ... applies above (issue #11556)
                validatePlainOrderByScope(statement.getReturnClause().getReturnItems(), statement.getOrderByClause(), scope);
              } else if (statement.getReturnClause().isDistinct()) {
                // After DISTINCT, ORDER BY can only reference returned columns
                validateCollapsedOrderByScope(statement.getReturnClause().getReturnItems(), statement.getOrderByClause(), false);
              } else if (statement.getReturnClause().hasAggregations()) {
                // Likewise after aggregation: what the projection did not keep, ORDER BY cannot sort
                // on. Reporting it beats the sort silently doing nothing (issue #5286).
                validateCollapsedOrderByScope(statement.getReturnClause().getReturnItems(), statement.getOrderByClause(), true);
              } else {
                // A plain RETURN keeps its input rows, so its ORDER BY sees the scope that reached the
                // RETURN plus the columns the RETURN projects - the same rule the non-aggregating WITH
                // above applies. A variable a preceding WITH dropped is in neither set, and without this
                // check the sort key resolved to nothing and the rows came back unsorted with no error
                // (issue #7426).
                validatePlainOrderByScope(statement.getReturnClause().getReturnItems(), statement.getOrderByClause(), scope);
              }
            }
          }
          break;
        case CALL:
          // Procedure CALL with YIELD — exported yield variables enter scope
          final CallClause callClause = entry.getTypedClause();
          if (callClause != null && callClause.hasYield()) {
            if (callClause.isYieldAll()) {
              final List<String> yieldAllFields = CypherSemanticValidator.resolveYieldAllFieldNames(callClause);
              if (yieldAllFields != null)
                scope.addAll(yieldAllFields);
            } else
              for (final CallClause.YieldItem item : callClause.getYieldItems())
                scope.add(item.getOutputName());
          }
          break;
        case SUBQUERY:
          // CALL { ... RETURN x AS y } — exported return variables enter outer scope
          final SubqueryClause subqueryClause = entry.getTypedClause();
          if (subqueryClause != null && subqueryClause.getInnerStatement() != null) {
            // Validate the subquery body: outer variables are visible only if imported (issue #5213).
            validateSubqueryScope(subqueryClause, scope, shadowed);
            final ReturnClause innerReturn = subqueryClause.getInnerStatement().getReturnClause();
            // Exports are checked against the outer scope as it was before this subquery, so a duplicate column inside
            // the RETURN keeps its ColumnNameConflict error instead of reading as a clash with an earlier export
            final Set<String> outerScope = innerReturn != null ? new HashSet<>(scope) : null;
            if (innerReturn != null)
              for (final ReturnClause.ReturnItem item : innerReturn.getReturnItems()) {
                if (item.isStar()) {
                  // The star exports the variables in scope at the inner RETURN, when they are known without running
                  // (issue #11556: CALL { UNWIND [5] AS k RETURN *, k * 2 AS d } exports k as well as d). The branches of a
                  // UNION body return the same columns, under the names of the first one
                  final CypherStatement body = subqueryClause.getInnerStatement();
                  final Set<String> exported = ReturnScope.scopeAtReturn(
                      body instanceof UnionStatement union ? union.getQueries().getFirst() : body);
                  if (exported != null)
                    scope.addAll(exported);
                } else {
                  final String exportedName = item.getAlias() != null ? item.getAlias()
                      : item.getExpression() instanceof VariableExpression variable ? variable.getVariableName() : null;
                  if (exportedName == null)
                    continue;
                  // A subquery cannot export a name the outer scope already binds, imported or not (issue #11689)
                  if (outerScope.contains(exportedName))
                    throw new CommandParsingException(
                        "VariableAlreadyBound: Variable '" + exportedName + "' already declared in outer scope");
                  scope.add(exportedName);
                }
              }
            // REPORT STATUS AS s binds s on every row (issue #11595)
            if (subqueryClause.getReportStatusVariable() != null)
              scope.add(subqueryClause.getReportStatusVariable());
          }
          break;
        case FOREACH:
          validateForeachClauseScope(entry.getTypedClause(), scope, shadowed);
          break;
      }
    }
  }

  /**
   * Validates one {@code FOREACH} clause against {@code outerScope}: its list expression is checked against the
   * outer scope, then its body is walked with a copy of that scope plus the loop variable. Shared by the top-level
   * switch and by {@link #validateForeachBodyScope}'s own nested-{@code FOREACH} case, which used to repeat these
   * same three steps (issue #8105).
   */
  private static void validateForeachClauseScope(final ForeachClause foreachClause, final Set<String> outerScope,
      final Set<String> shadowed) {
    if (foreachClause == null)
      return;
    checkExpressionScope(foreachClause.getListExpression(), outerScope);
    // The loop variable and whatever the body itself binds (a CREATE/MERGE pattern, a nested FOREACH's own
    // loop variable) live only inside the loop: a copy of the outer scope keeps the outer clauses from seeing
    // them once this FOREACH is done.
    final Set<String> bodyScope = new HashSet<>(outerScope);
    bodyScope.add(foreachClause.getVariable());
    validateForeachBodyScope(foreachClause, bodyScope, shadowed);
  }

  /**
   * Validates the updating clauses inside a {@code FOREACH} body against {@code scope} (the outer scope
   * plus the loop variable). Only the clause kinds the grammar allows inside a {@code FOREACH} body -
   * {@code SET}, {@code REMOVE}, {@code DELETE}, {@code CREATE}, {@code MERGE} and a nested
   * {@code FOREACH} - can appear, so this reuses the same per-clause checks the top-level statement runs
   * rather than the full {@link #validateVariableScope} switch (issue #8105).
   */
  private static void validateForeachBodyScope(final ForeachClause foreachClause, final Set<String> scope,
      final Set<String> shadowed) {
    for (final ClauseEntry entry : foreachClause.getInnerClauses()) {
      switch (entry.getType()) {
        case SET -> validateSetClauseScope(entry.getTypedClause(), scope);
        case REMOVE -> validateRemoveClauseScope(entry.getTypedClause(), scope);
        case DELETE -> validateDeleteClauseScope(entry.getTypedClause(), scope);
        case CREATE -> validateCreateClauseScope(entry.getTypedClause(), scope, shadowed);
        case MERGE -> validateMergeClauseScope(entry.getTypedClause(), scope, shadowed);
        case FOREACH -> validateForeachClauseScope(entry.getTypedClause(), scope, shadowed);
        default -> {
          // The parser rejects any other clause kind inside a FOREACH body before this runs.
        }
      }
    }
  }

  private static void validateCreateClauseScope(final CreateClause createClause, final Set<String> scope,
      final Set<String> shadowed) {
    if (createClause == null || createClause.isEmpty())
      return;
    for (final PathPattern path : createClause.getPathPatterns()) {
      // Check property value expressions for undefined variables
      for (final NodePattern node : path.getNodes())
        if (node.hasProperties())
          checkPropertyValuesScope(node.getProperties(), scope);
      for (final RelationshipPattern rel : path.getRelationships())
        if (rel.hasProperties())
          checkPropertyValuesScope(rel.getProperties(), scope);
      checkPatternVarsNotShadowed(path, scope, shadowed);
      VariableBindingValidator.addBoundVarsFromPattern(path, scope);
    }
  }

  private static void validateMergeClauseScope(final MergeClause mergeClause, final Set<String> scope,
      final Set<String> shadowed) {
    if (mergeClause == null)
      return;
    checkPatternVarsNotShadowed(mergeClause.getPathPattern(), scope, shadowed);
    VariableBindingValidator.addBoundVarsFromPattern(mergeClause.getPathPattern(), scope);
    // Validate ON CREATE SET / ON MATCH SET variables
    validateSetClauseScope(mergeClause.getOnCreateSet(), scope);
    validateSetClauseScope(mergeClause.getOnMatchSet(), scope);
  }

  private static void validateDeleteClauseScope(final DeleteClause deleteClause, final Set<String> scope) {
    if (deleteClause == null || deleteClause.isEmpty())
      return;
    for (final String var : deleteClause.getVariables())
      if (CypherSemanticValidator.isValidVariableName(var) && !scope.contains(var))
        throw new CommandSemanticException("UndefinedVariable: Variable '" + var + "' not defined");
  }

  /**
   * Validates a {@code REMOVE} clause's items against {@code scope}: the target variable of every item -
   * a property removal ({@code REMOVE n.prop}, {@code REMOVE n[expr]}) or a label removal
   * ({@code REMOVE n:Label}) - must already be bound, matching the check {@code SET} and {@code DELETE}
   * already make. A dynamic property key or a Cypher 25 {@code $(expr)} label expression is walked too,
   * since it can itself reference a variable the scope has lost (issue #8104).
   */
  private static void validateRemoveClauseScope(final RemoveClause removeClause, final Set<String> scope) {
    if (removeClause == null || removeClause.isEmpty())
      return;
    for (final RemoveClause.RemoveItem item : removeClause.getItems()) {
      final String var = item.getVariable();
      if (CypherSemanticValidator.isValidVariableName(var) && !scope.contains(var))
        throw new CommandSemanticException("UndefinedVariable: Variable '" + var + "' not defined");
      if (item.getKeyExpression() != null)
        checkExpressionScope(item.getKeyExpression(), scope);
      if (item.hasLabelExpressions())
        for (final Expression labelExpression : item.getLabelExpressions())
          checkExpressionScope(labelExpression, scope);
    }
  }

  /**
   * Validates the body of a CALL subquery against the outer scope, honouring Cypher import rules.
   * <ul>
   *   <li>Explicit scope {@code CALL (v1, v2) { ... }} imports only the listed variables (each must
   *   already be defined in the outer scope). {@code CALL (*) { ... }} imports all outer variables.</li>
   *   <li>Implicit scope {@code CALL { ... }} imports outer variables only through an <i>importing
   *   WITH</i> as the first clause of the body; without one, no outer variable is visible.</li>
   * </ul>
   * Referencing an outer variable that is not imported raises an undefined-variable error, matching
   * Neo4j (issue #5213).
   */
  private static void validateSubqueryScope(final SubqueryClause clause, final Set<String> outerScope,
      final Set<String> outerShadowed) {
    final List<String> scopeVariables = clause.getScopeVariables();

    // Explicit scope list applies uniformly to every UNION branch of the body.
    Set<String> explicitSeed = null;
    if (scopeVariables != null) {
      if (scopeVariables.size() == 1 && "*".equals(scopeVariables.get(0)))
        explicitSeed = new HashSet<>(outerScope);
      else {
        explicitSeed = new HashSet<>();
        for (final String v : scopeVariables) {
          if (CypherSemanticValidator.isValidVariableName(v) && !outerScope.contains(v))
            throw new CommandSemanticException("UndefinedVariable: Variable '" + v + "' not defined");
          explicitSeed.add(v);
        }
      }
    }

    final CypherStatement inner = clause.getInnerStatement();
    if (inner instanceof UnionStatement union) {
      for (final CypherStatement branch : union.getQueries())
        validateSubqueryBranchScope(branch, outerScope, outerShadowed, explicitSeed);
    } else
      validateSubqueryBranchScope(inner, outerScope, outerShadowed, explicitSeed);
  }

  private static void validateSubqueryBranchScope(final CypherStatement branch, final Set<String> outerScope,
      final Set<String> outerShadowed, final Set<String> explicitSeed) {
    final Set<String> seed;
    final Set<String> imported;
    if (explicitSeed != null) {
      seed = new HashSet<>(explicitSeed);
      imported = explicitSeed;
    } else if (startsWithImportingWith(branch)) {
      // The importing WITH may reference outer variables; it then resets the scope to its projections,
      // so exposing the whole outer scope here does not leak variables past the WITH.
      seed = new HashSet<>(outerScope);
      imported = importedNamesFromLeadingWith(branch, outerScope);
    } else {
      seed = new HashSet<>();
      imported = Set.of();
    }

    // Outer names the body did not import are shadowed: they may not be re-bound by a write pattern.
    final Set<String> shadowed = new HashSet<>(outerScope);
    shadowed.addAll(outerShadowed);
    shadowed.removeAll(imported);

    validateVariableScope(branch, seed, shadowed);
  }

  /**
   * Returns the outer names that survive the leading importing {@code WITH} of a subquery body, i.e. the
   * variables actually imported. {@code WITH *} imports the whole outer scope.
   * <p>
   * This tracks the <i>names still bound after the WITH</i>, not the source variables they were computed
   * from, because that is what the shadowing check needs: a re-aliased item ({@code WITH a AS x}) leaves
   * {@code a} unbound, so {@code a} stays shadowed and a later {@code CREATE (a)} in the body is rejected.
   */
  private static Set<String> importedNamesFromLeadingWith(final CypherStatement branch, final Set<String> outerScope) {
    final WithClause withClause = branch.getClausesInOrder().get(0).getTypedClause();
    final Set<String> imported = new HashSet<>();
    for (final ReturnClause.ReturnItem item : withClause.getItems()) {
      final Expression expr = item.getExpression();
      if (item.isStar())
        return new HashSet<>(outerScope);
      final String name = item.getAlias() != null ?
          item.getAlias() :
          (expr instanceof VariableExpression ? ((VariableExpression) expr).getVariableName() : null);
      if (name != null && outerScope.contains(name))
        imported.add(name);
    }
    return imported;
  }

  private static boolean startsWithImportingWith(final CypherStatement statement) {
    final List<ClauseEntry> clauses = statement.getClausesInOrder();
    if (clauses == null || clauses.isEmpty())
      return false;
    return clauses.get(0).getType() == ClauseEntry.ClauseType.WITH;
  }

  /**
   * Rejects a CREATE/MERGE pattern variable that is not visible in the current scope but is declared in
   * an enclosing one without having been imported. Binding it here would create a fresh anonymous entity
   * rather than write against the outer one, which is a silent data-loss bug (issue #5257).
   */
  private static void checkPatternVarsNotShadowed(final PathPattern path, final Set<String> scope, final Set<String> shadowed) {
    if (shadowed.isEmpty() || path == null)
      return;

    if (path.hasPathVariable())
      checkVarNotShadowed(path.getPathVariable(), scope, shadowed);
    for (final NodePattern node : path.getNodes())
      checkVarNotShadowed(node.getVariable(), scope, shadowed);
    for (final RelationshipPattern rel : path.getRelationships())
      checkVarNotShadowed(rel.getVariable(), scope, shadowed);
  }

  private static void checkVarNotShadowed(final String var, final Set<String> scope, final Set<String> shadowed) {
    if (var != null && CypherSemanticValidator.isValidVariableName(var) && !scope.contains(var) && shadowed.contains(var))
      throw new CommandSemanticException("UndefinedVariable: Variable '" + var
          + "' not defined: it is declared in an outer scope but not imported into the CALL subquery");
  }

  /**
   * Validates the ORDER BY of a collapsing projection, shared by RETURN and WITH in both their
   * DISTINCT (issues #5283, #5287) and aggregating (issue #5286) forms.
   * <p>
   * Collapsing the input rows leaves the variables that fed the projection without a single value per
   * surviving row, so ORDER BY is restricted to the projected columns. Whatever ORDER BY could still
   * resolve against those columns has already been rewritten to reference them by
   * {@link com.arcadedb.query.opencypher.rewriter.ProjectedOrderByNormalizer} - the whole item when it
   * repeats a projected expression, or the parts of it that do - so it reaches the check below as a
   * plain column reference. Anything left pointing at a collapsed variable genuinely has no value to
   * sort on and is reported here.
   *
   * An item that itself contains an aggregation is the exception: openCypher keeps the grouping-key
   * variables referencable inside it (ReturnOrderBy6/WithOrderBy4 [3]/[18] require
   * {@code ORDER BY me.age + count(you.age)} to compile when {@code me.age} is projected), and draws
   * the line between a simple grouping-key read and a complex expression in
   * {@link AggregationValidator#checkAmbiguousAggregation} instead. Such an item keeps the wider scope so that check, not
   * this one, gets to report it.
   *
   * @param aggregating whether the projection aggregates, which admits the grouping-key variables into
   *                    the scope of ORDER BY items that contain an aggregation
   */
  private static void validateCollapsedOrderByScope(final List<ReturnClause.ReturnItem> items, final OrderByClause orderBy,
      final boolean aggregating) {
    final Set<String> projectedNames = new HashSet<>();
    final Set<String> orderByScope = new HashSet<>();
    for (final ReturnClause.ReturnItem item : items) {
      projectedNames.add(item.getOutputName());
      if (item.getAlias() != null)
        orderByScope.add(item.getAlias());
      else if (item.getExpression() instanceof VariableExpression)
        orderByScope.add(((VariableExpression) item.getExpression()).getVariableName());
    }

    Set<String> aggregationScope = null;

    for (final OrderByClause.OrderByItem item : orderBy.getItems()) {
      // An item naming a projected output column resolves against the projected row, whatever the
      // column name looks like (an un-aliased projection is named after its own expression text)
      if (item.getExpression() != null && projectedNames.contains(item.getExpression()))
        continue;
      if (item.getExpressionAST() == null)
        continue;

      if (aggregating && item.getExpressionAST().containsAggregation()) {
        if (aggregationScope == null) {
          aggregationScope = new HashSet<>(orderByScope);
          for (final ReturnClause.ReturnItem projected : items)
            if (!projected.getExpression().containsAggregation())
              AggregationValidator.collectVariableNamesFromExpression(projected.getExpression(), aggregationScope);
        }
        checkExpressionScopeSkipAggArgs(item.getExpressionAST(), aggregationScope);
      } else
        checkExpressionScope(item.getExpressionAST(), orderByScope);
    }
  }

  /**
   * Validates the ORDER BY of a non-collapsing projection, shared by the plain forms of RETURN (issue
   * #7426) and WITH.
   * <p>
   * Such a projection keeps its input rows one for one, so its ORDER BY resolves against the scope that
   * reached the clause <i>widened</i> with the columns the clause projects. That is a strictly larger
   * scope than the collapsing form's, which is why {@link #validateCollapsedOrderByScope} cannot stand in
   * for this: there the feeding variables no longer have one value per surviving row, here they still do.
   * <p>
   * A variable in neither set has nothing to sort on. Before issue #7426 the plain RETURN form ran no
   * check at all and such a sort key silently resolved to nothing, so {@code WITH f.Name AS BusinessFunction
   * RETURN BusinessFunction ORDER BY f.Name} returned the rows in storage order with a 200 - wrong results
   * rather than the error openCypher and Neo4j raise.
   *
   * @param scope the variables live where the clause begins, which this method does not modify
   */
  private static void validatePlainOrderByScope(final List<ReturnClause.ReturnItem> items, final OrderByClause orderBy,
      final Set<String> scope) {
    final Set<String> projectedNames = new HashSet<>();
    final Set<String> orderByScope = new HashSet<>(scope);
    for (final ReturnClause.ReturnItem item : items) {
      projectedNames.add(item.getOutputName());
      if (item.getAlias() != null)
        orderByScope.add(item.getAlias());
      else if (item.getExpression() instanceof VariableExpression)
        orderByScope.add(((VariableExpression) item.getExpression()).getVariableName());
    }

    for (final OrderByClause.OrderByItem item : orderBy.getItems()) {
      // An item naming a projected output column resolves against the projected row, whatever the column
      // name looks like (an un-aliased projection is named after its own expression text)
      if (item.getExpression() != null && projectedNames.contains(item.getExpression()))
        continue;
      if (item.getExpressionAST() != null)
        checkExpressionScope(item.getExpressionAST(), orderByScope);
    }
  }

  private static boolean hasStar(final List<ReturnClause.ReturnItem> items) {
    for (final ReturnClause.ReturnItem item : items)
      if (item.isStar())
        return true;
    return false;
  }

  private static void checkExpressionScope(final Expression expr, final Set<String> scope) {
    if (expr == null)
      return;

    // Skip StarExpression (used in count(*), WITH *)
    if (expr instanceof StarExpression)
      return;

    if (expr instanceof VariableExpression) {
      final String varName = ((VariableExpression) expr).getVariableName();
      // Skip synthetic variable names that are really expression text (AST builder artifacts)
      // The AST builder creates VariableExpression from raw text in some cases
      if (!CypherSemanticValidator.isValidVariableName(varName))
        return;
      if (!scope.contains(varName))
        throw new CommandSemanticException("UndefinedVariable: Variable '" + varName + "' not defined");
    } else if (expr instanceof PropertyAccessExpression) {
      final String varName = ((PropertyAccessExpression) expr).getVariableName();
      if (CypherSemanticValidator.isValidVariableName(varName) && !scope.contains(varName))
        throw new CommandSemanticException("UndefinedVariable: Variable '" + varName + "' not defined");
    } else if (expr instanceof ChainedPropertyAccessExpression chained) {
      // (n {id:1}).id: the receiver of the property read is an expression of its own (issue #11693)
      checkExpressionScope(chained.getBaseExpression(), scope);
    } else if (expr instanceof MapProjectionExpression projection) {
      // n {.id} reads n just as RETURN n does, so an unbound n is refused rather than projected as null (issue #11693)
      final String varName = projection.getVariableName();
      if (CypherSemanticValidator.isValidVariableName(varName) && !scope.contains(varName))
        throw new CommandSemanticException("UndefinedVariable: Variable '" + varName + "' not defined");
      for (final MapProjectionExpression.ProjectionElement element : projection.getElements())
        checkExpressionScope(element.getExpression(), scope);
    } else if (expr instanceof FunctionCallExpression) {
      for (final Expression arg : ((FunctionCallExpression) expr).getArguments())
        checkExpressionScope(arg, scope);
    } else if (expr instanceof ArithmeticExpression) {
      checkExpressionScope(((ArithmeticExpression) expr).getLeft(), scope);
      checkExpressionScope(((ArithmeticExpression) expr).getRight(), scope);
    } else if (expr instanceof TernaryLogicalExpression) {
      checkExpressionScope(((TernaryLogicalExpression) expr).getLeft(), scope);
      if (((TernaryLogicalExpression) expr).getRight() != null)
        checkExpressionScope(((TernaryLogicalExpression) expr).getRight(), scope);
    } else if (expr instanceof ListExpression) {
      for (final Expression elem : ((ListExpression) expr).getElements())
        checkExpressionScope(elem, scope);
    } else if (expr instanceof MapExpression) {
      for (final Expression value : ((MapExpression) expr).getEntries().values())
        checkExpressionScope(value, scope);
    } else if (expr instanceof BooleanWrapperExpression) {
      checkBooleanExpressionScope(((BooleanWrapperExpression) expr).getBooleanExpression(), scope);
    } else if (expr instanceof ComparisonExpressionWrapper) {
      final ComparisonExpression comp = ((ComparisonExpressionWrapper) expr).getComparison();
      checkExpressionScope(comp.getLeft(), scope);
      checkExpressionScope(comp.getRight(), scope);
    } else if (expr instanceof CaseExpression) {
      final CaseExpression caseExpr = (CaseExpression) expr;
      if (caseExpr.getCaseExpression() != null)
        checkExpressionScope(caseExpr.getCaseExpression(), scope);
      for (final CaseAlternative alt : caseExpr.getAlternatives()) {
        checkExpressionScope(alt.getWhenExpression(), scope);
        checkExpressionScope(alt.getThenExpression(), scope);
      }
      if (caseExpr.getElseExpression() != null)
        checkExpressionScope(caseExpr.getElseExpression(), scope);
    } else if (expr instanceof ListComprehensionExpression) {
      final ListComprehensionExpression lce = (ListComprehensionExpression) expr;
      checkExpressionScope(lce.getListExpression(), scope);
      // The variable introduces a new scope binding for the inner expressions
      final Set<String> innerScope = new HashSet<>(scope);
      innerScope.add(lce.getVariable());
      if (lce.getWhereExpression() != null)
        checkExpressionScope(lce.getWhereExpression(), innerScope);
      if (lce.getMapExpression() != null)
        checkExpressionScope(lce.getMapExpression(), innerScope);
    } else if (expr instanceof ListIndexExpression) {
      final ListIndexExpression lie = (ListIndexExpression) expr;
      checkExpressionScope(lie.getListExpression(), scope);
      checkExpressionScope(lie.getIndexExpression(), scope);
    } else if (expr instanceof ListSliceExpression slice) {
      // l[0..3] with l never bound evaluated to null instead of being refused (issue #11686)
      checkExpressionScope(slice.getListExpression(), scope);
      checkExpressionScope(slice.getFromExpression(), scope);
      checkExpressionScope(slice.getToExpression(), scope);
    } else if (expr instanceof PatternComprehensionExpression pce) {
      // The pattern binds its own variables (and the optional path variable) for the WHERE and the projection only.
      // Without this branch an unbound name after | evaluated to null per match instead of being refused (issue #11686)
      final PathPattern path = pce.getPathPattern();
      final Set<String> innerScope = new HashSet<>(scope);
      if (pce.getPathVariable() != null)
        innerScope.add(pce.getPathVariable());
      if (path != null) {
        VariableBindingValidator.addBoundVarsFromPattern(path, innerScope);
        for (final NodePattern node : path.getNodes())
          if (node.getProperties() != null)
            checkPropertyValuesScope(node.getProperties(), innerScope);
        for (final RelationshipPattern rel : path.getRelationships())
          if (rel.getProperties() != null)
            checkPropertyValuesScope(rel.getProperties(), innerScope);
        checkGroupWhereScope(path, innerScope);
      }
      checkExpressionScope(pce.getWhereExpression(), innerScope);
      checkExpressionScope(pce.getMapExpression(), innerScope);
    } else if (expr instanceof ListPredicateExpression) {
      final ListPredicateExpression lpe = (ListPredicateExpression) expr;
      checkExpressionScope(lpe.getListExpression(), scope);
      // The variable introduces a new scope binding for the WHERE expression
      final Set<String> innerScope = new HashSet<>(scope);
      innerScope.add(lpe.getVariable());
      if (lpe.getWhereExpression() != null)
        checkExpressionScope(lpe.getWhereExpression(), innerScope);
    } else if (expr instanceof ExistsExpression exists) {
      checkSubqueryExpressionScope(exists.getParsedSubquery(), scope);
    } else if (expr instanceof CountExpression count) {
      checkSubqueryExpressionScope(count.getParsedSubquery(), scope);
    } else if (expr instanceof CollectExpression collect) {
      checkSubqueryExpressionScope(collect.getParsedSubquery(), scope);
    } else if (expr instanceof ReduceExpression reduce) {
      checkExpressionScope(reduce.getInitialValue(), scope);
      checkExpressionScope(reduce.getListExpression(), scope);
      // The accumulator and iterator variables introduce a new scope binding for the reduce expression
      final Set<String> innerScope = new HashSet<>(scope);
      innerScope.add(reduce.getAccumulatorVariable());
      innerScope.add(reduce.getIteratorVariable());
      checkExpressionScope(reduce.getReduceExpression(), innerScope);
    } else if (expr instanceof AllReduceExpression allReduce) {
      checkExpressionScope(allReduce.getInitialValue(), scope);
      checkExpressionScope(allReduce.getListExpression(), scope);
      // Same binding as ReduceExpression above; the predicate is evaluated in the same inner scope, since
      // it references the accumulator/iterator by name, not by the value the reduce step gives them.
      final Set<String> innerScope = new HashSet<>(scope);
      innerScope.add(allReduce.getAccumulatorVariable());
      innerScope.add(allReduce.getIteratorVariable());
      checkExpressionScope(allReduce.getReduceExpression(), innerScope);
      checkExpressionScope(allReduce.getPredicateExpression(), innerScope);
    }
    // LiteralExpression, ParameterExpression, StarExpression — no variables to check
  }

  /**
   * Validates the body of an {@code EXISTS { }} / {@code COUNT { }} / {@code COLLECT { }} expression against
   * {@code outerScope}, the scope live at the point the expression appears.
   * <p>
   * Unlike a {@code CALL { }} subquery, these bodies need no import gate: the runtime hands the whole outer row to
   * the body as a seed ({@link CorrelatedSubqueryRunner#run}), so every variable live in {@code outerScope} is
   * visible to it without an explicit or importing-{@code WITH} declaration. A variable {@code WITH} has already
   * dropped from {@code outerScope} must therefore be rejected here exactly as it would be if referenced directly
   * (issue #5825) — before this fix it silently resolved as missing/null instead, so the predicate just evaluated
   * to false/empty rather than raising a client error.
   */
  private static void checkSubqueryExpressionScope(final CypherStatement parsedSubquery, final Set<String> outerScope) {
    if (parsedSubquery == null)
      // Best-effort AST build declined the body (#5626 case): nothing to statically check, falls back to text execution.
      return;
    if (parsedSubquery instanceof UnionStatement union) {
      for (final CypherStatement branch : union.getQueries())
        validateVariableScope(branch, new HashSet<>(outerScope), Set.of());
    } else
      validateVariableScope(parsedSubquery, new HashSet<>(outerScope), Set.of());
  }

  private static void checkBooleanExpressionScope(final BooleanExpression boolExpr, final Set<String> scope) {
    if (boolExpr == null)
      return;

    if (boolExpr instanceof LogicalExpression) {
      final LogicalExpression logExpr = (LogicalExpression) boolExpr;
      checkBooleanExpressionScope(logExpr.getLeft(), scope);
      if (logExpr.getRight() != null)
        checkBooleanExpressionScope(logExpr.getRight(), scope);
    } else if (boolExpr instanceof ComparisonExpression) {
      final ComparisonExpression comp = (ComparisonExpression) boolExpr;
      checkExpressionScope(comp.getLeft(), scope);
      checkExpressionScope(comp.getRight(), scope);
    } else if (boolExpr instanceof InExpression) {
      final InExpression inExpr = (InExpression) boolExpr;
      checkExpressionScope(inExpr.getExpression(), scope);
      for (final Expression elem : inExpr.getList())
        checkExpressionScope(elem, scope);
    } else if (boolExpr instanceof PatternPredicateExpression) {
      // Pattern predicates in WHERE must not introduce new variables
      final PathPattern path = ((PatternPredicateExpression) boolExpr).getPathPattern();
      if (path != null) {
        // A single-node pattern in WHERE is no predicate: WHERE (n) with n a node is refused by
        // FunctionArgumentChecks#checkSingleNodePredicate, which knows the kind of n at that point. WHERE (flag) with flag bound to a value
        // is a parenthesized expression, resolved at runtime (issue #9542).
        checkPatternPredicateVariables(path, scope);
      }
    } else if (boolExpr instanceof IsNullExpression) {
      checkExpressionScope(((IsNullExpression) boolExpr).getExpression(), scope);
    } else if (boolExpr instanceof LabelCheckExpression labelCheck) {
      checkExpressionScope(labelCheck.getVariableExpression(), scope);
      for (final Expression dynamicLabel : labelCheck.getDynamicLabels())
        checkExpressionScope(dynamicLabel, scope);
    } else if (boolExpr instanceof BooleanCoercionExpression) {
      // A WHERE body that is a bare expression coerced to boolean (e.g. any()/all()/none()/single()
      // list predicates, a boolean-typed property, or a comprehension) is scope-checked as a plain
      // expression. This catches variables that leak out of a pattern/list comprehension into the
      // outer predicate scope (issue #5179).
      checkExpressionScope(((BooleanCoercionExpression) boolExpr).getExpression(), scope);
    } else if (boolExpr instanceof RegexExpression regex) {
      // zz.name =~ 'A.*' filtered the rows by a name no clause bound instead of refusing it (issue #10559)
      checkExpressionScope(regex.getExpression(), scope);
      checkExpressionScope(regex.getPattern(), scope);
    } else if (boolExpr instanceof StringMatchExpression match) {
      checkExpressionScope(match.getExpression(), scope);
      checkExpressionScope(match.getPattern(), scope);
    } else if (boolExpr instanceof IsNormalizedExpression normalized) {
      checkExpressionScope(normalized.getValueExpression(), scope);
    } else if (boolExpr instanceof IsTypedExpression typed) {
      checkExpressionScope(typed.getValueExpression(), scope);
    }
  }

  private static void checkPatternPredicateVariables(final PathPattern path, final Set<String> scope) {
    for (final NodePattern node : path.getNodes()) {
      final String var = node.getVariable();
      if (var != null && !var.isEmpty() && CypherSemanticValidator.isValidVariableName(var) && !scope.contains(var))
        throw new CommandSemanticException("UndefinedVariable: Variable '" + var + "' not defined");
    }
    for (final RelationshipPattern rel : path.getRelationships()) {
      final String var = rel.getVariable();
      if (var != null && !var.isEmpty() && CypherSemanticValidator.isValidVariableName(var) && !scope.contains(var))
        throw new CommandSemanticException("UndefinedVariable: Variable '" + var + "' not defined");
    }
  }

  static boolean isLiteralListExpression(final Expression expr) {
    if (!(expr instanceof ListExpression))
      return false;
    for (final Expression elem : ((ListExpression) expr).getElements())
      if (!(elem instanceof LiteralExpression))
        return false;
    return true;
  }

  static boolean containsNodeVariable(final Expression expr, final Map<String, VarType> scope) {
    if (expr instanceof ListExpression)
      for (final Expression elem : ((ListExpression) expr).getElements()) {
        if (elem instanceof VariableExpression) {
          final VarType type = scope.get(((VariableExpression) elem).getVariableName());
          if (type == VarType.NODE)
            return true;
        }
      }
    return false;
  }

  /**
   * Check expression scope but skip recursion into aggregation function arguments.
   * Used for ORDER BY after aggregating RETURN/WITH where aggregation arguments
   * reference pre-aggregation variables that are always valid.
   */
  private static void checkExpressionScopeSkipAggArgs(final Expression expr, final Set<String> scope) {
    if (expr == null)
      return;
    if (expr instanceof FunctionCallExpression) {
      final FunctionCallExpression func = (FunctionCallExpression) expr;
      if (func.isAggregation())
        return; // Don't check arguments of aggregation against restricted scope
      for (final Expression arg : func.getArguments())
        checkExpressionScopeSkipAggArgs(arg, scope);
    } else if (expr instanceof ArithmeticExpression) {
      checkExpressionScopeSkipAggArgs(((ArithmeticExpression) expr).getLeft(), scope);
      checkExpressionScopeSkipAggArgs(((ArithmeticExpression) expr).getRight(), scope);
    } else {
      checkExpressionScope(expr, scope);
    }
  }

  private static void checkPropertyValuesScope(final Map<String, Object> properties, final Set<String> scope) {
    for (final Object value : properties.values())
      if (value instanceof Expression)
        checkExpressionScope((Expression) value, scope);
  }

  /**
   * The inner {@code WHERE} of a quantified path pattern is held to the scope of the clause it is written in, as the {@code WHERE} of
   * the clause is: the variables the groups write, the ones of the rest of the pattern and the ones of the earlier clauses. A name
   * outside all of them used to evaluate as null, so the typo answered no row instead of an error (issue #10559).
   */
  private static void checkGroupWhereScope(final PathPattern path, final Set<String> scope) {
    for (final RelationshipPattern rel : path.getRelationships())
      if (rel instanceof QuantifiedPathPattern group) {
        checkBooleanExpressionScope(group.getInnerWhere(), scope);
        checkGroupWhereScope(group.getInnerPattern(), scope);
      } else if (rel.getWhereExpression() != null)
        // A relationship's inline predicate, a dynamic type [r:$(expr)] included, is not hoisted into the clause WHERE:
        // an undefined name in it would otherwise evaluate to null and match every relationship (issue #11776)
        checkBooleanExpressionScope(rel.getWhereExpression(), scope);
  }

  private static void validateSetClauseScope(final SetClause setClause, final Set<String> scope) {
    if (setClause == null || setClause.isEmpty())
      return;
    for (final SetClause.SetItem item : setClause.getItems()) {
      if (CypherSemanticValidator.isValidVariableName(item.getVariable()) && !scope.contains(item.getVariable()))
        throw new CommandSemanticException("UndefinedVariable: Variable '" + item.getVariable() + "' not defined");
      final Expression target = item.getTargetExpression();
      if (target != null) {
        checkExpressionScope(target, scope);
        checkSetReceiverIsNotAMap(target);
      }
      if (item.getValueExpression() != null)
        checkExpressionScope(item.getValueExpression(), scope);
    }
  }

  /**
   * A property is written on a node or a relationship only. A map projection ({@code SET (n {id:1}).s = ...}) or a map
   * literal builds a fresh map, so writing into it changes nothing in the graph: Neo4j refuses the receiver before the
   * query runs, and so does this, rather than accepting the SET as a silent no-op (issue #11693).
   */
  private static void checkSetReceiverIsNotAMap(final Expression target) {
    if (target instanceof MapProjectionExpression || target instanceof MapExpression)
      throw new CommandSemanticException(
          "TypeError: Type mismatch: expected Node or Relationship but was Map. '" + target.getText()
              + "' is a map, so a property cannot be set on it");
  }
}
