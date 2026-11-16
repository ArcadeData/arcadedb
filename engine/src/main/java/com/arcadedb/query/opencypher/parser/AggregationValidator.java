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

import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.opencypher.ast.*;
import java.util.*;

/**
 * The aggregation phase of the semantic validation: no aggregation nested in another, none in a WHERE or where a
 * clause cannot group, no non-deterministic argument, and no projection mixing an aggregation with a value that is
 * not one of its grouping keys.
 * <p>
 * Split out of {@link CypherSemanticValidator}, which runs the phases of the validation in order.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class AggregationValidator {
  private AggregationValidator() {
  }

  static void validateAggregations(final CypherStatement statement) {
    // Check for nested aggregations in RETURN
    if (statement.getReturnClause() != null)
      for (final ReturnClause.ReturnItem item : statement.getReturnClause().getReturnItems())
        checkNestedAggregation(item.getExpression(), false);

    // Check for nested aggregations in WITH
    for (final WithClause withClause : statement.getWithClauses())
      for (final ReturnClause.ReturnItem item : withClause.getItems())
        checkNestedAggregation(item.getExpression(), false);

    // Check for aggregation in WHERE
    final List<ClauseEntry> clauses = statement.getClausesInOrder();
    if (clauses != null) {
      for (final ClauseEntry entry : clauses) {
        if (entry.getType() == ClauseEntry.ClauseType.MATCH) {
          final MatchClause matchClause = entry.getTypedClause();
          if (matchClause.hasWhereClause())
            checkAggregationInWhere(matchClause.getWhereClause());
          if (matchClause.hasPathPatterns())
            for (final PathPattern path : matchClause.getPathPatterns())
              checkAggregationInGroupWhere(path);
        } else if (entry.getType() == ClauseEntry.ClauseType.WITH) {
          final WithClause withClause = entry.getTypedClause();
          if (withClause.getWhereClause() != null)
            checkAggregationInWhere(withClause.getWhereClause());
        }
      }
    }
    if (statement.getWhereClause() != null)
      checkAggregationInWhere(statement.getWhereClause());

    // Check for aggregation in ORDER BY after non-aggregating RETURN
    if (statement.getReturnClause() != null && statement.getOrderByClause() != null
        && !statement.getReturnClause().hasAggregations())
      for (final OrderByClause.OrderByItem item : statement.getOrderByClause().getItems())
        checkAggregationInOrderBy(item.getExpressionAST());

    // Check for aggregation in ORDER BY after non-aggregating WITH
    for (final WithClause withClause : statement.getWithClauses())
      if (withClause.getOrderByClause() != null && !withClause.hasAggregations())
        for (final OrderByClause.OrderByItem item : withClause.getOrderByClause().getItems())
          checkAggregationInOrderBy(item.getExpressionAST());

    // Check for ambiguous aggregation expressions in RETURN items
    if (statement.getReturnClause() != null)
      checkAmbiguousAggregation(statement.getReturnClause().getReturnItems());

    // Check for ambiguous aggregation in WITH items
    for (final WithClause withClause : statement.getWithClauses())
      checkAmbiguousAggregation(withClause.getItems());

    // Check for ambiguous aggregation in ORDER BY for aggregating RETURN
    if (statement.getReturnClause() != null && statement.getReturnClause().hasAggregations()
        && statement.getOrderByClause() != null) {
      final GroupingScope groupingVars = collectGroupingScope(statement.getReturnClause().getReturnItems(), true);
      for (final OrderByClause.OrderByItem item : statement.getOrderByClause().getItems()) {
        final Expression expr = item.getExpressionAST();
        if (expr != null && expr.containsAggregation())
          checkMixedAggregation(expr, groupingVars);
      }
    }

    // Check for ambiguous aggregation in ORDER BY for aggregating WITH
    for (final WithClause withClause : statement.getWithClauses()) {
      if (withClause.hasAggregations() && withClause.getOrderByClause() != null) {
        final GroupingScope groupingVars = collectGroupingScope(withClause.getItems(), true);
        // Collect projected names and expression texts for non-projected aggregation check
        final Set<String> projectedNames = new HashSet<>();
        for (final ReturnClause.ReturnItem projItem : withClause.getItems()) {
          projectedNames.add(projItem.getOutputName());
          if (projItem.getAlias() != null)
            projectedNames.add(projItem.getAlias());
          // Also add the expression text so ORDER BY sum(x) matches WITH sum(x) AS s
          if (projItem.getExpression() != null) {
            final String exprText = projItem.getExpression().getText();
            if (exprText != null)
              projectedNames.add(exprText);
          }
        }
        for (final OrderByClause.OrderByItem item : withClause.getOrderByClause().getItems()) {
          final Expression expr = item.getExpressionAST();
          if (expr == null)
            continue;
          // Check for non-projected aggregation function in ORDER BY
          if (expr instanceof FunctionCallExpression && ((FunctionCallExpression) expr).isAggregation()) {
            final String exprText = expr.getText();
            if (exprText != null && !projectedNames.contains(exprText))
              throw new CommandSemanticException("UndefinedVariable: Aggregation in ORDER BY is not projected");
          }
          if (expr.containsAggregation())
            checkMixedAggregation(expr, groupingVars);
        }
      }
    }
  }

  private static void checkNestedAggregation(final Expression expr, final boolean insideAggregation) {
    if (expr == null)
      return;

    if (expr instanceof FunctionCallExpression) {
      final FunctionCallExpression func = (FunctionCallExpression) expr;
      if (func.isAggregation()) {
        if (insideAggregation)
          throw new CommandParsingException("NestedAggregation: Nested aggregation functions are not allowed");
        // Check for non-deterministic functions inside aggregation (e.g., count(rand()))
        for (final Expression arg : func.getArguments())
          checkNonConstantInAggregation(arg);
        for (final Expression arg : func.getArguments())
          checkNestedAggregation(arg, true);
        return;
      }
      for (final Expression arg : func.getArguments())
        checkNestedAggregation(arg, insideAggregation);
    } else if (expr instanceof ArithmeticExpression) {
      checkNestedAggregation(((ArithmeticExpression) expr).getLeft(), insideAggregation);
      checkNestedAggregation(((ArithmeticExpression) expr).getRight(), insideAggregation);
    } else if (expr instanceof TernaryLogicalExpression) {
      checkNestedAggregation(((TernaryLogicalExpression) expr).getLeft(), insideAggregation);
      if (((TernaryLogicalExpression) expr).getRight() != null)
        checkNestedAggregation(((TernaryLogicalExpression) expr).getRight(), insideAggregation);
    } else if (expr instanceof BooleanWrapperExpression) {
      final BooleanExpression boolExpr = ((BooleanWrapperExpression) expr).getBooleanExpression();
      checkNestedAggregationInBoolean(boolExpr, insideAggregation);
    } else if (expr instanceof ListExpression) {
      for (final Expression elem : ((ListExpression) expr).getElements())
        checkNestedAggregation(elem, insideAggregation);
    } else if (expr instanceof ListComprehensionExpression) {
      final ListComprehensionExpression lce = (ListComprehensionExpression) expr;
      checkNestedAggregation(lce.getListExpression(), insideAggregation);
      if (lce.getMapExpression() != null)
        checkAggregationInListComprehension(lce.getMapExpression());
    } else if (expr instanceof PatternComprehensionExpression pce) {
      // The projection of a pattern comprehension runs once per match of the pattern, inside one outer row: an aggregate
      // there has no group to fold, as in a list comprehension (issue #12316)
      if ((pce.getMapExpression() != null && pce.getMapExpression().containsAggregation())
          || (pce.getWhereExpression() != null && pce.getWhereExpression().containsAggregation()))
        throw new CommandParsingException("InvalidAggregation: Aggregation functions are not allowed in pattern comprehensions");
    } else if (expr instanceof MapExpression map) {
      for (final Expression entry : map.getEntries().values())
        checkNestedAggregation(entry, insideAggregation);
    } else if (expr instanceof MapProjectionExpression projection) {
      for (final MapProjectionExpression.ProjectionElement element : projection.getElements())
        if (element.isKeyExpression())
          checkNestedAggregation(element.getExpression(), insideAggregation);
    } else if (expr instanceof ReduceExpression reduce) {
      checkNestedAggregation(reduce.getInitialValue(), insideAggregation);
      checkNestedAggregation(reduce.getListExpression(), insideAggregation);
      checkNestedAggregation(reduce.getReduceExpression(), insideAggregation);
    } else if (expr instanceof CaseExpression) {
      final CaseExpression caseExpr = (CaseExpression) expr;
      if (caseExpr.getCaseExpression() != null)
        checkNestedAggregation(caseExpr.getCaseExpression(), insideAggregation);
      for (final CaseAlternative alt : caseExpr.getAlternatives()) {
        checkNestedAggregation(alt.getWhenExpression(), insideAggregation);
        checkNestedAggregation(alt.getThenExpression(), insideAggregation);
      }
      if (caseExpr.getElseExpression() != null)
        checkNestedAggregation(caseExpr.getElseExpression(), insideAggregation);
    }
  }

  private static void checkNestedAggregationInBoolean(final BooleanExpression boolExpr, final boolean insideAggregation) {
    if (boolExpr instanceof ComparisonExpression) {
      checkNestedAggregation(((ComparisonExpression) boolExpr).getLeft(), insideAggregation);
      checkNestedAggregation(((ComparisonExpression) boolExpr).getRight(), insideAggregation);
    } else if (boolExpr instanceof LogicalExpression) {
      checkNestedAggregationInBoolean(((LogicalExpression) boolExpr).getLeft(), insideAggregation);
      if (((LogicalExpression) boolExpr).getRight() != null)
        checkNestedAggregationInBoolean(((LogicalExpression) boolExpr).getRight(), insideAggregation);
    }
  }

  private static final Set<String> NON_DETERMINISTIC_FUNCTIONS = Set.of("rand", "randomuuid");

  private static void checkNonConstantInAggregation(final Expression expr) {
    if (expr instanceof FunctionCallExpression) {
      final FunctionCallExpression func = (FunctionCallExpression) expr;
      if (NON_DETERMINISTIC_FUNCTIONS.contains(func.getFunctionName().toLowerCase(Locale.ROOT)))
        throw new CommandParsingException("NonConstantExpression: Non-constant expression is not allowed inside aggregation: " + func.getFunctionName());
    }
  }

  private static void checkAggregationInListComprehension(final Expression expr) {
    if (expr != null && expr.containsAggregation())
      throw new CommandParsingException("InvalidAggregation: Aggregation functions are not allowed in list comprehensions");
  }

  private static void checkAggregationInWhere(final WhereClause whereClause) {
    if (whereClause == null || whereClause.getConditionExpression() == null)
      return;
    checkAggregationInBooleanExpression(whereClause.getConditionExpression());
  }

  /** The inner {@code WHERE} of a quantified path pattern filters rows like the one of the clause: no aggregate (issue #10559). */
  private static void checkAggregationInGroupWhere(final PathPattern path) {
    for (final RelationshipPattern rel : path.getRelationships())
      if (rel instanceof QuantifiedPathPattern group) {
        checkAggregationInBooleanExpression(group.getInnerWhere());
        checkAggregationInGroupWhere(group.getInnerPattern());
      }
  }

  private static void checkAggregationInBooleanExpression(final BooleanExpression boolExpr) {
    if (boolExpr == null)
      return;

    if (boolExpr instanceof LogicalExpression) {
      final LogicalExpression logExpr = (LogicalExpression) boolExpr;
      checkAggregationInBooleanExpression(logExpr.getLeft());
      if (logExpr.getRight() != null)
        checkAggregationInBooleanExpression(logExpr.getRight());
    } else if (boolExpr instanceof ComparisonExpression) {
      final ComparisonExpression comp = (ComparisonExpression) boolExpr;
      checkAggregationInExpression(comp.getLeft());
      checkAggregationInExpression(comp.getRight());
    } else if (boolExpr instanceof InExpression) {
      final InExpression inExpr = (InExpression) boolExpr;
      checkAggregationInExpression(inExpr.getExpression());
      for (final Expression elem : inExpr.getList())
        checkAggregationInExpression(elem);
    } else if (boolExpr instanceof IsNullExpression isNull) {
      // count(x) IS NOT NULL is as much an aggregate in a WHERE as count(x) > 0 (issue #10559)
      checkAggregationInExpression(isNull.getExpression());
    } else if (boolExpr instanceof RegexExpression regex) {
      checkAggregationInExpression(regex.getExpression());
      checkAggregationInExpression(regex.getPattern());
    } else if (boolExpr instanceof StringMatchExpression match) {
      checkAggregationInExpression(match.getExpression());
      checkAggregationInExpression(match.getPattern());
    } else if (boolExpr instanceof IsNormalizedExpression normalized) {
      checkAggregationInExpression(normalized.getValueExpression());
    } else if (boolExpr instanceof IsTypedExpression typed) {
      checkAggregationInExpression(typed.getValueExpression());
    } else if (boolExpr instanceof BooleanCoercionExpression coercion) {
      checkAggregationInExpression(coercion.getExpression());
    }
  }

  private static void checkAggregationInOrderBy(final Expression expr) {
    if (expr == null)
      return;

    if (expr instanceof FunctionCallExpression) {
      if (((FunctionCallExpression) expr).isAggregation())
        throw new CommandParsingException("InvalidAggregation: Aggregation functions are not allowed in ORDER BY after RETURN");
      for (final Expression arg : ((FunctionCallExpression) expr).getArguments())
        checkAggregationInOrderBy(arg);
    } else if (expr instanceof ArithmeticExpression) {
      checkAggregationInOrderBy(((ArithmeticExpression) expr).getLeft());
      checkAggregationInOrderBy(((ArithmeticExpression) expr).getRight());
    }
  }

  /**
   * Check for ambiguous aggregation: expressions that mix aggregation calls with non-aggregated sub-expressions
   * (Neo4j 42I18). A non-aggregated part may read only what is single-valued per group:
   * - a variable projected as itself, {@code RETURN p, ...} (a "bare" grouping key);
   * - a property access projected as a grouping key itself, {@code a.name} in
   *   {@code RETURN a.name, {foo: a.name = 'x', kids: collect(c)}} (TCK Return6 [6]) - another property of the same
   *   variable is not one;
   * - the variable of an aggregating map projection, {@code p{.name, n: count(f)}}: the projection groups by it (issue
   *   #12316, the Neo4j idiom), see {@link ImplicitGroupingKeys};
   * - a name the expression binds itself (a comprehension's iterator, a reduce accumulator), a literal, a parameter.
   * Anything else is AMBIGUOUS, and so is a sub-expression that merely equals a complex grouping key: grouping by
   * {@code me.age + you.age} does not make {@code me} single-valued (TCK Return6 [21]).
   */
  private static void checkAmbiguousAggregation(final List<ReturnClause.ReturnItem> items) {
    final GroupingScope scope = collectGroupingScope(items, false);
    for (final ReturnClause.ReturnItem item : items) {
      final Expression expr = item.getExpression();
      if (!expr.containsAggregation())
        continue;
      // Pure aggregation call is fine
      if (expr instanceof FunctionCallExpression && ((FunctionCallExpression) expr).isAggregation())
        continue;
      // Mixed expression — check non-aggregated parts
      checkMixedAggregation(expr, scope);
    }
  }

  /**
   * What a non-aggregated part of an aggregating item may read: the variables single-valued per group ({@code bareVars})
   * and the property accesses that are grouping keys themselves ({@code keyedProperties}), see
   * {@link #checkAmbiguousAggregation}.
   */
  private record GroupingScope(Set<String> bareVars, Set<String> keyedProperties) {
    /** This scope with {@code names} single-valued too: names the expression binds. */
    GroupingScope with(final String... names) {
      final Set<String> bare = new HashSet<>(bareVars);
      for (final String name : names)
        if (name != null)
          bare.add(name);
      return new GroupingScope(bare, keyedProperties);
    }
  }

  /**
   * Recursively checks a mixed aggregation expression for ambiguous non-aggregated parts. Every operand is checked:
   * an aggregating one recursively, any other one as a non-aggregated part ({@link #checkOperand}).
   */
  private static void checkMixedAggregation(final Expression expr, final GroupingScope scope) {
    switch (expr) {
    case null -> {
    }
    case FunctionCallExpression function -> {
      // Stop at an aggregation boundary; a scalar function's arguments are operands, coalesce(p.age, count(f)) included
      if (!function.isAggregation())
        for (final Expression arg : function.getArguments())
          checkOperand(arg, scope);
    }
    case ArithmeticExpression arithmetic -> {
      checkOperand(arithmetic.getLeft(), scope);
      checkOperand(arithmetic.getRight(), scope);
    }
    case BooleanWrapperExpression wrapper ->
        BooleanWrapperExpression.forEachOperand(wrapper.getBooleanExpression(), operand -> checkOperand(operand, scope));
    case ComparisonExpressionWrapper wrapper -> {
      checkOperand(wrapper.getComparison().getLeft(), scope);
      checkOperand(wrapper.getComparison().getRight(), scope);
    }
    case MapProjectionExpression projection -> {
      // An aggregating projection groups by its own variable (in the scope already, see collectGroupingScope), so every
      // part of it may read that variable (issue #12316)
      for (final MapProjectionExpression.ProjectionElement element : projection.getElements())
        if (element.isKeyExpression())
          checkOperand(element.getExpression(), scope);
    }
    case MapExpression map -> {
      // A map literal has no variable of its own: {name: p.name, n: count(f)} reads p, which must be a grouping key
      for (final Expression entry : map.getEntries().values())
        checkOperand(entry, scope);
    }
    case ListExpression list -> {
      for (final Expression element : list.getElements())
        checkOperand(element, scope);
    }
    case ListIndexExpression index -> {
      checkOperand(index.getListExpression(), scope);
      checkOperand(index.getIndexExpression(), scope);
    }
    case ListSliceExpression slice -> {
      checkOperand(slice.getListExpression(), scope);
      checkOperand(slice.getFromExpression(), scope);
      checkOperand(slice.getToExpression(), scope);
    }
    case ChainedPropertyAccessExpression chained -> checkOperand(chained.getBaseExpression(), scope);
    case ReduceExpression reduce -> {
      checkOperand(reduce.getInitialValue(), scope);
      checkOperand(reduce.getListExpression(), scope);
      checkOperand(reduce.getReduceExpression(), scope.with(reduce.getAccumulatorVariable(), reduce.getIteratorVariable()));
    }
    case CaseExpression caseExpr -> {
      checkOperand(caseExpr.getCaseExpression(), scope);
      for (final CaseAlternative alt : caseExpr.getAlternatives()) {
        checkOperand(alt.getWhenExpression(), scope);
        checkOperand(alt.getThenExpression(), scope);
      }
      checkOperand(caseExpr.getElseExpression(), scope);
    }
    default -> {
      // A shape whose aggregate the other checks refuse (a comprehension) or that is not modelled
    }
    }
  }

  /** One operand of a composite aggregating expression: aggregating itself, or a non-aggregated part to validate. */
  private static void checkOperand(final Expression operand, final GroupingScope scope) {
    if (operand == null)
      return;
    if (operand.containsAggregation())
      checkMixedAggregation(operand, scope);
    else
      validateNonAggPart(operand, scope);
  }

  /**
   * Validates that a non-aggregated part in a mixed expression is not ambiguous: everything it reads is single-valued
   * per group (see {@link #checkAmbiguousAggregation}).
   */
  private static void validateNonAggPart(final Expression expr, final GroupingScope scope) {
    if (expr == null)
      return;
    final Set<String> read = new HashSet<>();
    collectFreeVariables(expr, Set.of(), scope.keyedProperties(), read);
    for (final String variable : read)
      if (!scope.bareVars().contains(variable))
        throw new CommandParsingException(
            "AmbiguousAggregationExpression: Ambiguous aggregation expression: '" + expr.getText() + "' reads '" + variable
                + "', which is not a grouping key, next to an aggregate. Project it as a grouping key (RETURN " + variable
                + ", ...) or aggregate it in a preceding WITH (WITH " + variable + ", collect(...) AS list ...)");
  }

  /**
   * Collects the variables {@code expr} reads outside every aggregate call, leaving out the ones it binds itself
   * ({@code locals}: a comprehension's iterator, a reduce accumulator, the variables of a pattern comprehension's
   * pattern) and the property accesses that are grouping keys ({@code keyed}). A shape not modelled here contributes
   * nothing, as it did before this walk existed.
   * <p>
   * The variables of a pattern comprehension's pattern are taken as its own: without the scope of the clause a variable
   * the pattern joins on cannot be told from one it introduces, and refusing a valid query is the worse of the two errors.
   */
  private static void collectFreeVariables(final Expression expr, final Set<String> locals, final Set<String> keyed,
      final Set<String> out) {
    switch (expr) {
    case VariableExpression variable -> addFree(variable.getVariableName(), locals, out);
    case PropertyAccessExpression property -> {
      if (!keyed.contains(property.getText()))
        addFree(property.getVariableName(), locals, out);
    }
    case FunctionCallExpression function -> {
      if (!function.isAggregation())
        for (final Expression arg : function.getArguments())
          collectFreeVariables(arg, locals, keyed, out);
    }
    case ArithmeticExpression arithmetic -> {
      collectFreeVariables(arithmetic.getLeft(), locals, keyed, out);
      collectFreeVariables(arithmetic.getRight(), locals, keyed, out);
    }
    case MapProjectionExpression projection -> {
      addFree(projection.getVariableName(), locals, out);
      for (final MapProjectionExpression.ProjectionElement element : projection.getElements())
        if (element.isKeyExpression())
          collectFreeVariables(element.getExpression(), locals, keyed, out);
    }
    case MapExpression map -> {
      for (final Expression entry : map.getEntries().values())
        collectFreeVariables(entry, locals, keyed, out);
    }
    case ListExpression list -> {
      for (final Expression element : list.getElements())
        collectFreeVariables(element, locals, keyed, out);
    }
    case ListIndexExpression index -> {
      collectFreeVariables(index.getListExpression(), locals, keyed, out);
      collectFreeVariables(index.getIndexExpression(), locals, keyed, out);
    }
    case ListSliceExpression slice -> {
      collectFreeVariables(slice.getListExpression(), locals, keyed, out);
      collectFreeVariables(slice.getFromExpression(), locals, keyed, out);
      collectFreeVariables(slice.getToExpression(), locals, keyed, out);
    }
    case ChainedPropertyAccessExpression chained -> collectFreeVariables(chained.getBaseExpression(), locals, keyed, out);
    case CaseExpression caseExpr -> {
      collectFreeVariables(caseExpr.getCaseExpression(), locals, keyed, out);
      for (final CaseAlternative alt : caseExpr.getAlternatives()) {
        collectFreeVariables(alt.getWhenExpression(), locals, keyed, out);
        collectFreeVariables(alt.getThenExpression(), locals, keyed, out);
      }
      collectFreeVariables(caseExpr.getElseExpression(), locals, keyed, out);
    }
    case ListComprehensionExpression comprehension -> {
      collectFreeVariables(comprehension.getListExpression(), locals, keyed, out);
      final Set<String> inner = withLocals(locals, comprehension.getVariable());
      collectFreeVariables(comprehension.getWhereExpression(), inner, keyed, out);
      collectFreeVariables(comprehension.getMapExpression(), inner, keyed, out);
    }
    case ReduceExpression reduce -> {
      collectFreeVariables(reduce.getInitialValue(), locals, keyed, out);
      collectFreeVariables(reduce.getListExpression(), locals, keyed, out);
      collectFreeVariables(reduce.getReduceExpression(),
          withLocals(locals, reduce.getAccumulatorVariable(), reduce.getIteratorVariable()), keyed, out);
    }
    case PatternComprehensionExpression comprehension -> {
      final Set<String> inner = withLocals(locals, comprehension.getPathVariable());
      final PathPattern pattern = comprehension.getPathPattern();
      if (pattern != null) {
        if (pattern.getNodes() != null)
          for (final NodePattern node : pattern.getNodes())
            if (node != null && node.getVariable() != null)
              inner.add(node.getVariable());
        if (pattern.getRelationships() != null)
          for (final RelationshipPattern relationship : pattern.getRelationships())
            if (relationship != null && relationship.getVariable() != null)
              inner.add(relationship.getVariable());
      }
      collectFreeVariables(comprehension.getWhereExpression(), inner, keyed, out);
      collectFreeVariables(comprehension.getMapExpression(), inner, keyed, out);
    }
    // Every operand of IN, IS NULL, STARTS WITH, =~, a label check..., through the walk the aggregation steps use too
    case BooleanWrapperExpression wrapper -> BooleanWrapperExpression.forEachOperand(wrapper.getBooleanExpression(),
        operand -> collectFreeVariables(operand, locals, keyed, out));
    case ComparisonExpressionWrapper wrapper -> {
      collectFreeVariables(wrapper.getComparison().getLeft(), locals, keyed, out);
      collectFreeVariables(wrapper.getComparison().getRight(), locals, keyed, out);
    }
    case null, default -> {
      // A literal, a parameter, or a shape not modelled
    }
    }
  }

  private static void addFree(final String variable, final Set<String> locals, final Set<String> out) {
    if (variable != null && !locals.contains(variable))
      out.add(variable);
  }

  private static Set<String> withLocals(final Set<String> locals, final String... names) {
    final Set<String> extended = new HashSet<>(locals);
    for (final String name : names)
      if (name != null)
        extended.add(name);
    return extended;
  }

  /**
   * The grouping scope of a projection's items.
   *
   * @param withAliases whether the output aliases are names of the scope: true for the {@code ORDER BY} that follows the
   *                    projection and reads what it projects, false for the projection's own items, which read the
   *                    incoming variables - {@code RETURN p.k AS p, {v: p.v, n: count(*)}} reads the incoming {@code p},
   *                    not the projected scalar
   */
  private static GroupingScope collectGroupingScope(final List<ReturnClause.ReturnItem> items, final boolean withAliases) {
    final Set<String> bare = new HashSet<>();
    final Set<String> keyed = new HashSet<>();
    for (final ReturnClause.ReturnItem item : items)
      if (!item.getExpression().containsAggregation()) {
        if (item.getExpression() instanceof VariableExpression variable)
          bare.add(variable.getVariableName());
        else if (item.getExpression() instanceof PropertyAccessExpression property)
          keyed.add(property.getText());
        if (withAliases && item.getAlias() != null)
          bare.add(item.getAlias());
      }
    // The variables the aggregation groups by without projecting them: the very set GroupByAggregationStep adds as hidden
    // keys, so what this accepts is what the step groups by (issue #12316)
    bare.addAll(ImplicitGroupingKeys.of(items));
    return new GroupingScope(bare, keyed);
  }

  static void collectVariableNamesFromExpression(final Expression expr, final Set<String> vars) {
    if (expr instanceof VariableExpression)
      vars.add(((VariableExpression) expr).getVariableName());
    else if (expr instanceof PropertyAccessExpression)
      vars.add(((PropertyAccessExpression) expr).getVariableName());
    else if (expr instanceof ArithmeticExpression) {
      collectVariableNamesFromExpression(((ArithmeticExpression) expr).getLeft(), vars);
      collectVariableNamesFromExpression(((ArithmeticExpression) expr).getRight(), vars);
    } else if (expr instanceof FunctionCallExpression) {
      for (final Expression arg : ((FunctionCallExpression) expr).getArguments())
        collectVariableNamesFromExpression(arg, vars);
    }
  }

  private static void checkAggregationInExpression(final Expression expr) {
    if (expr == null)
      return;

    if (expr instanceof FunctionCallExpression) {
      if (((FunctionCallExpression) expr).isAggregation())
        throw new CommandParsingException("InvalidAggregation: Aggregation functions are not allowed in WHERE");
      for (final Expression arg : ((FunctionCallExpression) expr).getArguments())
        checkAggregationInExpression(arg);
    } else if (expr instanceof ArithmeticExpression) {
      checkAggregationInExpression(((ArithmeticExpression) expr).getLeft());
      checkAggregationInExpression(((ArithmeticExpression) expr).getRight());
    }
  }
}
