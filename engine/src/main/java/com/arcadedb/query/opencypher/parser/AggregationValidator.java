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
      final GroupingScope groupingVars = collectGroupingScope(statement.getReturnClause().getReturnItems());
      for (final OrderByClause.OrderByItem item : statement.getOrderByClause().getItems()) {
        final Expression expr = item.getExpressionAST();
        if (expr != null && expr.containsAggregation())
          checkMixedAggregation(expr, groupingVars);
      }
    }

    // Check for ambiguous aggregation in ORDER BY for aggregating WITH
    for (final WithClause withClause : statement.getWithClauses()) {
      if (withClause.hasAggregations() && withClause.getOrderByClause() != null) {
        final GroupingScope groupingVars = collectGroupingScope(withClause.getItems());
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
   * Check for ambiguous aggregation: expressions that mix aggregation calls with
   * non-aggregated sub-expressions. The rules are:
   * - Simple variable/property references matching grouping keys are OK
   * - Literals and parameters are OK
   * - Complex expressions (arithmetic, a map, a list, a function call...) are OK only when every variable they read
   *   outside an aggregate is projected as itself (a "bare" grouping key, single-valued per group), is read through a
   *   property access projected as a grouping key itself, or is bound by the expression itself (a comprehension's
   *   iterator, a reduce accumulator); any other variable is AMBIGUOUS (Neo4j 42I18)
   * - Variable/property references NOT matching grouping keys are AMBIGUOUS
   * - The variable of an aggregating map projection is a grouping key of its own, {@code p{.name, n: count(f)}}: the
   *   projection groups by it (issue #12316, the Neo4j idiom), see {@link ImplicitGroupingKeys}
   */
  private static void checkAmbiguousAggregation(final List<ReturnClause.ReturnItem> items) {
    final GroupingScope scope = collectGroupingScope(items);
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
   * What a non-aggregated part of an aggregating item may read: the variables of the grouping items (coarse, as a simple
   * reference {@code n} or {@code n.p}), and among them the ones projected as themselves, which any expression may read
   * because they are single-valued per group, and the property accesses projected as grouping keys themselves
   * ({@code a.name} in {@code RETURN a.name, {foo: a.name = 'x', kids: collect(c)}}, TCK Return6 [6]): those are
   * single-valued too, where another property of the same variable is not (Neo4j 42I18).
   */
  private record GroupingScope(Set<String> groupingVars, Set<String> bareVars, Set<String> keyedProperties) {
    /** This scope with {@code names} single-valued too: an implicit grouping key, or names the expression binds. */
    GroupingScope with(final String... names) {
      final Set<String> grouping = new HashSet<>(groupingVars);
      final Set<String> bare = new HashSet<>(bareVars);
      for (final String name : names)
        if (name != null) {
          grouping.add(name);
          bare.add(name);
        }
      return new GroupingScope(grouping, bare, keyedProperties);
    }
  }

  /**
   * Recursively checks a mixed aggregation expression for ambiguous non-aggregated parts.
   */
  private static void checkMixedAggregation(final Expression expr, final GroupingScope scope) {
    if (expr == null)
      return;
    if (expr instanceof FunctionCallExpression && ((FunctionCallExpression) expr).isAggregation())
      return; // Stop at aggregation boundary

    if (expr instanceof ArithmeticExpression) {
      final Expression left = ((ArithmeticExpression) expr).getLeft();
      final Expression right = ((ArithmeticExpression) expr).getRight();
      final boolean leftAgg = left != null && left.containsAggregation();
      final boolean rightAgg = right != null && right.containsAggregation();

      if (leftAgg && !rightAgg) {
        validateNonAggPart(right, scope);
        checkMixedAggregation(left, scope);
      } else if (!leftAgg && rightAgg) {
        validateNonAggPart(left, scope);
        checkMixedAggregation(right, scope);
      } else {
        checkMixedAggregation(left, scope);
        checkMixedAggregation(right, scope);
      }
    } else if (expr instanceof FunctionCallExpression) {
      for (final Expression arg : ((FunctionCallExpression) expr).getArguments())
        checkMixedAggregation(arg, scope);
    } else if (expr instanceof BooleanWrapperExpression) {
      checkMixedAggregationInBoolean(((BooleanWrapperExpression) expr).getBooleanExpression(), scope);
    } else if (expr instanceof ComparisonExpressionWrapper) {
      final ComparisonExpression comp = ((ComparisonExpressionWrapper) expr).getComparison();
      checkMixedAggregation(comp.getLeft(), scope);
      checkMixedAggregation(comp.getRight(), scope);
    } else if (expr instanceof MapProjectionExpression projection) {
      // An aggregating projection groups by its own variable (in the scope already, see collectGroupingScope), so every
      // part of it may read that variable (issue #12316)
      for (final MapProjectionExpression.ProjectionElement element : projection.getElements())
        if (element.isKeyExpression())
          checkOperand(element.getExpression(), scope);
    } else if (expr instanceof MapExpression map) {
      // A map literal has no variable of its own: {name: p.name, n: count(f)} reads p, which must be a grouping key
      for (final Expression entry : map.getEntries().values())
        checkOperand(entry, scope);
    } else if (expr instanceof ListExpression list) {
      for (final Expression element : list.getElements())
        checkOperand(element, scope);
    } else if (expr instanceof ReduceExpression reduce) {
      checkOperand(reduce.getInitialValue(), scope);
      checkOperand(reduce.getListExpression(), scope);
      checkOperand(reduce.getReduceExpression(), scope.with(reduce.getAccumulatorVariable(), reduce.getIteratorVariable()));
    } else if (expr instanceof CaseExpression caseExpr) {
      checkOperand(caseExpr.getCaseExpression(), scope);
      for (final CaseAlternative alt : caseExpr.getAlternatives()) {
        checkOperand(alt.getWhenExpression(), scope);
        checkOperand(alt.getThenExpression(), scope);
      }
      checkOperand(caseExpr.getElseExpression(), scope);
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

  private static void checkMixedAggregationInBoolean(final BooleanExpression boolExpr, final GroupingScope scope) {
    if (boolExpr instanceof ComparisonExpression) {
      checkMixedAggregation(((ComparisonExpression) boolExpr).getLeft(), scope);
      checkMixedAggregation(((ComparisonExpression) boolExpr).getRight(), scope);
    } else if (boolExpr instanceof LogicalExpression) {
      checkMixedAggregationInBoolean(((LogicalExpression) boolExpr).getLeft(), scope);
      if (((LogicalExpression) boolExpr).getRight() != null)
        checkMixedAggregationInBoolean(((LogicalExpression) boolExpr).getRight(), scope);
    }
  }

  /**
   * Validates that a non-aggregated part in a mixed expression is not ambiguous.
   */
  private static void validateNonAggPart(final Expression expr, final GroupingScope scope) {
    if (expr == null)
      return;
    // Simple variable reference — OK if it's a grouping key
    if (expr instanceof VariableExpression) {
      if (!scope.groupingVars().contains(((VariableExpression) expr).getVariableName()))
        throw new CommandParsingException("AmbiguousAggregationExpression: Ambiguous aggregation expression");
      return;
    }
    // Simple property access — OK if the variable is a grouping key
    if (expr instanceof PropertyAccessExpression) {
      if (!scope.groupingVars().contains(((PropertyAccessExpression) expr).getVariableName()))
        throw new CommandParsingException("AmbiguousAggregationExpression: Ambiguous aggregation expression");
      return;
    }
    // Literals and stars are always OK
    if (expr instanceof LiteralExpression || expr instanceof StarExpression)
      return;
    // A complex expression is single-valued per group only when everything it reads is: a variable projected as itself,
    // or one the expression binds. Grouping by n.a + n.b does not make n single-valued (TCK Return6 [21]).
    final Set<String> read = new HashSet<>();
    collectFreeVariables(expr, Set.of(), scope.keyedProperties(), read);
    for (final String variable : read)
      if (!scope.bareVars().contains(variable))
        throw new CommandParsingException("AmbiguousAggregationExpression: Ambiguous aggregation expression");
  }

  /**
   * Collects the variables {@code expr} reads outside every aggregate call, leaving out the ones it binds itself
   * ({@code locals}: a comprehension's iterator, a reduce accumulator, the variables of a pattern comprehension's
   * pattern). A shape not modelled here contributes nothing, as it did before this walk existed.
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
    case BooleanWrapperExpression wrapper -> collectFreeVariables(wrapper.getBooleanExpression(), locals, keyed, out);
    case ComparisonExpressionWrapper wrapper -> collectFreeVariables(wrapper.getComparison(), locals, keyed, out);
    case null, default -> {
      // A literal, a parameter, or a shape not modelled
    }
    }
  }

  private static void collectFreeVariables(final BooleanExpression expr, final Set<String> locals, final Set<String> keyed,
      final Set<String> out) {
    switch (expr) {
    case ComparisonExpression comparison -> {
      collectFreeVariables(comparison.getLeft(), locals, keyed, out);
      collectFreeVariables(comparison.getRight(), locals, keyed, out);
    }
    case LogicalExpression logical -> {
      collectFreeVariables(logical.getLeft(), locals, keyed, out);
      collectFreeVariables(logical.getRight(), locals, keyed, out);
    }
    case IsNullExpression isNull -> collectFreeVariables(isNull.getExpression(), locals, keyed, out);
    case null, default -> {
      // A shape not modelled
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

  private static GroupingScope collectGroupingScope(final List<ReturnClause.ReturnItem> items) {
    final Set<String> vars = new HashSet<>();
    final Set<String> bare = new HashSet<>();
    final Set<String> keyed = new HashSet<>();
    for (final ReturnClause.ReturnItem item : items)
      if (!item.getExpression().containsAggregation()) {
        collectVariableNamesFromExpression(item.getExpression(), vars);
        if (item.getExpression() instanceof VariableExpression variable)
          bare.add(variable.getVariableName());
        else if (item.getExpression() instanceof PropertyAccessExpression property)
          keyed.add(property.getText());
        // Also include the alias as a valid grouping reference
        if (item.getAlias() != null) {
          vars.add(item.getAlias());
          bare.add(item.getAlias());
        }
      }
    // The variables the aggregation groups by without projecting them: the very set GroupByAggregationStep adds as hidden
    // keys, so what this accepts is what the step groups by (issue #12316)
    for (final String implicit : ImplicitGroupingKeys.of(items)) {
      vars.add(implicit);
      bare.add(implicit);
    }
    return new GroupingScope(vars, bare, keyed);
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
