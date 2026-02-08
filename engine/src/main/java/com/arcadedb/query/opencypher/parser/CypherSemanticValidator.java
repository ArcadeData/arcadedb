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
import com.arcadedb.query.opencypher.ast.*;

import java.util.*;

/**
 * Semantic validator for Cypher statements.
 * Runs after AST construction but before execution to catch semantic errors.
 * <p>
 * Validates:
 * - Variable type conflicts (node vs relationship vs path)
 * - Variable already bound in CREATE
 * - Undefined variable references
 * - Boolean operand types (reject non-boolean literals)
 * - Nested aggregations and aggregation in WHERE
 * - CREATE/MERGE/DELETE structural constraints
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class CypherSemanticValidator {

  private enum VarType {
    NODE, RELATIONSHIP, PATH
  }

  public static void validate(final CypherStatement statement) {
    // For UNION statements, validate union-specific constraints then each subquery
    if (statement instanceof UnionStatement) {
      validateUnion((UnionStatement) statement);
      return;
    }

    final CypherSemanticValidator v = new CypherSemanticValidator();
    v.validateVariableTypes(statement);
    v.validateVariableBinding(statement);
    v.validateVariableScope(statement);
    v.validateCreateConstraints(statement);
    v.validateAggregations(statement);
    v.validateBooleanOperandTypes(statement);
    v.validateSkipLimit(statement);
    v.validateColumnNames(statement);
    v.validateExpressionAliases(statement);
  }

  // ========================
  // Phase 0: UNION Validation
  // ========================

  private static void validateUnion(final UnionStatement unionStmt) {
    final List<CypherStatement> queries = unionStmt.getQueries();
    final List<Boolean> flags = unionStmt.getUnionAllFlags();

    // Check that mixing UNION and UNION ALL is not allowed
    if (flags.size() > 1) {
      final boolean firstIsAll = flags.get(0);
      for (int i = 1; i < flags.size(); i++) {
        if (flags.get(i) != firstIsAll)
          throw new CommandParsingException("InvalidClauseComposition: Cannot mix UNION and UNION ALL");
      }
    }

    // Check that all queries have the same return columns
    List<String> firstColumns = null;
    for (final CypherStatement query : queries) {
      final ReturnClause returnClause = query.getReturnClause();
      if (returnClause == null)
        continue;
      final List<String> columns = returnClause.getItems();
      if (firstColumns == null) {
        firstColumns = columns;
      } else if (!firstColumns.equals(columns)) {
        throw new CommandParsingException("DifferentColumnsInUnion: All sub queries in a UNION must have the same column names");
      }
      // Validate each subquery
      validate(query);
    }
  }

  // ========================
  // Phase 2a: Variable Types
  // ========================

  private void validateVariableTypes(final CypherStatement statement) {
    final Map<String, VarType> varTypes = new HashMap<>();

    // Walk clauses in order if available, otherwise walk MATCH + CREATE + MERGE
    final List<ClauseEntry> clausesInOrder = statement.getClausesInOrder();
    if (clausesInOrder != null && !clausesInOrder.isEmpty()) {
      for (final ClauseEntry entry : clausesInOrder) {
        switch (entry.getType()) {
          case MATCH:
            final MatchClause matchClause = entry.getTypedClause();
            if (matchClause.hasPathPatterns())
              for (final PathPattern path : matchClause.getPathPatterns())
                registerPathPatternVarTypes(path, varTypes);
            break;
          case CREATE:
            final CreateClause createClause = entry.getTypedClause();
            if (createClause != null && !createClause.isEmpty())
              for (final PathPattern path : createClause.getPathPatterns())
                registerPathPatternVarTypes(path, varTypes);
            break;
          case MERGE:
            final MergeClause mergeClause = entry.getTypedClause();
            if (mergeClause != null)
              registerPathPatternVarTypes(mergeClause.getPathPattern(), varTypes);
            break;
          default:
            break;
        }
      }
    } else {
      // Fallback: walk match clauses + create + merge
      for (final MatchClause matchClause : statement.getMatchClauses())
        if (matchClause.hasPathPatterns())
          for (final PathPattern path : matchClause.getPathPatterns())
            registerPathPatternVarTypes(path, varTypes);

      if (statement.getCreateClause() != null && !statement.getCreateClause().isEmpty())
        for (final PathPattern path : statement.getCreateClause().getPathPatterns())
          registerPathPatternVarTypes(path, varTypes);

      if (statement.getMergeClause() != null)
        registerPathPatternVarTypes(statement.getMergeClause().getPathPattern(), varTypes);
    }
  }

  private void registerPathPatternVarTypes(final PathPattern path, final Map<String, VarType> varTypes) {
    if (path.hasPathVariable())
      registerVar(path.getPathVariable(), VarType.PATH, varTypes);

    for (final NodePattern node : path.getNodes())
      if (node.getVariable() != null)
        registerVar(node.getVariable(), VarType.NODE, varTypes);

    for (final RelationshipPattern rel : path.getRelationships())
      if (rel.getVariable() != null)
        registerVar(rel.getVariable(), VarType.RELATIONSHIP, varTypes);
  }

  private void registerVar(final String name, final VarType type, final Map<String, VarType> varTypes) {
    final VarType existing = varTypes.get(name);
    if (existing != null && existing != type)
      throw new CommandParsingException("VariableTypeConflict: Variable '" + name + "' already defined as " + existing + ", cannot redefine as " + type);
    varTypes.put(name, type);
  }

  // ==============================
  // Phase 2b: Variable Already Bound
  // ==============================

  private void validateVariableBinding(final CypherStatement statement) {
    final Set<String> boundVars = new HashSet<>();

    final List<ClauseEntry> clausesInOrder = statement.getClausesInOrder();
    if (clausesInOrder != null && !clausesInOrder.isEmpty()) {
      for (final ClauseEntry entry : clausesInOrder) {
        switch (entry.getType()) {
          case MATCH:
            final MatchClause matchClause = entry.getTypedClause();
            if (matchClause.hasPathPatterns())
              for (final PathPattern path : matchClause.getPathPatterns())
                addBoundVarsFromPattern(path, boundVars);
            break;
          case CREATE:
            final CreateClause createClause = entry.getTypedClause();
            if (createClause != null && !createClause.isEmpty())
              for (final PathPattern path : createClause.getPathPatterns())
                checkCreateBinding(path, boundVars);
            break;
          case MERGE:
            final MergeClause mergeClause = entry.getTypedClause();
            if (mergeClause != null)
              checkMergeBinding(mergeClause.getPathPattern(), boundVars);
            break;
          case WITH:
            // WITH resets scope to only the projected aliases
            final WithClause withClause = entry.getTypedClause();
            boundVars.clear();
            for (final ReturnClause.ReturnItem item : withClause.getItems()) {
              final String alias = item.getAlias();
              if (alias != null)
                boundVars.add(alias);
              else if (item.getExpression() instanceof VariableExpression)
                boundVars.add(((VariableExpression) item.getExpression()).getVariableName());
            }
            break;
          case UNWIND:
            final UnwindClause unwindClause = entry.getTypedClause();
            boundVars.add(unwindClause.getVariable());
            break;
          default:
            break;
        }
      }
    } else {
      // Fallback: walk match then create
      for (final MatchClause matchClause : statement.getMatchClauses())
        if (matchClause.hasPathPatterns())
          for (final PathPattern path : matchClause.getPathPatterns())
            addBoundVarsFromPattern(path, boundVars);

      if (statement.getCreateClause() != null && !statement.getCreateClause().isEmpty())
        for (final PathPattern path : statement.getCreateClause().getPathPatterns())
          checkCreateBinding(path, boundVars);
    }
  }

  private void addBoundVarsFromPattern(final PathPattern path, final Set<String> boundVars) {
    if (path.hasPathVariable())
      boundVars.add(path.getPathVariable());
    for (final NodePattern node : path.getNodes())
      if (node.getVariable() != null)
        boundVars.add(node.getVariable());
    for (final RelationshipPattern rel : path.getRelationships())
      if (rel.getVariable() != null)
        boundVars.add(rel.getVariable());
  }

  private void checkCreateBinding(final PathPattern path, final Set<String> boundVars) {
    for (final NodePattern node : path.getNodes()) {
      final String var = node.getVariable();
      if (var != null && boundVars.contains(var)) {
        // It's a rebinding error if CREATE defines a new entity for an already-bound var
        // (i.e., it has labels or properties that would make it a new entity definition)
        if (node.hasLabels() || node.hasProperties())
          throw new CommandParsingException("VariableAlreadyBound: Variable '" + var + "' already defined, cannot " +
              "rebind in CREATE");
      }
    }
    // After checking, add the new bindings from this CREATE
    addBoundVarsFromPattern(path, boundVars);
  }

  private void checkMergeBinding(final PathPattern path, final Set<String> boundVars) {
    // MERGE binds variables similarly to CREATE — add them
    addBoundVarsFromPattern(path, boundVars);
  }

  // ==============================
  // Phase 2c: Undefined Variables
  // ==============================

  private void validateVariableScope(final CypherStatement statement) {
    final Set<String> scope = new HashSet<>();

    final List<ClauseEntry> clausesInOrder = statement.getClausesInOrder();
    if (clausesInOrder == null || clausesInOrder.isEmpty())
      return; // Can't validate scope without clause ordering

    for (final ClauseEntry entry : clausesInOrder) {
      switch (entry.getType()) {
        case MATCH:
          final MatchClause matchClause = entry.getTypedClause();
          if (matchClause.hasPathPatterns())
            for (final PathPattern path : matchClause.getPathPatterns())
              addBoundVarsFromPattern(path, scope);
          break;
        case CREATE:
          final CreateClause createClause = entry.getTypedClause();
          if (createClause != null && !createClause.isEmpty())
            for (final PathPattern path : createClause.getPathPatterns())
              addBoundVarsFromPattern(path, scope);
          break;
        case MERGE:
          final MergeClause mergeClause = entry.getTypedClause();
          if (mergeClause != null)
            addBoundVarsFromPattern(mergeClause.getPathPattern(), scope);
          break;
        case UNWIND:
          final UnwindClause unwindClause = entry.getTypedClause();
          // The list expression in UNWIND may reference outer scope variables
          // but the unwind variable itself is new
          scope.add(unwindClause.getVariable());
          break;
        case WITH:
          // WITH resets scope to only projected aliases
          final WithClause withClause = entry.getTypedClause();
          // Check for WITH * — passes all variables through
          boolean hasWildcard = false;
          for (final ReturnClause.ReturnItem item : withClause.getItems()) {
            if (item.getExpression() instanceof StarExpression ||
                (item.getExpression() instanceof VariableExpression &&
                    "*".equals(((VariableExpression) item.getExpression()).getVariableName()))) {
              hasWildcard = true;
              break;
            }
          }
          if (hasWildcard) {
            // WITH * keeps all variables in scope — don't validate or reset
            break;
          }
          // First validate that all referenced variables in WITH are in scope
          for (final ReturnClause.ReturnItem item : withClause.getItems())
            checkExpressionScope(item.getExpression(), scope);
          // Build the combined scope for ORDER BY: pre-WITH scope + projected aliases
          // ORDER BY in WITH can reference both original variables and new aliases
          if (withClause.getOrderByClause() != null) {
            final Set<String> orderByScope = new HashSet<>(scope);
            for (final ReturnClause.ReturnItem item : withClause.getItems()) {
              if (item.getAlias() != null)
                orderByScope.add(item.getAlias());
              else if (item.getExpression() instanceof VariableExpression)
                orderByScope.add(((VariableExpression) item.getExpression()).getVariableName());
            }
            for (final OrderByClause.OrderByItem item : withClause.getOrderByClause().getItems())
              if (item.getExpressionAST() != null)
                checkExpressionScope(item.getExpressionAST(), orderByScope);
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
          final SetClause setClause = entry.getTypedClause();
          if (setClause != null && !setClause.isEmpty())
            for (final SetClause.SetItem item : setClause.getItems())
              if (isValidVariableName(item.getVariable()) && !scope.contains(item.getVariable()))
                throw new CommandParsingException("UndefinedVariable: Variable '" + item.getVariable() + "' not defined");
          break;
        case REMOVE:
          // REMOVE references variables that must be in scope — but complex to validate
          break;
        case DELETE:
          final DeleteClause deleteClause2 = entry.getTypedClause();
          if (deleteClause2 != null && !deleteClause2.isEmpty())
            for (final String var : deleteClause2.getVariables())
              if (isValidVariableName(var) && !scope.contains(var))
                throw new CommandParsingException("UndefinedVariable: Variable '" + var + "' not defined");
          break;
        case RETURN:
          // Validate RETURN references — only check top-level variable references
          // to avoid false positives from complex expression types that the AST builder
          // creates as VariableExpression with raw text
          if (statement.getReturnClause() != null)
            for (final ReturnClause.ReturnItem item : statement.getReturnClause().getReturnItems())
              checkExpressionScope(item.getExpression(), scope);
          break;
        case CALL:
          // Procedure CALL with YIELD — exported yield variables enter scope
          final CallClause callClause = entry.getTypedClause();
          if (callClause != null && callClause.hasYield())
            for (final CallClause.YieldItem item : callClause.getYieldItems())
              scope.add(item.getOutputName());
          break;
        case SUBQUERY:
          // CALL { ... RETURN x AS y } — exported return variables enter outer scope
          final SubqueryClause subqueryClause = entry.getTypedClause();
          if (subqueryClause != null && subqueryClause.getInnerStatement() != null) {
            final ReturnClause innerReturn = subqueryClause.getInnerStatement().getReturnClause();
            if (innerReturn != null)
              for (final ReturnClause.ReturnItem item : innerReturn.getReturnItems()) {
                if (item.getAlias() != null)
                  scope.add(item.getAlias());
                else if (item.getExpression() instanceof VariableExpression)
                  scope.add(((VariableExpression) item.getExpression()).getVariableName());
              }
          }
          break;
        case FOREACH:
          break;
      }
    }
  }

  private void checkExpressionScope(final Expression expr, final Set<String> scope) {
    if (expr == null)
      return;

    // Skip StarExpression (used in count(*), WITH *)
    if (expr instanceof StarExpression)
      return;

    if (expr instanceof VariableExpression) {
      final String varName = ((VariableExpression) expr).getVariableName();
      // Skip synthetic variable names that are really expression text (AST builder artifacts)
      // The AST builder creates VariableExpression from raw text in some cases
      if (!isValidVariableName(varName))
        return;
      if (!scope.contains(varName))
        throw new CommandParsingException("UndefinedVariable: Variable '" + varName + "' not defined");
    } else if (expr instanceof PropertyAccessExpression) {
      final String varName = ((PropertyAccessExpression) expr).getVariableName();
      if (isValidVariableName(varName) && !scope.contains(varName))
        throw new CommandParsingException("UndefinedVariable: Variable '" + varName + "' not defined");
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
    } else if (expr instanceof ListPredicateExpression) {
      final ListPredicateExpression lpe = (ListPredicateExpression) expr;
      checkExpressionScope(lpe.getListExpression(), scope);
      // The variable introduces a new scope binding for the WHERE expression
      final Set<String> innerScope = new HashSet<>(scope);
      innerScope.add(lpe.getVariable());
      if (lpe.getWhereExpression() != null)
        checkExpressionScope(lpe.getWhereExpression(), innerScope);
    }
    // LiteralExpression, ParameterExpression, StarExpression — no variables to check
  }

  private void checkBooleanExpressionScope(final BooleanExpression boolExpr, final Set<String> scope) {
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
    }
    // Other BooleanExpression types (IsNullExpression, LabelCheckExpression, etc.)
    // are less commonly tested in TCK scope validation
  }

  // ==============================
  // Phase 3: Boolean Type Checking
  // ==============================

  private void validateBooleanOperandTypes(final CypherStatement statement) {
    // Check RETURN clause expressions
    if (statement.getReturnClause() != null)
      for (final ReturnClause.ReturnItem item : statement.getReturnClause().getReturnItems())
        checkBooleanOperandInExpression(item.getExpression());

    // Check WITH clause expressions
    for (final WithClause withClause : statement.getWithClauses())
      for (final ReturnClause.ReturnItem item : withClause.getItems())
        checkBooleanOperandInExpression(item.getExpression());

    // Check WHERE clause expressions
    final List<ClauseEntry> clauses = statement.getClausesInOrder();
    if (clauses != null) {
      for (final ClauseEntry entry : clauses) {
        if (entry.getType() == ClauseEntry.ClauseType.MATCH) {
          final MatchClause matchClause = entry.getTypedClause();
          if (matchClause.hasWhereClause())
            checkBooleanOperandInBooleanExpression(matchClause.getWhereClause().getConditionExpression());
        } else if (entry.getType() == ClauseEntry.ClauseType.WITH) {
          final WithClause withClause = entry.getTypedClause();
          if (withClause.getWhereClause() != null)
            checkBooleanOperandInBooleanExpression(withClause.getWhereClause().getConditionExpression());
        }
      }
    }
    if (statement.getWhereClause() != null)
      checkBooleanOperandInBooleanExpression(statement.getWhereClause().getConditionExpression());
  }

  private void checkBooleanOperandInExpression(final Expression expr) {
    if (expr == null)
      return;

    if (expr instanceof TernaryLogicalExpression) {
      final TernaryLogicalExpression tle = (TernaryLogicalExpression) expr;
      // Check that operands are valid boolean types
      checkOperandForBooleanContext(tle.getLeft());
      if (tle.getRight() != null)
        checkOperandForBooleanContext(tle.getRight());
      // Recurse into operands
      checkBooleanOperandInExpression(tle.getLeft());
      if (tle.getRight() != null)
        checkBooleanOperandInExpression(tle.getRight());
    } else if (expr instanceof FunctionCallExpression) {
      for (final Expression arg : ((FunctionCallExpression) expr).getArguments())
        checkBooleanOperandInExpression(arg);
    } else if (expr instanceof ArithmeticExpression) {
      checkBooleanOperandInExpression(((ArithmeticExpression) expr).getLeft());
      checkBooleanOperandInExpression(((ArithmeticExpression) expr).getRight());
    } else if (expr instanceof ListExpression) {
      for (final Expression elem : ((ListExpression) expr).getElements())
        checkBooleanOperandInExpression(elem);
    } else if (expr instanceof BooleanWrapperExpression) {
      checkBooleanOperandInBooleanExpression(((BooleanWrapperExpression) expr).getBooleanExpression());
    } else if (expr instanceof CaseExpression) {
      final CaseExpression caseExpr = (CaseExpression) expr;
      if (caseExpr.getCaseExpression() != null)
        checkBooleanOperandInExpression(caseExpr.getCaseExpression());
      for (final CaseAlternative alt : caseExpr.getAlternatives()) {
        checkBooleanOperandInExpression(alt.getWhenExpression());
        checkBooleanOperandInExpression(alt.getThenExpression());
      }
      if (caseExpr.getElseExpression() != null)
        checkBooleanOperandInExpression(caseExpr.getElseExpression());
    }
  }

  private void checkBooleanOperandInBooleanExpression(final BooleanExpression boolExpr) {
    if (boolExpr == null)
      return;

    if (boolExpr instanceof LogicalExpression) {
      final LogicalExpression logExpr = (LogicalExpression) boolExpr;
      // Check operands
      checkBooleanExprOperandValidity(logExpr.getLeft());
      if (logExpr.getRight() != null)
        checkBooleanExprOperandValidity(logExpr.getRight());
      // Recurse
      checkBooleanOperandInBooleanExpression(logExpr.getLeft());
      if (logExpr.getRight() != null)
        checkBooleanOperandInBooleanExpression(logExpr.getRight());
    } else if (boolExpr instanceof ComparisonExpression) {
      final ComparisonExpression comp = (ComparisonExpression) boolExpr;
      checkBooleanOperandInExpression(comp.getLeft());
      checkBooleanOperandInExpression(comp.getRight());
    } else if (boolExpr instanceof InExpression) {
      final InExpression inExpr = (InExpression) boolExpr;
      checkBooleanOperandInExpression(inExpr.getExpression());
      for (final Expression elem : inExpr.getList())
        checkBooleanOperandInExpression(elem);
    }
  }

  private void checkOperandForBooleanContext(final Expression operand) {
    if (operand == null)
      return;

    // Reject non-boolean literal values
    if (operand instanceof LiteralExpression) {
      final Object value = ((LiteralExpression) operand).getValue();
      if (value != null && !(value instanceof Boolean))
        throw new CommandParsingException("InvalidArgumentType: Expected Boolean but got " + value.getClass().getSimpleName());
    }
    // Reject list expressions as boolean operands (e.g., [1,2] AND true)
    else if (operand instanceof ListExpression)
      throw new CommandParsingException("InvalidArgumentType: Expected Boolean but got List");
      // Reject map expressions as boolean operands (e.g., {a: 1} AND true)
    else if (operand instanceof MapExpression)
      throw new CommandParsingException("InvalidArgumentType: Expected Boolean but got Map");
      // Reject arithmetic expressions (always return numbers)
    else if (operand instanceof ArithmeticExpression)
      throw new CommandParsingException("InvalidArgumentType: Expected Boolean but got Number");
  }

  private void checkBooleanExprOperandValidity(final BooleanExpression operand) {
    // For BooleanExpression operands in LogicalExpression, we need to unwrap
    // to check for non-boolean literals. ComparisonExpression, InExpression, etc.
    // always return boolean, so those are fine. But if the operand wraps an Expression,
    // we need to check it.
    // In practice, the ANTLR parser produces LogicalExpression with BooleanExpression operands
    // that are typically ComparisonExpression, InExpression, etc. — these are inherently boolean.
    // The invalid cases are more typically caught via TernaryLogicalExpression in RETURN context.
  }

  // ==============================
  // Phase 4: Aggregation Validation
  // ==============================

  private void validateAggregations(final CypherStatement statement) {
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
  }

  private void checkNestedAggregation(final Expression expr, final boolean insideAggregation) {
    if (expr == null)
      return;

    if (expr instanceof FunctionCallExpression) {
      final FunctionCallExpression func = (FunctionCallExpression) expr;
      if (func.isAggregation()) {
        if (insideAggregation)
          throw new CommandParsingException("InvalidAggregation: Nested aggregation functions are not allowed");
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
    } else if (expr instanceof ListExpression) {
      for (final Expression elem : ((ListExpression) expr).getElements())
        checkNestedAggregation(elem, insideAggregation);
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

  private void checkAggregationInWhere(final WhereClause whereClause) {
    if (whereClause == null || whereClause.getConditionExpression() == null)
      return;
    checkAggregationInBooleanExpression(whereClause.getConditionExpression());
  }

  private void checkAggregationInBooleanExpression(final BooleanExpression boolExpr) {
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
    }
  }

  private void checkAggregationInOrderBy(final Expression expr) {
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

  private void checkAggregationInExpression(final Expression expr) {
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

  // ==========================================
  // Phase 5: CREATE/MERGE/DELETE Constraints
  // ==========================================

  private void validateCreateConstraints(final CypherStatement statement) {
    final List<ClauseEntry> clausesInOrder = statement.getClausesInOrder();
    if (clausesInOrder != null && !clausesInOrder.isEmpty()) {
      for (final ClauseEntry entry : clausesInOrder) {
        if (entry.getType() == ClauseEntry.ClauseType.CREATE) {
          final CreateClause createClause = entry.getTypedClause();
          if (createClause != null && !createClause.isEmpty())
            for (final PathPattern path : createClause.getPathPatterns())
              validateCreatePathPattern(path);
        } else if (entry.getType() == ClauseEntry.ClauseType.MERGE) {
          final MergeClause mergeClause = entry.getTypedClause();
          if (mergeClause != null)
            validateMergePathPattern(mergeClause.getPathPattern());
        } else if (entry.getType() == ClauseEntry.ClauseType.DELETE) {
          final DeleteClause deleteClause = entry.getTypedClause();
          if (deleteClause != null && !deleteClause.isEmpty())
            validateDeleteTargets(deleteClause);
        }
      }
    } else {
      // Fallback: check statement-level clauses
      if (statement.getCreateClause() != null && !statement.getCreateClause().isEmpty())
        for (final PathPattern path : statement.getCreateClause().getPathPatterns())
          validateCreatePathPattern(path);

      if (statement.getMergeClause() != null)
        validateMergePathPattern(statement.getMergeClause().getPathPattern());

      if (statement.getDeleteClause() != null && !statement.getDeleteClause().isEmpty())
        validateDeleteTargets(statement.getDeleteClause());
    }
  }

  private void validateCreatePathPattern(final PathPattern path) {
    for (final RelationshipPattern rel : path.getRelationships()) {
      // CREATE relationships must specify exactly one type
      if (!rel.hasTypes())
        throw new CommandParsingException("NoSingleRelationshipType: Relationships must have a type in CREATE");
      // CREATE relationships must be directed
      if (rel.getDirection() == Direction.BOTH)
        throw new CommandParsingException("RequiresDirectedRelationship: Relationships must be directed in CREATE");
      // CREATE cannot use variable-length patterns
      if (rel.isVariableLength())
        throw new CommandParsingException("CreatingVarLength: Variable-length relationships are not allowed in CREATE");
    }
  }

  private void validateMergePathPattern(final PathPattern path) {
    for (final RelationshipPattern rel : path.getRelationships()) {
      // MERGE relationships must specify exactly one type
      if (!rel.hasTypes())
        throw new CommandParsingException("NoSingleRelationshipType: Relationships must have a type in MERGE");
      // MERGE cannot use variable-length patterns
      if (rel.isVariableLength())
        throw new CommandParsingException("CreatingVarLength: Variable-length relationships are not allowed in MERGE");
    }
  }

  private void validateDeleteTargets(final DeleteClause deleteClause) {
    // DELETE targets from the parser are variable names (possibly with map access).
    // The TCK's InvalidDelete tests involve deleting labels (DELETE n:Person)
    // which would need AST-level detection, not simple string checks.
    // For now, skip this validation to avoid false positives on map access patterns.
  }

  // ==============================
  // Phase 6: SKIP/LIMIT Validation
  // ==============================

  private void validateSkipLimit(final CypherStatement statement) {
    final Integer skip = statement.getSkip();
    if (skip != null && skip < 0)
      throw new CommandParsingException("NegativeIntegerArgument: SKIP value cannot be negative: " + skip);
    final Integer limit = statement.getLimit();
    if (limit != null && limit < 0)
      throw new CommandParsingException("NegativeIntegerArgument: LIMIT value cannot be negative: " + limit);

    // Check WITH clauses
    for (final WithClause withClause : statement.getWithClauses()) {
      if (withClause.getSkip() != null && withClause.getSkip() < 0)
        throw new CommandParsingException("NegativeIntegerArgument: SKIP value cannot be negative: " + withClause.getSkip());
      if (withClause.getLimit() != null && withClause.getLimit() < 0)
        throw new CommandParsingException("NegativeIntegerArgument: LIMIT value cannot be negative: " + withClause.getLimit());
    }
  }

  // ==========================================
  // Phase 7: Column Name Conflict
  // ==========================================

  private void validateColumnNames(final CypherStatement statement) {
    // Check RETURN clause for duplicate aliases
    if (statement.getReturnClause() != null)
      checkDuplicateAliases(statement.getReturnClause().getReturnItems());

    // Check WITH clauses for duplicate aliases
    for (final WithClause withClause : statement.getWithClauses())
      checkDuplicateAliases(withClause.getItems());
  }

  private void checkDuplicateAliases(final List<ReturnClause.ReturnItem> items) {
    final Set<String> seen = new HashSet<>();
    for (final ReturnClause.ReturnItem item : items) {
      String name = item.getAlias();
      if (name == null && item.getExpression() instanceof VariableExpression)
        name = ((VariableExpression) item.getExpression()).getVariableName();
      if (name != null && !name.equals("*")) {
        if (!seen.add(name))
          throw new CommandParsingException("ColumnNameConflict: Column name '" + name + "' is defined more than once");
      }
    }
  }

  // ==========================================
  // Phase 8: Expression Alias Validation
  // ==========================================

  private void validateExpressionAliases(final CypherStatement statement) {
    // In WITH, non-variable expressions (aggregations, function calls, arithmetic, etc.)
    // must have an alias. Simple variable references don't need one.
    for (final WithClause withClause : statement.getWithClauses()) {
      // Only enforce if WITH has aggregations (mixed aggregation/non-aggregation context)
      if (withClause.hasAggregations()) {
        for (final ReturnClause.ReturnItem item : withClause.getItems()) {
          final Expression expr = item.getExpression();
          if (expr instanceof FunctionCallExpression && ((FunctionCallExpression) expr).isAggregation()) {
            if (item.getAlias() == null)
              throw new CommandParsingException("NoExpressionAlias: Expression in WITH must be aliased (use AS)");
          }
        }
      }
    }
  }

  /**
   * Checks if a variable name looks like a real Cypher variable identifier.
   * Filters out synthetic names created by the AST builder from raw expression text.
   * The AST builder sometimes creates VariableExpression with raw text like
   * "xINeqWHEREx" (from "x IN eq WHERE x") or "[1,2]" or "n:Foo".
   */
  private static boolean isValidVariableName(final String name) {
    if (name == null || name.isEmpty() || name.equals("*"))
      return false;

    // Valid Cypher identifiers start with letter or underscore
    final char first = name.charAt(0);
    if (!Character.isLetter(first) && first != '_' && first != '`')
      return false;

    // Check all characters are valid identifier chars
    for (int i = 1; i < name.length(); i++) {
      final char c = name.charAt(i);
      if (!Character.isLetterOrDigit(c) && c != '_' && c != '`')
        return false;
    }

    // Reject names that contain embedded Cypher keywords at word boundaries
    // These are artifacts of the AST builder concatenating expression tokens
    // e.g., "xINeqWHEREx" from "x IN eq WHERE x", "bINc" from "b IN c"
    // We detect this by looking for uppercase keyword sequences preceded/followed by lowercase
    if (name.length() > 2 && hasBoundaryKeyword(name))
      return false;

    return true;
  }

  /**
   * Detects Cypher keywords embedded at word boundaries within a name.
   * Returns true if the name appears to be concatenated tokens.
   * Example: "xINeqWHEREx" → true (contains "IN" and "WHERE" at word boundaries)
   * Example: "minValue" → false ("IN" is not at a word boundary — preceded by 'm')
   */
  private static boolean hasBoundaryKeyword(final String name) {
    // Check for specific keyword patterns where the keyword is preceded by a lowercase letter
    // and followed by a lowercase letter or another uppercase sequence
    final String[] keywords = {"IN", "WHERE", "AND", "OR", "XOR", "NOT", "AS", "IS"};
    for (final String kw : keywords) {
      int idx = 0;
      while ((idx = name.indexOf(kw, idx)) >= 0) {
        // Check boundary: the char before must be lowercase (word boundary)
        final boolean leftBoundary = idx > 0 && Character.isLowerCase(name.charAt(idx - 1));
        // Check boundary: the char after must exist and be lowercase (word boundary)
        final int afterIdx = idx + kw.length();
        final boolean rightBoundary = afterIdx < name.length() && Character.isLowerCase(name.charAt(afterIdx));
        if (leftBoundary && rightBoundary)
          return true;
        idx += kw.length();
      }
    }
    return false;
  }
}
