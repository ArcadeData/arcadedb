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
    NODE, RELATIONSHIP, PATH, SCALAR
  }

  private final Map<String, VarType> varTypes = new HashMap<>();

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
    v.validateRelationshipUniqueness(statement);
    v.validateAggregations(statement);
    v.validateBooleanOperandTypes(statement);
    v.validateSkipLimit(statement);
    v.validateColumnNames(statement);
    v.validateExpressionAliases(statement);
    v.validateReturnStar(statement);
    v.validateFunctionArgumentTypes(statement);
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
          case WITH:
            // WITH resets scope. Variables projected through WITH may keep their type
            // if they directly reference a pattern variable. For literal values (numbers,
            // strings, booleans, maps), mark as SCALAR to detect type conflicts.
            // For complex expressions, don't track type to avoid false positives.
            final WithClause withClauseTypes = entry.getTypedClause();
            final Map<String, VarType> newVarTypes = new HashMap<>();
            for (final ReturnClause.ReturnItem item : withClauseTypes.getItems()) {
              final String outputName = item.getOutputName();
              if (outputName == null || "*".equals(outputName))
                continue;
              // Check if the expression is a simple variable reference that preserves type
              if (item.getExpression() instanceof VariableExpression) {
                final String srcVar = ((VariableExpression) item.getExpression()).getVariableName();
                final VarType srcType = varTypes.get(srcVar);
                if (srcType != null) {
                  newVarTypes.put(outputName, srcType);
                  continue;
                }
              }
              // Only mark as SCALAR for clear non-null literal values
              if (item.getExpression() instanceof LiteralExpression) {
                final Object litVal = ((LiteralExpression) item.getExpression()).getValue();
                if (litVal != null)
                  newVarTypes.put(outputName, VarType.SCALAR);
              } else if (item.getExpression() instanceof MapExpression
                  || isLiteralListExpression(item.getExpression()))
                newVarTypes.put(outputName, VarType.SCALAR);
              // A list containing node variables (e.g., [n]) cannot be used as a node
              else if (item.getExpression() instanceof ListExpression && containsNodeVariable(item.getExpression()))
                newVarTypes.put(outputName, VarType.SCALAR);
              // For complex expressions (list indexing, function calls, etc.)
              // don't track type to avoid false positives
            }
            varTypes.clear();
            varTypes.putAll(newVarTypes);
            break;
          case UNWIND:
            // UNWIND variable type depends on the list content, can't determine at parse time
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
          case LOAD_CSV:
            final LoadCSVClause loadCSVClause2 = entry.getTypedClause();
            boundVars.add(loadCSVClause2.getVariable());
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
        // It's a rebinding error if CREATE defines a new entity for an already-bound var:
        // 1. Node has labels or properties (redefining the entity)
        // 2. Node has explicit properties even if empty (e.g., n {})
        // 3. Node is standalone (single node in path, no relationships) - tries to create new node
        if (node.hasLabels() || node.hasProperties() || node.hasExplicitProperties() || path.isSingleNode())
          throw new CommandParsingException("VariableAlreadyBound: Variable '" + var + "' already defined, cannot " +
              "rebind in CREATE");
      }
    }
    // After checking, add the new bindings from this CREATE
    addBoundVarsFromPattern(path, boundVars);
  }

  private void checkMergeBinding(final PathPattern path, final Set<String> boundVars) {
    // Check if MERGE rebinds node variables with new labels/properties
    for (final NodePattern node : path.getNodes()) {
      final String var = node.getVariable();
      if (var != null && boundVars.contains(var) && (node.hasLabels() || node.hasProperties() || path.isSingleNode()))
        throw new CommandParsingException(
            "VariableAlreadyBound: Variable '" + var + "' already defined, cannot rebind in MERGE");
    }
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
          // Validate WHERE clause references against scope
          if (matchClause.getWhereClause() != null)
            checkBooleanExpressionScope(matchClause.getWhereClause().getConditionExpression(), scope);
          break;
        case CREATE:
          final CreateClause createClause = entry.getTypedClause();
          if (createClause != null && !createClause.isEmpty())
            for (final PathPattern path : createClause.getPathPatterns()) {
              // Check property value expressions for undefined variables
              for (final NodePattern node : path.getNodes())
                if (node.hasProperties())
                  checkPropertyValuesScope(node.getProperties(), scope);
              for (final RelationshipPattern rel : path.getRelationships())
                if (rel.hasProperties())
                  checkPropertyValuesScope(rel.getProperties(), scope);
              addBoundVarsFromPattern(path, scope);
            }
          break;
        case MERGE:
          final MergeClause mergeClause = entry.getTypedClause();
          if (mergeClause != null) {
            addBoundVarsFromPattern(mergeClause.getPathPattern(), scope);
            // Validate ON CREATE SET / ON MATCH SET variables
            validateSetClauseScope(mergeClause.getOnCreateSet(), scope);
            validateSetClauseScope(mergeClause.getOnMatchSet(), scope);
          }
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
          // Build the scope for ORDER BY
          if (withClause.getOrderByClause() != null) {
            final Set<String> orderByScope;
            if (withClause.hasAggregations()) {
              // After aggregation, restrict to output aliases + grouping key variables
              orderByScope = new HashSet<>();
              for (final ReturnClause.ReturnItem item : withClause.getItems()) {
                if (item.getAlias() != null)
                  orderByScope.add(item.getAlias());
                else if (item.getExpression() instanceof VariableExpression)
                  orderByScope.add(((VariableExpression) item.getExpression()).getVariableName());
                if (!item.getExpression().containsAggregation())
                  collectVariableNamesFromExpression(item.getExpression(), orderByScope);
              }
            } else {
              // Non-aggregating WITH: ORDER BY can reference both original scope + aliases
              orderByScope = new HashSet<>(scope);
              for (final ReturnClause.ReturnItem item : withClause.getItems()) {
                if (item.getAlias() != null)
                  orderByScope.add(item.getAlias());
                else if (item.getExpression() instanceof VariableExpression)
                  orderByScope.add(((VariableExpression) item.getExpression()).getVariableName());
              }
            }
            for (final OrderByClause.OrderByItem item : withClause.getOrderByClause().getItems())
              if (item.getExpressionAST() != null) {
                if (withClause.hasAggregations())
                  checkExpressionScopeSkipAggArgs(item.getExpressionAST(), orderByScope);
                else
                  checkExpressionScope(item.getExpressionAST(), orderByScope);
              }
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
            for (final SetClause.SetItem item : setClause.getItems()) {
              if (isValidVariableName(item.getVariable()) && !scope.contains(item.getVariable()))
                throw new CommandParsingException("UndefinedVariable: Variable '" + item.getVariable() + "' not defined");
              if (item.getValueExpression() != null)
                checkExpressionScope(item.getValueExpression(), scope);
            }
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
          if (statement.getReturnClause() != null) {
            for (final ReturnClause.ReturnItem item : statement.getReturnClause().getReturnItems())
              checkExpressionScope(item.getExpression(), scope);
            // Validate ORDER BY scope
            if (statement.getOrderByClause() != null) {
              if (statement.getReturnClause().isDistinct()) {
                // After DISTINCT, ORDER BY can only reference returned columns
                final Set<String> returnedNames = new HashSet<>();
                for (final ReturnClause.ReturnItem item : statement.getReturnClause().getReturnItems()) {
                  returnedNames.add(item.getOutputName());
                  if (item.getAlias() != null)
                    returnedNames.add(item.getAlias());
                  if (item.getExpression() instanceof VariableExpression)
                    returnedNames.add(((VariableExpression) item.getExpression()).getVariableName());
                }
                for (final OrderByClause.OrderByItem item : statement.getOrderByClause().getItems()) {
                  final String orderExprText = item.getExpression();
                  if (orderExprText != null && !returnedNames.contains(orderExprText)) {
                    if (item.getExpressionAST() != null) {
                      final Set<String> distinctScope = new HashSet<>();
                      for (final ReturnClause.ReturnItem rItem : statement.getReturnClause().getReturnItems()) {
                        if (rItem.getAlias() != null)
                          distinctScope.add(rItem.getAlias());
                        else if (rItem.getExpression() instanceof VariableExpression)
                          distinctScope.add(((VariableExpression) rItem.getExpression()).getVariableName());
                      }
                      checkExpressionScope(item.getExpressionAST(), distinctScope);
                    }
                  }
                }
              } else if (statement.getReturnClause().hasAggregations()) {
                // After aggregation, ORDER BY scope restricted to aliases + grouping key variables
                final Set<String> orderByScope = new HashSet<>();
                for (final ReturnClause.ReturnItem item : statement.getReturnClause().getReturnItems()) {
                  if (item.getAlias() != null)
                    orderByScope.add(item.getAlias());
                  else if (item.getExpression() instanceof VariableExpression)
                    orderByScope.add(((VariableExpression) item.getExpression()).getVariableName());
                  if (!item.getExpression().containsAggregation())
                    collectVariableNamesFromExpression(item.getExpression(), orderByScope);
                }
                for (final OrderByClause.OrderByItem item : statement.getOrderByClause().getItems())
                  if (item.getExpressionAST() != null)
                    checkExpressionScopeSkipAggArgs(item.getExpressionAST(), orderByScope);
              }
            }
          }
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
    } else if (boolExpr instanceof PatternPredicateExpression) {
      // Pattern predicates in WHERE must not introduce new variables
      final PathPattern path = ((PatternPredicateExpression) boolExpr).getPathPattern();
      if (path != null) {
        // A single-node pattern in WHERE is invalid: WHERE (n) is not a valid predicate
        if (path.isSingleNode())
          throw new CommandParsingException("InvalidArgumentType: Single node pattern is not a valid predicate in WHERE");
        checkPatternPredicateVariables(path, scope);
      }
    } else if (boolExpr instanceof IsNullExpression) {
      checkExpressionScope(((IsNullExpression) boolExpr).getExpression(), scope);
    } else if (boolExpr instanceof LabelCheckExpression) {
      checkExpressionScope(((LabelCheckExpression) boolExpr).getVariableExpression(), scope);
    }
  }

  private void checkPatternPredicateVariables(final PathPattern path, final Set<String> scope) {
    for (final NodePattern node : path.getNodes()) {
      final String var = node.getVariable();
      if (var != null && !var.isEmpty() && isValidVariableName(var) && !scope.contains(var))
        throw new CommandParsingException("UndefinedVariable: Variable '" + var + "' not defined");
    }
    for (final RelationshipPattern rel : path.getRelationships()) {
      final String var = rel.getVariable();
      if (var != null && !var.isEmpty() && isValidVariableName(var) && !scope.contains(var))
        throw new CommandParsingException("UndefinedVariable: Variable '" + var + "' not defined");
    }
  }

  private static boolean isLiteralListExpression(final Expression expr) {
    if (!(expr instanceof ListExpression))
      return false;
    for (final Expression elem : ((ListExpression) expr).getElements())
      if (!(elem instanceof LiteralExpression))
        return false;
    return true;
  }

  private boolean containsNodeVariable(final Expression expr) {
    if (expr instanceof ListExpression)
      for (final Expression elem : ((ListExpression) expr).getElements()) {
        if (elem instanceof VariableExpression) {
          final VarType type = varTypes.get(((VariableExpression) elem).getVariableName());
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
  private void checkExpressionScopeSkipAggArgs(final Expression expr, final Set<String> scope) {
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

  private void checkPropertyValuesScope(final Map<String, Object> properties, final Set<String> scope) {
    for (final Object value : properties.values())
      if (value instanceof Expression)
        checkExpressionScope((Expression) value, scope);
  }

  private void validateSetClauseScope(final SetClause setClause, final Set<String> scope) {
    if (setClause == null || setClause.isEmpty())
      return;
    for (final SetClause.SetItem item : setClause.getItems()) {
      if (isValidVariableName(item.getVariable()) && !scope.contains(item.getVariable()))
        throw new CommandParsingException("UndefinedVariable: Variable '" + item.getVariable() + "' not defined");
      if (item.getValueExpression() != null)
        checkExpressionScope(item.getValueExpression(), scope);
    }
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

    // Check for ambiguous aggregation expressions in RETURN items
    if (statement.getReturnClause() != null)
      checkAmbiguousAggregation(statement.getReturnClause().getReturnItems());

    // Check for ambiguous aggregation in WITH items
    for (final WithClause withClause : statement.getWithClauses())
      checkAmbiguousAggregation(withClause.getItems());

    // Check for ambiguous aggregation in ORDER BY for aggregating RETURN
    if (statement.getReturnClause() != null && statement.getReturnClause().hasAggregations()
        && statement.getOrderByClause() != null) {
      final Set<String> groupingVars = collectGroupingVariables(statement.getReturnClause().getReturnItems());
      for (final OrderByClause.OrderByItem item : statement.getOrderByClause().getItems()) {
        final Expression expr = item.getExpressionAST();
        if (expr != null && expr.containsAggregation())
          checkMixedAggregation(expr, groupingVars);
      }
    }

    // Check for ambiguous aggregation in ORDER BY for aggregating WITH
    for (final WithClause withClause : statement.getWithClauses()) {
      if (withClause.hasAggregations() && withClause.getOrderByClause() != null) {
        final Set<String> groupingVars = collectGroupingVariables(withClause.getItems());
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
              throw new CommandParsingException("UndefinedVariable: Aggregation in ORDER BY is not projected");
          }
          if (expr.containsAggregation())
            checkMixedAggregation(expr, groupingVars);
        }
      }
    }
  }

  private void checkNestedAggregation(final Expression expr, final boolean insideAggregation) {
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

  private void checkNestedAggregationInBoolean(final BooleanExpression boolExpr, final boolean insideAggregation) {
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

  private void checkNonConstantInAggregation(final Expression expr) {
    if (expr instanceof FunctionCallExpression) {
      final FunctionCallExpression func = (FunctionCallExpression) expr;
      if (NON_DETERMINISTIC_FUNCTIONS.contains(func.getFunctionName().toLowerCase(Locale.ROOT)))
        throw new CommandParsingException("NonConstantExpression: Non-constant expression is not allowed inside aggregation: " + func.getFunctionName());
    }
  }

  private void checkAggregationInListComprehension(final Expression expr) {
    if (expr == null)
      return;
    if (expr instanceof FunctionCallExpression && ((FunctionCallExpression) expr).isAggregation())
      throw new CommandParsingException("InvalidAggregation: Aggregation functions are not allowed in list comprehensions");
    if (expr instanceof ArithmeticExpression) {
      checkAggregationInListComprehension(((ArithmeticExpression) expr).getLeft());
      checkAggregationInListComprehension(((ArithmeticExpression) expr).getRight());
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

  /**
   * Check for ambiguous aggregation: expressions that mix aggregation calls with
   * non-aggregated sub-expressions. The rules are:
   * - Simple variable/property references matching grouping keys are OK
   * - Literals and parameters are OK
   * - Complex expressions (arithmetic, etc.) containing variables are AMBIGUOUS
   * - Variable/property references NOT matching grouping keys are AMBIGUOUS
   */
  private void checkAmbiguousAggregation(final List<ReturnClause.ReturnItem> items) {
    final Set<String> groupingVars = collectGroupingVariables(items);
    for (final ReturnClause.ReturnItem item : items) {
      final Expression expr = item.getExpression();
      if (!expr.containsAggregation())
        continue;
      // Pure aggregation call is fine
      if (expr instanceof FunctionCallExpression && ((FunctionCallExpression) expr).isAggregation())
        continue;
      // Mixed expression — check non-aggregated parts
      checkMixedAggregation(expr, groupingVars);
    }
  }

  /**
   * Recursively checks a mixed aggregation expression for ambiguous non-aggregated parts.
   */
  private void checkMixedAggregation(final Expression expr, final Set<String> groupingVars) {
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
        validateNonAggPart(right, groupingVars);
        checkMixedAggregation(left, groupingVars);
      } else if (!leftAgg && rightAgg) {
        validateNonAggPart(left, groupingVars);
        checkMixedAggregation(right, groupingVars);
      } else {
        checkMixedAggregation(left, groupingVars);
        checkMixedAggregation(right, groupingVars);
      }
    } else if (expr instanceof FunctionCallExpression) {
      for (final Expression arg : ((FunctionCallExpression) expr).getArguments())
        checkMixedAggregation(arg, groupingVars);
    } else if (expr instanceof BooleanWrapperExpression) {
      checkMixedAggregationInBoolean(((BooleanWrapperExpression) expr).getBooleanExpression(), groupingVars);
    } else if (expr instanceof ComparisonExpressionWrapper) {
      final ComparisonExpression comp = ((ComparisonExpressionWrapper) expr).getComparison();
      checkMixedAggregation(comp.getLeft(), groupingVars);
      checkMixedAggregation(comp.getRight(), groupingVars);
    }
  }

  private void checkMixedAggregationInBoolean(final BooleanExpression boolExpr, final Set<String> groupingVars) {
    if (boolExpr instanceof ComparisonExpression) {
      checkMixedAggregation(((ComparisonExpression) boolExpr).getLeft(), groupingVars);
      checkMixedAggregation(((ComparisonExpression) boolExpr).getRight(), groupingVars);
    } else if (boolExpr instanceof LogicalExpression) {
      checkMixedAggregationInBoolean(((LogicalExpression) boolExpr).getLeft(), groupingVars);
      if (((LogicalExpression) boolExpr).getRight() != null)
        checkMixedAggregationInBoolean(((LogicalExpression) boolExpr).getRight(), groupingVars);
    }
  }

  /**
   * Validates that a non-aggregated part in a mixed expression is not ambiguous.
   */
  private void validateNonAggPart(final Expression expr, final Set<String> groupingVars) {
    if (expr == null)
      return;
    // Simple variable reference — OK if it's a grouping key
    if (expr instanceof VariableExpression) {
      if (!groupingVars.contains(((VariableExpression) expr).getVariableName()))
        throw new CommandParsingException("AmbiguousAggregationExpression: Ambiguous aggregation expression");
      return;
    }
    // Simple property access — OK if the variable is a grouping key
    if (expr instanceof PropertyAccessExpression) {
      if (!groupingVars.contains(((PropertyAccessExpression) expr).getVariableName()))
        throw new CommandParsingException("AmbiguousAggregationExpression: Ambiguous aggregation expression");
      return;
    }
    // Literals and stars are always OK
    if (expr instanceof LiteralExpression || expr instanceof StarExpression)
      return;
    // Complex expressions containing variables are ambiguous
    if (hasVariableRefOutsideAggregation(expr))
      throw new CommandParsingException("AmbiguousAggregationExpression: Ambiguous aggregation expression");
  }

  private static boolean hasVariableRefOutsideAggregation(final Expression expr) {
    if (expr == null)
      return false;
    if (expr instanceof VariableExpression || expr instanceof PropertyAccessExpression)
      return true;
    if (expr instanceof LiteralExpression || expr instanceof StarExpression)
      return false;
    if (expr instanceof FunctionCallExpression) {
      final FunctionCallExpression func = (FunctionCallExpression) expr;
      if (func.isAggregation())
        return false; // Variables inside aggregation arguments are OK
      for (final Expression arg : func.getArguments())
        if (hasVariableRefOutsideAggregation(arg))
          return true;
      return false;
    }
    if (expr instanceof ArithmeticExpression)
      return hasVariableRefOutsideAggregation(((ArithmeticExpression) expr).getLeft())
          || hasVariableRefOutsideAggregation(((ArithmeticExpression) expr).getRight());
    if (expr instanceof BooleanWrapperExpression)
      return hasBooleanVarRefOutsideAgg(((BooleanWrapperExpression) expr).getBooleanExpression());
    if (expr instanceof ComparisonExpressionWrapper) {
      final ComparisonExpression comp = ((ComparisonExpressionWrapper) expr).getComparison();
      return hasVariableRefOutsideAggregation(comp.getLeft())
          || hasVariableRefOutsideAggregation(comp.getRight());
    }
    return false;
  }

  private static boolean hasBooleanVarRefOutsideAgg(final BooleanExpression boolExpr) {
    if (boolExpr == null)
      return false;
    if (boolExpr instanceof ComparisonExpression)
      return hasVariableRefOutsideAggregation(((ComparisonExpression) boolExpr).getLeft())
          || hasVariableRefOutsideAggregation(((ComparisonExpression) boolExpr).getRight());
    if (boolExpr instanceof LogicalExpression) {
      if (hasBooleanVarRefOutsideAgg(((LogicalExpression) boolExpr).getLeft()))
        return true;
      return ((LogicalExpression) boolExpr).getRight() != null
          && hasBooleanVarRefOutsideAgg(((LogicalExpression) boolExpr).getRight());
    }
    return false;
  }

  private Set<String> collectGroupingVariables(final List<ReturnClause.ReturnItem> items) {
    final Set<String> vars = new HashSet<>();
    for (final ReturnClause.ReturnItem item : items)
      if (!item.getExpression().containsAggregation()) {
        collectVariableNamesFromExpression(item.getExpression(), vars);
        // Also include the alias as a valid grouping reference
        if (item.getAlias() != null)
          vars.add(item.getAlias());
      }
    return vars;
  }

  private static void collectVariableNamesFromExpression(final Expression expr, final Set<String> vars) {
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

  private static void collectVariableRefsOutsideAggregation(final Expression expr, final Set<String> vars) {
    if (expr == null)
      return;
    if (expr instanceof VariableExpression)
      vars.add(((VariableExpression) expr).getVariableName());
    else if (expr instanceof PropertyAccessExpression)
      vars.add(((PropertyAccessExpression) expr).getVariableName());
    else if (expr instanceof FunctionCallExpression) {
      if (((FunctionCallExpression) expr).isAggregation())
        return; // Stop at aggregation boundary
      for (final Expression arg : ((FunctionCallExpression) expr).getArguments())
        collectVariableRefsOutsideAggregation(arg, vars);
    } else if (expr instanceof ArithmeticExpression) {
      collectVariableRefsOutsideAggregation(((ArithmeticExpression) expr).getLeft(), vars);
      collectVariableRefsOutsideAggregation(((ArithmeticExpression) expr).getRight(), vars);
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
      // CREATE relationships must not have multiple types
      if (rel.getTypes().size() > 1)
        throw new CommandParsingException("NoSingleRelationshipType: Relationships must have exactly one type in CREATE, got: " + rel.getTypes());
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
      // MERGE relationships must not have multiple types
      if (rel.getTypes().size() > 1)
        throw new CommandParsingException("NoSingleRelationshipType: Relationships must have exactly one type in MERGE, got: " + rel.getTypes());
      // MERGE cannot use variable-length patterns
      if (rel.isVariableLength())
        throw new CommandParsingException("CreatingVarLength: Variable-length relationships are not allowed in MERGE");
    }
    // MERGE cannot have null property values
    checkMergeNullProperties(path);
  }

  private void checkMergeNullProperties(final PathPattern path) {
    for (final NodePattern node : path.getNodes())
      if (node.hasProperties())
        for (final Object value : node.getProperties().values())
          checkMergePropertyNotNull(value);
    for (final RelationshipPattern rel : path.getRelationships())
      if (rel.hasProperties())
        for (final Object value : rel.getProperties().values())
          checkMergePropertyNotNull(value);
  }

  private void checkMergePropertyNotNull(final Object value) {
    if (value == null)
      throw new CommandParsingException("MergeReadOwnWrites: MERGE does not support null property values");
    if (value instanceof LiteralExpression && ((LiteralExpression) value).getValue() == null)
      throw new CommandParsingException("MergeReadOwnWrites: MERGE does not support null property values");
  }

  private void validateDeleteTargets(final DeleteClause deleteClause) {
    for (final String target : deleteClause.getVariables()) {
      if (target == null)
        continue;
      // DELETE n:Label or DELETE r:TYPE is invalid (InvalidDelete)
      if (target.contains(":"))
        throw new CommandParsingException("InvalidDelete: Cannot delete a label or relationship type: " + target);
      // DELETE <arithmetic expression> like DELETE 1+1 is invalid (InvalidArgumentType)
      if (!isValidVariableName(target) && !target.contains(".") && !target.contains("["))
        throw new CommandParsingException("InvalidArgumentType: DELETE requires a node, relationship, or path variable, got: " + target);
    }
  }

  // ============================================
  // Phase 5b: Relationship Uniqueness Validation
  // ============================================

  private void validateRelationshipUniqueness(final CypherStatement statement) {
    for (final MatchClause matchClause : statement.getMatchClauses()) {
      if (!matchClause.hasPathPatterns())
        continue;
      for (final PathPattern path : matchClause.getPathPatterns()) {
        final Set<String> relVars = new HashSet<>();
        for (final RelationshipPattern rel : path.getRelationships()) {
          final String var = rel.getVariable();
          if (var != null && !var.isEmpty() && !relVars.add(var))
            throw new CommandParsingException("RelationshipUniquenessViolation: Relationship variable '" + var + "' is used more than once in the same pattern");
        }
      }
    }
  }

  // ============================================
  // Phase 9: RETURN * Validation
  // ============================================

  private void validateReturnStar(final CypherStatement statement) {
    if (statement.getReturnClause() == null)
      return;
    for (final ReturnClause.ReturnItem item : statement.getReturnClause().getReturnItems()) {
      if (item.getExpression() instanceof StarExpression ||
          (item.getExpression() instanceof VariableExpression &&
              "*".equals(((VariableExpression) item.getExpression()).getVariableName()))) {
        // RETURN * requires at least one named variable in scope
        boolean hasNamedVars = false;
        for (final MatchClause matchClause : statement.getMatchClauses()) {
          if (matchClause.hasPathPatterns())
            for (final PathPattern path : matchClause.getPathPatterns()) {
              for (final NodePattern node : path.getNodes())
                if (node.getVariable() != null && !node.getVariable().isEmpty()) {
                  hasNamedVars = true;
                  break;
                }
              if (hasNamedVars)
                break;
              for (final RelationshipPattern rel : path.getRelationships())
                if (rel.getVariable() != null && !rel.getVariable().isEmpty()) {
                  hasNamedVars = true;
                  break;
                }
              if (hasNamedVars)
                break;
              if (path.hasPathVariable()) {
                hasNamedVars = true;
                break;
              }
            }
          if (hasNamedVars)
            break;
        }
        // Also check UNWIND and WITH for variables
        if (!hasNamedVars) {
          for (final UnwindClause unwind : statement.getUnwindClauses()) {
            if (unwind.getVariable() != null) {
              hasNamedVars = true;
              break;
            }
          }
        }
        if (!hasNamedVars)
          throw new CommandParsingException("NoVariablesInScope: RETURN * is not allowed when there are no variables in scope");
      }
    }
  }

  // ==============================
  // Phase 6: SKIP/LIMIT Validation
  // ==============================

  private void validateSkipLimit(final CypherStatement statement) {
    validateSkipLimitExpr(statement.getSkip(), "SKIP");
    validateSkipLimitExpr(statement.getLimit(), "LIMIT");

    // Check WITH clauses
    for (final WithClause withClause : statement.getWithClauses()) {
      validateSkipLimitExpr(withClause.getSkip(), "SKIP");
      validateSkipLimitExpr(withClause.getLimit(), "LIMIT");
    }
  }

  private void validateSkipLimitExpr(final Expression expr, final String clauseName) {
    if (expr == null)
      return;
    // Check for negative and floating-point literal values
    if (expr instanceof LiteralExpression) {
      final Object val = ((LiteralExpression) expr).getValue();
      if (val instanceof Number) {
        if (val instanceof Float || val instanceof Double) {
          final double d = ((Number) val).doubleValue();
          if (d != Math.floor(d) || Double.isInfinite(d))
            throw new CommandParsingException("InvalidArgumentType: " + clauseName + " value must be an integer, got: Float(" + d + ")");
        }
        if (((Number) val).intValue() < 0)
          throw new CommandParsingException("NegativeIntegerArgument: " + clauseName + " value cannot be negative: " + val);
      }
    }
    // Check that SKIP/LIMIT expressions don't reference query variables (NonConstantExpression)
    if (containsVariableReference(expr))
      throw new CommandParsingException("NonConstantExpression: " + clauseName + " expression must not reference variables");
  }

  private static boolean containsVariableReference(final Expression expr) {
    if (expr instanceof VariableExpression)
      return !"*".equals(((VariableExpression) expr).getVariableName());
    if (expr instanceof PropertyAccessExpression)
      return true;
    if (expr instanceof ArithmeticExpression) {
      return containsVariableReference(((ArithmeticExpression) expr).getLeft())
          || containsVariableReference(((ArithmeticExpression) expr).getRight());
    }
    if (expr instanceof FunctionCallExpression) {
      for (final Expression arg : ((FunctionCallExpression) expr).getArguments())
        if (containsVariableReference(arg))
          return true;
    }
    return false;
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

  // ============================================
  // Phase 10: Function Argument Type Validation
  // ============================================

  private void validateFunctionArgumentTypes(final CypherStatement statement) {
    // Check RETURN clause
    if (statement.getReturnClause() != null)
      for (final ReturnClause.ReturnItem item : statement.getReturnClause().getReturnItems())
        checkFunctionArgTypes(item.getExpression());
    // Check WITH clauses
    for (final WithClause withClause : statement.getWithClauses())
      for (final ReturnClause.ReturnItem item : withClause.getItems())
        checkFunctionArgTypes(item.getExpression());
    // Check WHERE clauses for property access on path variables
    final List<ClauseEntry> clauses = statement.getClausesInOrder();
    if (clauses != null)
      for (final ClauseEntry entry : clauses) {
        if (entry.getType() == ClauseEntry.ClauseType.MATCH) {
          final MatchClause matchClause = entry.getTypedClause();
          if (matchClause.hasWhereClause())
            checkPropertyAccessOnPathInBoolean(matchClause.getWhereClause().getConditionExpression());
        } else if (entry.getType() == ClauseEntry.ClauseType.WITH) {
          final WithClause withClause = entry.getTypedClause();
          if (withClause.getWhereClause() != null)
            checkPropertyAccessOnPathInBoolean(withClause.getWhereClause().getConditionExpression());
        }
      }
  }

  private void checkFunctionArgTypes(final Expression expr) {
    if (expr == null)
      return;
    if (expr instanceof FunctionCallExpression) {
      final FunctionCallExpression func = (FunctionCallExpression) expr;
      final String name = func.getFunctionName().toLowerCase();
      // Check for unknown functions (skip namespaced functions like date.truncate, they're handled by CypherFunctionRegistry)
      if (!name.contains(".") && !FunctionValidator.isKnownFunction(name))
        throw new CommandParsingException("UnknownFunction: Unknown function '" + func.getFunctionName() + "'");
      final List<Expression> args = func.getArguments();
      if (args.size() == 1) {
        final Expression arg = args.get(0);
        final VarType argType = getExpressionType(arg);
        if (argType != null) {
          switch (name) {
            case "length":
              // length() only works on paths and strings, not nodes or relationships
              if (argType == VarType.NODE)
                throw new CommandParsingException("InvalidArgumentType: length() cannot be applied to a node");
              if (argType == VarType.RELATIONSHIP)
                throw new CommandParsingException("InvalidArgumentType: length() cannot be applied to a relationship");
              break;
            case "type":
              // type() only works on relationships
              if (argType == VarType.NODE)
                throw new CommandParsingException("InvalidArgumentType: type() requires a relationship argument, got node");
              break;
            case "labels":
              // labels() only works on nodes
              if (argType == VarType.PATH)
                throw new CommandParsingException("InvalidArgumentType: labels() requires a node argument, got path");
              break;
            case "size":
              // size() works on strings and lists, not paths
              if (argType == VarType.PATH)
                throw new CommandParsingException("InvalidArgumentType: size() cannot be applied to a path");
              break;
          }
        }
      }
      // Recurse into arguments
      for (final Expression arg : args)
        checkFunctionArgTypes(arg);
    } else if (expr instanceof ArithmeticExpression) {
      checkFunctionArgTypes(((ArithmeticExpression) expr).getLeft());
      checkFunctionArgTypes(((ArithmeticExpression) expr).getRight());
    } else if (expr instanceof ListExpression) {
      for (final Expression elem : ((ListExpression) expr).getElements())
        checkFunctionArgTypes(elem);
    } else if (expr instanceof CaseExpression) {
      final CaseExpression caseExpr = (CaseExpression) expr;
      if (caseExpr.getCaseExpression() != null)
        checkFunctionArgTypes(caseExpr.getCaseExpression());
      for (final CaseAlternative alt : caseExpr.getAlternatives()) {
        checkFunctionArgTypes(alt.getWhenExpression());
        checkFunctionArgTypes(alt.getThenExpression());
      }
      if (caseExpr.getElseExpression() != null)
        checkFunctionArgTypes(caseExpr.getElseExpression());
    }
  }

  private void checkPropertyAccessOnPath(final Expression expr) {
    if (expr == null)
      return;
    if (expr instanceof PropertyAccessExpression) {
      final String varName = ((PropertyAccessExpression) expr).getVariableName();
      final VarType type = varTypes.get(varName);
      if (type == VarType.PATH)
        throw new CommandParsingException("InvalidArgumentType: Property access on a path variable is not allowed");
    } else if (expr instanceof FunctionCallExpression) {
      for (final Expression arg : ((FunctionCallExpression) expr).getArguments())
        checkPropertyAccessOnPath(arg);
    } else if (expr instanceof ArithmeticExpression) {
      checkPropertyAccessOnPath(((ArithmeticExpression) expr).getLeft());
      checkPropertyAccessOnPath(((ArithmeticExpression) expr).getRight());
    }
  }

  private void checkPropertyAccessOnPathInBoolean(final BooleanExpression boolExpr) {
    if (boolExpr == null)
      return;
    if (boolExpr instanceof ComparisonExpression) {
      checkPropertyAccessOnPath(((ComparisonExpression) boolExpr).getLeft());
      checkPropertyAccessOnPath(((ComparisonExpression) boolExpr).getRight());
    } else if (boolExpr instanceof LogicalExpression) {
      checkPropertyAccessOnPathInBoolean(((LogicalExpression) boolExpr).getLeft());
      if (((LogicalExpression) boolExpr).getRight() != null)
        checkPropertyAccessOnPathInBoolean(((LogicalExpression) boolExpr).getRight());
    } else if (boolExpr instanceof InExpression) {
      checkPropertyAccessOnPath(((InExpression) boolExpr).getExpression());
    } else if (boolExpr instanceof IsNullExpression) {
      checkPropertyAccessOnPath(((IsNullExpression) boolExpr).getExpression());
    }
  }

  private VarType getExpressionType(final Expression expr) {
    if (expr instanceof VariableExpression) {
      final String varName = ((VariableExpression) expr).getVariableName();
      return varTypes.get(varName);
    }
    return null;
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
