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
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.function.math.RoundFunction;
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
 * <p>
 * <b>A subquery body is validated exactly as the query around it.</b> The body of a {@code CALL { }} clause and of an
 * {@code EXISTS { }} / {@code COUNT { }} / {@code COLLECT { }} expression is a statement, and every phase below runs
 * against it - see {@link #validateNestedStatements}. Ten of them used to stop at the boundary (issue #5656), which
 * is how the same mistake came to be rejected written one way and accepted written one level in.
 * <p>
 * <b>Semantic vs syntax boundary:</b> only an undefined-variable violation throws
 * {@link CommandSemanticException}, which Bolt surfaces as {@code Neo.ClientError.Statement.SemanticError}
 * (certified by conformance scenario ERR-002). Every other validation failure in this class throws
 * {@link CommandParsingException}, surfaced over Bolt as {@code SyntaxError}. This split is deliberate but
 * not yet verified case-by-case against real Neo4j error classification; other violations here may turn
 * out to be genuine semantic errors on further scrutiny.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class CypherSemanticValidator {

  private enum VarType {
    NODE, RELATIONSHIP, PATH, SCALAR
  }

  private final Map<String, VarType> varTypes = new HashMap<>();

  private CypherSemanticValidator() {
  }

  /**
   * A validator bound to the variable kinds a subquery body sees - what its own clauses declare over what it inherits
   * from the enclosing scope. Used by {@link NestedStatementChecks} to run the phase list against a body.
   */
  private CypherSemanticValidator(final Map<String, VarType> scope) {
    varTypes.putAll(scope);
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
    v.validateRelationshipUniqueness(statement);
    v.validateAggregations(statement);
    v.validateBooleanOperandTypes(statement);
    v.validateSkipLimit(statement);
    v.validateColumnNames(statement);
    v.validateExpressionAliases(statement);
    v.validateReturnStar(statement);
    v.validateFunctionArgumentTypes(statement);
    v.validateNestedStatements(statement);
  }

  // ========================
  // Phase 0: UNION Validation
  // ========================

  private static void validateUnion(final UnionStatement unionStmt) {
    validateUnionShape(unionStmt);

    for (final CypherStatement query : unionStmt.getQueries())
      // A branch with no RETURN carries no columns to compare and is not a query this validates on its own.
      if (query.getReturnClause() != null)
        validate(query);
  }

  /**
   * The two checks that are about the {@code UNION} itself rather than about any one branch.
   * <p>
   * Separate from {@link #validateUnion} because a {@code UNION} can also appear as the body of a subquery, where the
   * branches are validated by the walk that descends into them ({@link NestedStatementChecks}) and only these two are
   * still owed. Before issue #5656 nothing ran them there: {@code CALL { RETURN 1 AS a UNION RETURN 2 AS b }} was
   * accepted, while the same mismatch written as the whole query was rejected.
   */
  private static void validateUnionShape(final UnionStatement unionStmt) {
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
    for (final CypherStatement query : unionStmt.getQueries()) {
      final ReturnClause returnClause = query.getReturnClause();
      if (returnClause == null)
        continue;
      final List<String> columns = returnClause.getItems();
      if (firstColumns == null)
        firstColumns = columns;
      else if (!firstColumns.equals(columns))
        throw new CommandParsingException(
            "DifferentColumnsInUnion: All sub queries in a UNION must have the same return column names");
    }
  }

  // ========================
  // Phase 2a: Variable Types
  // ========================

  private void validateVariableTypes(final CypherStatement statement) {
    // The field is refilled rather than replaced: the later phases hold this same map.
    varTypes.clear();
    varTypes.putAll(buildVarTypes(statement, Map.of()));
  }

  /**
   * Builds the variable kinds a statement's clauses declare, walking them in order: a graph pattern declares them, a
   * {@code WITH} resets the scope to what it projects, and a binding that says nothing about the kind - an
   * {@code UNWIND} variable, a {@code YIELD} output - drops whatever the name held.
   * <p>
   * One construction serves both the statement being validated and the body of a subquery inside it. They used to be
   * two, which is how the two spellings of the same import came to disagree (#5626) - the kind of drift that follows
   * from writing the same walk twice. What separates the two callers is one argument: {@code inherited} is empty for
   * the statement, which declares its scope from nothing, and the enclosing scope for a subquery body, which sees the
   * outer row. See {@link #nestedVarTypes} for what that inheritance means for a {@code CALL { }} body, which imports
   * nothing.
   * <p>
   * <b>Shadowing an inherited name is not the same as clashing with one declared here.</b> Declaring {@code x} a node
   * and then a relationship is the {@code VariableTypeConflict} this phase exists to raise, and it is raised inside a
   * body exactly as outside one - which is the whole point of #5626. Re-declaring a name the body only <i>inherited</i>
   * is not that: an implicit {@code CALL { }} imports nothing, so a body is free to bind a name the enclosing query
   * happens to use for something else. The two are told apart by tracking which names this scope declared itself,
   * rather than by a flag that would have to turn the check off wholesale to allow the second.
   */
  private static Map<String, VarType> buildVarTypes(final CypherStatement statement,
      final Map<String, VarType> inherited) {
    final Map<String, VarType> scope = new HashMap<>(inherited);
    // The names this scope declares for itself, as opposed to the ones it inherited: only a clash between two of
    // these is a conflict. Empty for a statement, so there every clash is one.
    final Set<String> declaredHere = new HashSet<>();

    final List<ClauseEntry> clauses = statement.getClausesInOrder();
    if (clauses == null || clauses.isEmpty()) {
      // Some builder paths leave the ordered list empty; the per-kind getters still answer.
      for (final MatchClause matchClause : statement.getMatchClauses())
        if (matchClause.hasPathPatterns())
          for (final PathPattern path : matchClause.getPathPatterns())
            declarePatternVarTypes(path, scope, declaredHere);

      if (statement.getCreateClause() != null && !statement.getCreateClause().isEmpty())
        for (final PathPattern path : statement.getCreateClause().getPathPatterns())
          declarePatternVarTypes(path, scope, declaredHere);

      if (statement.getMergeClause() != null)
        declarePatternVarTypes(statement.getMergeClause().getPathPattern(), scope, declaredHere);

      return scope;
    }

    for (final ClauseEntry entry : clauses) {
      switch (entry.getType()) {
      case MATCH -> {
        final MatchClause matchClause = entry.getTypedClause();
        if (matchClause.hasPathPatterns())
          for (final PathPattern path : matchClause.getPathPatterns())
            declarePatternVarTypes(path, scope, declaredHere);
      }
      case CREATE -> {
        final CreateClause createClause = entry.getTypedClause();
        if (createClause != null && !createClause.isEmpty())
          for (final PathPattern path : createClause.getPathPatterns())
            declarePatternVarTypes(path, scope, declaredHere);
      }
      case MERGE -> {
        final MergeClause mergeClause = entry.getTypedClause();
        if (mergeClause != null)
          declarePatternVarTypes(mergeClause.getPathPattern(), scope, declaredHere);
      }
      case WITH -> applyWithProjection(entry.getTypedClause(), scope, declaredHere);
      case UNWIND -> forget(((UnwindClause) entry.getTypedClause()).getVariable(), scope, declaredHere);
      case LOAD_CSV -> forget(((LoadCSVClause) entry.getTypedClause()).getVariable(), scope, declaredHere);
      // Only the FOREACH variable itself: what the clause's inner CREATE/MERGE bind lives and dies inside the loop
      // and is not in scope after it, so declaring those kinds here would be declaring them where they cannot be
      // referenced. The expression walk still descends into the inner clauses and checks them against this scope.
      case FOREACH -> forget(((ForeachClause) entry.getTypedClause()).getVariable(), scope, declaredHere);
      case CALL -> {
        final CallClause callClause = entry.getTypedClause();
        if (callClause.hasYield())
          for (final CallClause.YieldItem item : callClause.getYieldItems())
            forget(item.getOutputName(), scope, declaredHere);
      }
      case SUBQUERY -> {
        // What a nested CALL subquery returns enters this scope under a kind this phase does not track back through
        // it; that subquery's body is a scope of its own and gets its own map when the walk descends.
        final SubqueryClause subqueryClause = entry.getTypedClause();
        if (subqueryClause.getInnerStatement() != null) {
          final ReturnClause innerReturn = subqueryClause.getInnerStatement().getReturnClause();
          if (innerReturn != null)
            for (final ReturnClause.ReturnItem item : innerReturn.getReturnItems())
              forget(item.getOutputName(), scope, declaredHere);
        }
      }
      default -> {
        // No binding of its own.
      }
      }
    }
    return scope;
  }

  /**
   * Applies a {@code WITH} to the kinds in scope: the clause resets the scope to what it projects, and each item
   * carries through whatever kind {@link #projectedVarType} can read off its expression. That is what keeps an
   * importing {@code WITH p} - and a renaming {@code WITH p AS q} - a path, while {@code WITH 1 AS p} stops being one.
   * <p>
   * {@code WITH *} passes the incoming scope through, so a kind survives a projection that does not name it.
   * <p>
   * What the clause projects becomes this scope's own, whether the name came from here or was inherited: past a
   * {@code WITH p} the body has restated {@code p}, so a later clause declaring it something else is the conflict a
   * restatement makes it, not the shadowing an inherited name would have allowed.
   */
  private static void applyWithProjection(final WithClause withClause, final Map<String, VarType> scope,
      final Set<String> declaredHere) {
    boolean star = false;
    final Map<String, VarType> carried = new HashMap<>();
    final Set<String> lost = new HashSet<>();

    for (final ReturnClause.ReturnItem item : withClause.getItems()) {
      final Expression expr = item.getExpression();
      if (expr instanceof StarExpression
          || (expr instanceof VariableExpression variable && "*".equals(variable.getVariableName()))) {
        star = true;
        continue;
      }
      final String outputName = item.getOutputName();
      if (outputName == null || "*".equals(outputName))
        continue;

      final VarType projected = projectedVarType(expr, scope);
      if (projected != null)
        carried.put(outputName, projected);
      else
        lost.add(outputName);
    }

    if (!star) {
      scope.clear();
      declaredHere.clear();
    }
    scope.keySet().removeAll(lost);
    declaredHere.removeAll(lost);
    scope.putAll(carried);
    declaredHere.addAll(carried.keySet());
  }

  /**
   * The kind a {@code WITH} item projects, or null when the expression says nothing about it - which is the answer
   * for anything computed (a function call, an index into a list), because guessing there is how a check starts
   * rejecting valid queries.
   */
  private static VarType projectedVarType(final Expression expr, final Map<String, VarType> scope) {
    // A plain reference carries the source variable's kind, and carries none when the source has none.
    if (expr instanceof VariableExpression variable)
      return scope.get(variable.getVariableName());

    // A value that is plainly not a graph element is SCALAR rather than untracked, so using it as one is caught.
    if (expr instanceof LiteralExpression literal)
      return literal.getValue() != null ? VarType.SCALAR : null;
    if (expr instanceof MapExpression || isLiteralListExpression(expr))
      return VarType.SCALAR;
    // A list holding node variables, [n], is itself not a node.
    if (expr instanceof ListExpression && containsNodeVariable(expr, scope))
      return VarType.SCALAR;

    return null;
  }

  private static void declarePatternVarTypes(final PathPattern path, final Map<String, VarType> scope,
      final Set<String> declaredHere) {
    if (path == null)
      return;

    if (path.hasPathVariable())
      declareVar(path.getPathVariable(), VarType.PATH, scope, declaredHere);
    for (final NodePattern node : path.getNodes())
      if (node.getVariable() != null)
        declareVar(node.getVariable(), VarType.NODE, scope, declaredHere);
    for (final RelationshipPattern rel : path.getRelationships())
      if (rel.getVariable() != null)
        declareVar(rel.getVariable(), VarType.RELATIONSHIP, scope, declaredHere);
  }

  /**
   * Declares one name as one kind. A second declaration of a name <i>this scope already declared</i>, as a different
   * kind, is the conflict; a first declaration of a name the scope merely inherited is shadowing, and is allowed.
   */
  private static void declareVar(final String name, final VarType type, final Map<String, VarType> scope,
      final Set<String> declaredHere) {
    final VarType existing = scope.get(name);
    if (existing != null && existing != type && declaredHere.contains(name))
      throw new CommandParsingException(
          "VariableTypeConflict: Variable '" + name + "' already defined as " + existing + ", cannot redefine as "
              + type);

    scope.put(name, type);
    declaredHere.add(name);
  }

  /**
   * Drops a name bound by something that says nothing about its kind - an {@code UNWIND} variable, a {@code YIELD}
   * output. It leaves this scope's declarations too, so a later pattern may declare it afresh without clashing with
   * a kind the name no longer has.
   */
  private static void forget(final String name, final Map<String, VarType> scope, final Set<String> declaredHere) {
    scope.remove(name);
    declaredHere.remove(name);
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
  private void validateVariableScope(final CypherStatement statement, final Set<String> scope,
      final Set<String> shadowed) {
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
              checkPatternVarsNotShadowed(path, scope, shadowed);
              addBoundVarsFromPattern(path, scope);
            }
          break;
        case MERGE:
          final MergeClause mergeClause = entry.getTypedClause();
          if (mergeClause != null) {
            checkPatternVarsNotShadowed(mergeClause.getPathPattern(), scope, shadowed);
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
            // WITH * keeps all variables in scope.
            // First validate all extra item expressions against the incoming scope (no scope
            // mutation during validation — aliases must not be visible to sibling expressions).
            for (final ReturnClause.ReturnItem item : withClause.getItems()) {
              if (item.getExpression() instanceof StarExpression ||
                  (item.getExpression() instanceof VariableExpression &&
                      "*".equals(((VariableExpression) item.getExpression()).getVariableName())))
                continue;
              checkExpressionScope(item.getExpression(), scope);
            }
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
            } else {
              // Non-aggregating WITH: ORDER BY can reference both original scope + aliases
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
                throw new CommandSemanticException("UndefinedVariable: Variable '" + item.getVariable() + "' not defined");
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
                throw new CommandSemanticException("UndefinedVariable: Variable '" + var + "' not defined");
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
                validateCollapsedOrderByScope(statement.getReturnClause().getReturnItems(), statement.getOrderByClause(), false);
              } else if (statement.getReturnClause().hasAggregations()) {
                // Likewise after aggregation: what the projection did not keep, ORDER BY cannot sort
                // on. Reporting it beats the sort silently doing nothing (issue #5286).
                validateCollapsedOrderByScope(statement.getReturnClause().getReturnItems(), statement.getOrderByClause(), true);
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
            // Validate the subquery body: outer variables are visible only if imported (issue #5213).
            validateSubqueryScope(subqueryClause, scope, shadowed);
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
  private void validateSubqueryScope(final SubqueryClause clause, final Set<String> outerScope,
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
          if (isValidVariableName(v) && !outerScope.contains(v))
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

  private void validateSubqueryBranchScope(final CypherStatement branch, final Set<String> outerScope,
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
      if (expr instanceof StarExpression ||
          (expr instanceof VariableExpression && "*".equals(((VariableExpression) expr).getVariableName())))
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
  private void checkPatternVarsNotShadowed(final PathPattern path, final Set<String> scope, final Set<String> shadowed) {
    if (shadowed.isEmpty() || path == null)
      return;

    if (path.hasPathVariable())
      checkVarNotShadowed(path.getPathVariable(), scope, shadowed);
    for (final NodePattern node : path.getNodes())
      checkVarNotShadowed(node.getVariable(), scope, shadowed);
    for (final RelationshipPattern rel : path.getRelationships())
      checkVarNotShadowed(rel.getVariable(), scope, shadowed);
  }

  private void checkVarNotShadowed(final String var, final Set<String> scope, final Set<String> shadowed) {
    if (var != null && isValidVariableName(var) && !scope.contains(var) && shadowed.contains(var))
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
   * {@link #checkAmbiguousAggregation} instead. Such an item keeps the wider scope so that check, not
   * this one, gets to report it.
   *
   * @param aggregating whether the projection aggregates, which admits the grouping-key variables into
   *                    the scope of ORDER BY items that contain an aggregation
   */
  private void validateCollapsedOrderByScope(final List<ReturnClause.ReturnItem> items, final OrderByClause orderBy,
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
              collectVariableNamesFromExpression(projected.getExpression(), aggregationScope);
        }
        checkExpressionScopeSkipAggArgs(item.getExpressionAST(), aggregationScope);
      } else
        checkExpressionScope(item.getExpressionAST(), orderByScope);
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
        throw new CommandSemanticException("UndefinedVariable: Variable '" + varName + "' not defined");
    } else if (expr instanceof PropertyAccessExpression) {
      final String varName = ((PropertyAccessExpression) expr).getVariableName();
      if (isValidVariableName(varName) && !scope.contains(varName))
        throw new CommandSemanticException("UndefinedVariable: Variable '" + varName + "' not defined");
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
    } else if (boolExpr instanceof BooleanCoercionExpression) {
      // A WHERE body that is a bare expression coerced to boolean (e.g. any()/all()/none()/single()
      // list predicates, a boolean-typed property, or a comprehension) is scope-checked as a plain
      // expression. This catches variables that leak out of a pattern/list comprehension into the
      // outer predicate scope (issue #5179).
      checkExpressionScope(((BooleanCoercionExpression) boolExpr).getExpression(), scope);
    }
  }

  private void checkPatternPredicateVariables(final PathPattern path, final Set<String> scope) {
    for (final NodePattern node : path.getNodes()) {
      final String var = node.getVariable();
      if (var != null && !var.isEmpty() && isValidVariableName(var) && !scope.contains(var))
        throw new CommandSemanticException("UndefinedVariable: Variable '" + var + "' not defined");
    }
    for (final RelationshipPattern rel : path.getRelationships()) {
      final String var = rel.getVariable();
      if (var != null && !var.isEmpty() && isValidVariableName(var) && !scope.contains(var))
        throw new CommandSemanticException("UndefinedVariable: Variable '" + var + "' not defined");
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

  private static boolean containsNodeVariable(final Expression expr, final Map<String, VarType> scope) {
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
        throw new CommandSemanticException("UndefinedVariable: Variable '" + item.getVariable() + "' not defined");
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
              throw new CommandSemanticException("UndefinedVariable: Aggregation in ORDER BY is not projected");
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

  /**
   * A relationship variable may name one relationship of a pattern, not two: {@code (a)-[r]->()<-[r]-()} asks for a
   * relationship that is simultaneously two different ones, which no graph can answer.
   * <p>
   * Run against every pattern the statement contains, wherever it was written. It used to iterate the path patterns of
   * the {@code MATCH} clauses only, so the same pattern written as a {@code WHERE} predicate or inside an
   * {@code EXISTS { }} was accepted - and then answered by a path that could not correlate a relationship variable
   * either, which is how it came to be reported as "no match" rather than as the contradiction it is (issue #5656).
   * Neo4j rejects all three spellings.
   */
  private void validateRelationshipUniqueness(final CypherStatement statement) {
    CypherExpressionWalker.walk(statement, new RelationshipUniquenessChecks());
  }

  private static final class RelationshipUniquenessChecks implements CypherExpressionWalker.Visitor {
    @Override
    public void visit(final Expression expression) {
      // Patterns only.
    }

    @Override
    public void visitPattern(final PathPattern path) {
      final List<RelationshipPattern> relationships = path.getRelationships();
      if (relationships == null || relationships.size() < 2)
        return;

      final Set<String> relVars = new HashSet<>();
      for (final RelationshipPattern rel : relationships) {
        final String var = rel.getVariable();
        if (var != null && !var.isEmpty() && !relVars.add(var))
          throw new CommandParsingException(
              "RelationshipUniquenessViolation: Relationship variable '" + var + "' is used more than once in the "
                  + "same pattern");
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
      if (name != null && !"*".equals(name)) {
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

  /**
   * Applies the function checks - unknown name, argument count, statically-known argument types - to every expression
   * the statement contains, wherever it appears.
   * <p>
   * Until #5602 this walked {@code RETURN} and {@code WITH} items only, so {@code MATCH (n:Nothing) WHERE abs('x') > 0
   * RETURN n} ran and failed at runtime (or, on a query matching no row, silently succeeded) while the same call in a
   * {@code RETURN} was rejected before the query started. The clause an expression sits in has no bearing on whether
   * the call is valid, so the walk now covers {@code WHERE}, {@code UNWIND}, {@code SET}, {@code CREATE},
   * {@code MERGE}, {@code DELETE}, {@code FOREACH}, {@code SKIP}/{@code LIMIT} and the pattern properties of
   * {@code MATCH} as well - all through one traversal ({@link CypherExpressionWalker}) rather than a per-clause
   * recursion.
   * <p>
   * This widens the set of queries rejected at parse time. No new check is introduced: the same two that ran before -
   * the function checks and the path-property check, the latter previously reaching only the {@code WHERE} of a
   * {@code MATCH} or a {@code WITH} - now run wherever an expression appears. A call that reaches a row still fails
   * with the same message from the function's own runtime guard, so what changes is when the client is told, not what
   * they are told.
   * <p>
   * Issue #5626 closed the last gap: the body of a {@code CALL { ... }} clause and of the three subquery expressions
   * ({@code EXISTS}, {@code COUNT}, {@code COLLECT}) are walked too. Each body is a scope of its own, so the visitor
   * re-binds itself to the variable kinds visible inside it - see {@link FunctionArgumentChecks#forNestedStatement}.
   */
  private void validateFunctionArgumentTypes(final CypherStatement statement) {
    CypherExpressionWalker.walk(statement, new FunctionArgumentChecks(varTypes));
  }

  /**
   * The function checks bound to one variable scope.
   * <p>
   * Most of what they look at - the function name, how many arguments it was given, whether a literal argument is of
   * a type the function accepts - is the same wherever the call was written. What is not is the <i>kind</i> of a
   * variable ({@code length(node)}, {@code p.name} on a path), and a subquery body has a variable scope of its own:
   * hence one instance per scope rather than one per statement.
   */
  private final class FunctionArgumentChecks implements CypherExpressionWalker.Visitor {
    private final Map<String, VarType> scope;

    private FunctionArgumentChecks(final Map<String, VarType> scope) {
      this.scope = scope;
    }

    @Override
    public void visit(final Expression expression) {
      checkFunctionArgTypes(expression, scope);
      checkPropertyAccessOnPath(expression, scope);
    }

    @Override
    public CypherExpressionWalker.Visitor forNestedStatement(final CypherStatement nested) {
      return new FunctionArgumentChecks(nestedVarTypes(scope, nested));
    }
  }

  // ==================================================
  // Phase 11: Every phase above, applied to each body
  // ==================================================

  /**
   * Runs the phase list against the body of every subquery the statement contains, at any depth.
   * <p>
   * Twelve phases ran above and only two of them reached inside a body: {@link #validateVariableScope}, which models
   * the import rules of a {@code CALL { }} (since #5213), and {@link #validateFunctionArgumentTypes}, whose traversal
   * descends (since #5626). The other ten stopped at the boundary, so a mistake this class rejects when written one
   * way was accepted written one level in - {@code COUNT { MATCH (m) RETURN count(count(m)) }} passed while
   * {@code RETURN count(count(n))} did not, and so did a negative {@code SKIP}, a repeated relationship variable, a
   * duplicated column name (issue #5656).
   * <p>
   * The boundary is closed here rather than by teaching each of the ten to recurse, which would be ten new partial
   * recursions - the shape #5602 removed. Being a property of {@code validate()} also means a phase added later is
   * inside a body from the day it is written, without knowing subqueries exist.
   * <p>
   * The two that already descend are deliberately <b>not</b> in the body list. {@link #validateVariableScope} needs
   * the scope <i>at the point the expression was written</i>, which this traversal does not carry - it would have to
   * seed a body with an empty scope and would then report every correlated reference as undefined. Re-running
   * {@link #validateFunctionArgumentTypes} would only repeat a walk that already covers every depth.
   */
  private void validateNestedStatements(final CypherStatement statement) {
    CypherExpressionWalker.walk(statement, new NestedStatementChecks(varTypes));
  }

  /**
   * Applies the body phase list at each nesting boundary the walk crosses. It visits no expression of its own: what it
   * is watching for is the boundary, and the walk reports one through
   * {@link CypherExpressionWalker.Visitor#forNestedStatement}.
   */
  private static final class NestedStatementChecks implements CypherExpressionWalker.Visitor {
    private final Map<String, VarType> scope;

    private NestedStatementChecks(final Map<String, VarType> scope) {
      this.scope = scope;
    }

    @Override
    public void visit(final Expression expression) {
      // Boundaries only: an expression is the business of the phases that ran on the statement holding it.
    }

    @Override
    public CypherExpressionWalker.Visitor forNestedStatement(final CypherStatement nested) {
      // Building the body's kinds over the inherited ones IS the variable-type phase for that body: a name the body
      // declares twice as two different kinds raises here, from the same construction validateVariableTypes runs on
      // the statement.
      final Map<String, VarType> nestedScope = nestedVarTypes(scope, nested);

      if (nested instanceof UnionStatement union)
        // A UNION declares nothing of its own; each branch arrives as a nested statement in its own right and gets
        // the body phases then. What is owed here is only what is about the UNION rather than about a branch.
        validateUnionShape(union);
      else
        new CypherSemanticValidator(nestedScope).validateBodyPhases(nested);

      return new NestedStatementChecks(nestedScope);
    }
  }

  /**
   * The phases of {@link #validate} that a body still owed. Same methods, same order, run against the body instead of
   * against the statement.
   */
  private void validateBodyPhases(final CypherStatement body) {
    validateVariableBinding(body);
    validateCreateConstraints(body);
    // validateRelationshipUniqueness is not here: it runs through the same walk, which already covers every body.
    validateAggregations(body);
    validateBooleanOperandTypes(body);
    validateSkipLimit(body);
    validateColumnNames(body);
    validateExpressionAliases(body);
    validateReturnStar(body);
  }

  /**
   * The variable kinds visible inside a nested subquery body: what the body's own clauses declare, over what it
   * inherits from the enclosing scope.
   * <p>
   * The body's clauses are walked by {@link #buildVarTypes}, the same construction {@link #validateVariableTypes}
   * runs on the statement, because the kinds a body ends up with are built the same way - and because two copies of
   * that walk is what let the two spellings of the same import disagree in the first place. What differs is only what
   * this passes it: an inherited scope instead of an empty one, and no raise on a kind clash, since a name the body
   * binds for itself may legitimately shadow an outer one of another kind.
   * <p>
   * Inheriting the enclosing kinds is what the three subquery expressions need, since they see the outer row. An
   * implicit {@code CALL { ... }} imports nothing, so it inherits kinds for names its body cannot legally reference -
   * harmless, because {@link #validateVariableScope} runs first and reports such a reference as the undefined variable
   * it is, before this phase gets to read a kind for it.
   * <p>
   * What comes back is the body's <i>end</i> state, one map applied to every expression in it wherever that expression
   * sits - deliberately, because it is the same approximation the top-level statement already runs on ({@code varTypes}
   * is one map for the whole statement), and answering a body more precisely than the query around it would put back a
   * clause-dependent asymmetry of exactly the kind #5602 and this issue exist to remove. It errs one way only: a name a
   * later {@code WITH} re-binds to something kindless loses its kind for the clauses before it too, so a check is
   * missed, never invented. It cannot err the other way, because the only rebinding Cypher allows on a bound name is a
   * {@code WITH} projection, which can drop a kind but never turn a name into a path.
   */
  private static Map<String, VarType> nestedVarTypes(final Map<String, VarType> outer, final CypherStatement nested) {
    // A UNION declares nothing of its own - each branch is a scope of its own and is entered as a nested statement
    // in its own right, so it is the branch, not the union, that builds a scope over what is inherited here.
    // (UnionStatement.getClausesInOrder() answers with its FIRST branch's clauses, which is why this returns before
    // delegating rather than letting the shared build walk them as if they were the union's.)
    if (nested instanceof UnionStatement)
      return new HashMap<>(outer);

    return buildVarTypes(nested, outer);
  }

  /**
   * The per-expression function checks. Called on every node of the traversal, so it inspects only the node handed to
   * it: descending into a function's arguments is {@link CypherExpressionWalker}'s job.
   */
  private void checkFunctionArgTypes(final Expression expr, final Map<String, VarType> scope) {
    if (!(expr instanceof FunctionCallExpression func))
      return;

    final String name = func.getFunctionName().toLowerCase(Locale.ROOT);
    // Check for unknown functions (skip namespaced functions like date.truncate, they're handled by CypherFunctionRegistry)
    if (!name.contains(".") && !FunctionValidator.isKnownFunction(name))
      // Echo the spelling the client wrote, not the folded one: "Unknown function 'charat'" sends someone looking for
      // a name that never appeared in their query.
      throw new CommandParsingException("UnknownFunction: Unknown function '" + func.getOriginalFunctionName() + "'");
    final List<Expression> args = func.getArguments();
    // The wrong number of arguments is the primary defect and must be reported as such, before the single-argument type
    // check below decides that e.g. atan2('x') - a binary function called with one argument - is a type error (#5484).
    final String arityError = FunctionValidator.validateArgumentCount(name, args.size());
    if (arityError != null)
      throw new CommandSemanticException(arityError);
    // The numeric family is checked at every argument position, so atan2('hello', 1) and round(x, 2, 'SIDEWAYS') are
    // rejected before the query runs just like the single-argument abs('hello') is.
    final CypherFunctionHelper.NumericSignature numeric = CypherFunctionHelper.NUMERIC_ARGUMENT_FUNCTIONS.get(name);
    if (numeric != null)
      checkStaticallyKnownNumericArgs(numeric, args);
    if (args.size() == 1) {
      final Expression arg = args.get(0);
      if (numeric == null)
        checkStaticallyKnownArgType(name, arg);
      final VarType argType = getExpressionType(arg, scope);
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
            // type() only works on relationships. Point at valueType(), which is what callers who
            // expect a value-type name are actually after (issue #5292).
            if (argType == VarType.NODE)
              throw new CommandParsingException("InvalidArgumentType: type() requires a relationship argument, got node"
                  + ". Use valueType() to inspect the type of a value");
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
  }

  /**
   * Rejects an argument whose type is already readable in the query text - a literal or a map constructor - when it falls
   * outside the function's input domain. The functions repeat the check at runtime for values known only then; doing it here
   * as well matches Neo4j, which fails {@code MATCH (n:Nothing) RETURN size(42)} even though the query matches no row and the
   * function would never run. Same message and same exception as the runtime check, so the client sees one behaviour.
   * See issues #5477 (size) and #5476 (head, last, tail). The numeric family is handled by
   * {@link #checkStaticallyKnownNumericArgs}, which covers every argument rather than only a single one.
   */
  private void checkStaticallyKnownArgType(final String functionName, final Expression arg) {
    final boolean isMap = arg instanceof MapExpression;
    // A null literal is legal everywhere: null propagation is not a type error.
    final Object literal = arg instanceof LiteralExpression ? ((LiteralExpression) arg).getValue() : null;
    if (!isMap && literal == null)
      return;

    // A literal holding a collection is a LIST, which every function handled below accepts.
    if (literal instanceof Collection || (literal != null && literal.getClass().isArray()))
      return;

    switch (functionName) {
      case "size":
        // size() counts characters of a STRING and entries of a LIST or a MAP.
        if (!isMap && !(literal instanceof CharSequence))
          throw CypherFunctionHelper.typeMismatch("size", "a STRING, a LIST<ANY> or a MAP", literal);
        break;
      case "head":
      case "last":
      case "tail":
        // LIST-only: even a string literal is a type error.
        throw CypherFunctionHelper.typeMismatch(functionName, "a LIST<ANY>", isMap ? Map.of() : literal);
      default:
        break;
    }
  }

  /**
   * Rejects a statically-known argument of a numeric function - {@code abs()}, {@code atan2()}, {@code round()}, ... - that
   * falls outside {@code INTEGER | FLOAT}. Unlike {@link #checkStaticallyKnownArgType} this walks every argument, so the
   * parse-time guarantee covers the binary and ternary members of the family and not only the unary ones: without it
   * {@code MATCH (n:Nothing) RETURN atan2('hello', 1)} succeeded silently because the projection never ran, while the
   * single-argument {@code abs('hello')} failed. See issue #5484.
   * <p>
   * The trailing rounding mode of {@code round(value, precision, mode)} is not numeric; it is validated against the same
   * mode names the function itself accepts.
   */
  private void checkStaticallyKnownNumericArgs(final CypherFunctionHelper.NumericSignature signature,
      final List<Expression> args) {
    for (int i = 0; i < args.size(); i++) {
      final Expression arg = args.get(i);
      final boolean isMap = arg instanceof MapExpression;
      // A bracketed list is a ListExpression rather than a literal holding a Collection, so it has to be recognised
      // separately or abs([1,2]) would reach the parse-time check looking like a value of unknown type and be let
      // through, leaving it to fail only on a query that matches a row.
      final boolean isList = arg instanceof ListExpression;
      // A null literal is legal everywhere: null propagation is not a type error.
      final Object literal = arg instanceof LiteralExpression ? ((LiteralExpression) arg).getValue() : null;
      if (!isMap && !isList && literal == null)
        continue;

      // Stands in for the literal purely so the message names the type: a MAP or a LIST<ANY>.
      final Object rendered = isMap ? Map.of() : isList ? List.of() : literal;

      if (i < signature.numericArgs()) {
        if (isMap || isList || !(literal instanceof Number))
          throw CypherFunctionHelper.typeMismatch(signature.name(), CypherFunctionHelper.NUMERIC_DOMAIN, rendered);
      } else if ("round".equals(signature.name()))
        // A map or list literal names no rounding mode either, so it is rejected here too rather than only once the
        // query runs.
        RoundFunction.parseRoundingMode(rendered);
    }
  }

  /**
   * A path variable holds a whole path, so {@code p.name} names nothing. Called on every node of the traversal, which
   * is why it inspects only the node handed to it rather than recursing.
   */
  private void checkPropertyAccessOnPath(final Expression expr, final Map<String, VarType> scope) {
    if (expr instanceof PropertyAccessExpression access && scope.get(access.getVariableName()) == VarType.PATH)
      throw new CommandParsingException("InvalidArgumentType: Property access on a path variable is not allowed");
  }

  private VarType getExpressionType(final Expression expr, final Map<String, VarType> scope) {
    if (expr instanceof VariableExpression) {
      final String varName = ((VariableExpression) expr).getVariableName();
      return scope.get(varName);
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
    if (name == null || name.isEmpty() || "*".equals(name))
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
