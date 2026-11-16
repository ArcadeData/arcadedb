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
import com.arcadedb.query.opencypher.procedures.CypherProcedure;

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
  /**
   * The message of a null MERGE property, the same whether the null is a literal refused here or arrived at run time and
   * was refused by {@code MergeStep} (issue #10191).
   */
  public static final String NULL_MERGE_PROPERTY =
      "MergeReadOwnWrites: 'MERGE' cannot be used with a graph element property value that is null";

  /**
   * The kind of value a variable holds. {@link #RELATIONSHIP_LIST} is the variable of a variable-length relationship
   * ({@code [r*1..3]}), which holds a {@code LIST<RELATIONSHIP>} rather than one relationship: Neo4j types it so, and
   * refuses {@code r.weight} on it before the query runs (issue #11211).
   */
  enum VarType {
    NODE, RELATIONSHIP, RELATIONSHIP_LIST, PATH, SCALAR
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

    if (statement instanceof ConditionalStatement conditional) {
      validateConditional(conditional);
      return;
    }

    final CypherSemanticValidator v = new CypherSemanticValidator();
    v.validateVariableTypes(statement);
    VariableBindingValidator.validateVariableBinding(statement);
    VariableScopeValidator.validateVariableScope(statement);
    WriteClauseValidator.validateCreateConstraints(statement);
    WriteClauseValidator.validateRelationshipUniqueness(statement);
    AggregationValidator.validateAggregations(statement);
    BooleanOperandValidator.validateBooleanOperandTypes(statement);
    ProjectionValidator.validateSkipLimit(statement);
    ProjectionValidator.validateColumnNames(statement);
    ProjectionValidator.validateExpressionAliases(statement);
    ProjectionValidator.validateReturnStar(statement);
    FunctionArgumentValidator.validateFunctionArgumentTypes(statement);
    v.validateCallYieldConclusion(statement);
    validateYieldedOutputs(statement);
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
   * A conditional query (issue #9940) validates each branch as the query it is, and requires them all to return the same
   * columns: which branch runs is decided per execution, so a caller could not know otherwise what it gets back.
   */
  private static void validateConditional(final ConditionalStatement conditional) {
    List<String> firstColumns = null;
    for (final CypherStatement branch : conditional.getAllBranches()) {
      validate(branch);
      final ReturnClause returnClause = branch.getReturnClause();
      final List<String> columns = returnClause != null ? returnClause.getItems() : List.of();
      if (firstColumns == null)
        firstColumns = columns;
      else if (!firstColumns.equals(columns))
        throw new CommandParsingException(
            "DifferentColumnsInConditionalQuery: every branch of a conditional query (WHEN ... THEN ... ELSE ...) must return the "
                + "same columns in the same order");
    }
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

    for (final ClauseEntry entry : clauses)
      applyClauseToVarTypes(entry, scope, declaredHere);
    return scope;
  }

  /**
   * Applies one clause's effect on the variable kinds in scope - a graph pattern declares them, a {@code WITH}
   * resets the scope to what it projects, a binding that says nothing about the kind drops whatever the name held.
   * <p>
   * Factored out of {@link #buildVarTypes} so the same per-clause rule serves two callers: that method applying it
   * to every clause up front to build one end-state map, and {@link FunctionArgumentValidator.FunctionArgumentChecks#forClauseEntry} applying
   * it one clause at a time as {@link CypherExpressionWalker} walks the statement, so a check reads a clause's own
   * declarations positionally instead of the whole statement's end state (issue #5671, part 2).
   */
  static void applyClauseToVarTypes(final ClauseEntry entry, final Map<String, VarType> scope,
      final Set<String> declaredHere) {
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
      if (callClause.hasYield()) {
        if (callClause.isYieldAll()) {
          final List<String> yieldAllFields = resolveYieldAllFieldNames(callClause);
          if (yieldAllFields != null)
            for (final String field : yieldAllFields)
              forget(field, scope, declaredHere);
        } else
          for (final CallClause.YieldItem item : callClause.getYieldItems())
            forget(item.getOutputName(), scope, declaredHere);
      }
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
      if (subqueryClause.getReportStatusVariable() != null)
        forget(subqueryClause.getReportStatusVariable(), scope, declaredHere);
    }
    default -> {
      // No binding of its own.
    }
    }
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
      if (item.isStar()) {
        star = true;
        continue;
      }
      // A projection whose output is literally named "*" (WITH n AS `*`) is an ordinary projection carrying an
      // unusual name, not the star: it must be carried like any other (issue #6334).
      final String outputName = item.getOutputName();
      if (outputName == null)
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
    if (expr instanceof MapExpression || VariableScopeValidator.isLiteralListExpression(expr))
      return VarType.SCALAR;
    // A list holding node variables, [n], is itself not a node.
    if (expr instanceof ListExpression && VariableScopeValidator.containsNodeVariable(expr, scope))
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
    for (final RelationshipPattern rel : path.getRelationships()) {
      if (rel.getVariable() != null)
        declareVar(rel.getVariable(), rel.isVariableLength() ? VarType.RELATIONSHIP_LIST : VarType.RELATIONSHIP, scope,
            declaredHere);
      // A GQL quantified path pattern (issue #4531) also declares its inner variables, which bind
      // outside the group to one list element per repetition. They are declared as plain values, not
      // as NODE/RELATIONSHIP: the name holds a LIST<NODE> or LIST<RELATIONSHIP>, so the structural
      // rules those kinds carry do not apply to it.
      if (rel instanceof QuantifiedPathPattern quantified)
        for (final String groupVariable : quantified.getGroupVariables())
          declareVar(groupVariable, VarType.SCALAR, scope, declaredHere);
    }
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

  /**
   * Resolves the concrete field names a {@code CALL proc() YIELD *} puts into scope (issue #6148).
   * <p>
   * {@code YIELD *} carries no {@link CallClause.YieldItem}s of its own ({@link CallClause#getYieldItems()} is
   * empty even though {@link CallClause#isYieldAll()} is true), so the three CALL-handling switches in this class
   * cannot learn what it exports the same way they learn from an explicit {@code YIELD a, b}. What it exports is
   * exactly the calling procedure's declared {@link CypherProcedure#getYieldFields()} - the same registry
   * {@code CallStep} consults to run the call - so this looks the procedure up there rather than guessing.
   * <p>
   * Returns {@code null}, not an empty list, when the name does not resolve to a registered procedure (a custom
   * {@code DEFINE FUNCTION}, an arbitrary SQL function, or a genuinely unknown name): none of those have a
   * statically declared output signature, so callers fall back to today's behavior for them instead of treating an
   * unresolvable {@code YIELD *} as exporting nothing.
   */
  static List<String> resolveYieldAllFieldNames(final CallClause callClause) {
    if (!callClause.isYieldAll())
      return null;
    final CypherProcedure procedure = callClause.resolveProcedure();
    return procedure != null ? procedure.getYieldFields() : null;
  }

  // ============================================
  // Phase 10: Function Argument Type Validation
  // ============================================

  // ================================================
  // Phase 10b: CALL ... YIELD Conclusion Validation
  // ================================================

  /**
   * Rejects a {@code CALL ... YIELD} that concludes the statement (or a subquery body, since this also runs through
   * {@link #validateBodyPhases}) without a {@code RETURN} or some other clause consuming the yielded columns -
   * mirroring Neo4j's {@code "Query cannot conclude with CALL together with YIELD"} compile-time error.
   * <p>
   * A <b>standalone</b> {@code CALL ... YIELD} - the statement's only clause - is deliberately exempt: it owns its
   * own projection (issue #6446/#6448, {@code CypherExecutionPlan}'s {@code bareCallOwnsProjection}) and needs no
   * trailing {@code RETURN}, exactly as real Neo4j allows {@code CALL db.labels() YIELD label} to stand alone. What
   * this rejects is the narrower gap #6448 left open on purpose: a {@code CALL ... YIELD} chained after (or before)
   * other clauses, that is still the LAST clause of the statement, with nothing left to project its yielded
   * columns. Before this check, {@code CypherExecutionPlan.execute()}'s write branch silently treated "no RETURN"
   * as "side effects only" for that shape too, so {@code WITH 1 AS x CALL math.add(x, 1) YIELD value} parsed fine
   * and quietly returned zero rows - a worse failure mode than an error, since the shape looks like it should
   * produce rows. A {@code CALL} with no {@code YIELD} at all chained the same way is untouched: that is the
   * ordinary "side effects only" terminal write clause, and Neo4j accepts it without a trailing {@code RETURN}.
   */
  private static void validateCallYieldConclusion(final CypherStatement statement) {
    final List<ClauseEntry> clauses = statement.getClausesInOrder();
    // A CALL that is the statement's only clause is the standalone/bare-CALL exception - not this check's business.
    if (clauses == null || clauses.size() < 2)
      return;

    final ClauseEntry last = clauses.get(clauses.size() - 1);
    if (last.getType() != ClauseEntry.ClauseType.CALL)
      return;

    final CallClause callClause = last.getTypedClause();
    if (callClause.hasYield())
      throw new CommandParsingException(
          "QueryCannotConcludeWithCallYield: Query cannot conclude with CALL together with YIELD - add a RETURN "
              + "(or another clause that consumes the yielded columns), or drop the YIELD to call it as a "
              + "side-effect-only statement");
  }

  /**
   * A {@code YIELD} names columns of the procedure's declared output (issue #11975). An undeclared name used to be bound to
   * null on every row, so a misspelt column read as a procedure that answered nothing; Neo4j refuses it before the query
   * runs with "Unknown procedure output". Only a procedure the registry resolves has a declared signature: a custom
   * {@code DEFINE FUNCTION} or SQL function called through {@code CALL} has none and is not checked.
   */
  private static void validateYieldedOutputs(final CypherStatement statement) {
    final List<ClauseEntry> clauses = statement.getClausesInOrder();
    if (clauses == null)
      return;

    for (final ClauseEntry entry : clauses) {
      if (entry.getType() != ClauseEntry.ClauseType.CALL)
        continue;
      final CallClause callClause = entry.getTypedClause();
      if (!callClause.hasYield() || callClause.isYieldAll())
        continue;
      final CypherProcedure procedure = callClause.resolveProcedure();
      if (procedure == null)
        continue;
      final List<String> declared = procedure.getYieldFields();
      if (declared == null || declared.isEmpty())
        continue;
      for (final CallClause.YieldItem item : callClause.getYieldItems())
        if (!declared.contains(item.getFieldName()))
          throw new CommandParsingException(
              "Unknown procedure output: `" + item.getFieldName() + "`. " + callClause.getProcedureName() + "() yields " + declared);
    }
  }

  // ==================================================
  // Phase 11: Every phase above, applied to each body
  // ==================================================

  /**
   * Runs the phase list against the body of every subquery the statement contains, at any depth.
   * <p>
   * Twelve phases ran above and only two of them reached inside a body: {@link VariableScopeValidator#validateVariableScope}, which models
   * the import rules of a {@code CALL { }} (since #5213), and {@link FunctionArgumentValidator#validateFunctionArgumentTypes}, whose traversal
   * descends (since #5626). The other ten stopped at the boundary, so a mistake this class rejects when written one
   * way was accepted written one level in - {@code COUNT { MATCH (m) RETURN count(count(m)) }} passed while
   * {@code RETURN count(count(n))} did not, and so did a negative {@code SKIP}, a repeated relationship variable, a
   * duplicated column name (issue #5656).
   * <p>
   * The boundary is closed here rather than by teaching each of the ten to recurse, which would be ten new partial
   * recursions - the shape #5602 removed. Being a property of {@code validate()} also means a phase added later is
   * inside a body from the day it is written, without knowing subqueries exist.
   * <p>
   * The two that already descend are deliberately <b>not</b> in the body list. {@link VariableScopeValidator#validateVariableScope} needs
   * the scope <i>at the point the expression was written</i>, which this traversal does not carry - it would have to
   * seed a body with an empty scope and would then report every correlated reference as undefined. Re-running
   * {@link FunctionArgumentValidator#validateFunctionArgumentTypes} would only repeat a walk that already covers every depth.
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
    VariableBindingValidator.validateVariableBinding(body);
    WriteClauseValidator.validateCreateConstraints(body);
    // validateRelationshipUniqueness is not here: it runs through the same walk, which already covers every body.
    AggregationValidator.validateAggregations(body);
    BooleanOperandValidator.validateBooleanOperandTypes(body);
    ProjectionValidator.validateSkipLimit(body);
    ProjectionValidator.validateColumnNames(body);
    ProjectionValidator.validateExpressionAliases(body);
    ProjectionValidator.validateReturnStar(body);
    validateCallYieldConclusion(body);
    validateYieldedOutputs(body);
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
   * harmless, because {@link VariableScopeValidator#validateVariableScope} runs first and reports such a reference as the undefined variable
   * it is, before this phase gets to read a kind for it.
   * <p>
   * What comes back is the body's <i>end</i> state, one map seeding every check that runs against it. For nine of the
   * ten body phases (see {@link #validateBodyPhases}) and for {@link NestedStatementChecks} itself that is also the
   * final answer, deliberately: it is the same approximation the top-level statement's own {@code varTypes} runs on,
   * and answering a body more precisely than the query around it would put back a clause-dependent asymmetry of
   * exactly the kind #5602 exists to remove. {@link FunctionArgumentValidator.FunctionArgumentChecks} is the one exception - it refines this
   * seed further, clause by clause, as {@link CypherExpressionWalker} descends through the body (issue #5671, part
   * 2), because unlike the other nine it already had to run positionally against the top-level statement too, and a
   * body deserves the same precision as the query around it rather than less.
   */
  private static Map<String, VarType> nestedVarTypes(final Map<String, VarType> outer, final CypherStatement nested) {
    // A UNION declares nothing of its own - each branch is a scope of its own and is entered as a nested statement
    // in its own right, so it is the branch, not the union, that builds a scope over what is inherited here.
    if (nested instanceof UnionStatement)
      return new HashMap<>(outer);

    return buildVarTypes(nested, outer);
  }

  /**
   * Checks if a variable name looks like a real Cypher variable identifier.
   * Filters out synthetic names created by the AST builder from raw expression text.
   * The AST builder sometimes creates VariableExpression with raw text like
   * "xINeqWHEREx" (from "x IN eq WHERE x") or "[1,2]" or "n:Foo".
   */
  static boolean isValidVariableName(final String name) {
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
