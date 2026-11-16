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
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.function.math.RoundFunction;
import com.arcadedb.query.opencypher.ast.*;
import java.util.*;

/**
 * The phase of the semantic validation that checks the arguments a function is called with, where their type is known
 * when the statement is parsed: a literal of a type the function does not accept, a node where a path is required,
 * a property read off a path. The kind of a variable is the one it has in the clause the call is written in.
 * <p>
 * Split out of {@link CypherSemanticValidator}, which runs the phases of the validation in order.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class FunctionArgumentValidator {
  private FunctionArgumentValidator() {
  }

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
   * <p>
   * Issue #5671 (part 2) closed the last asymmetry in the <i>other</i> dimension: which kind a variable has when a
   * check reads it. The scope used to be one end-state map built once for the whole statement (or body), so
   * {@code MATCH (m) WHERE type(m) = 'X' WITH 1 AS m RETURN m} read {@code m}'s kind as of the last clause rather
   * than as of the {@code MATCH} the {@code type()} call is actually written against, and a check that should have
   * fired on the clause as written silently didn't. {@link FunctionArgumentChecks#forClauseEntry} advances the
   * scope clause by clause instead, so each clause's own expressions are checked against the scope as of that
   * clause - which is why this seeds with an empty scope rather than {@code varTypes}: {@code varTypes} IS the
   * end-state map this phase used to over-apply, and seeding the incremental walk from it would still start every
   * top-level clause already contaminated with clauses written after it. A statement inherits nothing, so empty is
   * the correct starting scope; {@link FunctionArgumentChecks#forClauseEntry} builds every declaration back up as
   * the walk reaches it.
   */
  static void validateFunctionArgumentTypes(final CypherStatement statement) {
    CypherExpressionWalker.walk(statement, new FunctionArgumentChecks(Map.of()));
  }

  /**
   * The function checks bound to one variable scope.
   * <p>
   * Most of what they look at - the function name, how many arguments it was given, whether a literal argument is of
   * a type the function accepts - is the same wherever the call was written. What is not is the <i>kind</i> of a
   * variable ({@code length(node)}, {@code p.name} on a path), and both a subquery body and each clause within a
   * statement or body have a variable scope of their own: hence one instance per scope, advanced by
   * {@link #forClauseEntry} as the walk crosses a clause boundary and by {@link #forNestedStatement} as it crosses
   * into a subquery body, rather than one shared instance checking every expression against a single end-state map
   * (issue #5671, part 2). {@code type(m) = 'KNOWS'} in a {@code MATCH} is checked against the scope as of that
   * {@code MATCH}, so a later {@code WITH 1 AS m} that drops {@code m}'s kind no longer erases the check on the
   * clause written before it.
   */
  private static final class FunctionArgumentChecks implements CypherExpressionWalker.Visitor {
    private final Map<String, VarType> scope;
    // The patterns the clause being walked declares itself: they are the source of the kinds in scope, so they are
    // never checked against them. Everything else reached here is a pattern that only uses names (a WHERE predicate,
    // a pattern comprehension) and must agree with the kind each name was declared as (issue #9292).
    private final Set<PathPattern> declaringPatterns;

    private FunctionArgumentChecks(final Map<String, VarType> scope) {
      this(scope, Set.of());
    }

    private FunctionArgumentChecks(final Map<String, VarType> scope, final Set<PathPattern> declaringPatterns) {
      this.scope = scope;
      this.declaringPatterns = declaringPatterns;
    }

    @Override
    public void visit(final Expression expression) {
      checkFunctionArgTypes(expression, scope);
      checkPropertyAccessOnPath(expression, scope);
    }

    @Override
    public CypherExpressionWalker.Visitor forLocalBindings(final String... variables) {
      final Map<String, VarType> shadowed = new HashMap<>(scope);
      for (final String variable : variables)
        if (variable != null)
          shadowed.remove(variable);
      return new FunctionArgumentChecks(shadowed, declaringPatterns);
    }

    /**
     * The inline predicate of a variable-length relationship tests one relationship at a time, so there its variable is
     * a relationship and {@code r.w} reads a property (issue #11211); outside it, the same name holds the list.
     */
    @Override
    public CypherExpressionWalker.Visitor forInlinePredicate(final RelationshipPattern relationship) {
      final String variable = relationship.getVariable();
      if (variable == null || scope.get(variable) != VarType.RELATIONSHIP_LIST)
        return this;
      final Map<String, VarType> single = new HashMap<>(scope);
      single.put(variable, VarType.RELATIONSHIP);
      return new FunctionArgumentChecks(single, declaringPatterns);
    }

    @Override
    public void visitPattern(final PathPattern path) {
      if (declaringPatterns.contains(path))
        return;
      if (path.isSingleNode())
        checkSingleNodePredicate(path.getFirstNode());
      for (final NodePattern node : path.getNodes())
        checkUsedAs(node.getVariable(), VarType.NODE);
      for (final RelationshipPattern rel : path.getRelationships())
        if (!(rel instanceof QuantifiedPathPattern))
          checkUsedAs(rel.getVariable(), VarType.RELATIONSHIP);
    }

    /**
     * {@code WHERE (name)} is a parenthesized variable, not a pattern, unless {@code name} is a graph entity: a node,
     * a relationship or a path is no Boolean, and a lone node pattern is no predicate. A name of unknown kind (a
     * {@code WITH}, an {@code UNWIND}, a {@code collect}) is left to the runtime check, which sees the value (issue #9542).
     */
    private void checkSingleNodePredicate(final NodePattern node) {
      // a label or a property map makes it a node pattern whatever the variable holds
      if (node.hasLabels() || node.hasProperties())
        throw new CommandParsingException("InvalidArgumentType: Single node pattern is not a valid predicate in WHERE");
      final String name = node.getVariable();
      if (name == null)
        return;
      final VarType declared = scope.get(name);
      if (declared == VarType.NODE || declared == VarType.RELATIONSHIP || declared == VarType.RELATIONSHIP_LIST
          || declared == VarType.PATH)
        throw new CommandParsingException("InvalidArgumentType: Single node pattern is not a valid predicate in WHERE");
    }

    /**
     * A name already bound as a node cannot be a relationship in a later pattern, nor the reverse. Only that pair is
     * checked: the other kinds a {@code WITH} can leave behind are approximations, and guessing there is how a check
     * starts rejecting valid queries.
     */
    private void checkUsedAs(final String name, final VarType used) {
      if (name == null)
        return;
      final VarType declared = scope.get(name);
      // a variable-length relationship's variable is a relationship kind too: reusing it as a node is the same conflict
      if (declared == VarType.NODE ? used != VarType.NODE : isRelationshipKind(declared) && !isRelationshipKind(used))
        throw new CommandParsingException(
            "VariableTypeConflict: Variable '" + name + "' already defined as " + declared + ", cannot redefine as "
                + used);
    }

    private static boolean isRelationshipKind(final VarType type) {
      return type == VarType.RELATIONSHIP || type == VarType.RELATIONSHIP_LIST;
    }

    /**
     * Seeds the nested body's walk from {@code this.scope} - the outer scope exactly as it stands at the point the
     * walk is crossing into the body, already advanced by every {@link #forClauseEntry} call for the clauses before
     * this one - rather than from {@link CypherSemanticValidator#nestedVarTypes}, which is the body's own <i>end</i> state (issue #5671,
     * part 2 regression found in review). Seeding from the end state would seed the body's first clause with kinds
     * that clause has not been declared yet - including kinds the body itself only reaches by walking past this
     * point - the exact contamination this phase exists to remove, just relocated to the nested-body boundary
     * instead of removed. {@link CypherSemanticValidator#buildVarTypes}/{@link CypherSemanticValidator#nestedVarTypes}
     * are still exactly right for the other nine
     * body phases in {@link CypherSemanticValidator#validateBodyPhases}, which do want the body's precomputed end state; this is the one
     * phase that has to build the body's kinds up positionally as it walks, the same as it does for a top-level
     * statement.
     */
    @Override
    public CypherExpressionWalker.Visitor forNestedStatement(final CypherStatement nested) {
      return new FunctionArgumentChecks(new HashMap<>(scope));
    }

    @Override
    public CypherExpressionWalker.Visitor forClauseEntry(final ClauseEntry entry) {
      // A throwaway declaredHere: applyClauseToVarTypes' only use for it is deciding whether a re-declaration is a
      // VariableTypeConflict, and that conflict is always raised independently of this walk - for the statement by
      // validateVariableTypes, for a subquery body by validateNestedStatements (both run as their own phases of
      // CypherSemanticValidator#validate, seeing every clause of their scope, not just the one this call was handed).
      // This walk only needs the resulting kinds, not to re-decide a question those phases already settle.
      final Map<String, VarType> advanced = new HashMap<>(scope);
      CypherSemanticValidator.applyClauseToVarTypes(entry, advanced, new HashSet<>());
      return new FunctionArgumentChecks(advanced, declaredPatterns(entry));
    }
  }

  /**
   * The patterns a clause declares variables through (the ones {@link CypherSemanticValidator#applyClauseToVarTypes} reads), by identity.
   */
  private static Set<PathPattern> declaredPatterns(final ClauseEntry entry) {
    final Set<PathPattern> patterns = Collections.newSetFromMap(new IdentityHashMap<>());
    switch (entry.getType()) {
    case MATCH -> {
      final MatchClause matchClause = entry.getTypedClause();
      if (matchClause.hasPathPatterns())
        for (final PathPattern path : matchClause.getPathPatterns())
          addWithGroupPatterns(path, patterns);
    }
    case CREATE -> {
      final CreateClause createClause = entry.getTypedClause();
      if (createClause != null && !createClause.isEmpty())
        for (final PathPattern path : createClause.getPathPatterns())
          addWithGroupPatterns(path, patterns);
    }
    case MERGE -> {
      final MergeClause mergeClause = entry.getTypedClause();
      if (mergeClause != null)
        addWithGroupPatterns(mergeClause.getPathPattern(), patterns);
    }
    default -> {
    }
    }
    return patterns;
  }

  /**
   * A pattern, and the patterns its quantified path patterns repeat: the variables written in a group are declared by the
   * clause as well, as {@link CypherSemanticValidator#applyClauseToVarTypes} reads them, and the walk reaches those inner patterns since issue #10525.
   */
  private static void addWithGroupPatterns(final PathPattern path, final Set<PathPattern> patterns) {
    if (path == null)
      return;
    patterns.add(path);
    for (final RelationshipPattern relationship : path.getRelationships())
      if (relationship instanceof QuantifiedPathPattern group)
        addWithGroupPatterns(group.getInnerPattern(), patterns);
  }

  /**
   * The per-expression function checks. Called on every node of the traversal, so it inspects only the node handed to
   * it: descending into a function's arguments is {@link CypherExpressionWalker}'s job.
   */
  private static void checkFunctionArgTypes(final Expression expr, final Map<String, VarType> scope) {
    if (!(expr instanceof FunctionCallExpression func))
      return;

    final String name = func.getFunctionName().toLowerCase(Locale.ROOT);
    // Check for unknown functions (skip namespaced functions like date.truncate, they're handled by CypherFunctionRegistry)
    if (!name.contains(".") && !FunctionValidator.isKnownFunction(name))
      // Echo the spelling the client wrote, not the folded one: "Unknown function 'charat'" sends someone looking for
      // a name that never appeared in their query.
      throw FunctionValidator.unknownFunction(func.getOriginalFunctionName());
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
            if (argType == VarType.RELATIONSHIP_LIST)
              throw new CommandParsingException(
                  "InvalidArgumentType: length() cannot be applied to a list of relationships. Use size() to count them");
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
   * See issues #5477 (size), #5476 (head, last, tail) and #5798 (toUpper, toLower, trim, lTrim, rTrim - their
   * single-argument spelling; split() and replace() are never called with one argument, so they rely on the runtime
   * check in {@link CypherFunctionHelper#requireStringArgument} alone). The numeric family is handled by
   * {@link #checkStaticallyKnownNumericArgs}, which covers every argument rather than only a single one.
   */
  private static void checkStaticallyKnownArgType(final String functionName, final Expression arg) {
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
      case "toupper":
      case "upper":
      case "tolower":
      case "lower":
      case "trim":
      case "btrim":
      case "ltrim":
      case "rtrim":
        // STRING-only primary argument (issue #5798): neither a MAP nor any other non-STRING literal is in domain.
        // Named after the executor's own getName() (e.g. "toUpper", not the parsed-and-lower-cased "toupper"), the
        // same spelling the runtime check in CypherFunctionHelper#requireStringArgument uses, so the client sees one
        // message whichever path caught the mistake.
        if (isMap || !(literal instanceof CharSequence))
          throw CypherFunctionHelper.typeMismatch(canonicalStringFunctionName(functionName), CypherFunctionHelper.STRING_DOMAIN,
              isMap ? Map.of() : literal);
        break;
      default:
        break;
    }
  }

  /**
   * Maps the parsed-and-lower-cased name of a STRING-only function (and its aliases) to the spelling its executor's
   * {@code getName()} reports, so {@link #checkStaticallyKnownArgType}'s message matches the runtime check in
   * {@link CypherFunctionHelper#requireStringArgument} regardless of which one caught the mistake (issue #5798).
   */
  private static String canonicalStringFunctionName(final String functionName) {
    return switch (functionName) {
      case "toupper", "upper" -> "toUpper";
      case "tolower", "lower" -> "toLower";
      case "ltrim" -> "lTrim";
      case "rtrim" -> "rTrim";
      case "btrim" -> "trim";
      default -> functionName;
    };
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
   * <p>
   * {@code left()}/{@code right()}/{@code substring()} (issue #6609) declare their numeric arguments starting at
   * {@link CypherFunctionHelper.NumericSignature#startArg()} rather than at position 0: the leading arguments are the
   * STRING being sliced, so a statically-known one outside STRING is rejected with the same message as the runtime check
   * in {@link CypherFunctionHelper#requireStringArgument} (issue #11692).
   */
  private static void checkStaticallyKnownNumericArgs(final CypherFunctionHelper.NumericSignature signature,
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

      if (signature.isNumericPosition(i)) {
        if (isMap || isList || !(literal instanceof Number))
          throw CypherFunctionHelper.typeMismatch(signature.name(), CypherFunctionHelper.NUMERIC_DOMAIN, rendered);
      } else if (i < signature.startArg()) {
        if (isMap || isList || !(literal instanceof CharSequence))
          throw CypherFunctionHelper.typeMismatch(signature.name(), CypherFunctionHelper.STRING_DOMAIN, rendered);
      } else if ("round".equals(signature.name()))
        // A map or list literal names no rounding mode either, so it is rejected here too rather than only once the
        // query runs.
        RoundFunction.parseRoundingMode(rendered);
    }
  }

  /**
   * A path variable holds a whole path, so {@code p.name} names nothing; nor does {@code r.weight} on the variable of a
   * variable-length relationship, which holds a list of relationships (issue #11211). Neo4j refuses both before the
   * query runs, whatever it would match. Called on every node of the traversal, which is why it inspects only the node
   * handed to it rather than recursing.
   */
  private static void checkPropertyAccessOnPath(final Expression expr, final Map<String, VarType> scope) {
    if (!(expr instanceof PropertyAccessExpression access))
      return;
    final VarType declared = scope.get(access.getVariableName());
    if (declared == VarType.PATH)
      throw new CommandParsingException("InvalidArgumentType: Property access on a path variable is not allowed");
    if (declared == VarType.RELATIONSHIP_LIST)
      throw new CommandSemanticException("TypeError: Type mismatch: expected Map, Node, Relationship, Point, Duration, Date, Time, "
          + "LocalTime, LocalDateTime or DateTime but was List<Relationship>. '" + access.getVariableName()
          + "' is a variable-length relationship and holds a list of relationships, so '" + access.getVariableName() + "."
          + access.getPropertyName() + "' reads no property: read it per relationship, e.g. [rel IN "
          + access.getVariableName() + " | rel." + access.getPropertyName() + "]");
  }

  private static VarType getExpressionType(final Expression expr, final Map<String, VarType> scope) {
    if (expr instanceof VariableExpression) {
      final String varName = ((VariableExpression) expr).getVariableName();
      return scope.get(varName);
    }
    return null;
  }
}
