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

import com.arcadedb.query.opencypher.ast.BooleanCoercionExpression;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.InExpression;
import com.arcadedb.query.opencypher.ast.IsNullExpression;
import com.arcadedb.query.opencypher.ast.LabelCheckExpression;
import com.arcadedb.query.opencypher.ast.LabelPredicate;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.PatternPredicateExpression;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.RegexExpression;
import com.arcadedb.query.opencypher.ast.StringMatchExpression;
import com.arcadedb.query.opencypher.grammar.Cypher25Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Builds the boolean expression of a WHERE: it descends the OR, XOR, AND and NOT levels of the grammar down to the
 * comparisons, the null, type and label checks, the string and list predicates and the pattern predicates, and falls
 * back on the expression read as a truth value where none of them applies.
 * <p>
 * Split out of {@link CypherASTBuilder}, the visitor that builds a statement from its parse tree.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class BooleanExpressionBuilder {
  private final CypherASTBuilder        ast;
  private final CypherExpressionBuilder expressionBuilder;

  BooleanExpressionBuilder(final CypherASTBuilder ast, final CypherExpressionBuilder expressionBuilder) {
    this.ast = ast;
    this.expressionBuilder = expressionBuilder;
  }

  /**
   * Parse an expression context into a BooleanExpression for WHERE clauses.
   * Handles logical operators (AND, OR, NOT), comparisons, IS NULL, IN, regex, etc.
   */
  BooleanExpression parseBooleanExpression(final Cypher25Parser.ExpressionContext ctx) {
    // Traverse the expression tree to find boolean operations
    // The grammar has: expression: expression11 (OR expression11)*

    // Parse all expression11 children and combine with OR if needed
    final List<Cypher25Parser.Expression11Context> expr11List = ctx.expression11();

    if (expr11List.size() == 1) {
      // No OR operator, just delegate to expression11
      return parseBooleanFromExpression11(expr11List.get(0));
    } else if (expr11List.size() > 1) {
      // Multiple expression11 connected with OR
      BooleanExpression result = parseBooleanFromExpression11(expr11List.get(0));
      for (int i = 1; i < expr11List.size(); i++) {
        final BooleanExpression right = parseBooleanFromExpression11(expr11List.get(i));
        result = new LogicalExpression(LogicalExpression.Operator.OR, result, right);
      }
      return result;
    }

    // Fallback: create a comparison from the expression text (legacy mode)
    // This handles cases we haven't explicitly parsed yet
    return createFallbackComparison(ctx);
  }

  private Cypher25Parser.Expression11Context findExpression11(final ParseTree node) {
    if (node instanceof Cypher25Parser.Expression11Context) {
      return (Cypher25Parser.Expression11Context) node;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.Expression11Context found = findExpression11(node.getChild(i));
      if (found != null)
        return found;
    }
    return null;
  }

  private Cypher25Parser.ParenthesizedExpressionContext findParenthesizedExpressionRecursive(final ParseTree node) {
    if (node instanceof Cypher25Parser.ParenthesizedExpressionContext) {
      return (Cypher25Parser.ParenthesizedExpressionContext) node;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.ParenthesizedExpressionContext found =
          findParenthesizedExpressionRecursive(node.getChild(i));
      if (found != null)
        return found;
    }
    return null;
  }

  /**
   * Recursively find a PatternExpressionContext in the parse tree.
   * Pattern expressions are used for pattern predicates in WHERE clauses.
   */
  Cypher25Parser.PatternExpressionContext findPatternExpressionRecursive(final ParseTree node) {
    if (node instanceof Cypher25Parser.PatternExpressionContext) {
      return (Cypher25Parser.PatternExpressionContext) node;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      final Cypher25Parser.PatternExpressionContext found = findPatternExpressionRecursive(node.getChild(i));
      if (found != null)
        return found;
    }
    return null;
  }

  /**
   * Returns true when the given pattern expression is the argument of a function-style
   * {@code exists(pattern)} predicate. Such a pattern is a boolean existence check, not a projected
   * path, so it is permitted in contexts (e.g. RETURN) where a bare pattern would be rejected.
   */
  boolean isArgumentOfExistsFunction(final ParseTree node) {
    for (ParseTree p = node.getParent(); p != null; p = p.getParent()) {
      if (p instanceof Cypher25Parser.FunctionInvocationContext funcCtx)
        return "exists".equalsIgnoreCase(functionName(funcCtx));
    }
    return false;
  }

  /**
   * True when {@code pattern} is all of {@code operand}, or all of the single argument of an {@code exists()} call that is
   * all of {@code operand}.
   */
  private static boolean isWholePatternPredicate(final Cypher25Parser.PatternExpressionContext pattern,
      final Cypher25Parser.Expression6Context operand) {
    if (spansSameTokens(pattern, operand))
      return true;
    for (ParserRuleContext p = pattern.getParent(); p != operand; p = p.getParent())
      if (p instanceof Cypher25Parser.FunctionInvocationContext function)
        return function.DISTINCT() == null && function.functionArgument().size() == 1 && "exists".equalsIgnoreCase(
            functionName(function)) && spansSameTokens(pattern, function.functionArgument(0)) && spansSameTokens(function,
            operand);
    return false;
  }

  private static boolean spansSameTokens(final ParserRuleContext a, final ParserRuleContext b) {
    return a.getStart() == b.getStart() && a.getStop() == b.getStop();
  }

  /**
   * Parses the expression of a predicate position (a clause {@code WHERE} or {@code FILTER}, the inline {@code WHERE} of a
   * node, relationship or parenthesized path) after refusing the pattern expressions it holds outside a boolean position.
   */
  BooleanExpression parsePredicate(final Cypher25Parser.ExpressionContext ctx) {
    rejectNonBooleanPatternExpressions(ctx);
    return parseBooleanExpression(ctx);
  }

  /**
   * Refuses a pattern expression that is not used as a boolean, as Neo4j 5 does and as
   * {@link CypherASTBuilder#visitReturnClause} does for a projection (issue #11888). {@code (p)-->()} tests whether the
   * pattern exists, so it may stand where a boolean is expected: a predicate, an operand of NOT, AND, OR or XOR, a CASE WHEN
   * condition, the WHERE of a comprehension or of a list predicate, and the argument of {@code exists()}. Elsewhere (inside
   * {@code size()}, {@code toString()} or {@code coalesce()}, as an operand of a comparison, IS NULL, IN or arithmetic) it
   * used to evaluate to null or to a different predicate without error, so {@code WHERE size((p)-->()) > 5} matched nothing.
   * The subquery expressions {@code EXISTS}, {@code COUNT} and {@code COLLECT} are skipped: their clauses are checked when
   * their body is built, which lets the refusal through (see {@link NonBooleanPatternExpressionException}). Also run on the
   * inline WHERE of a node or relationship pattern that the expression builder parses (a pattern expression, a pattern
   * comprehension).
   */
  static void rejectNonBooleanPatternExpressions(final ParseTree node) {
    if (node instanceof Cypher25Parser.ExistsExpressionContext || node instanceof Cypher25Parser.CountExpressionContext
        || node instanceof Cypher25Parser.CollectExpressionContext)
      return;
    if (node instanceof Cypher25Parser.PatternExpressionContext pattern)
      rejectUnlessBoolean(pattern);
    for (int i = 0; i < node.getChildCount(); i++)
      rejectNonBooleanPatternExpressions(node.getChild(i));
  }

  private static void rejectUnlessBoolean(final Cypher25Parser.PatternExpressionContext pattern) {
    ParserRuleContext node = pattern;
    while (true) {
      final ParserRuleContext parent = node.getParent();
      if (node instanceof Cypher25Parser.ExpressionContext expression && isBooleanSlot(parent, expression))
        return;
      if (isBooleanOperator(parent))
        return;
      if (!isBooleanPassThrough(parent))
        break;
      node = parent;
    }
    final String text = CypherASTBuilder.getOriginalText(pattern);
    if (node.getParent() instanceof Cypher25Parser.FunctionArgumentContext argument
        && "size".equalsIgnoreCase(functionName((Cypher25Parser.FunctionInvocationContext) argument.getParent())))
      throw new NonBooleanPatternExpressionException("A pattern expression should only be used in order to test the "
          + "existence of a pattern. It can no longer be used inside the function size(), an alternative is to replace "
          + "size() with COUNT {}: size(" + text + ")");
    throw new NonBooleanPatternExpressionException("A pattern expression should only be used in order to test the "
        + "existence of a pattern. It should therefore only be used in contexts that evaluate to a boolean, e.g. inside "
        + "the function exists() or in a WHERE-clause. No other uses are allowed, instead they should be replaced by a "
        + "pattern comprehension: " + text);
  }

  /**
   * True when {@code parent} applies NOT, AND, OR or XOR to the child it was reached from: the operand of a boolean
   * operator is a boolean position, wherever the result of the operator goes.
   */
  private static boolean isBooleanOperator(final ParserRuleContext parent) {
    return switch (parent) {
      case Cypher25Parser.ExpressionContext or -> or.expression11().size() > 1;
      case Cypher25Parser.Expression11Context xor -> xor.expression10().size() > 1;
      case Cypher25Parser.Expression10Context and -> and.expression9().size() > 1;
      case Cypher25Parser.Expression9Context not -> !not.NOT().isEmpty();
      case null, default -> false;
    };
  }

  /**
   * True when a child of {@code parent} passes its value up unchanged: a parenthesis, or a level of the expression grammar
   * that holds a single operand and no operator.
   */
  private static boolean isBooleanPassThrough(final ParserRuleContext parent) {
    return switch (parent) {
      case Cypher25Parser.ExpressionContext ignored -> true;
      case Cypher25Parser.Expression11Context ignored -> true;
      case Cypher25Parser.Expression10Context ignored -> true;
      case Cypher25Parser.Expression9Context ignored -> true;
      case Cypher25Parser.Expression8Context e8 -> e8.expression7().size() == 1;
      case Cypher25Parser.Expression7Context e7 -> e7.comparisonExpression6() == null;
      case Cypher25Parser.Expression6Context e6 -> e6.expression5().size() == 1;
      case Cypher25Parser.Expression5Context e5 -> e5.expression4().size() == 1;
      case Cypher25Parser.Expression4Context e4 -> e4.expression3().size() == 1;
      case Cypher25Parser.Expression3Context e3 -> e3.PLUS() == null && e3.MINUS() == null;
      case Cypher25Parser.Expression2Context e2 -> e2.postFix().isEmpty();
      case Cypher25Parser.Expression1Context ignored -> true;
      case Cypher25Parser.ParenthesizedExpressionContext ignored -> true;
      case null, default -> false;
    };
  }

  /**
   * True when {@code expression}, a child of {@code parent}, is evaluated as a boolean: a predicate (clause or inline
   * WHERE, FILTER), a CASE WHEN or GQL WHEN condition, the WHERE of a list or pattern comprehension or of a list predicate,
   * or the single argument of {@code exists()}.
   */
  private static boolean isBooleanSlot(final ParserRuleContext parent, final Cypher25Parser.ExpressionContext expression) {
    return switch (parent) {
      case Cypher25Parser.WhereClauseContext ignored -> true;
      case Cypher25Parser.FilterClauseContext ignored -> true;
      // the only expression a node, relationship or parenthesized path pattern holds directly is its inline WHERE
      case Cypher25Parser.NodePatternContext ignored -> true;
      case Cypher25Parser.RelationshipPatternContext ignored -> true;
      case Cypher25Parser.ParenthesizedPathContext ignored -> true;
      case Cypher25Parser.WhenBranchContext ignored -> true;
      case Cypher25Parser.CaseAlternativeContext alternative -> alternative.expression(0) == expression;
      case Cypher25Parser.ListComprehensionContext comprehension -> comprehension.whereExp == expression;
      case Cypher25Parser.PatternComprehensionContext comprehension -> comprehension.whereExp == expression;
      case Cypher25Parser.ListItemsPredicateContext predicate -> predicate.whereExp == expression;
      case Cypher25Parser.FunctionArgumentContext argument -> {
        final Cypher25Parser.FunctionInvocationContext function = (Cypher25Parser.FunctionInvocationContext) argument.getParent();
        yield function.DISTINCT() == null && function.functionArgument().size() == 1 && "exists".equalsIgnoreCase(
            functionName(function));
      }
      case null, default -> false;
    };
  }

  private static String functionName(final Cypher25Parser.FunctionInvocationContext function) {
    return ParserUtils.qualifiedName(function.functionName().namespace(), function.functionName().symbolicNameString());
  }

  private BooleanExpression parseBooleanFromExpression11(final Cypher25Parser.Expression11Context ctx) {
    // expression11: expression10 (XOR expression10)*
    final List<Cypher25Parser.Expression10Context> expr10List = ctx.expression10();

    if (expr10List.size() == 1)
      return parseBooleanFromExpression10(expr10List.get(0));

    // Multiple expression10 connected with XOR
    BooleanExpression result = parseBooleanFromExpression10(expr10List.get(0));
    for (int i = 1; i < expr10List.size(); i++) {
      final BooleanExpression right = parseBooleanFromExpression10(expr10List.get(i));
      result = new LogicalExpression(LogicalExpression.Operator.XOR, result, right);
    }
    return result;
  }

  private BooleanExpression parseBooleanFromExpression10(final Cypher25Parser.Expression10Context ctx) {
    // expression10: expression9 (AND expression9)*
    final List<Cypher25Parser.Expression9Context> expr9List = ctx.expression9();

    if (expr9List.size() == 1) {
      // No AND operator, just delegate
      return parseBooleanFromExpression9(expr9List.get(0));
    } else if (expr9List.size() > 1) {
      // Multiple expression9 connected with AND
      BooleanExpression result = parseBooleanFromExpression9(expr9List.get(0));
      for (int i = 1; i < expr9List.size(); i++) {
        final BooleanExpression right = parseBooleanFromExpression9(expr9List.get(i));
        result = new LogicalExpression(LogicalExpression.Operator.AND, result, right);
      }
      return result;
    }

    return createFallbackComparison(ctx);
  }

  private BooleanExpression parseBooleanFromExpression9(final Cypher25Parser.Expression9Context ctx) {
    // expression9: NOT* expression8
    BooleanExpression result = parseBooleanFromExpression8(ctx.expression8());

    // Apply one NOT per matched token: a bare chain such as `NOT NOT p` must keep both negations
    // (issue #5360), exactly like the parenthesized form `NOT (NOT p)`.
    final int notCount = ctx.NOT() == null ? 0 : ctx.NOT().size();
    for (int i = 0; i < notCount; i++)
      result = new LogicalExpression(LogicalExpression.Operator.NOT, result);

    return result;
  }

  private BooleanExpression parseBooleanFromExpression8(final Cypher25Parser.Expression8Context ctx) {
    // expression8 handles comparisons (=, !=, <, >, <=, >=)
    // expression8: expression7 ((EQ | NEQ | LT | GT | LE | GE) expression7)*

    // Try to find comparison operator
    final Cypher25Parser.Expression7Context expr7 = ctx.expression7(0);

    // Check for comparison operators in expression8
    final int expr7Count = ctx.expression7().size();
    if (expr7Count > 1) {
      // Collect all operators and operands for chained comparisons (e.g., 1 < n.num < 3)
      final List<ComparisonExpression.Operator> operators = new ArrayList<>();
      for (int i = 0; i < ctx.getChildCount(); i++) {
        if (ctx.getChild(i) instanceof TerminalNode) {
          final TerminalNode terminal = (TerminalNode) ctx.getChild(i);
          final int type = terminal.getSymbol().getType();
          ComparisonExpression.Operator op = null;
          if (type == Cypher25Parser.EQ)
            op = ComparisonExpression.Operator.EQUALS;
          else if (type == Cypher25Parser.NEQ || type == Cypher25Parser.INVALID_NEQ)
            op = ComparisonExpression.Operator.NOT_EQUALS;
          else if (type == Cypher25Parser.LT)
            op = ComparisonExpression.Operator.LESS_THAN;
          else if (type == Cypher25Parser.GT)
            op = ComparisonExpression.Operator.GREATER_THAN;
          else if (type == Cypher25Parser.LE)
            op = ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
          else if (type == Cypher25Parser.GE)
            op = ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
          if (op != null)
            operators.add(op);
        }
      }

      if (!operators.isEmpty()) {
        if (operators.size() == 1) {
          // Simple two-operand comparison
          final Expression left = expressionBuilder.parseExpressionFromText(ctx.expression7(0));
          final Expression right = expressionBuilder.parseExpressionFromText(ctx.expression7(1));
          return new ComparisonExpression(left, operators.get(0), right);
        }
        // Chained comparison: a < b < c becomes (a < b) AND (b < c)
        BooleanExpression result = null;
        for (int i = 0; i < operators.size(); i++) {
          final Expression left = expressionBuilder.parseExpressionFromText(ctx.expression7(i));
          final Expression right = expressionBuilder.parseExpressionFromText(ctx.expression7(i + 1));
          final ComparisonExpression comp = new ComparisonExpression(left, operators.get(i), right);
          if (result == null)
            result = comp;
          else
            result = new LogicalExpression(LogicalExpression.Operator.AND, result, comp);
        }
        return result;
      }
    }

    // Delegate to expression7 which handles IS NULL, IN, REGEX, etc.
    return parseBooleanFromExpression7(expr7);
  }

  private BooleanExpression parseBooleanFromExpression7(final Cypher25Parser.Expression7Context ctx) {
    // expression7: expression6 comparisonExpression6?
    final Cypher25Parser.ComparisonExpression6Context compCtx = ctx.comparisonExpression6();

    // Check if expression6 contains an EXISTS expression
    // This handles cases like: WHERE EXISTS { (p)-[:WORKS_AT]->(:Company) }
    // Require the EXISTS to span the entire expression6 text (allowing for whitespace),
    // otherwise an inner EXISTS inside a parenthesized OR/AND would be returned alone
    // and silently drop the surrounding boolean operators (issue #4126).
    final Cypher25Parser.Expression6Context expr6 = ctx.expression6();
    final Cypher25Parser.ExistsExpressionContext existsExpr = ParseTreeSearch.findExistsExpressionRecursive(expr6);
    if (existsExpr != null && compCtx == null
        && existsExpr.getText().length() >= expr6.getText().length() - 2) {
      // ExistsExpression implements Expression, so it has to be adapted to be returned as a BooleanExpression.
      // Wrap via BooleanCoercionExpression (rather than an anonymous adapter, which the walk of
      // CypherExpressionWalker could only treat as a leaf) so that the parse-time checks reach the subquery body
      // inside it - it was the anonymous adapter that hid an EXISTS written in a WHERE (issue #5626).
      return new BooleanCoercionExpression(SubqueryExpressionParser.parseExistsExpression(existsExpr));
    }

    // Check if expression6 contains a parenthesized expression
    // This handles cases like: (p.age < 26 OR p.age > 35)
    // IMPORTANT: Check parentheses BEFORE patterns to handle cases like (NOT (pattern))
    // where the NOT operator should be preserved during recursive parsing
    final Cypher25Parser.ParenthesizedExpressionContext parenExpr = findParenthesizedExpressionRecursive(expr6);
    // Only treat this as a parenthesized boolean predicate when the parentheses wrap the WHOLE
    // expression (e.g. "(a = 1 OR b = 2)"). The recursive finder also matches parentheses nested
    // deep inside a larger expression - notably the inner predicate of a list quantifier such as
    // all(a IN list WHERE (a = false)) - and returning parseBooleanExpression on that inner node
    // would silently discard the enclosing all()/any()/none()/single() and evaluate the bare inner
    // predicate against unbound iteration variables, filtering out every row (issue #5145). The span
    // check mirrors the EXISTS guard above (issue #4126): parenExpr is a descendant of expr6, so its
    // original text length equals expr6's only when it spans the entire expression.
    final boolean parenSpansWholeExpression = parenExpr != null
        && CypherASTBuilder.getOriginalText(parenExpr).trim().length() >= CypherASTBuilder.getOriginalText(expr6).trim().length();
    if (parenExpr != null && compCtx == null && parenSpansWholeExpression) {
      // Check if the parenthesized expression contains just a bare variable (e.g., WHERE (n)).
      // It is kept as a single-node pattern because the grammar cannot tell a node from a Boolean variable: the
      // validator refuses it when the variable is a graph entity, and the predicate reads the value otherwise (#9542).
      // Use getOriginalText (whitespace-preserving) rather than getText(): getText() strips all
      // whitespace, so a genuine predicate such as (friend IN inactive_nodes) collapses to
      // "friendINinactive_nodes" and spuriously matches the bare-identifier regex, getting
      // misparsed as an invalid single-node pattern (issue #5138).
      final String innerText = CypherASTBuilder.getOriginalText(parenExpr.expression()).trim();
      // true/false/null are literals, not variables: "(true)" is a parenthesized boolean (issue #8994).
      if (innerText.matches("^[a-zA-Z_`][a-zA-Z0-9_`]*$") && !CypherASTBuilder.isBooleanOrNullKeyword(innerText)) {
        final NodePattern nodePattern = new NodePattern(CypherASTBuilder.stripBackticks(innerText), null, null);
        final PathPattern singleNodePath = new PathPattern(List.of(nodePattern), List.of(), null);
        return new PatternPredicateExpression(singleNodePath, false);
      }
      // Recursively parse the inner expression
      return parseBooleanExpression(parenExpr.expression());
    }

    // Check if expression6 is a pattern expression (pattern predicate), bare or as the argument of exists()
    // This handles cases like: WHERE (n)-[:KNOWS]->()
    // The pattern must be the whole operand: one found deeper (a CASE WHEN condition, the WHERE of a list predicate or
    // comprehension) is a boolean inside a larger expression, and returning it alone would drop that expression (#11888).
    final Cypher25Parser.PatternExpressionContext patternExpr = findPatternExpressionRecursive(expr6);
    if (patternExpr != null && compCtx == null && isWholePatternPredicate(patternExpr, expr6)) {
      // Parse the pattern and create a pattern predicate expression
      final PathPattern pathPattern = ast.visitPatternExpression(patternExpr);
      return new PatternPredicateExpression(pathPattern, false, CypherASTBuilder.getOriginalText(patternExpr));
    }

    if (compCtx != null) {
      final Expression leftExpr = expressionBuilder.parseExpressionFromText(ctx.expression6());

      // Check which alternative of comparisonExpression6 was matched
      // NullComparison: IS NOT? NULL
      if (compCtx instanceof Cypher25Parser.NullComparisonContext) {
        final Cypher25Parser.NullComparisonContext nullCtx = (Cypher25Parser.NullComparisonContext) compCtx;
        final boolean isNot = nullCtx.NOT() != null;
        return new IsNullExpression(leftExpr, isNot);
      }

      // StringAndListComparison: (REGEQ | STARTS WITH | ENDS WITH | CONTAINS | IN) expression6
      if (compCtx instanceof Cypher25Parser.StringAndListComparisonContext) {
        final Cypher25Parser.StringAndListComparisonContext strListCtx =
            (Cypher25Parser.StringAndListComparisonContext) compCtx;

        // Check for IN operator
        if (strListCtx.IN() != null) {
          // Parse the right side as a single expression so that complex expressions like [3]+4 work correctly
          final Expression rhsExpr = expressionBuilder.parseExpressionFromText(strListCtx.expression6());
          final List<Expression> listItems = new ArrayList<>();
          listItems.add(rhsExpr);
          return new InExpression(leftExpr, listItems);
        }

        // Check for REGEX (=~)
        if (strListCtx.REGEQ() != null) {
          final Expression pattern = expressionBuilder.parseExpressionFromText(strListCtx.expression6());
          return new RegexExpression(leftExpr, pattern);
        }

        // Check for STARTS WITH
        if (strListCtx.STARTS() != null && strListCtx.WITH() != null) {
          final Expression pattern = expressionBuilder.parseExpressionFromText(strListCtx.expression6());
          return new StringMatchExpression(leftExpr, pattern, StringMatchExpression.MatchType.STARTS_WITH);
        }

        // Check for ENDS WITH
        if (strListCtx.ENDS() != null && strListCtx.WITH() != null) {
          final Expression pattern = expressionBuilder.parseExpressionFromText(strListCtx.expression6());
          return new StringMatchExpression(leftExpr, pattern, StringMatchExpression.MatchType.ENDS_WITH);
        }

        // Check for CONTAINS
        if (strListCtx.CONTAINS() != null) {
          final Expression pattern = expressionBuilder.parseExpressionFromText(strListCtx.expression6());
          return new StringMatchExpression(leftExpr, pattern, StringMatchExpression.MatchType.CONTAINS);
        }

        // Fallback for any unhandled operators
        return createFallbackComparison(ctx);
      }

      // LabelComparison: labelExpression (e.g., n:Person, n:Person|Developer)
      if (compCtx instanceof Cypher25Parser.LabelComparisonContext) {
        final Cypher25Parser.LabelComparisonContext labelCtx = (Cypher25Parser.LabelComparisonContext) compCtx;
        TypePredicateBuilder.rejectDirectedPredicate(labelCtx.labelExpression());
        return parseLabelCheckExpression(leftExpr, labelCtx.labelExpression(), ctx.getText());
      }

      // TypeComparison: <expr> IS [NOT] TYPED <type>  or  <expr> :: <type>  (issue #3365 section 3.3)
      if (compCtx instanceof Cypher25Parser.TypeComparisonContext) {
        return TypePredicateBuilder.buildIsTypedExpression(leftExpr, (Cypher25Parser.TypeComparisonContext) compCtx);
      }

      // NormalFormComparison: <expr> IS [NOT] [NFC|NFD|NFKC|NFKD] NORMALIZED (issue #9940)
      if (compCtx instanceof Cypher25Parser.NormalFormComparisonContext normalForm)
        return TypePredicateBuilder.buildIsNormalizedExpression(leftExpr, normalForm);

      return createFallbackComparison(ctx);
    }

    // Check if the expression is a list predicate (any/all/none/single) used as a boolean
    final Cypher25Parser.ListItemsPredicateContext listPredCtx = ParseTreeSearch.findListItemsPredicateRecursive(expr6);
    if (listPredCtx != null) {
      final Expression listPredExpr = expressionBuilder.parseListItemsPredicate(listPredCtx);
      // Wrap via BooleanCoercionExpression (rather than an anonymous adapter) so the semantic
      // validator can reach the underlying predicate and scope-check its WHERE body. This catches
      // variables that leak out of a pattern/list comprehension into the outer predicate (issue #5179).
      return new BooleanCoercionExpression(listPredExpr);
    }

    // No special boolean form matched. Parse as a generic expression and adapt
    // it to a boolean predicate. Covers bare boolean literals (WHERE true /
    // WHERE false), boolean-typed properties, parameters, and a function call used as a predicate
    // (isEmpty(x), exists(x), ...) - that last one used to get an anonymous adapter of its own here, byte-for-byte
    // equivalent to BooleanCoercionExpression except that CypherExpressionWalker could only treat it as a leaf, so
    // the call inside it escaped the parse-time checks (issue #5626).
    final Expression parsedExpr = expressionBuilder.parseExpressionFromText(expr6);
    if (parsedExpr != null)
      return new BooleanCoercionExpression(parsedExpr);

    return createFallbackComparison(ctx);
  }

  private BooleanExpression createFallbackComparison(final ParseTree ctx) {
    // Legacy fallback: parse simple comparisons from text
    // This handles cases we haven't explicitly parsed yet
    final String text = ctx.getText();

    // Try to parse as "variable.property operator value"
    final Pattern pattern = Pattern.compile("(\\w+)\\.(\\w+)\\s*([><=!]+)\\s*(\\w+|'[^']*'|\"[^\"]*\"|\\d+(?:\\.\\d+)?)");
    final Matcher matcher = pattern.matcher(text);

    if (matcher.find()) {
      final String variable = matcher.group(1);
      final String property = matcher.group(2);
      final String operatorStr = matcher.group(3);
      final String valueStr = matcher.group(4);

      final Expression left = new PropertyAccessExpression(variable, property);
      final Expression right = new LiteralExpression(parseValueString(valueStr), valueStr);
      final ComparisonExpression.Operator op = ComparisonExpression.Operator.fromString(operatorStr);

      return new ComparisonExpression(left, op, right);
    }

    // Ultimate fallback: create a dummy true expression
    // This should rarely happen in practice
    return new ComparisonExpression(
        new LiteralExpression(true, "true"),
        ComparisonExpression.Operator.EQUALS,
        new LiteralExpression(true, "true")
    );
  }

  /**
   * Parse a label check expression from WHERE clause (e.g., n:Person, n:Person|Developer).
   * The grammar for labelExpression is:
   * labelExpression : (COLON | IS) labelExpression4
   * labelExpression4 : labelExpression3 (BAR COLON? labelExpression3)*  # OR
   * labelExpression3 : labelExpression2 ((AMPERSAND | COLON) labelExpression2)*  # AND
   * labelExpression2 : EXCLAMATION_MARK* labelExpression1
   * labelExpression1 : symbolicNameString  # label name
   */
  private LabelCheckExpression parseLabelCheckExpression(final Expression variableExpr,
      final Cypher25Parser.LabelExpressionContext labelExprCtx,
      final String text) {
    // A negation, the wildcard or a mix of '&' and '|' is evaluated as the tree it is (issue #8992).
    final LabelPredicate predicate = ParserUtils.buildLabelPredicate(labelExprCtx, expressionBuilder::parseExpression);
    if (predicate != null)
      return new LabelCheckExpression(variableExpr, predicate, text);
    // Same extraction the node pattern uses, so `n:`Event Message`` in a WHERE resolves the very
    // label that (n:`Event Message`) resolves in a pattern - backticks stripped, quoted names kept
    // whole. Reading them off the text instead left the backticks on the label, which made the
    // predicate unsatisfiable and its negation a silent no-op.
    final List<String> labels = ParserUtils.extractLabels(labelExprCtx);
    final LabelCheckExpression.LabelOperator operator = ParserUtils.isLabelDisjunction(labelExprCtx) ?
        LabelCheckExpression.LabelOperator.OR :
        LabelCheckExpression.LabelOperator.AND;

    // The dynamic $(expr) labels are evaluated per row with the static ones; dropped, the check was always true (issue #11776)
    return new LabelCheckExpression(variableExpr, labels,
        ParserUtils.parseDynamicLabels(labelExprCtx, expressionBuilder::parseExpression), operator, text);
  }

  private Object parseValueString(final String value) {
    // Delegate to ParserUtils for value parsing
    return ParserUtils.parseValueString(value);
  }
}
