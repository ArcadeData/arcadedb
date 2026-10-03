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
import com.arcadedb.query.opencypher.InternalVariables;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.LabelCheckExpression;
import com.arcadedb.query.opencypher.ast.LabelPredicate;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.grammar.Cypher25Parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility methods for Cypher parser operations.
 * Provides common parsing utilities to reduce code duplication and improve maintainability.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ParserUtils {
  // Numbers the internal variables newLabelExpressionVariable() hands out; global, see that method.
  private static final AtomicLong LABEL_EXPRESSION_VARIABLE_COUNTER = new AtomicLong();


  /**
   * Strips backticks from an escaped symbolic name.
   * Handles both regular backticks and double backticks (escaped backticks).
   *
   * @param name the name potentially wrapped in backticks
   * @return the name without backticks
   */
  public static String stripBackticks(final String name) {
    if (name == null || name.length() < 2)
      return name;

    // Check if wrapped in backticks
    if (name.startsWith("`") && name.endsWith("`")) {
      // Remove outer backticks
      String inner = name.substring(1, name.length() - 1);
      // Replace double backticks (escaped backticks) with single backticks
      inner = inner.replace("``", "`");
      return inner;
    }

    return name;
  }

  /**
   * The hop bounds a relationship pattern's {@code *} suffix declares, as the pair
   * {@code RelationshipPattern} carries: {@code null} on either side means "unbounded in that direction", and both
   * {@code null} means the pattern is not variable-length at all.
   *
   * @param minHops lower bound, inclusive
   * @param maxHops upper bound, inclusive, {@code null} when unbounded
   */
  public record PathLength(Integer minHops, Integer maxHops) {
    /** The bounds of a relationship pattern carrying no {@code *} suffix: a single fixed hop. */
    public static final PathLength FIXED_SINGLE_HOP = new PathLength(null, null);
  }

  /**
   * Reads the hop bounds off a {@code pathLength} context, normalizing the bare {@code [*]} spelling.
   * <p>
   * {@code [*]} means {@code [*1..]} everywhere in Cypher, and normalizing it is not decoration: with both bounds
   * left {@code null}, {@code RelationshipPattern.isVariableLength()} answers false and the hop is planned as one
   * fixed hop, so {@code [(a)-[*]->(b:A) | b.n]} silently answered as though the user had written a single-hop
   * pattern. That is what happened while the MATCH-side builder normalized it and the expression-side copy of the
   * same block did not (issue #6370), so the normalization lives here and both builders read it from one place
   * rather than each carrying its own copy to drift from.
   *
   * @param ctx the {@code pathLength} context, which the caller has already established is present
   *
   * @return the declared bounds, never {@code null}
   */
  public static PathLength parsePathLength(final Cypher25Parser.PathLengthContext ctx) {
    if (ctx == null)
      return PathLength.FIXED_SINGLE_HOP;
    if (ctx.single != null) {
      final int exact = Integer.parseInt(ctx.single.getText());
      return new PathLength(exact, exact);
    }
    final Integer minHops = ctx.from != null ? Integer.parseInt(ctx.from.getText()) : null;
    final Integer maxHops = ctx.to != null ? Integer.parseInt(ctx.to.getText()) : null;
    // Bare * with no range: [*] means [*1..] (min=1, max=unbounded).
    return minHops == null && maxHops == null ? new PathLength(1, null) : new PathLength(minHops, maxHops);
  }

  /**
   * Extract labels from a label expression context using grammar-based parsing.
   * Handles multiple labels, alternative labels, and combinations.
   * Examples:
   * - :Person:Developer -> ["Person", "Developer"]
   * - :Person|Developer -> ["Person", "Developer"]
   * - :Person:Developer|Manager -> ["Person", "Developer", "Manager"]
   *
   * @param ctx the label expression context
   * @return list of label names with backticks stripped
   */
  public static List<String> extractLabels(final Cypher25Parser.LabelExpressionContext ctx) {
    // Always walk the grammar tree: it reaches each label name as its own token, so backticks are
    // stripped from every one of them and a quoted name is never mistaken for a separator list
    // (`Event Message` stays one label; so would a name containing ':', '&' or '|'). Dynamic
    // $(expression) labels are skipped here and returned by collectDynamicLabelContexts instead.
    return collectStaticLabels(ctx);
  }

  /**
   * Returns true when the label expression combines multiple labels with the
   * disjunction operator {@code |} (e.g. {@code :A|B}). Conjunction ({@code :A:B}
   * or {@code :A&B}) returns false. Single-label expressions also return false.
   * Used by the AST builder so node patterns can carry the OR semantics through
   * to the executor (issue #4105).
   */
  public static boolean isLabelDisjunction(final Cypher25Parser.LabelExpressionContext ctx) {
    if (ctx == null)
      return false;
    // Read the operators off the grammar rather than off the text, so a '|' or '&' inside a
    // backtick-quoted label name is a character of that name and not an operator.
    return hasOperator(ctx, Cypher25Parser.LabelExpression4Context.class)
        && !hasOperator(ctx, Cypher25Parser.LabelExpression3Context.class);
  }

  /**
   * Returns the full label expression as a {@link LabelPredicate} tree when a label list plus the disjunction flag
   * cannot say what it means, or {@code null} when they can.
   * <p>
   * {@link #extractLabels} and {@link #isLabelDisjunction} describe a plain conjunction ({@code :A:B}, {@code :A&B})
   * and a plain disjunction ({@code :A|B}) exactly, and that is the form the planner turns into a label scan. A
   * negation ({@code !A}), the wildcard ({@code %}) or a mix of {@code &} and {@code |} has no such description: read
   * through those two methods alone it lost the operator without an error, so {@code (n:!A)} matched the {@code A}
   * nodes (issue #8992). Every builder asks this first and evaluates the returned tree instead.
   *
   * @throws CommandParsingException when the expression combines a dynamic {@code $(...)} label with one of those
   *                                 operators, which is not supported
   */
  public static LabelPredicate buildLabelPredicate(final Cypher25Parser.LabelExpressionContext ctx) {
    if (ctx == null || isPlainLabelExpression(ctx))
      return null;
    return buildLabelPredicate(ctx.labelExpression4(), ctx);
  }

  /**
   * Relationship-pattern form of {@link #buildLabelPredicate(Cypher25Parser.LabelExpressionContext)}. A relationship
   * pattern's type list always means "any of these types", so a conjunction of types ({@code [r:R&S]}) has no
   * description in it either: it used to match both {@code R} and {@code S} relationships, while a relationship has
   * exactly one type and Neo4j matches none.
   */
  public static LabelPredicate buildRelationshipTypePredicate(final Cypher25Parser.LabelExpressionContext ctx) {
    if (ctx == null)
      return null;
    if (isPlainLabelExpression(ctx) && !hasOperator(ctx, Cypher25Parser.LabelExpression3Context.class))
      return null;
    return buildLabelPredicate(ctx.labelExpression4(), ctx);
  }

  /**
   * A fresh name for a pattern element the query left anonymous but whose label expression has to be evaluated as a
   * predicate over it. The {@link InternalVariables#PREFIX} keeps it out of {@code RETURN *} and out of the scope
   * checks; the counter is global because a subquery body is built by a builder of its own, and two clauses binding
   * the same generated name would be joined on it.
   */
  public static String newLabelExpressionVariable() {
    return InternalVariables.PREFIX + "lblexpr" + LABEL_EXPRESSION_VARIABLE_COUNTER.getAndIncrement();
  }

  /**
   * The predicate a pattern element carries for a label expression {@link #buildLabelPredicate} returned, checked
   * against the element bound to {@code variable}.
   */
  public static LabelCheckExpression labelCheckOn(final String variable, final LabelPredicate predicate,
      final Cypher25Parser.LabelExpressionContext ctx) {
    return new LabelCheckExpression(new VariableExpression(variable), predicate, variable + ctx.getText());
  }

  /** ANDs a pattern element's label check (may be null) with its inline WHERE predicate (may be null). */
  public static BooleanExpression andLabelCheck(final BooleanExpression labelCheck, final BooleanExpression where) {
    if (labelCheck == null)
      return where;
    if (where == null)
      return labelCheck;
    return new LogicalExpression(LogicalExpression.Operator.AND, labelCheck, where);
  }

  /**
   * The variable-length expansion filters each hop on a type list only, so a relationship label expression that
   * needs a predicate is refused there rather than run with its operators dropped (issue #9117 tracks support).
   */
  public static void rejectOnVariableLengthRelationship(final Cypher25Parser.LabelExpressionContext ctx) {
    throw new CommandParsingException("UnexpectedSyntax: the label expression '" + ctx.getText()
        + "' is not supported on a variable-length relationship yet: only a type or a '|' of types is");
  }

  private static boolean isPlainLabelExpression(final ParseTree node) {
    if (!isFreeOfNegationAndWildcard(node))
      return false;
    return !(hasOperator(node, Cypher25Parser.LabelExpression4Context.class)
        && hasOperator(node, Cypher25Parser.LabelExpression3Context.class));
  }

  private static boolean isFreeOfNegationAndWildcard(final ParseTree node) {
    if (node instanceof Cypher25Parser.AnyLabelContext)
      return false;
    if (node instanceof Cypher25Parser.LabelExpression2Context le2 && !le2.EXCLAMATION_MARK().isEmpty())
      return false;
    if (node instanceof Cypher25Parser.DynamicLabelContext)
      return true;
    for (int i = 0; i < node.getChildCount(); i++)
      if (!isFreeOfNegationAndWildcard(node.getChild(i)))
        return false;
    return true;
  }

  private static LabelPredicate buildLabelPredicate(final Cypher25Parser.LabelExpression4Context ctx,
      final Cypher25Parser.LabelExpressionContext root) {
    final List<Cypher25Parser.LabelExpression3Context> operands = ctx.labelExpression3();
    if (operands.size() == 1)
      return buildLabelPredicate(operands.getFirst(), root);
    final LabelPredicate[] out = new LabelPredicate[operands.size()];
    for (int i = 0; i < out.length; i++)
      out[i] = buildLabelPredicate(operands.get(i), root);
    return new LabelPredicate.Or(out);
  }

  private static LabelPredicate buildLabelPredicate(final Cypher25Parser.LabelExpression3Context ctx,
      final Cypher25Parser.LabelExpressionContext root) {
    final List<Cypher25Parser.LabelExpression2Context> operands = ctx.labelExpression2();
    if (operands.size() == 1)
      return buildLabelPredicate(operands.getFirst(), root);
    final LabelPredicate[] out = new LabelPredicate[operands.size()];
    for (int i = 0; i < out.length; i++)
      out[i] = buildLabelPredicate(operands.get(i), root);
    return new LabelPredicate.And(out);
  }

  private static LabelPredicate buildLabelPredicate(final Cypher25Parser.LabelExpression2Context ctx,
      final Cypher25Parser.LabelExpressionContext root) {
    final Cypher25Parser.LabelExpression1Context inner = ctx.labelExpression1();
    LabelPredicate result;
    if (inner instanceof Cypher25Parser.ParenthesizedLabelExpressionContext parenthesized)
      result = buildLabelPredicate(parenthesized.labelExpression4(), root);
    else if (inner instanceof Cypher25Parser.AnyLabelContext)
      result = LabelPredicate.AnyLabel.INSTANCE;
    else if (inner instanceof Cypher25Parser.LabelNameContext)
      result = new LabelPredicate.Name(stripBackticks(inner.getText()));
    else
      throw new CommandParsingException("UnexpectedSyntax: a dynamic label $(...) cannot be combined with '!', '%' or a"
          + " mix of '&' and '|' in the label expression '" + root.getText() + "'");
    // '!!A' is A: only the parity of the marks matters.
    if ((ctx.EXCLAMATION_MARK().size() & 1) == 1)
      result = new LabelPredicate.Not(result);
    return result;
  }

  /**
   * Returns true when some node of the given label-expression rule class combined more than one
   * operand, i.e. the operator that rule encodes ({@code |} for labelExpression4, {@code &} or
   * {@code :} for labelExpression3) is actually present in the expression.
   */
  private static boolean hasOperator(final ParseTree node, final Class<? extends ParserRuleContext> ruleClass) {
    if (ruleClass.isInstance(node)) {
      int operands = 0;
      for (int i = 0; i < node.getChildCount(); i++) {
        if (node.getChild(i) instanceof ParserRuleContext)
          operands++;
      }
      if (operands > 1)
        return true;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (hasOperator(node.getChild(i), ruleClass))
        return true;
    }
    return false;
  }

  /**
   * Walks the label expression grammar tree collecting only static label names
   * ({@code LabelNameContext}). Dynamic {@code $(expression)} labels are skipped.
   */
  public static List<String> collectStaticLabels(final ParserRuleContext ctx) {
    final List<String> labels = new ArrayList<>();
    collectStaticLabelsRecursive(ctx, labels);
    return labels;
  }

  private static void collectStaticLabelsRecursive(final ParseTree node, final List<String> out) {
    if (node instanceof Cypher25Parser.LabelNameContext) {
      out.add(stripBackticks(node.getText()));
      return;
    }
    if (node instanceof Cypher25Parser.DynamicLabelContext)
      return; // skip dynamic labels; they are collected separately

    for (int i = 0; i < node.getChildCount(); i++)
      collectStaticLabelsRecursive(node.getChild(i), out);
  }

  /**
   * Walks the label expression grammar tree collecting dynamic label expression contexts.
   * Returns the inner {@link Cypher25Parser.ExpressionContext} of each {@code $(expression)}
   * dynamic label, so callers can compile them into runtime expressions.
   */
  public static List<Cypher25Parser.ExpressionContext> collectDynamicLabelContexts(final ParserRuleContext ctx) {
    final List<Cypher25Parser.ExpressionContext> out = new ArrayList<>();
    collectDynamicLabelContextsRecursive(ctx, out);
    return out;
  }

  private static void collectDynamicLabelContextsRecursive(final ParseTree node,
      final List<Cypher25Parser.ExpressionContext> out) {
    if (node instanceof Cypher25Parser.DynamicLabelContext) {
      final Cypher25Parser.DynamicLabelContext dyn = (Cypher25Parser.DynamicLabelContext) node;
      final Cypher25Parser.DynamicAnyAllExpressionContext inner = dyn.dynamicAnyAllExpression();
      if (inner != null && inner.expression() != null)
        out.add(inner.expression());
      return;
    }
    if (node instanceof Cypher25Parser.LabelNameContext)
      return;

    for (int i = 0; i < node.getChildCount(); i++)
      collectDynamicLabelContextsRecursive(node.getChild(i), out);
  }

  /**
   * Parse a property expression in the form "variable.property" and return the parts.
   *
   * @param propertyExpression the property expression text (e.g., "n.name")
   * @return array of [variable, property], or null if invalid format
   */
  public static String[] extractPropertyParts(final String propertyExpression) {
    if (propertyExpression == null || !propertyExpression.contains("."))
      return null;

    final String[] parts = propertyExpression.split("\\.", 2);
    if (parts.length == 2)
      return parts;

    return null;
  }

  /**
   * Parse a value string into its appropriate type (String, Number, Boolean, null).
   * Handles quoted strings, numbers (integer/decimal), booleans, and null.
   *
   * @param value the string representation of the value
   * @return the parsed value object
   */
  public static Object parseValueString(final String value) {
    if (value == null)
      return null;

    // Remove quotes from strings
    if (value.startsWith("'") && value.endsWith("'"))
      return value.substring(1, value.length() - 1);

    if (value.startsWith("\"") && value.endsWith("\""))
      return value.substring(1, value.length() - 1);

    // Check for null
    if ("null".equalsIgnoreCase(value))
      return null;

    // Check for boolean
    if ("true".equalsIgnoreCase(value))
      return Boolean.TRUE;

    if ("false".equalsIgnoreCase(value))
      return Boolean.FALSE;

    // Try to parse as number
    try {
      if (value.contains("."))
        return Double.parseDouble(value);
      else
        return Long.parseLong(value);
    } catch (final NumberFormatException e) {
      // Not a number, return as string
      return value;
    }
  }

  /**
   * Decodes escape sequences in a string literal.
   * Handles: \n (newline), \t (tab), \r (carriage return), \\ (backslash), \' (single quote), \" (double quote)
   *
   * @param input the string with escape sequences (without surrounding quotes)
   * @return the decoded string
   */
  public static String decodeStringLiteral(final String input) {
    if (input == null || input.isEmpty())
      return input;

    // Quick check: if no backslash, return as-is to avoid allocation
    if (input.indexOf('\\') == -1)
      return input;

    final StringBuilder result = new StringBuilder(input.length());
    boolean escaped = false;

    for (int i = 0; i < input.length(); i++) {
      final char c = input.charAt(i);

      if (escaped) {
        escaped = false;
        switch (c) {
          case 'n':
            result.append('\n');
            break;
          case 't':
            result.append('\t');
            break;
          case 'r':
            result.append('\r');
            break;
          case 'b':
            result.append('\b');
            break;
          case 'f':
            result.append('\f');
            break;
          case '\\':
            result.append('\\');
            break;
          case '\'':
            result.append('\'');
            break;
          case '"':
            result.append('"');
            break;
          case '0':
            result.append('\0');
            break;
          case 'u':
          case 'U':
            // Unicode escape: 4 or 8 hex digits
            final int hexLen = c == 'u' ? 4 : 8;
            if (i + hexLen <= input.length()) {
              final String hex = input.substring(i + 1, i + 1 + hexLen);
              try {
                final int codePoint = Integer.parseInt(hex, 16);
                result.appendCodePoint(codePoint);
                i += hexLen;
              } catch (final NumberFormatException e) {
                throw new CommandParsingException("InvalidUnicodeLiteral: Invalid unicode escape sequence: \\" + c + hex);
              }
            } else {
              throw new CommandParsingException("InvalidUnicodeLiteral: Incomplete unicode escape sequence at end of string");
            }
            break;
          default:
            // For unrecognized escape sequences, preserve the backslash and the
            // following character verbatim (matches Neo4j behavior - issue #4093).
            result.append('\\').append(c);
            break;
        }
      } else if (c == '\\') {
        escaped = true;
      } else {
        result.append(c);
      }
    }

    // Handle trailing backslash (keep it as-is)
    if (escaped)
      result.append('\\');

    return result.toString();
  }

  /**
   * Find an operator outside parentheses in an expression string.
   * This is used to parse comparison expressions while respecting parenthesized sub-expressions.
   * Also tracks string literals and bracket depth.
   *
   * @param text the expression text
   * @param operator the operator to find
   * @return the index of the operator, or -1 if not found outside parentheses
   */
  public static int findOperatorOutsideParentheses(final String text, final String operator) {
    int parenDepth = 0;
    int bracketDepth = 0;
    boolean inString = false;
    char stringChar = 0;
    final int opLen = operator.length();

    for (int i = 0; i <= text.length() - opLen; i++) {
      final char c = text.charAt(i);

      // Track string literals
      if ((c == '\'' || c == '"') && (i == 0 || text.charAt(i - 1) != '\\')) {
        if (!inString) {
          inString = true;
          stringChar = c;
        } else if (c == stringChar) {
          inString = false;
        }
        continue;
      }

      if (inString)
        continue;

      // Track parentheses
      if (c == '(') {
        parenDepth++;
        continue;
      }
      if (c == ')') {
        parenDepth--;
        continue;
      }

      // Track brackets
      if (c == '[') {
        bracketDepth++;
        continue;
      }
      if (c == ']') {
        bracketDepth--;
        continue;
      }

      // Only match operator at top level
      if (parenDepth == 0 && bracketDepth == 0 && text.substring(i, i + opLen).equals(operator))
        return i;
    }

    return -1;
  }
}
