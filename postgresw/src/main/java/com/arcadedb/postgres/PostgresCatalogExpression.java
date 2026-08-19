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
package com.arcadedb.postgres;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * The slice of PostgreSQL expression syntax that a client's catalog query is written in, parsed once and
 * then evaluated against one emulated catalog row at a time (issue #6412).
 * <p>
 * A tool does not ask "list the tables"; it sends the query its own driver writes, and that query carries the
 * answer's shape inside itself. The JDBC driver's table list, for instance, projects the literal
 * {@code CASE c.relkind WHEN 'r' THEN 'TABLE' ... END} - so a catalog that evaluates the client's own
 * expressions against a row saying {@code relkind = 'r'} produces exactly the string that client expects,
 * without this code having to know which tool asked or what it calls a table. That is what replaces the
 * {@code application_name}-keyed answers: the shape of the query, not the name of the sender, decides.
 * <p>
 * Deliberately partial. Anything outside this slice evaluates to {@link #UNKNOWN} rather than to a guess, and
 * the caller decides what that means: in a projection it means the query cannot be answered honestly and is
 * declined whole; in a WHERE clause it means that one predicate does not get to remove rows (see
 * {@link PostgresCatalog} for why that is the safe direction for a catalog whose entire content is the user's
 * own types).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
abstract class PostgresCatalogExpression {
  /**
   * The value of an expression this class cannot evaluate - an unknown function, a column no emulated
   * relation has, a construct outside the supported slice. Distinct from SQL NULL, which is a value.
   */
  static final Object UNKNOWN = new Object() {
    @Override
    public String toString() {
      return "UNKNOWN";
    }
  };

  /** Distinguishes "this is not one of the scalar functions" from a scalar function that answered UNKNOWN. */
  private static final Object NOT_A_SCALAR_FUNCTION = new Object();

  /** Supplies the column values of the row being evaluated, and the session values around it. */
  interface Resolver {
    /**
     * @param qualifier the table alias the reference carried, or null when it was unqualified
     *
     * @return the column's value, or {@link #UNKNOWN} when no relation in this query has such a column
     */
    Object column(String qualifier, String name);

    /**
     * @return the value of a niladic session function ({@code current_schema}, {@code current_user}, ...),
     * or {@link #UNKNOWN}
     */
    Object function(String name, List<Object> arguments);

    /**
     * @return the value a window function takes on the row being evaluated, computed by the caller over the
     * whole row set before any row is projected, or {@link #UNKNOWN} when it was not computed
     */
    default Object window(final WindowCall call) {
      return UNKNOWN;
    }
  }

  abstract Object evaluate(Resolver resolver);

  /**
   * Parses one expression and returns it, or null when it is not written in the supported slice. The whole
   * token range must be consumed: a trailing token means the text was something more than one expression.
   */
  static PostgresCatalogExpression parse(final List<PostgresCatalogToken> tokens) {
    final Parser parser = new Parser(tokens);
    final PostgresCatalogExpression expression = parser.parseExpression();
    if (expression == null || !parser.atEnd())
      return null;
    return expression;
  }

  /**
   * Parses the longest expression the token list starts with, leaving the parser positioned after it. Used
   * where an expression is followed by something else the caller reads itself - a column alias, ORDER BY's
   * ASC/DESC.
   */
  static Parser parser(final List<PostgresCatalogToken> tokens) {
    return new Parser(tokens);
  }

  // ---------------------------------------------------------------- value helpers

  /**
   * How a WHERE predicate that could not be evaluated is read: as "does not exclude this row". A catalog
   * whose entire content is the user's own types has no system rows for an unrecognised predicate to be
   * filtering out, so the permissive direction cannot hide a row the client asked for - while the strict
   * direction would hide every row over one construct nobody parsed.
   */
  static boolean isTrue(final Object value) {
    if (value == UNKNOWN)
      return true;
    if (value == null)
      return false;
    if (value instanceof Boolean b)
      return b;
    if (value instanceof Number n)
      return n.doubleValue() != 0;
    return false;
  }

  static String asString(final Object value) {
    if (value == null || value == UNKNOWN)
      return null;
    if (value instanceof Boolean b)
      return b ? "t" : "f";
    return value.toString();
  }

  // ---------------------------------------------------------------- nodes

  private static class Literal extends PostgresCatalogExpression {
    private final Object value;

    Literal(final Object value) {
      this.value = value;
    }

    @Override
    Object evaluate(final Resolver resolver) {
      return value;
    }
  }

  static class ColumnReference extends PostgresCatalogExpression {
    final String qualifier;
    final String name;

    ColumnReference(final String qualifier, final String name) {
      this.qualifier = qualifier;
      this.name = name;
    }

    @Override
    Object evaluate(final Resolver resolver) {
      return resolver.column(qualifier, name);
    }
  }

  static class FunctionCall extends PostgresCatalogExpression {
    final String                              name;
    final List<PostgresCatalogExpression> arguments;

    FunctionCall(final String name, final List<PostgresCatalogExpression> arguments) {
      this.name = name;
      this.arguments = arguments;
    }

    @Override
    Object evaluate(final Resolver resolver) {
      final List<Object> values = new ArrayList<>(arguments.size());
      for (final PostgresCatalogExpression argument : arguments)
        values.add(argument.evaluate(resolver));

      // NULLIF and COALESCE are evaluated here rather than delegated to the resolver, because they are the
      // functions whose answer is about their own arguments rather than about the session or the row.
      if ("nullif".equals(name)) {
        if (values.size() != 2)
          return UNKNOWN;
        final Object first = values.get(0);
        final Object second = values.get(1);
        if (first == UNKNOWN || second == UNKNOWN)
          return UNKNOWN;
        return first != null && first.equals(second) ? null : first;
      }

      if ("coalesce".equals(name)) {
        for (final Object value : values)
          if (value != null && value != UNKNOWN)
            return value;
        return values.isEmpty() ? null : values.get(values.size() - 1);
      }

      // The plain scalar functions a catalog query wraps a column in - most often lower() on a name it is
      // about to compare. They are about their arguments alone, so they are evaluated here too.
      final Object scalar = scalarFunction(name, values);
      if (scalar != NOT_A_SCALAR_FUNCTION)
        return scalar;

      return resolver.function(name, values);
    }
  }

  /**
   * A window function: {@code row_number() OVER (PARTITION BY ... ORDER BY ...)}. Its value depends on the
   * other rows, not only on this one, so it is computed by the caller over the whole row set and read back
   * here - see {@link Resolver#window}. The JDBC driver's column list is written with one of these, to number
   * a table's columns from 1 without counting the dropped ones, which is why it is worth having.
   */
  static class WindowCall extends PostgresCatalogExpression {
    final String                          name;
    final List<PostgresCatalogExpression> partitionBy;
    final List<PostgresCatalogExpression> orderBy;
    /** One flag per ORDER BY key: the direction it was written with, which decides the numbering. */
    final List<Boolean>                   orderByDescending;

    WindowCall(final String name, final List<PostgresCatalogExpression> partitionBy,
        final List<PostgresCatalogExpression> orderBy, final List<Boolean> orderByDescending) {
      this.name = name;
      this.partitionBy = partitionBy;
      this.orderBy = orderBy;
      this.orderByDescending = orderByDescending;
    }

    @Override
    Object evaluate(final Resolver resolver) {
      return resolver.window(this);
    }
  }

  private static Object scalarFunction(final String name, final List<Object> values) {
    switch (name) {
    case "lower", "upper", "length", "abs", "trim", "btrim", "ltrim", "rtrim", "replace", "concat" -> {
      for (final Object value : values)
        if (value == UNKNOWN)
          return UNKNOWN;
    }
    default -> {
      return NOT_A_SCALAR_FUNCTION;
    }
    }

    if ("concat".equals(name)) {
      final StringBuilder buffer = new StringBuilder();
      for (final Object value : values)
        if (value != null)
          buffer.append(asString(value));
      return buffer.toString();
    }

    if (values.isEmpty() || values.get(0) == null)
      return values.isEmpty() ? UNKNOWN : null;

    final String first = asString(values.get(0));
    return switch (name) {
      case "lower" -> first.toLowerCase(Locale.ENGLISH);
      case "upper" -> first.toUpperCase(Locale.ENGLISH);
      case "length" -> (long) first.length();
      case "trim", "btrim" -> first.trim();
      case "ltrim" -> first.stripLeading();
      case "rtrim" -> first.stripTrailing();
      case "abs" -> values.get(0) instanceof Number n ? (Object) Math.abs(n.doubleValue()) : UNKNOWN;
      case "replace" -> values.size() == 3 && values.get(1) != null && values.get(2) != null ?
          first.replace(asString(values.get(1)), asString(values.get(2))) : (values.size() == 3 ? null : UNKNOWN);
      default -> UNKNOWN;
    };
  }

  private static class Not extends PostgresCatalogExpression {
    private final PostgresCatalogExpression operand;

    Not(final PostgresCatalogExpression operand) {
      this.operand = operand;
    }

    @Override
    Object evaluate(final Resolver resolver) {
      final Object value = operand.evaluate(resolver);
      if (value == UNKNOWN)
        return UNKNOWN;
      if (value == null)
        return null;
      return !isTrue(value);
    }
  }

  private static class Negate extends PostgresCatalogExpression {
    private final PostgresCatalogExpression operand;

    Negate(final PostgresCatalogExpression operand) {
      this.operand = operand;
    }

    @Override
    Object evaluate(final Resolver resolver) {
      final Object value = operand.evaluate(resolver);
      if (value == UNKNOWN)
        return UNKNOWN;
      if (value instanceof Number n)
        return n instanceof Double || n instanceof Float ? (Object) (-n.doubleValue()) : (Object) (-n.longValue());
      return UNKNOWN;
    }
  }

  private static class Binary extends PostgresCatalogExpression {
    private final String                    operator;
    private final PostgresCatalogExpression left;
    private final PostgresCatalogExpression right;

    Binary(final String operator, final PostgresCatalogExpression left, final PostgresCatalogExpression right) {
      this.operator = operator;
      this.left = left;
      this.right = right;
    }

    @Override
    Object evaluate(final Resolver resolver) {
      final Object l = left.evaluate(resolver);

      // AND and OR keep their short-circuit behaviour over the three values, so that a predicate whose other
      // half is unevaluable still decides the row when this half is enough: FALSE AND anything is FALSE.
      if ("AND".equals(operator)) {
        if (l != UNKNOWN && l != null && !isTrue(l))
          return Boolean.FALSE;
        final Object r = right.evaluate(resolver);
        if (l == UNKNOWN || r == UNKNOWN)
          return r != UNKNOWN && r != null && !isTrue(r) ? Boolean.FALSE : UNKNOWN;
        if (l == null || r == null)
          return isTrue(l) && isTrue(r) ? null : Boolean.FALSE;
        return isTrue(l) && isTrue(r);
      }

      if ("OR".equals(operator)) {
        if (l != UNKNOWN && l != null && isTrue(l))
          return Boolean.TRUE;
        final Object r = right.evaluate(resolver);
        if (l == UNKNOWN || r == UNKNOWN)
          return r != UNKNOWN && r != null && isTrue(r) ? Boolean.TRUE : UNKNOWN;
        if (l == null || r == null)
          return isTrue(l) || isTrue(r) ? Boolean.TRUE : null;
        return isTrue(l) || isTrue(r);
      }

      final Object r = right.evaluate(resolver);
      if (l == UNKNOWN || r == UNKNOWN)
        return UNKNOWN;

      return switch (operator) {
        case "||" -> l == null || r == null ? null : asString(l) + asString(r);
        case "+", "-", "*", "/" -> arithmetic(operator, l, r);
        case "~", "!~", "~*", "!~*" -> regex(operator, l, r);
        case "LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE" -> like(operator, l, r);
        default -> compare(operator, l, r);
      };
    }

    private static Object arithmetic(final String operator, final Object l, final Object r) {
      if (l == null || r == null)
        return null;
      if (!(l instanceof Number ln) || !(r instanceof Number rn))
        return UNKNOWN;
      final double result = switch (operator) {
        case "+" -> ln.doubleValue() + rn.doubleValue();
        case "-" -> ln.doubleValue() - rn.doubleValue();
        case "*" -> ln.doubleValue() * rn.doubleValue();
        default -> rn.doubleValue() == 0 ? Double.NaN : ln.doubleValue() / rn.doubleValue();
      };
      if (Double.isNaN(result))
        return null;
      return result == Math.rint(result) ? (Object) (long) result : (Object) result;
    }

    private static Object regex(final String operator, final Object l, final Object r) {
      if (l == null || r == null)
        return null;
      final String value = asString(l);
      final String pattern = asString(r);
      try {
        final boolean matches = Pattern.compile(pattern, operator.endsWith("*") ? Pattern.CASE_INSENSITIVE : 0)
            .matcher(value).find();
        return operator.startsWith("!") != matches;
      } catch (final PatternSyntaxException e) {
        // A POSIX regex Java cannot compile: not something to guess the answer of.
        return UNKNOWN;
      }
    }

    private static Object like(final String operator, final Object l, final Object r) {
      if (l == null || r == null)
        return null;
      final boolean matches = Pattern.compile(likeToRegex(asString(r)),
              operator.contains("ILIKE") ? Pattern.CASE_INSENSITIVE : 0)
          .matcher(asString(l)).matches();
      return operator.startsWith("NOT") != matches;
    }

    private static Object compare(final String operator, final Object l, final Object r) {
      if (l == null || r == null)
        return null;

      final int comparison;
      if (l instanceof Number ln && r instanceof Number rn)
        comparison = Double.compare(ln.doubleValue(), rn.doubleValue());
      else if (l instanceof Boolean || r instanceof Boolean)
        comparison = Boolean.compare(isTrue(l), isTrue(r));
      else
        comparison = asString(l).compareTo(asString(r));

      return switch (operator) {
        case "=" -> comparison == 0;
        case "<>", "!=" -> comparison != 0;
        case "<" -> comparison < 0;
        case "<=" -> comparison <= 0;
        case ">" -> comparison > 0;
        case ">=" -> comparison >= 0;
        default -> UNKNOWN;
      };
    }
  }

  /** Translates a SQL LIKE pattern into the equivalent regular expression. */
  static String likeToRegex(final String pattern) {
    final StringBuilder regex = new StringBuilder(pattern.length() * 2);
    for (int i = 0; i < pattern.length(); i++) {
      final char c = pattern.charAt(i);
      switch (c) {
      case '%' -> regex.append(".*");
      case '_' -> regex.append('.');
      case '\\' -> {
        if (i + 1 < pattern.length())
          regex.append(Pattern.quote(String.valueOf(pattern.charAt(++i))));
      }
      default -> regex.append(Pattern.quote(String.valueOf(c)));
      }
    }
    return regex.toString();
  }

  private static class In extends PostgresCatalogExpression {
    private final PostgresCatalogExpression       operand;
    private final List<PostgresCatalogExpression> candidates;
    private final boolean                         negated;

    In(final PostgresCatalogExpression operand, final List<PostgresCatalogExpression> candidates, final boolean negated) {
      this.operand = operand;
      this.candidates = candidates;
      this.negated = negated;
    }

    @Override
    Object evaluate(final Resolver resolver) {
      final Object value = operand.evaluate(resolver);
      if (value == UNKNOWN)
        return UNKNOWN;
      if (value == null)
        return null;

      boolean found = false;
      for (final PostgresCatalogExpression candidate : candidates) {
        final Object other = candidate.evaluate(resolver);
        if (other == UNKNOWN)
          return UNKNOWN;
        if (other == null)
          continue;
        if (Boolean.TRUE.equals(Binary.compare("=", value, other))) {
          found = true;
          break;
        }
      }
      return negated != found;
    }
  }

  private static class IsNull extends PostgresCatalogExpression {
    private final PostgresCatalogExpression operand;
    private final boolean                   negated;

    IsNull(final PostgresCatalogExpression operand, final boolean negated) {
      this.operand = operand;
      this.negated = negated;
    }

    @Override
    Object evaluate(final Resolver resolver) {
      final Object value = operand.evaluate(resolver);
      if (value == UNKNOWN)
        return UNKNOWN;
      return negated != (value == null);
    }
  }

  private static class Case extends PostgresCatalogExpression {
    private final PostgresCatalogExpression       operand;
    private final List<PostgresCatalogExpression> conditions;
    private final List<PostgresCatalogExpression> results;
    private final PostgresCatalogExpression       fallback;

    Case(final PostgresCatalogExpression operand, final List<PostgresCatalogExpression> conditions,
        final List<PostgresCatalogExpression> results, final PostgresCatalogExpression fallback) {
      this.operand = operand;
      this.conditions = conditions;
      this.results = results;
      this.fallback = fallback;
    }

    @Override
    Object evaluate(final Resolver resolver) {
      final Object subject = operand == null ? null : operand.evaluate(resolver);
      if (operand != null && subject == UNKNOWN)
        return UNKNOWN;

      for (int i = 0; i < conditions.size(); i++) {
        final Object condition = conditions.get(i).evaluate(resolver);
        if (condition == UNKNOWN)
          return UNKNOWN;

        final boolean matched;
        if (operand == null)
          matched = condition != null && isTrue(condition);
        else
          matched = Boolean.TRUE.equals(Binary.compare("=", subject, condition));

        if (matched)
          return results.get(i).evaluate(resolver);
      }

      return fallback == null ? null : fallback.evaluate(resolver);
    }
  }

  private static class Subscript extends PostgresCatalogExpression {
    private final PostgresCatalogExpression operand;
    private final PostgresCatalogExpression index;

    Subscript(final PostgresCatalogExpression operand, final PostgresCatalogExpression index) {
      this.operand = operand;
      this.index = index;
    }

    @Override
    Object evaluate(final Resolver resolver) {
      final Object value = operand.evaluate(resolver);
      final Object position = index.evaluate(resolver);
      if (value == UNKNOWN || position == UNKNOWN)
        return UNKNOWN;
      if (!(value instanceof List<?> list) || !(position instanceof Number n))
        return UNKNOWN;

      // PostgreSQL arrays are 1-based by default, which is the only base a catalog query ever uses.
      final int i = n.intValue() - 1;
      return i < 0 || i >= list.size() ? null : list.get(i);
    }
  }

  // ---------------------------------------------------------------- parser

  /**
   * A recursive-descent parser over the token list. Every method returns null on a construct outside the
   * supported slice, which propagates up and makes the whole parse fail - the caller then declines the query
   * rather than answering part of it.
   */
  static class Parser {
    /**
     * How deeply an expression may nest before the query is declined. Every level of parentheses, CASE arm,
     * function argument or subscript costs one, and a catalog query written by a real client nests a handful
     * deep - the JDBC driver's table list, the deepest this catalog has to read, reaches four.
     * <p>
     * The bound is what keeps "a shape outside the slice is declined" true for <i>every</i> shape: without it
     * a query with tens of thousands of nested parentheses - well inside the wire protocol's message budget,
     * and reachable by any authenticated client - would recurse until the stack gave out, and a
     * StackOverflowError is an Error rather than an Exception, so it would kill the connection thread instead
     * of being answered with an empty result.
     */
    private static final int MAX_DEPTH = 100;

    private final List<PostgresCatalogToken> tokens;
    private       int                        position;
    private       int                        depth;

    Parser(final List<PostgresCatalogToken> tokens) {
      this.tokens = tokens;
    }

    boolean atEnd() {
      return position >= tokens.size();
    }

    int getPosition() {
      return position;
    }

    PostgresCatalogToken peek() {
      return position < tokens.size() ? tokens.get(position) : null;
    }

    boolean skipKeyword(final String keyword) {
      final PostgresCatalogToken token = peek();
      if (token != null && token.isKeyword(keyword)) {
        ++position;
        return true;
      }
      return false;
    }

    private boolean skipSymbol(final String symbol) {
      final PostgresCatalogToken token = peek();
      if (token != null && token.isSymbol(symbol)) {
        ++position;
        return true;
      }
      return false;
    }

    PostgresCatalogExpression parseExpression() {
      return guardDepth(this::parseOr);
    }

    /**
     * Runs a production under the depth guard {@link #parseExpression} enforces, for the productions that
     * recurse on themselves directly - {@code parseNot} over a chain of {@code NOT}, {@code parseUnary} over
     * a chain of unary {@code -}/{@code +} - and so reach the stack the same way nested parentheses do
     * without ever calling back through {@link #parseExpression}.
     */
    private PostgresCatalogExpression guardDepth(final Supplier<PostgresCatalogExpression> production) {
      if (++depth > MAX_DEPTH) {
        --depth;
        return null;
      }
      try {
        return production.get();
      } finally {
        --depth;
      }
    }

    private PostgresCatalogExpression parseOr() {
      PostgresCatalogExpression left = parseAnd();
      if (left == null)
        return null;

      while (skipKeyword("OR")) {
        final PostgresCatalogExpression right = parseAnd();
        if (right == null)
          return null;
        left = new Binary("OR", left, right);
      }
      return left;
    }

    private PostgresCatalogExpression parseAnd() {
      PostgresCatalogExpression left = parseNot();
      if (left == null)
        return null;

      while (skipKeyword("AND")) {
        final PostgresCatalogExpression right = parseNot();
        if (right == null)
          return null;
        left = new Binary("AND", left, right);
      }
      return left;
    }

    private PostgresCatalogExpression parseNot() {
      if (skipKeyword("NOT")) {
        final PostgresCatalogExpression operand = guardDepth(this::parseNot);
        return operand == null ? null : new Not(operand);
      }
      return parseComparison();
    }

    private PostgresCatalogExpression parseComparison() {
      PostgresCatalogExpression left = parseConcat();
      if (left == null)
        return null;

      while (true) {
        final PostgresCatalogToken token = peek();
        if (token == null)
          return left;

        if (token.isKeyword("IS")) {
          ++position;
          final boolean negated = skipKeyword("NOT");
          if (!skipKeyword("NULL"))
            // IS TRUE / IS DISTINCT FROM and friends are outside the slice.
            return null;
          left = new IsNull(left, negated);
        } else {
          final boolean negated = token.isKeyword("NOT") && lookaheadIsKeyword(1, "IN", "LIKE", "ILIKE");
          if (negated)
            ++position;

          final PostgresCatalogToken next = peek();
          if (next == null)
            return negated ? null : left;

          if (next.isKeyword("IN")) {
            ++position;
            if (!skipSymbol("("))
              return null;
            final List<PostgresCatalogExpression> candidates = new ArrayList<>();
            do {
              final PostgresCatalogExpression candidate = parseExpression();
              if (candidate == null)
                return null;
              candidates.add(candidate);
            } while (skipSymbol(","));
            if (!skipSymbol(")"))
              return null;
            left = new In(left, candidates, negated);
          } else if (next.isKeyword("LIKE") || next.isKeyword("ILIKE")) {
            ++position;
            final PostgresCatalogExpression right = parseConcat();
            if (right == null)
              return null;
            left = new Binary((negated ? "NOT " : "") + next.text.toUpperCase(Locale.ENGLISH), left, right);
          } else if (negated)
            return null;
          else if (isComparisonOperator(next)) {
            ++position;
            final PostgresCatalogExpression right = parseConcat();
            if (right == null)
              return null;
            left = new Binary(next.text, left, right);
          } else
            return left;
        }
      }
    }

    private static boolean isComparisonOperator(final PostgresCatalogToken token) {
      return token.isSymbol("=") || token.isSymbol("<>") || token.isSymbol("!=") || token.isSymbol("<")
          || token.isSymbol(">") || token.isSymbol("<=") || token.isSymbol(">=") || token.isSymbol("~")
          || token.isSymbol("!~") || token.isSymbol("~*") || token.isSymbol("!~*");
    }

    private boolean lookaheadIsKeyword(final int offset, final String... keywords) {
      final int index = position + offset;
      if (index >= tokens.size())
        return false;
      for (final String keyword : keywords)
        if (tokens.get(index).isKeyword(keyword))
          return true;
      return false;
    }

    private PostgresCatalogExpression parseConcat() {
      PostgresCatalogExpression left = parseAdditive();
      if (left == null)
        return null;

      while (peek() != null && peek().isSymbol("||")) {
        ++position;
        final PostgresCatalogExpression right = parseAdditive();
        if (right == null)
          return null;
        left = new Binary("||", left, right);
      }
      return left;
    }

    private PostgresCatalogExpression parseAdditive() {
      PostgresCatalogExpression left = parseMultiplicative();
      if (left == null)
        return null;

      while (peek() != null && (peek().isSymbol("+") || peek().isSymbol("-"))) {
        final String operator = peek().text;
        ++position;
        final PostgresCatalogExpression right = parseMultiplicative();
        if (right == null)
          return null;
        left = new Binary(operator, left, right);
      }
      return left;
    }

    private PostgresCatalogExpression parseMultiplicative() {
      PostgresCatalogExpression left = parseUnary();
      if (left == null)
        return null;

      while (peek() != null && (peek().isSymbol("*") || peek().isSymbol("/"))) {
        final String operator = peek().text;
        ++position;
        final PostgresCatalogExpression right = parseUnary();
        if (right == null)
          return null;
        left = new Binary(operator, left, right);
      }
      return left;
    }

    private PostgresCatalogExpression parseUnary() {
      if (peek() != null && peek().isSymbol("-")) {
        ++position;
        final PostgresCatalogExpression operand = guardDepth(this::parseUnary);
        return operand == null ? null : new Negate(operand);
      }
      if (peek() != null && peek().isSymbol("+")) {
        ++position;
        return guardDepth(this::parseUnary);
      }
      return parsePostfix();
    }

    /** Applies the suffixes that bind tightest: a cast and an array subscript. */
    private PostgresCatalogExpression parsePostfix() {
      PostgresCatalogExpression operand = parsePrimary();
      if (operand == null)
        return null;

      while (true) {
        final PostgresCatalogToken token = peek();

        if (token != null && token.isSymbol("::")) {
          // A cast never changes what a catalog query means here: 'pg_class'::regclass is the name, and
          // int4/text casts are there for PostgreSQL's type resolution, which this evaluator does not have.
          ++position;
          if (!skipCastType())
            return null;
        } else if (token != null && token.isSymbol("[")) {
          ++position;
          final PostgresCatalogExpression index = parseExpression();
          if (index == null || !skipSymbol("]"))
            return null;
          operand = new Subscript(operand, index);
        } else
          return operand;
      }
    }

    /** Consumes a type name after {@code ::}, including {@code varchar(10)} and {@code text[]}. */
    private boolean skipCastType() {
      final PostgresCatalogToken token = peek();
      if (token == null || token.type != PostgresCatalogToken.Type.IDENTIFIER)
        return false;
      ++position;

      while (peek() != null && peek().isSymbol(".")) {
        ++position;
        if (peek() == null || peek().type != PostgresCatalogToken.Type.IDENTIFIER)
          return false;
        ++position;
      }

      // Some type names are several words: "double precision", "character varying", "timestamp with time zone".
      while (peek() != null && peek().type == PostgresCatalogToken.Type.IDENTIFIER && isTypeNameWord(peek().text))
        ++position;

      if (peek() != null && peek().isSymbol("(")) {
        int depth = 0;
        while (peek() != null) {
          if (peek().isSymbol("("))
            ++depth;
          else if (peek().isSymbol(")"))
            --depth;
          ++position;
          if (depth == 0)
            break;
        }
      }

      while (peek() != null && peek().isSymbol("[")) {
        ++position;
        if (!skipSymbol("]"))
          return false;
      }

      return true;
    }

    private static boolean isTypeNameWord(final String text) {
      return switch (text.toLowerCase(Locale.ENGLISH)) {
        case "precision", "varying", "with", "without", "time", "zone" -> true;
        default -> false;
      };
    }

    private PostgresCatalogExpression parsePrimary() {
      final PostgresCatalogToken token = peek();
      if (token == null)
        return null;

      if (token.isSymbol("(")) {
        ++position;
        final PostgresCatalogExpression inner = parseExpression();
        if (inner == null || !skipSymbol(")"))
          return null;
        return inner;
      }

      if (token.isKeyword("CASE"))
        return parseCase();

      switch (token.type) {
      case STRING -> {
        ++position;
        return new Literal(token.text);
      }
      case NUMBER -> {
        final Object number = parseNumber(token.text);
        if (number == null)
          // Something the lexer read as one numeric token but that is not a number - "1.2.3". Reading it as
          // SQL NULL would answer the query with a value the client never wrote.
          return null;
        ++position;
        return new Literal(number);
      }
      case QUOTED_IDENTIFIER -> {
        ++position;
        return parseQualifiedReference(null, token.text);
      }
      case IDENTIFIER -> {
        if (token.isKeyword("NULL")) {
          ++position;
          return new Literal(null);
        }
        if (token.isKeyword("TRUE")) {
          ++position;
          return new Literal(Boolean.TRUE);
        }
        if (token.isKeyword("FALSE")) {
          ++position;
          return new Literal(Boolean.FALSE);
        }
        if (token.isKeyword("SELECT") || token.isKeyword("FROM") || token.isKeyword("WHERE"))
          // A sub-select, or the end of the expression where the caller expected more.
          return null;

        ++position;
        return parseQualifiedReference(null, token.text);
      }
      default -> {
        return null;
      }
      }
    }

    /**
     * Reads what follows an identifier: further dotted parts, and then either an argument list (a function
     * call) or nothing (a column reference). {@code pg_catalog.} is dropped, being the schema every one of
     * these lives in.
     */
    private PostgresCatalogExpression parseQualifiedReference(final String qualifier, final String first) {
      String currentQualifier = qualifier;
      String name = first;

      while (peek() != null && peek().isSymbol(".")) {
        ++position;
        final PostgresCatalogToken next = peek();
        if (next == null)
          return null;
        if (next.isSymbol("*")) {
          // "t.*" is only meaningful in a projection, which parses star items before reaching this.
          return null;
        }
        if (next.type != PostgresCatalogToken.Type.IDENTIFIER && next.type != PostgresCatalogToken.Type.QUOTED_IDENTIFIER)
          return null;
        ++position;
        currentQualifier = name;
        name = next.text;
      }

      if ("pg_catalog".equalsIgnoreCase(currentQualifier) || "information_schema".equalsIgnoreCase(currentQualifier))
        currentQualifier = null;

      if (peek() != null && peek().isSymbol("(")) {
        ++position;
        final List<PostgresCatalogExpression> arguments = new ArrayList<>();
        if (!skipSymbol(")")) {
          do {
            final PostgresCatalogExpression argument = parseExpression();
            if (argument == null)
              return null;
            arguments.add(argument);
          } while (skipSymbol(","));
          if (!skipSymbol(")"))
            return null;
        }
        final String functionName = name.toLowerCase(Locale.ENGLISH);
        if (peek() != null && peek().isKeyword("OVER"))
          return parseWindow(functionName);

        return new FunctionCall(functionName, arguments);
      }

      return new ColumnReference(currentQualifier == null ? null : currentQualifier.toLowerCase(Locale.ENGLISH),
          name.toLowerCase(Locale.ENGLISH));
    }

    /**
     * Reads the {@code OVER (PARTITION BY ... ORDER BY ...)} that follows a window function. Only the two
     * clauses are read: a frame specification changes what the function means, so a query carrying one is
     * declined rather than answered as if it were absent.
     */
    private PostgresCatalogExpression parseWindow(final String functionName) {
      ++position; // OVER
      if (!skipSymbol("("))
        return null;

      final List<PostgresCatalogExpression> partitionBy = new ArrayList<>();
      final List<PostgresCatalogExpression> orderBy = new ArrayList<>();
      final List<Boolean>                   orderByDescending = new ArrayList<>();

      if (skipKeyword("PARTITION")) {
        if (!skipKeyword("BY"))
          return null;
        do {
          final PostgresCatalogExpression key = parseExpression();
          if (key == null)
            return null;
          partitionBy.add(key);
        } while (skipSymbol(","));
      }

      if (skipKeyword("ORDER")) {
        if (!skipKeyword("BY"))
          return null;
        do {
          final PostgresCatalogExpression key = parseExpression();
          if (key == null)
            return null;
          orderBy.add(key);

          // The direction decides the numbering, so it is carried rather than consumed and forgotten: a key
          // read as ascending when the client wrote DESC numbers the partition backwards, and a wrong number
          // in a system catalog is worse than no answer.
          boolean descending = false;
          if (peek() != null && (peek().isKeyword("ASC") || peek().isKeyword("DESC"))) {
            descending = peek().isKeyword("DESC");
            ++position;
          }
          // NULLS FIRST/LAST would reorder the ties this numbering breaks by position, so a window that asks
          // for it is declined rather than numbered by a rule it did not ask for.
          if (peek() != null && peek().isKeyword("NULLS"))
            return null;
          orderByDescending.add(descending);
        } while (skipSymbol(","));
      }

      if (!skipSymbol(")"))
        return null;

      return new WindowCall(functionName, partitionBy, orderBy, orderByDescending);
    }

    private PostgresCatalogExpression parseCase() {
      ++position; // CASE

      PostgresCatalogExpression operand = null;
      if (peek() != null && !peek().isKeyword("WHEN")) {
        operand = parseExpression();
        if (operand == null)
          return null;
      }

      final List<PostgresCatalogExpression> conditions = new ArrayList<>();
      final List<PostgresCatalogExpression> results = new ArrayList<>();
      while (skipKeyword("WHEN")) {
        final PostgresCatalogExpression condition = parseExpression();
        if (condition == null || !skipKeyword("THEN"))
          return null;
        final PostgresCatalogExpression result = parseExpression();
        if (result == null)
          return null;
        conditions.add(condition);
        results.add(result);
      }

      if (conditions.isEmpty())
        return null;

      PostgresCatalogExpression fallback = null;
      if (skipKeyword("ELSE")) {
        fallback = parseExpression();
        if (fallback == null)
          return null;
      }

      if (!skipKeyword("END"))
        return null;

      return new Case(operand, conditions, results, fallback);
    }

    private static Object parseNumber(final String text) {
      try {
        if (text.indexOf('.') < 0 && text.indexOf('e') < 0 && text.indexOf('E') < 0)
          return Long.parseLong(text);
      } catch (final NumberFormatException e) {
        // Falls through to the floating-point reading, which is what an out-of-range integer literal is.
      }
      try {
        return Double.parseDouble(text);
      } catch (final NumberFormatException e) {
        return null;
      }
    }
  }
}
