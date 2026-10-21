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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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

  /**
   * The cast target types that are a catalog lookup rather than a change of representation. PostgreSQL's
   * OID-alias types hold an OID and print as the object's name, so a cast to one of them is the only place
   * in a catalog query where {@code ::} carries meaning this evaluator has to reproduce.
   */
  private static final Set<String> LOOKUP_CASTS = Set.of("regclass", "regtype");

  /** Stands for a cast type that is more than a bare name, and so is never one of {@link #LOOKUP_CASTS}. */
  private static final String COMPOUND_CAST_TYPE = "";

  /** POSIX bracket-expression class names and the java.util.regex class each means. */
  private static final Map<String, String> POSIX_CLASSES = Map.ofEntries(Map.entry("alpha", "\\p{Alpha}"),
      Map.entry("digit", "\\p{Digit}"), Map.entry("alnum", "\\p{Alnum}"), Map.entry("upper", "\\p{Upper}"),
      Map.entry("lower", "\\p{Lower}"), Map.entry("space", "\\p{Space}"), Map.entry("punct", "\\p{Punct}"),
      Map.entry("xdigit", "\\p{XDigit}"), Map.entry("cntrl", "\\p{Cntrl}"), Map.entry("print", "\\p{Print}"),
      Map.entry("graph", "\\p{Graph}"), Map.entry("blank", "\\p{Blank}"), Map.entry("word", "\\w"),
      Map.entry("ascii", "\\p{ASCII}"));

  /** The aggregate functions an aggregated catalog query may project. */
  private static final Set<String> AGGREGATES = Set.of("count", "sum", "avg", "min", "max", "bool_and", "bool_or",
      "every", "string_agg", "array_agg");

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

    /**
     * @return the value of an aggregate over the group of rows being projected, or {@link #UNKNOWN} when this
     * resolver is not evaluating a group - an aggregate in a WHERE clause, for one
     */
    default Object aggregate(final AggregateCall call) {
      return UNKNOWN;
    }

    /**
     * Evaluates a sub-select, correlated to the row this resolver is evaluating.
     *
     * @return for {@link SubqueryKind#SCALAR} the one value of its one row (NULL with no row), for
     * {@link SubqueryKind#EXISTS} whether it has a row, for {@link SubqueryKind#ARRAY} the values of its one
     * column; {@link #UNKNOWN} when the sub-select cannot be answered
     */
    default Object subquery(final List<PostgresCatalogToken> tokens, final SubqueryKind kind) {
      return UNKNOWN;
    }

    /**
     * @return the position, from 0, of the row a set-returning function of the projection is producing for the row being
     * evaluated, or -1 when the row is not being projected
     */
    default int expansion() {
      return -1;
    }
  }

  /** What a sub-select is asked for, which is decided by where it is written. */
  enum SubqueryKind {
    /** {@code (SELECT ...)} as an operand: the value of its single row and column. */
    SCALAR,
    /** {@code EXISTS (SELECT ...)}. */
    EXISTS,
    /** {@code ARRAY(SELECT ...)}, and the right-hand side of {@code IN (SELECT ...)}: the values of its column. */
    ARRAY
  }

  abstract Object evaluate(Resolver resolver);

  /** Receives the columns an expression reads, see {@link #collectColumns}. */
  interface Columns {
    /** The expression reads the column {@code name} of {@code qualifier}, null when it was written bare. */
    void column(String qualifier, String name);

    /**
     * The expression reads columns that cannot be listed: a sub-select is correlated to the row it is evaluated against by names
     * its own text carries, so it depends on every relation of the statement.
     */
    void anyColumn();
  }

  /**
   * Reports every column this expression reads, without evaluating it. What lets a join evaluate a condition as soon as the
   * relations it reads are bound, rather than once they all are.
   */
  abstract void collectColumns(Columns columns);

  /** An equality between two expressions, see {@link #equalityOf}. */
  record Equality(PostgresCatalogExpression left, PostgresCatalogExpression right) {
  }

  /** The two sides of {@code expression} when it is a plain {@code =}, null for anything else. */
  static Equality equalityOf(final PostgresCatalogExpression expression) {
    return expression instanceof Binary binary && "=".equals(binary.operator) ? new Equality(binary.left, binary.right) : null;
  }

  /** The column {@code expression} is, when it is nothing but a column reference, null otherwise. */
  static ColumnReference columnOf(final PostgresCatalogExpression expression) {
    return expression instanceof ColumnReference reference ? reference : null;
  }

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

  /**
   * A value read as a boolean, for a comparison with one: text is read the way PostgreSQL reads the input of a {@code boolean}
   * ({@code indisprimary = 't'} is a test of the flag), anything that is neither is {@link #isTrue}.
   */
  static boolean truth(final Object value) {
    if (value instanceof String text) {
      switch (text.trim().toLowerCase(Locale.ENGLISH)) {
      case "t", "true", "y", "yes", "on", "1":
        return true;
      default:
        return false;
      }
    }
    return isTrue(value);
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
    void collectColumns(final Columns columns) {
      // a literal reads no column
    }

    @Override
    Object evaluate(final Resolver resolver) {
      return value;
    }
  }

  /**
   * A sub-select, evaluated by the resolver against the row it is correlated to (issue #10228: psql's {@code \d}
   * reads a column's default and collation through one each). Its tokens are kept rather than parsed here,
   * because what they mean - which relations, which family of rows - is the catalog's to decide, not this
   * evaluator's.
   * <p>
   * A resolver that cannot evaluate sub-selects answers {@link #UNKNOWN}, which is what every sub-select used
   * to be (issue #7224): an operand that does not abort the parse of the predicate containing it, so that
   * three-valued logic can still settle {@code t.typrelid = 0 OR (SELECT ...)} on its readable half.
   */
  static class Subquery extends PostgresCatalogExpression {
    final List<PostgresCatalogToken> tokens;
    final SubqueryKind               kind;

    Subquery(final List<PostgresCatalogToken> tokens, final SubqueryKind kind) {
      this.tokens = tokens;
      this.kind = kind;
    }

    @Override
    void collectColumns(final Columns columns) {
      columns.anyColumn();
    }

    @Override
    Object evaluate(final Resolver resolver) {
      return resolver.subquery(tokens, kind);
    }
  }

  /** {@code x [NOT] IN (SELECT ...)}, with SQL's answer when the sub-select's column holds a NULL. */
  private static class InSubquery extends PostgresCatalogExpression {
    private final PostgresCatalogExpression operand;
    private final Subquery                  subquery;
    private final boolean                   negated;

    InSubquery(final PostgresCatalogExpression operand, final Subquery subquery, final boolean negated) {
      this.operand = operand;
      this.subquery = subquery;
      this.negated = negated;
    }

    @Override
    void collectColumns(final Columns columns) {
      operand.collectColumns(columns);
      subquery.collectColumns(columns);
    }

    @Override
    Object evaluate(final Resolver resolver) {
      final Object value = operand.evaluate(resolver);
      if (value == UNKNOWN)
        return UNKNOWN;

      final Object candidates = subquery.evaluate(resolver);
      if (!(candidates instanceof List<?> list))
        return UNKNOWN;
      if (value == null)
        return list.isEmpty() ? negated : null;

      boolean sawNull = false;
      for (final Object candidate : list) {
        if (candidate == null)
          sawNull = true;
        else if (Boolean.TRUE.equals(Binary.compare("=", value, candidate)))
          return !negated;
      }
      // "1 NOT IN (2, NULL)" is NULL rather than TRUE: the NULL might have been the 1.
      return sawNull ? null : negated;
    }
  }

  /**
   * An aggregate: {@code count(*)}, {@code count(DISTINCT x)}, {@code min(x)}, ... Like a window function its
   * value is about other rows than the one being evaluated, so the resolver computes it over the group the
   * projection is evaluating (issue #10228).
   */
  static class AggregateCall extends PostgresCatalogExpression {
    final String                          name;
    /** The aggregated expression, null for {@code count(*)}. */
    final PostgresCatalogExpression       argument;
    /** What follows the aggregated expression: {@code string_agg}'s separator. */
    final List<PostgresCatalogExpression> extraArguments;
    final boolean                         distinct;

    AggregateCall(final String name, final PostgresCatalogExpression argument,
        final List<PostgresCatalogExpression> extraArguments, final boolean distinct) {
      this.name = name;
      this.argument = argument;
      this.extraArguments = extraArguments;
      this.distinct = distinct;
    }

    @Override
    void collectColumns(final Columns columns) {
      if (argument != null)
        argument.collectColumns(columns);
      for (final PostgresCatalogExpression extra : extraArguments)
        extra.collectColumns(columns);
    }

    @Override
    Object evaluate(final Resolver resolver) {
      return resolver.aggregate(this);
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
    void collectColumns(final Columns columns) {
      columns.column(qualifier, name);
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
    void collectColumns(final Columns columns) {
      for (final PostgresCatalogExpression argument : arguments)
        argument.collectColumns(columns);
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
    void collectColumns(final Columns columns) {
      for (final PostgresCatalogExpression expression : partitionBy)
        expression.collectColumns(columns);
      for (final PostgresCatalogExpression expression : orderBy)
        expression.collectColumns(columns);
    }

    @Override
    Object evaluate(final Resolver resolver) {
      return resolver.window(this);
    }
  }

  private static Object scalarFunction(final String name, final List<Object> values) {
    switch (name) {
    case "lower", "upper", "length", "abs", "trim", "btrim", "ltrim", "rtrim", "replace", "concat", "substring", "substr",
         "array_upper", "array_lower", "array_length", "cardinality" -> {
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

    if (name.startsWith("array_") || "cardinality".equals(name))
      return arrayFunction(name, values);

    if (values.isEmpty() || values.get(0) == null)
      return values.isEmpty() ? UNKNOWN : null;

    final String first = asString(values.get(0));
    // btrim(string, characters) takes the characters named off the ends instead of the spaces
    if (values.size() == 2 && ("trim".equals(name) || "btrim".equals(name) || "ltrim".equals(name) || "rtrim".equals(name))) {
      if (values.get(1) == null)
        return null;
      return strip(first, asString(values.get(1)), !"rtrim".equals(name), !"ltrim".equals(name));
    }
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
      case "substring", "substr" -> substring(first, values);
      default -> UNKNOWN;
    };
  }

  /**
   * {@code substring(text, from [, count])}: the characters at the positions {@code from} (counted from 1) up to {@code count} of
   * them, that lie inside the text. A start before the first character takes its length from the count, as PostgreSQL does, so
   * {@code substring('abc', 0, 2)} is {@code 'a'}. A NULL position or count is NULL; a negative count is an error in PostgreSQL and
   * is not read here.
   */
  private static Object substring(final String text, final List<Object> values) {
    if (values.size() < 2 || values.size() > 3)
      return UNKNOWN;
    if (values.get(1) == null || values.size() == 3 && values.get(2) == null)
      return null;
    if (!(values.get(1) instanceof Number start) || values.size() == 3 && !(values.get(2) instanceof Number))
      return UNKNOWN;

    final long from = start.longValue();
    long to = Long.MAX_VALUE;
    if (values.size() == 3) {
      final long count = ((Number) values.get(2)).longValue();
      if (count < 0)
        return UNKNOWN;
      // a window that would pass the largest position is the rest of the text
      to = from > Long.MAX_VALUE - count ? Long.MAX_VALUE : from + count;
    }
    final long first = Math.max(from, 1L);
    final long end = Math.min(to, text.length() + 1L);
    return end <= first ? "" : text.substring((int) (first - 1), (int) (end - 1));
  }

  /**
   * {@code array_upper}, {@code array_lower}, {@code array_length} and {@code cardinality}: the bounds of an array, which is a
   * one-dimensional list here. An empty array has no bounds, so they are NULL for it (not {@code cardinality}, which is 0), and so
   * they are for a dimension other than the first. The key arrays of an index ({@link PostgresCatalogIndexes.Vector}) start at 0.
   */
  private static Object arrayFunction(final String name, final List<Object> values) {
    final boolean cardinality = "cardinality".equals(name);
    if (values.size() != (cardinality ? 1 : 2))
      return UNKNOWN;
    if (values.get(0) == null || !cardinality && values.get(1) == null)
      return null;
    if (!(values.get(0) instanceof List<?> list) || !cardinality && !(values.get(1) instanceof Number))
      return UNKNOWN;

    if (cardinality)
      return (long) list.size();
    if (((Number) values.get(1)).longValue() != 1L || list.isEmpty())
      return null;

    final long lower = list instanceof PostgresCatalogIndexes.Vector ? 0L : 1L;
    return switch (name) {
      case "array_lower" -> lower;
      case "array_upper" -> lower + list.size() - 1;
      case "array_length" -> (long) list.size();
      default -> UNKNOWN;
    };
  }

  /**
   * {@code (expression).field}: one member of a composite value, such as the {@code x} and {@code n} of what
   * {@code information_schema._pg_expandarray()} produces. A value that is no composite, or has no such member, is not read.
   */
  private static class Field extends PostgresCatalogExpression {
    private final PostgresCatalogExpression operand;
    private final String                    name;

    Field(final PostgresCatalogExpression operand, final String name) {
      this.operand = operand;
      this.name = name;
    }

    @Override
    void collectColumns(final Columns columns) {
      operand.collectColumns(columns);
    }

    @Override
    Object evaluate(final Resolver resolver) {
      final Object value = operand.evaluate(resolver);
      if (value == UNKNOWN)
        return UNKNOWN;
      if (value == null)
        return null;
      return value instanceof Map<?, ?> composite && composite.containsKey(name) ? composite.get(name) : UNKNOWN;
    }
  }

  /**
   * {@code information_schema._pg_expandarray(array)}: a set-returning function, one row for each element of the array, each the
   * pair {@code (x, n)} of the element and its position from 1 (the JDBC driver lists the key columns of an index and of a
   * primary key with it). A projection holding it is evaluated once for each element, the calls of one row in step with each
   * other; a NULL array has no element, and so no row. See {@link Resolver#expansion}.
   */
  static final class ExpandArray extends PostgresCatalogExpression {
    private final PostgresCatalogExpression array;

    ExpandArray(final PostgresCatalogExpression array) {
      this.array = array;
    }

    @Override
    void collectColumns(final Columns columns) {
      array.collectColumns(columns);
    }

    /**
     * How many rows this call produces for the row {@code resolver} is on.
     *
     * @return -1 when the array cannot be read
     */
    int rows(final Resolver resolver) {
      final Object value = array.evaluate(resolver);
      if (value == null)
        return 0;
      return value instanceof List<?> list ? list.size() : -1;
    }

    @Override
    Object evaluate(final Resolver resolver) {
      final int position = resolver.expansion();
      if (position < 0)
        return UNKNOWN;
      final Object value = array.evaluate(resolver);
      if (value == UNKNOWN)
        return UNKNOWN;
      if (value == null)
        return null;
      if (!(value instanceof List<?> list))
        return UNKNOWN;
      // a call with fewer elements than another of the same row reads NULL for the rows it has none for
      if (position >= list.size())
        return null;
      final Map<String, Object> pair = new LinkedHashMap<>(2);
      pair.put("x", list.get(position));
      pair.put("n", position + 1);
      return pair;
    }
  }

  /** {@code text} without the characters of {@code characters} at its start and at its end, as the flags say. */
  private static String strip(final String text, final String characters, final boolean start, final boolean end) {
    int from = 0;
    int to = text.length();
    while (start && from < to && characters.indexOf(text.charAt(from)) >= 0)
      ++from;
    while (end && to > from && characters.indexOf(text.charAt(to - 1)) >= 0)
      --to;
    return text.substring(from, to);
  }

  private static class Not extends PostgresCatalogExpression {
    private final PostgresCatalogExpression operand;

    Not(final PostgresCatalogExpression operand) {
      this.operand = operand;
    }

    @Override
    void collectColumns(final Columns columns) {
      operand.collectColumns(columns);
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
    void collectColumns(final Columns columns) {
      operand.collectColumns(columns);
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
    /**
     * The last regular expression this node compiled, with the client's text it was translated from: one reference,
     * so it can never be read half-updated. An expression tree is built per statement and evaluated by the one
     * thread answering it, so nothing else ever reads it anyway.
     */
    private       CompiledRegex             compiled;
    /**
     * The character reads this node's regular expression has left. One match is bounded by
     * {@link BoundedCharSequence#MATCH_BUDGET}, and this bounds the sum over every row the node is evaluated
     * against, so a pattern that exhausts the per-match budget on each of many rows cannot add up to a long run. A
     * correlated sub-select is parsed again for each outer row, into fresh nodes; what bounds that case is the
     * statement's work budget, which every re-parse is charged to (pinned by
     * {@code Issue10228PsqlOdbcCatalogShapesTest.aBacktrackingRegularExpressionIsBounded}). Like {@link #compiled} it is
     * per-statement state: a tree is parsed for one statement, evaluated by the one thread answering it, and never
     * cached or reused, so a spent budget cannot carry into another statement.
     */
    private       long                      regexBudget = BoundedCharSequence.STATEMENT_BUDGET;

    Binary(final String operator, final PostgresCatalogExpression left, final PostgresCatalogExpression right) {
      this.operator = operator;
      this.left = left;
      this.right = right;
    }

    @Override
    void collectColumns(final Columns columns) {
      left.collectColumns(columns);
      right.collectColumns(columns);
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
        case "&" -> l == null || r == null ? null : l instanceof Number ln && r instanceof Number rn ?
            (Object) (ln.longValue() & rn.longValue()) : UNKNOWN;
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

    private Object regex(final String operator, final Object l, final Object r) {
      if (l == null || r == null)
        return null;
      final String value = asString(l);
      final String pattern = asString(r);
      try {
        // The pattern is a literal in every query a client writes and the predicate runs once per row, so it is
        // translated and compiled once per pattern text, not once per row (psql's \d sends one, issue #10228).
        CompiledRegex cached = compiled;
        if (cached == null || !cached.source().equals(pattern)) {
          cached = new CompiledRegex(pattern,
              Pattern.compile(javaRegex(pattern), operator.endsWith("*") ? Pattern.CASE_INSENSITIVE : 0));
          compiled = cached;
        }
        final Pattern regex = cached.regex();
        if (regexBudget <= 0)
          throw new BudgetExceeded();
        final BoundedCharSequence text = new BoundedCharSequence(value,
            (int) Math.min(BoundedCharSequence.MATCH_BUDGET, regexBudget));
        try {
          return operator.startsWith("!") != regex.matcher(text).find();
        } finally {
          regexBudget -= text.used();
        }
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

    /**
     * Compares two values the way a catalog predicate needs them compared. Mixed types fall back to comparing
     * their text, which is what lets a client that writes {@code typreceive != 0} against a column this
     * catalog answers with a function <i>name</i> still get the answer it expects: PostgreSQL's own column is
     * a {@code regproc}, an OID that formats as that name, and no name ever reads as {@code 0}. That is a
     * property of the text comparison rather than a modelled regproc - an ordering test on such a column
     * ({@code typreceive > 0}) would compare names against "0" and answer something the client did not mean.
     */
    private static Object compare(final String operator, final Object l, final Object r) {
      if (l == null || r == null)
        return null;

      final int comparison;
      if (l instanceof Number ln && r instanceof Number rn)
        comparison = Double.compare(ln.doubleValue(), rn.doubleValue());
      else if (l instanceof Boolean || r instanceof Boolean)
        comparison = Boolean.compare(truth(l), truth(r));
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

  /**
   * The text a client's regular expression is matched against, counting the characters the matcher reads. The
   * pattern is the client's own, and {@code java.util.regex} backtracks: a pattern like {@code (a+)+$} takes time
   * exponential in the length of the name it is matched against, once per row. The budget turns that into a
   * declined statement after a bounded amount of work instead of a connection thread spinning, and it counts steps rather
   * than time, so the answer does not depend on how loaded the machine is.
   */
  private static final class BoundedCharSequence implements CharSequence {
    /** Character reads one match may make: some thousand times what any catalog name needs. */
    static final int  MATCH_BUDGET     = 1_000_000;
    /** Character reads one regular-expression predicate may make over all the rows of a statement. */
    static final long STATEMENT_BUDGET = 5_000_000L;

    private final String text;
    private final int[]  budget;
    private final int    allowance;

    BoundedCharSequence(final String text, final int allowance) {
      this(text, new int[] { allowance }, allowance);
    }

    private BoundedCharSequence(final String text, final int[] budget, final int allowance) {
      this.text = text;
      this.budget = budget;
      this.allowance = allowance;
    }

    /** The character reads the match made, shared by every sub-sequence the matcher took. */
    int used() {
      return allowance - Math.max(budget[0], 0);
    }

    @Override
    public int length() {
      return text.length();
    }

    @Override
    public char charAt(final int index) {
      if (--budget[0] < 0)
        throw new BudgetExceeded();
      return text.charAt(index);
    }

    @Override
    public CharSequence subSequence(final int start, final int end) {
      return new BoundedCharSequence(text.substring(start, end), budget, allowance);
    }

    @Override
    public String toString() {
      return text;
    }
  }

  /**
   * Thrown when a statement has spent the work it is allowed: a client regular expression's character reads, or
   * the catalog's statement budget. It is not an UNKNOWN operand: the permissive filter keeps a row an unreadable
   * predicate is evaluated on, and a name filter that ran out of budget would then answer rows that do not match.
   * The catalog refuses the whole statement instead (see {@link PostgresCatalog.WorkLimitExceeded}). No stack trace: it is control flow, thrown on a hostile
   * query's hot path.
   */
  static final class BudgetExceeded extends RuntimeException {
    BudgetExceeded() {
      super(null, null, false, false);
    }
  }

  /**
   * A PostgreSQL regular expression as java.util.regex reads it. The two agree on everything a catalog client
   * writes - anchors, groups, alternation, quantifiers, ordinary bracket expressions - except POSIX character
   * classes: Java compiles {@code [[:alpha:]]} without complaint as a set of the characters {@code [:alph}, which
   * would answer a different question rather than fail. Those are translated - the twelve standard classes plus
   * {@code word} and {@code ascii}, and the {@code [[:<:]]} / {@code [[:>:]]} word boundaries - to Java's ASCII
   * classes, and any other one is refused as unreadable rather than misread: PostgreSQL
   * takes them from the locale, which for the ASCII names a catalog holds is the same. PostgreSQL's own word-boundary
   * escapes ({@code \m}, {@code \M}, {@code \y}) are not Java syntax and fail to compile, so a pattern using
   * them reads as UNKNOWN instead of being guessed at.
   */
  static String javaRegex(final String pattern) {
    // Any bracket expression needs the scan, not only one holding a class: a '[' or '&' inside one is a literal in
    // POSIX, and Java would read "[a&&b]" as an intersection and "[a[b]c]" as a nested class.
    if (pattern.indexOf('[') < 0)
      return pattern;

    // A class is only a class inside a bracket expression: a standalone "[:word:]" is itself a bracket expression,
    // the set of the characters ":word", which Java reads the same way. So the pattern is scanned for bracket
    // expressions, and only what is inside one is translated.
    final int length = pattern.length();
    final StringBuilder java = new StringBuilder(length + 16);
    int i = 0;
    while (i < length) {
      final char c = pattern.charAt(i);
      if (c == '\\' && i + 1 < length) {
        java.append(c).append(pattern.charAt(i + 1));
        i += 2;
      } else if (c != '[') {
        java.append(c);
        ++i;
      } else if (pattern.startsWith("[[:<:]]", i)) {
        // PostgreSQL's word-boundary brackets are whole bracket expressions of their own.
        java.append("\\b(?=\\w)");
        i += 7;
      } else if (pattern.startsWith("[[:>:]]", i)) {
        java.append("\\b(?<=\\w)");
        i += 7;
      } else
        i = translateBracket(pattern, i, java);
    }
    return java.toString();
  }

  /**
   * Copies the bracket expression starting at {@code open} into {@code java}, translating the POSIX classes in it.
   * A {@code [} or {@code &} inside it is a literal in POSIX and is escaped, because Java would read the first as a
   * nested class and {@code &&} as an intersection.
   *
   * @return the index after the closing {@code ]}
   */
  private static int translateBracket(final String pattern, final int open, final StringBuilder java) {
    final int length = pattern.length();
    int j = open + 1;
    java.append('[');
    if (j < length && pattern.charAt(j) == '^')
      java.append(pattern.charAt(j++));
    if (j < length && pattern.charAt(j) == ']') {
      // A ']' right after the opening is a literal in POSIX.
      java.append("\\]");
      ++j;
    }

    while (j < length && pattern.charAt(j) != ']') {
      final char d = pattern.charAt(j);
      if (d == '[' && j + 1 < length && pattern.charAt(j + 1) == ':') {
        final int end = pattern.indexOf(":]", j + 2);
        final String javaClass = end < 0 ? null : POSIX_CLASSES.get(pattern.substring(j + 2, end));
        if (javaClass == null)
          // A class this catalog does not translate: unreadable, rather than misread as a set of its letters.
          throw new PatternSyntaxException("POSIX character class this catalog does not translate", pattern, j);
        java.append(javaClass);
        j = end + 2;
      } else if (d == '\\' && j + 1 < length) {
        java.append(d).append(pattern.charAt(j + 1));
        j += 2;
      } else {
        if (d == '[' || d == '&')
          java.append('\\');
        java.append(d);
        ++j;
      }
    }

    if (j >= length)
      throw new PatternSyntaxException("unterminated bracket expression", pattern, open);
    java.append(']');
    return j + 1;
  }

  /** A compiled regular expression and the client's text it was translated from. */
  private record CompiledRegex(String source, Pattern regex) {
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
    void collectColumns(final Columns columns) {
      operand.collectColumns(columns);
      for (final PostgresCatalogExpression candidate : candidates)
        candidate.collectColumns(columns);
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

  /**
   * {@code value op ANY (array)} and {@code value op ALL (array)} ({@code SOME} is {@code ANY}): the comparison against every
   * element of an array, pgjdbc's {@code n.nspname = ANY(current_schemas(true))} being the one that matters here. As SQL says,
   * {@code ANY} is TRUE when one element satisfies it, else NULL when one comparison was NULL (a NULL operand or element), else
   * FALSE, and an empty array is FALSE whatever the operand; {@code ALL} is the same with TRUE and FALSE swapped, so an empty array
   * is TRUE. A NULL array is NULL.
   */
  private static class Quantified extends PostgresCatalogExpression {
    private final String                    operator;
    private final boolean                   all;
    private final PostgresCatalogExpression operand;
    private final PostgresCatalogExpression array;

    Quantified(final String operator, final boolean all, final PostgresCatalogExpression operand,
        final PostgresCatalogExpression array) {
      this.operator = operator;
      this.all = all;
      this.operand = operand;
      this.array = array;
    }

    @Override
    void collectColumns(final Columns columns) {
      operand.collectColumns(columns);
      array.collectColumns(columns);
    }

    @Override
    Object evaluate(final Resolver resolver) {
      final Object value = operand.evaluate(resolver);
      final Object elements = array.evaluate(resolver);
      if (value == UNKNOWN || elements == UNKNOWN)
        return UNKNOWN;
      if (elements == null)
        return null;
      if (!(elements instanceof List<?> list))
        return UNKNOWN;

      boolean sawNull = false;
      for (final Object element : list) {
        final Object result = Binary.compare(operator, value, element);
        if (result == UNKNOWN)
          return UNKNOWN;
        if (result == null)
          sawNull = true;
        else if (isTrue(result) != all)
          // the element that decides it: a match for ANY, a miss for ALL
          return !all;
      }
      return sawNull ? null : all;
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
    void collectColumns(final Columns columns) {
      operand.collectColumns(columns);
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
    void collectColumns(final Columns columns) {
      if (operand != null)
        operand.collectColumns(columns);
      for (final PostgresCatalogExpression condition : conditions)
        condition.collectColumns(columns);
      for (final PostgresCatalogExpression result : results)
        result.collectColumns(columns);
      if (fallback != null)
        fallback.collectColumns(columns);
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
    void collectColumns(final Columns columns) {
      operand.collectColumns(columns);
      index.collectColumns(columns);
    }

    @Override
    Object evaluate(final Resolver resolver) {
      final Object value = operand.evaluate(resolver);
      final Object position = index.evaluate(resolver);
      if (value == UNKNOWN || position == UNKNOWN)
        return UNKNOWN;
      if (!(value instanceof List<?> list) || !(position instanceof Number n))
        return UNKNOWN;

      // PostgreSQL arrays are 1-based by default, which is the base a catalog query uses - but for int2vector and oidvector, the
      // arrays an index keeps its key columns in, which start at 0: psqlodbc subscripts indkey[attnum - 1]
      final int i = n.intValue() - (list instanceof PostgresCatalogIndexes.Vector ? 0 : 1);
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
    private       boolean                    aggregated;
    private final List<ExpandArray>          expansions = new ArrayList<>();

    Parser(final List<PostgresCatalogToken> tokens) {
      this.tokens = tokens;
    }

    /** The set-returning calls the expression read, which multiply the row they are projected on. */
    List<ExpandArray> expansions() {
      return expansions;
    }

    boolean atEnd() {
      return position >= tokens.size();
    }

    int getPosition() {
      return position;
    }

    PostgresCatalogToken peek() {
      return peek(0);
    }

    PostgresCatalogToken peek(final int ahead) {
      return position + ahead < tokens.size() ? tokens.get(position + ahead) : null;
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
            if (peek() != null && peek().isSymbol("(") && peek(1) != null && peek(1).isKeyword("SELECT")) {
              final List<PostgresCatalogToken> inner = readParenthesised();
              if (inner == null)
                return null;
              left = new InSubquery(left, new Subquery(inner, SubqueryKind.ARRAY), negated);
              continue;
            }
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
            final PostgresCatalogToken quantifier = peek();
            if (quantifier != null && (quantifier.isKeyword("ANY") || quantifier.isKeyword("SOME") || quantifier.isKeyword("ALL"))
                && peek(1) != null && peek(1).isSymbol("(")) {
              left = parseQuantified(next, quantifier.isKeyword("ALL"), left);
              if (left == null)
                return null;
              continue;
            }
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

    /**
     * {@code ANY (array)}, {@code SOME (array)} or {@code ALL (array)} after the comparison operator {@code operator}, the position
     * on the quantifier. The array is an expression or a sub-select, whose one column is the elements. Only the operators the
     * comparison evaluates are read: a regular-expression operator is not compared element by element here.
     */
    private PostgresCatalogExpression parseQuantified(final PostgresCatalogToken operator, final boolean all,
        final PostgresCatalogExpression operand) {
      if (operator.isSymbol("~") || operator.isSymbol("!~") || operator.isSymbol("~*") || operator.isSymbol("!~*"))
        return null;

      ++position;
      final PostgresCatalogExpression array;
      if (peek(1) != null && peek(1).isKeyword("SELECT")) {
        final List<PostgresCatalogToken> inner = readParenthesised();
        if (inner == null)
          return null;
        array = new Subquery(inner, SubqueryKind.ARRAY);
      } else {
        ++position;
        array = parseExpression();
        if (array == null || !skipSymbol(")"))
          return null;
      }
      return new Quantified(operator.text, all, operand, array);
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

      // "&" is PostgreSQL's bitwise AND, which binds with the other operators that are no arithmetic (the JDBC driver reads the
      // descending flag of an index key with it)
      while (peek() != null && (peek().isSymbol("||") || peek().isSymbol("&"))) {
        final String operator = peek().text;
        ++position;
        final PostgresCatalogExpression right = parseAdditive();
        if (right == null)
          return null;
        left = new Binary(operator, left, right);
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
          ++position;
          final String castType = skipCastType();
          if (castType == null)
            return null;
          // Most casts are there for PostgreSQL's own type resolution and do not change what a catalog query
          // means, so they stay transparent. The OID-alias types are the exception: they are a catalog
          // lookup written as a cast, and reading them as transparent gives the wrong answer rather than an
          // unknown one - 'Article'::regclass compared against pg_class.oid would compare a name to a number
          // and quietly select nothing (issue #7180). Those are handed to the resolver as the function call
          // they are, which is also how PostgreSQL spells them when written out: to_regclass('Article').
          if (LOOKUP_CASTS.contains(castType))
            operand = new FunctionCall(castType, List.of(operand));
        } else if (token != null && token.isSymbol("[")) {
          ++position;
          final PostgresCatalogExpression index = parseExpression();
          if (index == null || !skipSymbol("]"))
            return null;
          operand = new Subscript(operand, index);
        } else if (token != null && token.isSymbol(".") && peek(1) != null
            && (peek(1).type == PostgresCatalogToken.Type.IDENTIFIER || peek(1).type == PostgresCatalogToken.Type.QUOTED_IDENTIFIER)) {
          // (expression).member: a bare column reference has taken its own dots already, so this follows a parenthesis
          operand = new Field(operand, peek(1).text.toLowerCase(Locale.ENGLISH));
          position += 2;
        } else
          return operand;
      }
    }

    /**
     * Consumes a type name after {@code ::}, including {@code varchar(10)} and {@code text[]}.
     *
     * @return the bare type name in lower case, {@link #COMPOUND_CAST_TYPE} when it carried a length
     * modifier, an array suffix or more than one word - none of which an OID-alias type ever does - or null
     * when what follows the {@code ::} is not a type name at all
     */
    private String skipCastType() {
      final PostgresCatalogToken token = peek();
      if (token == null || token.type != PostgresCatalogToken.Type.IDENTIFIER)
        return null;
      ++position;

      String name = token.text.toLowerCase(Locale.ENGLISH);

      while (peek() != null && peek().isSymbol(".")) {
        ++position;
        if (peek() == null || peek().type != PostgresCatalogToken.Type.IDENTIFIER)
          return null;
        // pg_catalog.regclass is regclass: the schema a type name is qualified by never changes which it is.
        name = peek().text.toLowerCase(Locale.ENGLISH);
        ++position;
      }

      // Some type names are several words: "double precision", "character varying", "timestamp with time zone".
      while (peek() != null && peek().type == PostgresCatalogToken.Type.IDENTIFIER && isTypeNameWord(peek().text)) {
        ++position;
        name = COMPOUND_CAST_TYPE;
      }

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
        name = COMPOUND_CAST_TYPE;
      }

      while (peek() != null && peek().isSymbol("[")) {
        ++position;
        if (!skipSymbol("]"))
          return null;
        name = COMPOUND_CAST_TYPE;
      }

      return name;
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
        final PostgresCatalogToken next = peek(1);
        if (next != null && next.isKeyword("SELECT"))
          return parseScalarSubquery();

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
        if ((token.isKeyword("EXISTS") || token.isKeyword("ARRAY")) && peek(1) != null && peek(1).isSymbol("(")
            && peek(2) != null && peek(2).isKeyword("SELECT")) {
          ++position;
          final List<PostgresCatalogToken> inner = readParenthesised();
          return inner == null ? null :
              new Subquery(inner, token.isKeyword("EXISTS") ? SubqueryKind.EXISTS : SubqueryKind.ARRAY);
        }

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
        if (currentQualifier == null && AGGREGATES.contains(name.toLowerCase(Locale.ENGLISH)))
          return parseAggregate(name.toLowerCase(Locale.ENGLISH));
        if (currentQualifier == null && "trim".equalsIgnoreCase(name))
          return parseTrim();

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

        if ("_pg_expandarray".equals(functionName) && arguments.size() == 1) {
          final ExpandArray expansion = new ExpandArray(arguments.get(0));
          expansions.add(expansion);
          return expansion;
        }
        return new FunctionCall(functionName, arguments);
      }

      return new ColumnReference(currentQualifier == null ? null : currentQualifier.toLowerCase(Locale.ENGLISH),
          name.toLowerCase(Locale.ENGLISH));
    }

    /**
     * Reads {@code trim([BOTH | LEADING | TRAILING] [characters] FROM string)}, SQL's own spelling, and the plain
     * {@code trim(string)}: as the functions PostgreSQL turns them into, {@code btrim}, {@code ltrim} and {@code rtrim} with the
     * characters as a second argument (the JDBC driver strips the quotes off an index column with it).
     */
    private PostgresCatalogExpression parseTrim() {
      ++position; // (
      String function = "btrim";
      boolean sided = false;
      if (skipKeyword("BOTH"))
        sided = true;
      else if (skipKeyword("LEADING")) {
        function = "ltrim";
        sided = true;
      } else if (skipKeyword("TRAILING")) {
        function = "rtrim";
        sided = true;
      }

      PostgresCatalogExpression characters = null;
      PostgresCatalogExpression string = null;
      if (!skipKeyword("FROM")) {
        string = parseExpression();
        if (string == null)
          return null;
        if (skipKeyword("FROM")) {
          characters = string;
          string = null;
        } else if (sided)
          // a side with nothing to take it off
          return null;
      }
      if (string == null) {
        string = parseExpression();
        if (string == null)
          return null;
      }
      if (!skipSymbol(")"))
        return null;

      return new FunctionCall(function, characters == null ? List.of(string) : List.of(string, characters));
    }

    /**
     * Reads an aggregate call: {@code count(*)}, {@code count(DISTINCT x)}, {@code string_agg(x, ', ')}. An
     * ORDER BY or FILTER clause changes what the aggregate answers, so a call carrying one is not read; followed
     * by OVER it is a window function instead, which {@link #parseWindow} reads.
     */
    private PostgresCatalogExpression parseAggregate(final String functionName) {
      ++position; // (
      final boolean distinct = skipKeyword("DISTINCT");
      if (!distinct)
        skipKeyword("ALL");

      PostgresCatalogExpression argument = null;
      final List<PostgresCatalogExpression> extraArguments = new ArrayList<>();
      if ("count".equals(functionName) && !distinct && peek() != null && peek().isSymbol("*") && peek(1) != null
          && peek(1).isSymbol(")"))
        position += 2;
      else {
        argument = parseExpression();
        if (argument == null)
          return null;
        while (skipSymbol(",")) {
          final PostgresCatalogExpression extra = parseExpression();
          if (extra == null)
            return null;
          extraArguments.add(extra);
        }
        if (!skipSymbol(")"))
          return null;
        if (extraArguments.size() != ("string_agg".equals(functionName) ? 1 : 0))
          return null;
      }

      if (peek() != null && peek().isKeyword("OVER"))
        return parseWindow(functionName);
      if (peek() != null && peek().isKeyword("FILTER"))
        return null;

      aggregated = true;
      return new AggregateCall(functionName, argument, extraArguments, distinct);
    }

    /** Whether the expressions this parser has read so far contain an aggregate call. */
    boolean isAggregated() {
      return aggregated;
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

    /**
     * Reads a parenthesised sub-select as an operand the resolver evaluates. The tokens are collected rather
     * than parsed: what they mean is the catalog's to decide when it evaluates them. The scan is iterative, so
     * however deeply the sub-select nests it costs no stack here, and an unbalanced tail declines the query.
     */
    private PostgresCatalogExpression parseScalarSubquery() {
      final List<PostgresCatalogToken> inner = readParenthesised();
      return inner == null ? null : new Subquery(inner, SubqueryKind.SCALAR);
    }

    /**
     * Consumes a parenthesised token range starting at the current {@code (} and returns what is inside it, or
     * null when the parentheses do not balance.
     */
    private List<PostgresCatalogToken> readParenthesised() {
      final int start = position + 1;
      int open = 0;
      while (position < tokens.size()) {
        final PostgresCatalogToken token = tokens.get(position++);
        if (token.isSymbol("("))
          ++open;
        else if (token.isSymbol(")") && --open == 0)
          return new ArrayList<>(tokens.subList(start, position - 1));
      }
      return null;
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
