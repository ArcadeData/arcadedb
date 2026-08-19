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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the expression slice a catalog query is written in (issue #6412).
 * <p>
 * The three values that flow through it are what these tests are about: a real value, SQL NULL, and
 * {@link PostgresCatalogExpression#UNKNOWN} - "this evaluator cannot say". Keeping the third distinct from the
 * second is what lets a projection decline a query it cannot answer honestly while a WHERE clause reads the
 * same result as "does not exclude this row".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresCatalogExpressionTest {

  /** A row of made-up catalog columns, plus the session functions the evaluator delegates. */
  private static class TestResolver implements PostgresCatalogExpression.Resolver {
    private final Map<String, Object> columns = new HashMap<>();

    TestResolver() {
      columns.put("relname", "Article");
      columns.put("relkind", "r");
      columns.put("nspname", "mydb");
      columns.put("attnum", 3L);
      columns.put("attnotnull", Boolean.TRUE);
      columns.put("description", null);
    }

    @Override
    public Object column(final String qualifier, final String name) {
      return columns.containsKey(name) ? columns.get(name) : PostgresCatalogExpression.UNKNOWN;
    }

    @Override
    public Object function(final String name, final List<Object> arguments) {
      return switch (name) {
        case "current_schema", "current_database" -> "mydb";
        case "current_schemas" -> List.of("mydb");
        default -> PostgresCatalogExpression.UNKNOWN;
      };
    }
  }

  private static Object evaluate(final String expression) {
    final PostgresCatalogExpression parsed = PostgresCatalogExpression.parse(PostgresCatalogToken.tokenize(expression));
    assertThat(parsed).as("expression did not parse: %s", expression).isNotNull();
    return parsed.evaluate(new TestResolver());
  }

  private static boolean parses(final String expression) {
    return PostgresCatalogExpression.parse(PostgresCatalogToken.tokenize(expression)) != null;
  }

  // ---------------------------------------------------------------- values

  @Test
  void literalsAndColumnsEvaluateToThemselves() {
    assertThat(evaluate("'abc'")).isEqualTo("abc");
    assertThat(evaluate("42")).isEqualTo(42L);
    assertThat(evaluate("1.5")).isEqualTo(1.5d);
    assertThat(evaluate("NULL")).isNull();
    assertThat(evaluate("TRUE")).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("FALSE")).isEqualTo(Boolean.FALSE);
    assertThat(evaluate("relname")).isEqualTo("Article");
    assertThat(evaluate("c.relname")).isEqualTo("Article");
  }

  @Test
  void aColumnNoRelationHasIsUnknownRatherThanNull() {
    assertThat(evaluate("nosuchcolumn")).isSameAs(PostgresCatalogExpression.UNKNOWN);
    assertThat(evaluate("description")).isNull();
  }

  // ---------------------------------------------------------------- comparison

  @Test
  void comparisonsWorkOnStringsNumbersAndBooleans() {
    assertThat(evaluate("relname = 'Article'")).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("relname <> 'Author'")).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("relname != 'Article'")).isEqualTo(Boolean.FALSE);
    assertThat(evaluate("attnum > 2")).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("attnum >= 3")).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("attnum < 3")).isEqualTo(Boolean.FALSE);
    assertThat(evaluate("attnum <= 3")).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("attnotnull = TRUE")).isEqualTo(Boolean.TRUE);
  }

  @Test
  void comparingWithNullIsNullAndComparingWithUnknownIsUnknown() {
    assertThat(evaluate("description = 'anything'")).isNull();
    assertThat(evaluate("nosuchcolumn = 'anything'")).isSameAs(PostgresCatalogExpression.UNKNOWN);
  }

  @Test
  void isNullAndIsNotNull() {
    assertThat(evaluate("description IS NULL")).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("description IS NOT NULL")).isEqualTo(Boolean.FALSE);
    assertThat(evaluate("relname IS NOT NULL")).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("nosuchcolumn IS NULL")).isSameAs(PostgresCatalogExpression.UNKNOWN);
    // IS TRUE and IS DISTINCT FROM are outside the slice, and are declined rather than approximated.
    assertThat(parses("attnotnull IS TRUE")).isFalse();
  }

  // ---------------------------------------------------------------- three-valued logic

  @Test
  void andShortCircuitsOnFalseEvenWhenTheOtherSideIsUnreadable() {
    // This is what keeps a readable filter working next to one this evaluator cannot parse.
    assertThat(evaluate("relname = 'Author' AND has_table_privilege(oid, 'SELECT')")).isEqualTo(Boolean.FALSE);
    assertThat(evaluate("has_table_privilege(oid, 'SELECT') AND relname = 'Author'")).isEqualTo(Boolean.FALSE);
    assertThat(evaluate("has_table_privilege(oid, 'SELECT') AND relname = 'Article'")).isSameAs(
        PostgresCatalogExpression.UNKNOWN);
    assertThat(evaluate("relname = 'Article' AND attnum = 3")).isEqualTo(Boolean.TRUE);
  }

  @Test
  void orShortCircuitsOnTrueEvenWhenTheOtherSideIsUnreadable() {
    assertThat(evaluate("relname = 'Article' OR replace(nspname, 'a', 'b') = 'x'")).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("nosuchcolumn = 1 OR relname = 'Article'")).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("nosuchcolumn = 1 OR relname = 'Author'")).isSameAs(PostgresCatalogExpression.UNKNOWN);
  }

  @Test
  void anUnreadablePredicateReadsAsKeepingTheRow() {
    assertThat(PostgresCatalogExpression.isTrue(PostgresCatalogExpression.UNKNOWN)).isTrue();
    assertThat(PostgresCatalogExpression.isTrue(null)).isFalse();
    assertThat(PostgresCatalogExpression.isTrue(Boolean.FALSE)).isFalse();
    assertThat(PostgresCatalogExpression.isTrue(1L)).isTrue();
    assertThat(PostgresCatalogExpression.isTrue("text")).isFalse();
  }

  @Test
  void not() {
    assertThat(evaluate("NOT relname = 'Author'")).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("NOT attnotnull")).isEqualTo(Boolean.FALSE);
    assertThat(evaluate("NOT description IS NULL")).isEqualTo(Boolean.FALSE);
    assertThat(evaluate("NOT nosuchcolumn")).isSameAs(PostgresCatalogExpression.UNKNOWN);
  }

  // ---------------------------------------------------------------- pattern matching

  @Test
  void inAndNotIn() {
    assertThat(evaluate("relkind IN ('r','p','v')")).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("relkind IN ('v','m')")).isEqualTo(Boolean.FALSE);
    assertThat(evaluate("relkind NOT IN ('v','m')")).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("description IN ('a')")).isNull();
  }

  @Test
  void likeAndItsVariants() {
    assertThat(evaluate("relname LIKE 'Art%'")).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("relname LIKE 'Art'")).isEqualTo(Boolean.FALSE);
    assertThat(evaluate("relname LIKE 'Articl_'")).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("relname NOT LIKE 'Aut%'")).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("relname ILIKE 'article'")).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("relname NOT ILIKE 'article'")).isEqualTo(Boolean.FALSE);
    // A percent or underscore escaped with a backslash stands for itself.
    assertThat(evaluate("relname LIKE 'Article\\%'")).isEqualTo(Boolean.FALSE);
  }

  @Test
  void posixRegularExpressions() {
    assertThat(evaluate("nspname ~ '^my'")).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("nspname !~ '^pg_'")).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("nspname ~* '^MY'")).isEqualTo(Boolean.TRUE);
    assertThat(evaluate("nspname !~* '^MY'")).isEqualTo(Boolean.FALSE);
    // A pattern Java's regex engine will not compile is not something to guess the answer of.
    assertThat(evaluate("nspname ~ '['")).isSameAs(PostgresCatalogExpression.UNKNOWN);
  }

  // ---------------------------------------------------------------- arithmetic and text

  @Test
  void arithmetic() {
    assertThat(evaluate("2 + 3")).isEqualTo(5L);
    assertThat(evaluate("2 - 3")).isEqualTo(-1L);
    assertThat(evaluate("2 * 3")).isEqualTo(6L);
    assertThat(evaluate("7 / 2")).isEqualTo(3.5d);
    assertThat(evaluate("-1")).isEqualTo(-1L);
    assertThat(evaluate("+1")).isEqualTo(1L);
    // Division by zero is not an answer this evaluator invents.
    assertThat(evaluate("1 / 0")).isNull();
    assertThat(evaluate("1 + NULL")).isNull();
    assertThat(evaluate("1 + relname")).isSameAs(PostgresCatalogExpression.UNKNOWN);
  }

  @Test
  void concatenation() {
    assertThat(evaluate("nspname || '.' || relname")).isEqualTo("mydb.Article");
    assertThat(evaluate("nspname || description")).isNull();
  }

  @Test
  void scalarFunctions() {
    assertThat(evaluate("lower(relname)")).isEqualTo("article");
    assertThat(evaluate("upper(relname)")).isEqualTo("ARTICLE");
    assertThat(evaluate("length(relname)")).isEqualTo(7L);
    assertThat(evaluate("trim('  x  ')")).isEqualTo("x");
    assertThat(evaluate("ltrim('  x')")).isEqualTo("x");
    assertThat(evaluate("rtrim('x  ')")).isEqualTo("x");
    assertThat(evaluate("replace(relname, 'Art', 'Ent')")).isEqualTo("Enticle");
    assertThat(evaluate("concat(nspname, '/', relname)")).isEqualTo("mydb/Article");
    assertThat(evaluate("abs(-3)")).isEqualTo(3.0d);
    assertThat(evaluate("lower(description)")).isNull();
    assertThat(evaluate("lower(nosuchcolumn)")).isSameAs(PostgresCatalogExpression.UNKNOWN);
  }

  @Test
  void coalesceAndNullif() {
    assertThat(evaluate("coalesce(description, relname)")).isEqualTo("Article");
    assertThat(evaluate("coalesce(description, NULL)")).isNull();
    assertThat(evaluate("nullif(relkind, 'r')")).isNull();
    assertThat(evaluate("nullif(relkind, 'v')")).isEqualTo("r");
    assertThat(evaluate("nullif(relkind)")).isSameAs(PostgresCatalogExpression.UNKNOWN);
  }

  @Test
  void sessionFunctionsAreDelegatedToTheResolver() {
    assertThat(evaluate("current_schema()")).isEqualTo("mydb");
    assertThat(evaluate("pg_catalog.current_database()")).isEqualTo("mydb");
    assertThat(evaluate("nosuchfunction(1)")).isSameAs(PostgresCatalogExpression.UNKNOWN);
  }

  // ---------------------------------------------------------------- structure

  @Test
  void caseWithAnOperandIsWhatTheJdbcDriverWritesItsTableTypeWith() {
    assertThat(evaluate("CASE relkind WHEN 'r' THEN 'TABLE' WHEN 'v' THEN 'VIEW' ELSE NULL END")).isEqualTo("TABLE");
    assertThat(evaluate("CASE relkind WHEN 'v' THEN 'VIEW' END")).isNull();
    assertThat(evaluate("CASE nosuchcolumn WHEN 'v' THEN 'VIEW' ELSE 'X' END")).isSameAs(
        PostgresCatalogExpression.UNKNOWN);
  }

  @Test
  void caseWithSearchConditions() {
    assertThat(evaluate("CASE WHEN lower(nspname) = 'pg_catalog' THEN 'Y' ELSE 'N' END")).isEqualTo("N");
    assertThat(evaluate("CASE WHEN attnum > 2 THEN 'big' WHEN attnum > 0 THEN 'small' END")).isEqualTo("big");
    assertThat(evaluate("CASE WHEN description IS NULL THEN 'none' END")).isEqualTo("none");
  }

  @Test
  void nestedCaseOverABooleanOperand() {
    // The exact shape of the JDBC driver's table list: a CASE whose operand is itself a boolean expression.
    assertThat(evaluate("CASE nspname ~ '^pg_' OR nspname = 'information_schema' "
        + "WHEN true THEN CASE relkind WHEN 'r' THEN 'SYSTEM TABLE' ELSE NULL END "
        + "WHEN false THEN CASE relkind WHEN 'r' THEN 'TABLE' ELSE NULL END ELSE NULL END")).isEqualTo("TABLE");
  }

  @Test
  void castsAreTransparentAndSubscriptsIndexFromOne() {
    assertThat(evaluate("'pg_class'::regclass")).isEqualTo("pg_class");
    assertThat(evaluate("attnum::int4")).isEqualTo(3L);
    assertThat(evaluate("relname::character varying(10)")).isEqualTo("Article");
    assertThat(evaluate("relname::text[]")).isEqualTo("Article");
    assertThat(evaluate("(current_schemas(true))[1]")).isEqualTo("mydb");
    assertThat(evaluate("(current_schemas(true))[9]")).isNull();
    assertThat(evaluate("relname[1]")).isSameAs(PostgresCatalogExpression.UNKNOWN);
  }

  @Test
  void parenthesesGroup() {
    assertThat(evaluate("(1 + 2) * 3")).isEqualTo(9L);
    assertThat(evaluate("(relname = 'Author' OR attnum = 3) AND relkind = 'r'")).isEqualTo(Boolean.TRUE);
  }

  @Test
  void whatIsOutsideTheSliceDoesNotParse() {
    assertThat(parses("SELECT 1")).isFalse();
    assertThat(parses("relname =")).isFalse();
    assertThat(parses("CASE relkind END")).isFalse();
    assertThat(parses("CASE WHEN relkind = 'r' THEN 'TABLE'")).isFalse();
    assertThat(parses("relname IN ('a'")).isFalse();
    assertThat(parses("relname LIKE")).isFalse();
    assertThat(parses("relname ~")).isFalse();
    assertThat(parses("(1 + 2")).isFalse();
    assertThat(parses("relname[1")).isFalse();
    assertThat(parses("relname::")).isFalse();
    assertThat(parses("$1")).isFalse();
    assertThat(parses("relname extra")).isFalse();
  }

  @Test
  void aWindowFunctionIsRecognisedAndItsValueComesFromTheCaller() {
    final PostgresCatalogExpression parsed = PostgresCatalogExpression.parse(
        PostgresCatalogToken.tokenize("row_number() OVER (PARTITION BY attrelid ORDER BY attnum ASC)"));

    assertThat(parsed).isInstanceOf(PostgresCatalogExpression.WindowCall.class);
    final PostgresCatalogExpression.WindowCall call = (PostgresCatalogExpression.WindowCall) parsed;
    assertThat(call.name).isEqualTo("row_number");
    assertThat(call.partitionBy).hasSize(1);
    assertThat(call.orderBy).hasSize(1);
    assertThat(call.orderByDescending).containsExactly(Boolean.FALSE);
    // Its value is defined by the other rows, so a resolver that was not given one answers UNKNOWN.
    assertThat(parsed.evaluate(new TestResolver())).isSameAs(PostgresCatalogExpression.UNKNOWN);

    // A frame specification changes what the function means, so it is declined rather than ignored.
    assertThat(parses("row_number() OVER (ORDER BY attnum ROWS UNBOUNDED PRECEDING)")).isFalse();
    assertThat(parses("row_number() OVER (PARTITION attrelid)")).isFalse();
  }

  @Test
  void aWindowOrderByCarriesTheDirectionTheClientWrote() {
    // The direction decides the numbering, so consuming DESC and forgetting it would number the partition
    // backwards from what the client asked for - a wrong number rather than no answer.
    final PostgresCatalogExpression.WindowCall descending = window("row_number() OVER (ORDER BY attnum DESC)");
    assertThat(descending.orderByDescending).containsExactly(Boolean.TRUE);

    final PostgresCatalogExpression.WindowCall mixed = window(
        "row_number() OVER (PARTITION BY attrelid ORDER BY attnum DESC, attname ASC)");
    assertThat(mixed.orderByDescending).containsExactly(Boolean.TRUE, Boolean.FALSE);

    // NULLS FIRST/LAST would reorder the ties this numbering breaks by position, so it is declined.
    assertThat(parses("row_number() OVER (ORDER BY attnum DESC NULLS LAST)")).isFalse();
  }

  private static PostgresCatalogExpression.WindowCall window(final String text) {
    final PostgresCatalogExpression parsed = PostgresCatalogExpression.parse(PostgresCatalogToken.tokenize(text));
    assertThat(parsed).as("window call did not parse: %s", text).isInstanceOf(
        PostgresCatalogExpression.WindowCall.class);
    return (PostgresCatalogExpression.WindowCall) parsed;
  }

  @Test
  void aPathologicallyNestedExpressionIsDeclinedRatherThanFatal() {
    // Any authenticated client can send this, and it is well inside the wire protocol's message budget. An
    // unbounded parser would recurse until the stack gave out, and a StackOverflowError is an Error, not an
    // Exception: it would kill the connection thread instead of being answered with an empty result.
    final int depth = 50_000;
    final String pathological = "(".repeat(depth) + "1" + ")".repeat(depth);

    assertThat(parses(pathological)).isFalse();

    // The bound is far above anything a real catalog query nests: the JDBC driver's table list reaches four.
    assertThat(parses("(".repeat(20) + "relname" + ")".repeat(20))).isTrue();
  }

  @Test
  void aChainOfNotOrUnaryMinusIsBoundedTheSameWayNestedParenthesesAre() {
    // parseNot() and parseUnary() recurse on themselves directly for a chain of NOT or unary -/+, without
    // ever calling back through parseExpression() - so the same MAX_DEPTH guard has to be applied at their
    // own recursion point, not only at parseExpression()'s.
    assertThat(parses("NOT ".repeat(20) + "relname = 'Article'")).isTrue();
    assertThat(parses("NOT ".repeat(50_000) + "relname = 'Article'")).isFalse();

    // Space-separated: "--" is a line comment to the lexer, not two unary minuses.
    assertThat(parses("- ".repeat(20) + "1")).isTrue();
    assertThat(parses("- ".repeat(50_000) + "1")).isFalse();
    assertThat(parses("+ ".repeat(50_000) + "1")).isFalse();
  }

  @Test
  void aNumericLiteralThatIsNotANumberIsDeclinedRatherThanReadAsNull() {
    // The lexer reads "1.2.3" as one numeric token; answering the query with a NULL the client never wrote
    // would be worse than declining it.
    assertThat(parses("1.2.3")).isFalse();
    assertThat(parses("attnum = 1.2.3")).isFalse();
  }

  @Test
  void valuesRenderAsPostgresWouldWriteThem() {
    assertThat(PostgresCatalogExpression.asString(Boolean.TRUE)).isEqualTo("t");
    assertThat(PostgresCatalogExpression.asString(Boolean.FALSE)).isEqualTo("f");
    assertThat(PostgresCatalogExpression.asString(42L)).isEqualTo("42");
    assertThat(PostgresCatalogExpression.asString(null)).isNull();
    assertThat(PostgresCatalogExpression.asString(PostgresCatalogExpression.UNKNOWN)).isNull();
    assertThat(PostgresCatalogExpression.UNKNOWN.toString()).isEqualTo("UNKNOWN");
  }
}
