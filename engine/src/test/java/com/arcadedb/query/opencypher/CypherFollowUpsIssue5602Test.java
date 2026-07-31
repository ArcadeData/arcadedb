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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.parser.CypherExpressionWalker;
import com.arcadedb.query.opencypher.parser.FunctionValidator;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5602, the five follow-ups left by #5484.
 *
 * <ol>
 *   <li>The argument-count guard could not reach the functions that go through {@code SQLFunctionBridge} - covered by
 *       {@code CypherFunctionArityRegistryTest}, which now compares every registered name instead of none.</li>
 *   <li>Procedure names, variable names and type keywords were folded with the default locale, so a name containing
 *       {@code I} misresolved under a Turkish default exactly as {@code ISNAN} had.</li>
 *   <li>{@code charAt}, {@code charLength} and {@code isNormalized} were registered as known to the parser with no
 *       executor behind them, so a call parsed and then failed with "Unknown function".</li>
 *   <li>Parse-time argument validation walked {@code RETURN} and {@code WITH} only, so the same bad call was rejected
 *       before the query ran or not depending on which clause it sat in.</li>
 *   <li>An arithmetic error - integer overflow, division by zero - was reported as a plain
 *       {@code CommandExecutionException}, which HTTP turns into a 500 although the values are the caller's.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherFollowUpsIssue5602Test extends TestHelper {

  private static final String TYPE = "Issue5602";

  /** 'a' followed by U+0301 COMBINING ACUTE ACCENT: the NFD form of 'a with acute'. */
  private static final String NFD = "a\u0301";
  /** U+00E1 LATIN SMALL LETTER A WITH ACUTE, a single code point: the NFC form of the same character. */
  private static final String NFC = "\u00e1";

  @Override
  protected void beginTest() {
    database.transaction(() -> database.getSchema().createVertexType(TYPE));
  }

  // ===================== 2. locale-independent name resolution =====================

  @Test
  void aProcedureNameResolvesUnderATurkishDefaultLocale() {
    // "DB.RELATIONSHIPTYPES".toLowerCase() is "db.relatıonshıptypes" with dotless i's under a Turkish default,
    // matching neither the built-in switch nor the registry, so the call would be reported as an unknown procedure.
    withDefaultLocale(Locale.forLanguageTag("tr"), () -> {
      assertThatCode(() -> consume("CALL DB.RELATIONSHIPTYPES()")).doesNotThrowAnyException();
      assertThatCode(() -> consume("CALL DB.SCHEMA.VISUALIZATION()")).doesNotThrowAnyException();
      assertThatCode(() -> consume("CALL db.labels()")).doesNotThrowAnyException();
    });
  }

  @Test
  void aVariableNameWithAnIResolvesUnderATurkishDefaultLocale() {
    withDefaultLocale(Locale.forLanguageTag("tr"), () -> {
      assertThat(single("WITH 1 AS ID RETURN ID AS r")).isEqualTo(1L);
      assertThat(single("UNWIND [1, 2] AS Item WITH Item WHERE Item > 1 RETURN Item AS r")).isEqualTo(2L);
    });
  }

  @Test
  void aTypePredicateResolvesUnderATurkishDefaultLocale() {
    // The type name is upper-cased before being matched; under a Turkish default "integer" upper-cases to "İNTEGER".
    withDefaultLocale(Locale.forLanguageTag("tr"), () -> {
      assertThat(single("RETURN 1 IS :: INTEGER AS r")).isEqualTo(true);
      assertThat(single("RETURN 'x' IS :: INTEGER AS r")).isEqualTo(false);
    });
  }

  @Test
  void anExplainPrefixIsRecognisedUnderATurkishDefaultLocale() {
    // The engine upper-cases the whole query text to spot the EXPLAIN / PROFILE prefix; "explain" would become
    // "EXPLAİN" under a Turkish default and the prefix would go unrecognised.
    withDefaultLocale(Locale.forLanguageTag("tr"),
        () -> assertThatCode(() -> consume("explain RETURN 1 AS r")).doesNotThrowAnyException());
  }

  @Test
  void noOpenCypherSourceFoldsCaseWithTheDefaultLocale() {
    // The fix is only as good as its coverage: a new toLowerCase()/toUpperCase() with no explicit locale reintroduces
    // exactly the #5484 defect somewhere else, and nothing else in the suite would notice - the two forms behave
    // identically under every locale CI runs in.
    final List<String> scanned = new ArrayList<>();
    final List<String> offenders = defaultLocaleFoldingSites(scanned);
    assertThat(scanned).as("no sources were scanned, so this guard proves nothing - check the working directory")
        .isNotEmpty();
    assertThat(offenders)
        .as("fold identifiers and keywords with Locale.ROOT - the default locale makes 'I' a dotless 'ı' in Turkish")
        .isEmpty();
  }

  /**
   * Every AST expression type is either descended into by {@link CypherExpressionWalker} or listed below as a leaf.
   * <p>
   * The walker's {@code default} arm is the one place a regression can hide in silence: a composite type added later
   * without a case falls into it, and everything it nests escapes every check routed through the walker with nothing
   * failing to say so. That is not hypothetical - {@code PatternComprehensionExpression} sat in the default arm until
   * this issue, so a function call in its {@code WHERE} or projection was never validated. Turning the "add a case
   * here" comment into something the build enforces is the same move as the source scan above.
   */
  @Test
  void everyExpressionTypeIsEitherWalkedOrDeclaredALeaf() {
    final List<String> types = expressionTypeNames();
    assertThat(types).as("no AST expression types were found - check the source path").isNotEmpty();

    final String walker = readSource(Path.of(WALKER_SOURCE));
    final List<String> unhandled = new ArrayList<>();
    for (final String type : types)
      if (!LEAF_EXPRESSIONS.contains(type) && !walker.contains("case " + type + " "))
        unhandled.add(type);

    assertThat(unhandled)
        .as("add a case to CypherExpressionWalker, or list the type in LEAF_EXPRESSIONS if it nests no expression")
        .isEmpty();
    // A leaf that has since grown children, or was removed, leaves a stale entry that quietly excuses a real type.
    assertThat(types).as("LEAF_EXPRESSIONS names a type that no longer exists").containsAll(LEAF_EXPRESSIONS);
  }

  private static final String WALKER_SOURCE =
      "src/main/java/com/arcadedb/query/opencypher/parser/CypherExpressionWalker.java";

  /**
   * Expression types that genuinely nest nothing, so the walker's {@code default} arm is the right answer for them.
   * The last three hold their body as unparsed text: it is parsed as a statement of its own and validated then, and
   * there is no AST here to descend into.
   */
  private static final Set<String> LEAF_EXPRESSIONS = Set.of("LiteralExpression", "VariableExpression",
      "ParameterExpression", "PropertyAccessExpression", "StarExpression", "ExistsExpression", "CollectExpression",
      "CountExpression");

  /**
   * The names of every {@code Expression} / {@code BooleanExpression} implementation in the AST package, read from the
   * sources so a type added in a later commit is picked up without anyone remembering to register it here.
   */
  private static List<String> expressionTypeNames() {
    final Path astPackage = Path.of("src/main/java/com/arcadedb/query/opencypher/ast");
    if (!Files.isDirectory(astPackage))
      return List.of();

    final List<String> names = new ArrayList<>();
    try (final Stream<Path> files = Files.list(astPackage)) {
      files.filter(path -> path.toString().endsWith("Expression.java")).forEach(path -> {
        final String source = readSource(path);
        // Only concrete implementations: the two interfaces themselves declare nothing to walk.
        if (source.contains("implements Expression") || source.contains("implements BooleanExpression"))
          names.add(path.getFileName().toString().replace(".java", ""));
      });
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return names;
  }

  private static String readSource(final Path path) {
    try {
      return Files.readString(path);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // ===================== 3. registered-but-unimplemented functions =====================

  @Test
  void charLengthIsAnAliasOfCharacterLength() {
    assertThat(single("RETURN charLength('hello') AS r")).isEqualTo(5L);
    assertThat(single("RETURN char_length('hello') AS r")).isEqualTo(5L);
    assertThat(single("RETURN character_length('hello') AS r")).isEqualTo(5L);
  }

  @Test
  void isNormalizedAnswersWhetherAStringIsAlreadyInTheGivenForm() {
    assertThat(single("RETURN isNormalized('" + NFD + "') AS r")).isEqualTo(false);
    assertThat(single("RETURN isNormalized('" + NFD + "', 'NFD') AS r")).isEqualTo(true);
    assertThat(single("RETURN isNormalized('" + NFC + "') AS r")).isEqualTo(true);
    // The pair is consistent: whatever normalize() produces is normalized.
    assertThat(single("RETURN isNormalized(normalize('" + NFD + "')) AS r")).isEqualTo(true);
    // Cypher null semantics: a null input propagates rather than answering false.
    assertThat(single("RETURN isNormalized(null) AS r")).isNull();
  }

  @Test
  void isNormalizedRejectsAnUnknownNormalFormAsAClientError() {
    assertThatThrownBy(() -> consume("RETURN isNormalized('x', 'NFQ') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("NFQ")
        .hasMessageContaining("NFC, NFD, NFKC, NFKD");
  }

  @Test
  void isNormalizedRejectsANonStringAsAClientError() {
    assertThatThrownBy(() -> consume("RETURN isNormalized(42) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("isNormalized")
        .hasMessageContaining("STRING");
  }

  @Test
  void normalizeRejectsANonStringTheSameWayItsCounterpartDoes() {
    // The pair has to agree on its input domain, not only on the form names: normalize() used to toString() whatever
    // arrived, so normalize(123) answered '123' instead of raising the type error Neo4j raises.
    assertThatThrownBy(() -> consume("RETURN normalize(42) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("normalize")
        .hasMessageContaining("STRING");
    // Null still propagates, in both.
    assertThat(single("RETURN normalize(null) AS r")).isNull();
    assertThat(single("RETURN normalize('" + NFD + "') AS r")).isEqualTo(NFC);
  }

  @Test
  void aBridgedFunctionEnforcesItsOwnArityAtRuntime() {
    // distance() reaches a SQL function through SQLFunctionBridge, which now runs the same runtime guard every
    // other executor does, from the bounds the wrapped function declares (2-3).
    assertThatThrownBy(() -> consume("RETURN distance(point({latitude: 0, longitude: 0})) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("distance")
        .hasMessageContaining("2-3 arguments");
  }

  @Test
  void charAtIsRejectedUpFrontInsteadOfFailingAtExecution() {
    // It named no function in ArcadeDB and names none in Neo4j either, so the honest answer is the ordinary
    // unknown-function error at parse time rather than a late "Unknown function" from the executor lookup.
    assertThatThrownBy(() -> consume("RETURN charAt('abc', 1) AS r"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Unknown function")
        .hasMessageContaining("charAt");
  }

  @Test
  void everyNameTheParserAcceptsHasAnExecutor() {
    // The condition item 3 was about, asserted directly: the parser must never declare a name valid that execution
    // then rejects.
    final CypherFunctionFactory factory = new CypherFunctionFactory(DefaultSQLFunctionFactory.getInstance());
    for (final String name : FunctionValidator.getKnownFunctionNames()) {
      final StatelessFunction executor;
      try {
        executor = factory.getFunctionExecutor(name);
      } catch (final Exception e) {
        throw new AssertionError(name + " is accepted by the parser but has no executor", e);
      }
      assertThat(executor).as("%s", name).isNotNull();
    }
  }

  // ===================== 4. parse-time validation outside RETURN / WITH =====================

  @Test
  void aBadArgumentInWhereIsRejectedBeforeTheQueryRuns() {
    // The type is empty, so nothing would ever call abs(). Rejected all the same, as in a RETURN and as Neo4j does -
    // the clause an expression sits in has no bearing on whether the call is valid.
    assertThatThrownBy(() -> run("MATCH (n:" + TYPE + ") WHERE abs('x') > 0 RETURN n"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs")
        .hasMessageContaining("INTEGER");
  }

  @Test
  void aBadArgumentIsRejectedInEveryClauseThatCanHoldOne() {
    for (final String query : new String[] { //
        "MATCH (n:" + TYPE + ") WHERE abs('x') > 0 RETURN n", //
        "UNWIND range(abs('x'), 3) AS i RETURN i", //
        "MATCH (n:" + TYPE + ") SET n.v = abs('x')", //
        "CREATE (n:" + TYPE + " {v: abs('x')})", //
        "MERGE (n:" + TYPE + " {v: abs('x')})", //
        "MATCH (n:" + TYPE + ") FOREACH (x IN [1] | SET n.v = abs('x'))", //
        "MATCH (n:" + TYPE + " {v: abs('x')}) RETURN n", //
        "MATCH (n:" + TYPE + ") WITH n WHERE abs('x') > 0 RETURN n", //
        "MATCH (n:" + TYPE + ") WITH n LIMIT abs('x') RETURN n", //
        "MATCH (n:" + TYPE + ") RETURN CASE WHEN true THEN abs('x') ELSE 0 END AS r", //
        "MATCH (n:" + TYPE + ") RETURN {v: abs('x')} AS r" }) {
      assertThatThrownBy(() -> run(query)).as("%s", query)
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("abs");
    }
  }

  /**
   * Asserted through EXPLAIN, which parses and plans without executing, so a rejection here can only have come from
   * the parse-time walk. The plain form of these queries would pass either way: SKIP/LIMIT are evaluated even when
   * the match yields no row, so the function's own runtime guard catches them regardless - which is exactly why
   * asserting the parse-time guarantee needs a query that never runs.
   * <p>
   * The top-level SKIP/LIMIT hang off the statement rather than off a clause entry, so they are reached by a
   * different walk from the WITH ones, and were the last place where the same call was judged differently depending
   * on where it sat.
   */
  @Test
  void skipAndLimitAreValidatedAtParseTimeWhereverTheySit() {
    for (final String query : new String[] { //
        "EXPLAIN MATCH (n:" + TYPE + ") RETURN n LIMIT abs('x')", //
        "EXPLAIN MATCH (n:" + TYPE + ") RETURN n SKIP abs('x') LIMIT 1", //
        "EXPLAIN MATCH (n:" + TYPE + ") WITH n LIMIT abs('x') RETURN n", //
        "EXPLAIN MATCH (n:" + TYPE + ") WHERE abs('x') > 0 RETURN n" }) {
      assertThatThrownBy(() -> consume(query)).as("%s", query)
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("abs");
    }
  }

  @Test
  void theWrongArgumentCountIsAlsoRejectedOutsideReturn() {
    assertThatThrownBy(() -> run("MATCH (n:" + TYPE + ") WHERE substring('abc', 1, 2, 3) = 'x' RETURN n"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("substring")
        .hasMessageContaining("2-3 arguments");
  }

  @Test
  void anUnknownFunctionIsRejectedOutsideReturn() {
    assertThatThrownBy(() -> run("MATCH (n:" + TYPE + ") WHERE bogus5602(n) = 1 RETURN n"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("bogus5602");
  }

  @Test
  void theWiderWalkStillAcceptsValidCallsInEveryClause() {
    // The point of the widening is to reject earlier, not to reject more: a correct call must survive it wherever it
    // appears.
    database.transaction(() -> database.command("opencypher", "CREATE (:" + TYPE + " {name: 'a', v: abs(-3)})"));

    assertThat(single("MATCH (n:" + TYPE + ") WHERE abs(n.v) > 2 RETURN count(n) AS r")).isEqualTo(1L);
    assertThat(single("UNWIND range(1, abs(-3)) AS i RETURN count(i) AS r")).isEqualTo(3L);
    assertThat(single("MATCH (n:" + TYPE + " {name: toString('a')}) RETURN count(n) AS r")).isEqualTo(1L);
    assertThat(single("MATCH (n:" + TYPE + ") RETURN n.name AS r ORDER BY toUpper(n.name)")).isEqualTo("a");

    database.transaction(() -> database.command("opencypher", "MATCH (n:" + TYPE + ") SET n.v = abs(-9)"));
    assertThat(single("MATCH (n:" + TYPE + ") RETURN n.v AS r")).isEqualTo(9L);
  }

  // ===================== 5. arithmetic errors are the caller's, not the server's =====================

  @Test
  void integerOverflowIsAnArithmeticError() {
    for (final String query : new String[] { //
        "RETURN abs(-9223372036854775808) AS r", //
        "RETURN 9223372036854775807 + 1 AS r", //
        "RETURN -9223372036854775808 - 1 AS r", //
        "RETURN 9223372036854775807 * 2 AS r", //
        "RETURN -9223372036854775808 / -1 AS r" }) {
      assertThatThrownBy(() -> consume(query)).as("%s", query)
          .isInstanceOf(ArithmeticErrorException.class)
          .hasMessageContaining("overflow");
    }
  }

  @Test
  void divisionByZeroIsAnArithmeticError() {
    assertThatThrownBy(() -> consume("RETURN 1 / 0 AS r"))
        .isInstanceOf(ArithmeticErrorException.class)
        .hasMessage("/ by zero");
    assertThatThrownBy(() -> consume("RETURN 1 % 0 AS r"))
        .isInstanceOf(ArithmeticErrorException.class)
        .hasMessage("% by zero");
    assertThatThrownBy(() -> consume("RETURN duration('P1D') / 0 AS r"))
        .isInstanceOf(ArithmeticErrorException.class)
        .hasMessageContaining("zero");
  }

  @Test
  void anArithmeticErrorIsStillACommandExecutionException() {
    // #5164 and #5494 settled the class embedded code catches; the new type refines the classification for the wire
    // layers without breaking that.
    assertThatThrownBy(() -> consume("RETURN 1 / 0 AS r")).isInstanceOf(CommandExecutionException.class);
  }

  @Test
  void floatingPointOverflowStaysIeee754() {
    // Only integer arithmetic raises: a float overflow is Infinity in Cypher, as in Neo4j, and must not start failing
    // now that overflow has its own exception type.
    assertThat(single("RETURN 1.0e308 * 10 AS r")).isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(single("RETURN 1.0 / 0.0 AS r")).isEqualTo(Double.POSITIVE_INFINITY);
  }

  // ===================== helpers =====================

  private static void withDefaultLocale(final Locale locale, final Runnable body) {
    final Locale previous = Locale.getDefault();
    try {
      Locale.setDefault(locale);
      body.run();
    } finally {
      Locale.setDefault(previous);
    }
  }

  /**
   * The OpenCypher and shared-function sources that call {@code toLowerCase()} or {@code toUpperCase()} with no
   * explicit locale. Reads the sources rather than the compiled classes because the defect is textual: the two forms
   * behave identically under the JVM's usual default and differ only under a locale the suite never runs in.
   *
   * @param scanned collects the files actually read, so the caller can tell "nothing is wrong" from "nothing was read"
   */
  private static List<String> defaultLocaleFoldingSites(final List<String> scanned) {
    final List<String> offenders = new ArrayList<>();
    for (final String root : new String[] { "src/main/java/com/arcadedb/query/opencypher",
        "src/main/java/com/arcadedb/function" }) {
      final Path base = Path.of(root);
      if (!Files.isDirectory(base))
        continue;
      try (final Stream<Path> files = Files.walk(base)) {
        files.filter(path -> path.toString().endsWith(".java")).forEach(path -> {
          scanned.add(path.toString());
          for (final String line : readLines(path)) {
            final String code = line.strip();
            // Skip prose: the surviving mentions are all in Javadoc and comments explaining why the locale matters.
            if (code.startsWith("*") || code.startsWith("//") || code.startsWith("/*"))
              continue;
            if (code.contains(".toLowerCase()") || code.contains(".toUpperCase()"))
              offenders.add(path + ": " + code);
          }
        });
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return offenders;
  }

  private static List<String> readLines(final Path path) {
    try {
      return Files.readAllLines(path);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).as("%s returned no row", query).isTrue();
      return rs.next().getProperty("r");
    }
  }

  private void consume(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rs.next();
    }
  }

  /**
   * Runs a statement that may write, so the parse-time rejection is exercised on the same path a client would use.
   */
  private void run(final String query) {
    try (final ResultSet rs = database.command("opencypher", query)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
