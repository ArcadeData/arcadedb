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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.parser.FunctionValidator;
import com.arcadedb.query.opencypher.parser.FunctionValidator.FunctionSignature;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Guards the argument-count declarations in {@link FunctionValidator}, which issue #5484 promoted from unused metadata into
 * a parse-time check: a signature narrower than what the function actually accepts now rejects a query that used to work.
 * That is not hypothetical - {@code distance()} had always taken an optional unit while being declared as exactly two
 * arguments, and only a test calling it with three caught it.
 *
 * <p>So every function whose argument count is not fixed is exercised here at each of its supported arities. A function
 * added with a wrong declaration fails the build rather than the user's query.
 *
 * <p><b>A function declared with a fixed count that really accepts more</b> - the shape the {@code distance} bug had -
 * is invisible to a test that only calls each function at the counts it declares. That is what
 * {@link #noRegisteredSignatureIsNarrowerThanWhatItsExecutorDeclares} covers, by comparing each registered signature
 * against the bounds its executor declares. Until #5602 no executor declared any, so the comparison was empty and the
 * one-time manual sweep recorded here was all that stood behind it; every executor now declares its own contract and
 * enforces it from that declaration, including the seven that reach a SQL function through {@code SQLFunctionBridge}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherFunctionArityRegistryTest extends TestHelper {

  /**
   * Registered as known to the parser but with no executor behind them, so calling one fails at execution with "Unknown
   * function" however it is written. Empty since #5602 resolved the three that were here: {@code charLength} became an
   * alias of the already-implemented {@code char_length}, {@code isNormalized} got the executor its {@code normalize}
   * counterpart already had, and {@code charAt} - which names no Cypher function in Neo4j either - was unregistered so
   * the parser rejects it up front. Kept as the pin it always was, so a new one cannot appear unnoticed.
   */
  private static final Set<String> KNOWN_WITHOUT_EXECUTOR = Set.of();

  @Test
  void everyVariadicFunctionWorksAtEachOfItsSupportedArities() {
    // One call per supported argument count, so both the minimum and the maximum of each declared range is exercised.
    for (final String query : new String[] { //
        "RETURN trim('  x  ') AS r", "RETURN trim(' x ', ' ') AS r", //
        "RETURN ltrim('  x') AS r", "RETURN ltrim('xxa', 'x') AS r", //
        "RETURN rtrim('x  ') AS r", "RETURN rtrim('axx', 'x') AS r", //
        "RETURN btrim('  x  ') AS r", "RETURN btrim('xxaxx', 'x') AS r", //
        "RETURN substring('hello', 1) AS r", "RETURN substring('hello', 1, 2) AS r", //
        "RETURN format(date('2020-01-02')) AS r", "RETURN format(date('2020-01-02'), 'yyyy-MM-dd') AS r", //
        "RETURN range(1, 5) AS r", "RETURN range(1, 10, 2) AS r", //
        "RETURN round(3.14159) AS r", "RETURN round(3.14159, 2) AS r", "RETURN round(3.14159, 2, 'FLOOR') AS r", //
        "RETURN point({latitude: 1, longitude: 2}) AS r", "RETURN point(1, 2) AS r", //
        "RETURN distance(point({latitude: 0, longitude: 0}), point({latitude: 0, longitude: 1})) AS r", //
        "RETURN distance(point({latitude: 0, longitude: 0}), point({latitude: 0, longitude: 1}), 'km') AS r", //
        "RETURN normalize('x') AS r", "RETURN normalize('x', 'NFD') AS r", //
        "RETURN isNormalized('x') AS r", "RETURN isNormalized('x', 'NFD') AS r", //
        "RETURN coalesce(null) AS r", "RETURN coalesce(null, null, 1) AS r", //
        "RETURN date() AS r", "RETURN date('2020-01-02') AS r", //
        "RETURN time() AS r", "RETURN datetime() AS r", "RETURN localtime() AS r", "RETURN localdatetime() AS r", //
        "RETURN vector.create([1.0, 2.0]) AS r", //
        "RETURN vector.distance([1.0, 2.0], [3.0, 4.0]) AS r", //
        "RETURN vector.distance([1.0, 2.0], [3.0, 4.0], 'euclidean') AS r" }) {
      assertThat(runs(query)).as("%s", query).isTrue();
    }
  }

  @Test
  void theKeywordFormsOfTrimAreNotMiscountedAsArguments() {
    // trim(LEADING 'x' FROM '...') carries its arguments through keywords rather than a comma-separated list; the arity
    // check must read it as the one-argument call it is.
    assertThat(single("RETURN trim(LEADING 'x' FROM 'xxaxx') AS r")).isEqualTo("axx");
    assertThat(single("RETURN trim(TRAILING 'x' FROM 'xxaxx') AS r")).isEqualTo("xxa");
    assertThat(single("RETURN trim(BOTH 'x' FROM 'xxaxx') AS r")).isEqualTo("a");
  }

  @Test
  void goingOverTheDeclaredMaximumIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN substring('abc', 1, 2, 3) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("substring")
        .hasMessageContaining("2-3 arguments");
    assertThatThrownBy(() -> consume("RETURN left('abc', 1, 2) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("left")
        .hasMessageContaining("2 arguments");
    assertThatThrownBy(() -> consume("RETURN range(1, 2, 3, 4) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("range")
        .hasMessageContaining("2-3 arguments");
  }

  /**
   * The Cypher functions the parser declares more narrowly than their executor accepts, on purpose. Each entry names a
   * function whose executor is a general-purpose SQL one reached through {@code SQLFunctionBridge}, where the Cypher
   * language contract is the narrower of the two; every other function must let through everything its executor
   * accepts. Kept small and explained, because "the parser is stricter than the code" is exactly the shape of the
   * #5484 bug and must never be the default answer to a failing assertion below.
   */
  private static final Map<String, String> NARROWER_IN_CYPHER = Map.of(//
      // SQLFunctionCount accepts count(*) with no argument at all; in Cypher the star form is parsed as a distinct
      // construct rather than as a zero-argument call, so count() with no argument is not valid Cypher.
      "count", "count(*) is a separate parser construct in Cypher, not a zero-argument call", //
      // SQLFunctionSum is variadic: sum(a, b) adds one row's arguments. Cypher exposes only the aggregation.
      "sum", "Cypher exposes only the single-argument aggregation, not SQL's per-row variadic sum");

  /**
   * Holds every executor to its declaration, so the parser can never refuse a call the executor would have accepted.
   * <p>
   * This is what caught nothing before #5602: no executor declared {@code getMinArgs()}/{@code getMaxArgs()}, so every
   * function was skipped and the test asserted it had compared zero of them. Both sides now declare their bounds - the
   * Cypher executors directly, the seven that reach a SQL function through {@code SQLFunctionBridge} by passing the
   * wrapped function's through - so a {@link FunctionValidator} entry narrower than the code it gates fails the build
   * rather than the user's query. {@code distance()}, the entry that was actually wrong, is among the seven.
   * <p>
   * Note that the executor's bounds are not a second copy of the registry's: each executor declares them once and its
   * own runtime guard reads them ({@code Function.checkArity}), so what is compared here is the parser's view against
   * the code's, not two hand-written copies of the same number.
   */
  @Test
  void noRegisteredSignatureIsNarrowerThanWhatItsExecutorDeclares() {
    final CypherFunctionFactory factory = new CypherFunctionFactory(DefaultSQLFunctionFactory.getInstance());
    final Set<String> compared = new HashSet<>();

    for (final String name : FunctionValidator.getKnownFunctionNames()) {
      final StatelessFunction executor;
      try {
        executor = factory.getFunctionExecutor(name);
      } catch (final Exception ignored) {
        continue;
      }
      final int executorMin = executor.getMinArgs();
      final int executorMax = executor.getMaxArgs();
      final FunctionSignature signature = FunctionValidator.getSignature(name);

      if (NARROWER_IN_CYPHER.containsKey(name)) {
        // A pin that stopped being narrower is a pin that now hides a real check, so require it to still bite.
        assertThat(signature.getMinArgs() > executorMin || (signature.getMaxArgs() != -1 && signature.getMaxArgs() < executorMax))
            .as("%s is pinned as deliberately narrower (%s) but no longer is - drop the pin", name,
                NARROWER_IN_CYPHER.get(name))
            .isTrue();
        continue;
      }

      // Both at their defaults means the executor declares nothing, so there is nothing to compare against.
      if (executorMin == 0 && executorMax == Integer.MAX_VALUE)
        continue;

      compared.add(name);
      assertThat(signature.getMinArgs()).as("%s: parser demands more arguments than its executor does", name)
          .isLessThanOrEqualTo(executorMin);
      if (signature.getMaxArgs() != -1)
        assertThat(signature.getMaxArgs()).as("%s: parser allows fewer arguments than its executor accepts", name)
            .isGreaterThanOrEqualTo(executorMax);
    }

    // Every registered name either has an executor that declares bounds, is pinned above, or has no executor at all
    // (KNOWN_WITHOUT_EXECUTOR). Asserting the count keeps a future executor that quietly drops its declaration - which
    // would make this loop skip it - from silently reducing coverage.
    final Set<String> uncovered = new HashSet<>(FunctionValidator.getKnownFunctionNames());
    uncovered.removeAll(compared);
    uncovered.removeAll(NARROWER_IN_CYPHER.keySet());
    uncovered.removeAll(KNOWN_WITHOUT_EXECUTOR);
    assertThat(uncovered).as("registered functions whose executor declares no argument bounds, so nothing checks them")
        .isEmpty();
  }

  @Test
  void everyFunctionKnownToTheParserHasAnExecutor() {
    final CypherFunctionFactory factory = new CypherFunctionFactory(DefaultSQLFunctionFactory.getInstance());

    for (final String name : FunctionValidator.getKnownFunctionNames()) {
      if (KNOWN_WITHOUT_EXECUTOR.contains(name))
        continue;
      assertThat(catchThrown(() -> factory.getFunctionExecutor(name)))
          .as("%s is accepted by the parser but has no executor, so every call to it fails at execution", name)
          .isNull();
    }
  }

  private static Throwable catchThrown(final Runnable runnable) {
    try {
      runnable.run();
      return null;
    } catch (final Throwable t) {
      return t;
    }
  }

  private boolean runs(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rs.next();
      return true;
    }
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("r");
    }
  }

  private void consume(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
