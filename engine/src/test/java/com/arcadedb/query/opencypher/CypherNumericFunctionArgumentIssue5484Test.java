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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.function.cypher.CypherFunctionHelper.NumericSignature;
import com.arcadedb.function.math.AbsFunction;
import com.arcadedb.function.math.IsNaNFunction;
import com.arcadedb.function.math.MathBinaryFunction;
import com.arcadedb.function.math.MathUnaryFunction;
import com.arcadedb.function.math.RoundFunction;
import com.arcadedb.function.math.SignFunction;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.parser.FunctionValidator;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5484: {@code RETURN abs('hello')} reported the right message with the wrong class,
 * {@code CommandExecutionException}, which the HTTP layer turns into a 500 "internal server error". Handing a STRING to a
 * function declared as {@code abs(input :: INTEGER | FLOAT)} is the client's mistake, not an internal fault: Neo4j answers a
 * {@code Neo.ClientError.Statement.TypeError} and ArcadeDB must answer 400.
 *
 * <p>The whole numeric family is covered, not only {@code abs()}: every function that reaches a shared numeric-argument
 * check had the same defect. Same class of fix as issues #5476, #5477, #5294 and #5203.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherNumericFunctionArgumentIssue5484Test extends TestHelper {

  // ===================== the reproducer =====================

  @Test
  void absOfStringIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN abs('hello') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs()")
        .hasMessageContaining("STRING");
  }

  @Test
  void absOfBooleanIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN abs(true) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs()")
        .hasMessageContaining("BOOLEAN");
  }

  @Test
  void absOfListIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN abs([1,2]) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs()")
        .hasMessageContaining("LIST");
  }

  @Test
  void absOfMapIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN abs({a: 1}) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs()")
        .hasMessageContaining("MAP");
  }

  @Test
  void absOfStringPropertyIsRejectedAtRuntime() {
    // The type is known only while the query runs, so this exercises AbsFunction itself rather than the
    // statically-known-argument check in the validator.
    database.transaction(() -> database.command("opencypher", "CREATE (:Issue5484 {name: 'a', age: 42})"));
    assertThatThrownBy(() -> consume("MATCH (n:Issue5484) RETURN abs(n.name) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs()")
        .hasMessageContaining("STRING");
  }

  @Test
  void absOfNodeIsAClientTypeError() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Issue5484 {name: 'a', age: 42})"));
    assertThatThrownBy(() -> consume("MATCH (n:Issue5484) RETURN abs(n) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs()");
  }

  @Test
  void absOfStringLiteralFailsEvenWhenNoRowMatches() {
    // Neo4j rejects an out-of-domain literal before running the query, so it fails even where the function would
    // never be called.
    database.transaction(() -> database.command("opencypher", "CREATE (:Issue5484 {name: 'a'})"));
    assertThatThrownBy(() -> consume("MATCH (n:Issue5484) WHERE n.name = 'nobody' RETURN abs('hello') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs()");
  }

  // ===================== the rest of the numeric family =====================

  @Test
  void everyUnaryNumericFunctionRejectsAStringArgument() {
    // ln() is included on purpose: it is an alias of log() but keeps its own name, so its message must say ln().
    for (final String function : new String[] { "ceil", "ceiling", "floor", "sqrt", "sign", "round", "isNaN", "exp", "log",
        "ln", "log10", "sin", "cos", "tan", "asin", "acos", "atan", "cot", "coth", "sinh", "cosh", "tanh", "degrees",
        "radians", "haversin" }) {
      assertThatThrownBy(() -> consume("RETURN " + function + "('hello') AS r"))
          .as("%s('hello')", function)
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining(function + "()")
          .hasMessageContaining("STRING");
    }
  }

  @Test
  void binaryNumericFunctionRejectsAStringArgument() {
    assertThatThrownBy(() -> consume("RETURN atan2('hello', 1) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("atan2()")
        .hasMessageContaining("STRING");
    assertThatThrownBy(() -> consume("RETURN atan2(1, true) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("atan2()")
        .hasMessageContaining("BOOLEAN");
  }

  @Test
  void roundRejectsANonNumericPrecision() {
    assertThatThrownBy(() -> consume("RETURN round(3.14159, 'two') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("round()")
        .hasMessageContaining("STRING");
  }

  @Test
  void roundRejectsAnUnknownRoundingMode() {
    assertThatThrownBy(() -> consume("RETURN round(3.14159, 2, 'SIDEWAYS') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("round()")
        .hasMessageContaining("SIDEWAYS");
  }

  @Test
  void aListLiteralFailsEvenWhenNoRowMatches() {
    // A bracketed list is a ListExpression, not a literal holding a Collection, so it needs recognising on its own:
    // otherwise abs([1,2]) was only caught once a row reached the function.
    database.transaction(() -> database.command("opencypher", "CREATE (:Issue5484 {name: 'a'})"));
    assertThatThrownBy(() -> consume("MATCH (n:Issue5484) WHERE n.name = 'nobody' RETURN abs([1,2]) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs()")
        .hasMessageContaining("LIST");
    assertThatThrownBy(() -> consume("MATCH (n:Issue5484) WHERE n.name = 'nobody' RETURN round(3.14, 2, [1]) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("rounding mode");
  }

  @Test
  void multiArgumentNumericLiteralsFailEvenWhenNoRowMatches() {
    // The parse-time guarantee covers the binary and ternary members of the family, not only the unary ones: these
    // projections are never evaluated, so without the static check they used to succeed silently.
    database.transaction(() -> database.command("opencypher", "CREATE (:Issue5484 {name: 'a'})"));
    assertThatThrownBy(() -> consume("MATCH (n:Issue5484) WHERE n.name = 'nobody' RETURN atan2('hello', 1) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("atan2()")
        .hasMessageContaining("STRING");
    assertThatThrownBy(() -> consume("MATCH (n:Issue5484) WHERE n.name = 'nobody' RETURN round(3.14, 'two') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("round()")
        .hasMessageContaining("STRING");
    assertThatThrownBy(() -> consume("MATCH (n:Issue5484) WHERE n.name = 'nobody' RETURN round(3.14, 2, 'SIDEWAYS') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("round()")
        .hasMessageContaining("SIDEWAYS");
  }

  @Test
  void aMapLiteralIsNotARoundingModeEither() {
    // The numeric positions reject a map literal at parse time, so the mode position must too rather than deferring to
    // the runtime check.
    assertThatThrownBy(() -> consume("MATCH (n:Issue5484) WHERE false RETURN round(3.14, 2, {a: 1}) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("round()")
        .hasMessageContaining("rounding mode");
  }

  @Test
  void theRoundingModeOfRoundIsNotANumericArgument() {
    // round()'s third argument is a STRING, so the per-position check must not reject it as non-numeric.
    assertThat(single("RETURN round(3.14159, 2, 'FLOOR') AS r")).isEqualTo(3.14d);
    assertThat(single("RETURN round(3.14159, 2, 'CEILING') AS r")).isEqualTo(3.15d);
    // A null mode is not rejected as non-numeric either, which is what this test is about. What it answers was settled
    // separately by issue #5629: an explicitly written null propagates rather than selecting the HALF_UP default.
    assertThat(single("RETURN round(3.14159, 2, null) AS r")).isNull();
  }

  // ===================== the wrong number of arguments is a client error too =====================

  @Test
  void theWrongNumberOfArgumentsIsReportedAsSuch() {
    // The arity is the primary defect, so it must be reported before the single-argument type check gets a chance to
    // call a binary function handed one argument a type error.
    assertThatThrownBy(() -> consume("RETURN atan2('hello') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("atan2")
        .hasMessageContaining("2 arguments")
        .hasMessageNotContaining("Type mismatch");
    assertThatThrownBy(() -> consume("RETURN abs(1, 2) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs")
        .hasMessageContaining("1 argument");
    assertThatThrownBy(() -> consume("RETURN sqrt() AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("sqrt");
  }

  @Test
  void bothPathsWordAWrongArgumentCountIdentically() {
    // The parse-time check and the function's own guard describe the same mistake with the same sentence, so which one
    // happened to catch it is not observable.
    assertThatThrownBy(() -> consume("RETURN abs(1, 2) AS r"))
        .hasMessageContaining("Function 'abs' expects 1 argument but got 2");
    assertThatThrownBy(() -> new AbsFunction().execute(new Object[] { 1, 2 }, null))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("Function 'abs' expects 1 argument but got 2");
  }

  @Test
  void roundChecksItsOtherArgumentsEvenWhenTheValueIsNull() {
    // The parse-time check looks at each argument independently, so the function must too: otherwise round(n.missing,
    // 'two') answered null while round(null, 'two') written as a literal was a type error.
    database.transaction(() -> database.command("opencypher", "CREATE (:Issue5484 {name: 'a'})"));
    assertThatThrownBy(() -> consume("MATCH (n:Issue5484) RETURN round(n.missing, 'two') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("round()")
        .hasMessageContaining("STRING");
    assertThatThrownBy(() -> consume("MATCH (n:Issue5484) RETURN round(n.missing, 2, 'SIDEWAYS') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("SIDEWAYS");
    // A well-formed call over a null value still propagates null rather than failing.
    assertThat(single("MATCH (n:Issue5484) RETURN round(n.missing, 2, 'FLOOR') AS r")).isNull();
  }

  @Test
  void theOptionalUnitOfDistanceIsStillAccepted() {
    // distance() takes an optional third argument; the arity check must not reject it (it was registered as 2-only).
    assertThat((Double) single("RETURN distance(point({latitude: 0, longitude: 0}), point({latitude: 0, longitude: 1}), 'km')"
        + " AS r")).isGreaterThan(0d);
  }

  // ===================== the parse-time list must not drift from the executors =====================

  @Test
  void everyNumericFunctionNameResolvesToANumericExecutorAndBack() {
    // The parse-time check reads NUMERIC_ARGUMENT_FUNCTIONS while the runtime check lives in the executor the factory
    // builds. This locks the two together: a numeric function added to one but not the other fails here.
    final CypherFunctionFactory factory = new CypherFunctionFactory(DefaultSQLFunctionFactory.getInstance());

    for (final Map.Entry<String, NumericSignature> entry : CypherFunctionHelper.NUMERIC_ARGUMENT_FUNCTIONS.entrySet()) {
      assertThat(entry.getKey()).as("map key must be the lower-case name the parser produces")
          .isEqualTo(entry.getValue().name().toLowerCase(Locale.ROOT));
      assertThat(isNumericExecutor(factory.getFunctionExecutor(entry.getKey())))
          .as("%s is declared numeric but its executor is not", entry.getKey()).isTrue();
    }

    for (final String name : FunctionValidator.getKnownFunctionNames()) {
      final StatelessFunction executor;
      try {
        executor = factory.getFunctionExecutor(name);
      } catch (final CommandExecutionException ignored) {
        // A name known to the parser with no executor behind it yet: nothing to keep in sync.
        continue;
      }
      if (isNumericExecutor(executor))
        assertThat(CypherFunctionHelper.NUMERIC_ARGUMENT_FUNCTIONS)
            .as("%s has a numeric executor but is missing from NUMERIC_ARGUMENT_FUNCTIONS", name).containsKey(name);
    }
  }

  @Test
  void theFunctionNameLookupDoesNotDependOnTheDefaultLocale() {
    // The map is keyed with Locale.ROOT while the parser lower-cases the name it read. Under a Turkish default locale
    // "ISNAN".toLowerCase() is "ısnan" with a dotless i, which matches neither the map nor the known-function registry:
    // a valid query would be rejected as calling an unknown function.
    final Locale previous = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr"));
      assertThat(single("RETURN ISNAN(1.0) AS r")).isEqualTo(false);
      assertThatThrownBy(() -> consume("RETURN ISNAN('hello') AS r"))
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("STRING");
    } finally {
      Locale.setDefault(previous);
    }
  }

  private static boolean isNumericExecutor(final StatelessFunction executor) {
    return executor instanceof AbsFunction || executor instanceof MathUnaryFunction
        || executor instanceof MathBinaryFunction || executor instanceof SignFunction || executor instanceof IsNaNFunction
        || executor instanceof RoundFunction;
  }

  @Test
  void namespacedMathFunctionRejectsAStringArgument() {
    // math.* are ArcadeDB extensions but share the same contract: an argument outside the input domain is a client error,
    // not the NumberFormatException-driven 500 they used to answer. They do accept a numeric string, so an unparseable one
    // is worded as such rather than as a STRING-vs-number type mismatch.
    assertThatThrownBy(() -> consume("RETURN math.sigmoid('hello') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("math.sigmoid()")
        .hasMessageContaining("'hello'")
        .hasMessageNotContaining("Type mismatch");
    assertThatThrownBy(() -> consume("RETURN math.sigmoid(true) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("math.sigmoid()")
        .hasMessageContaining("BOOLEAN");
  }

  @Test
  void namespacedMathFunctionStillAcceptsANumericString() {
    assertThat((Double) single("RETURN math.sigmoid('0') AS r")).isEqualTo(0.5d);
  }

  @Test
  void anUnsupportedDistanceUnitIsAlreadyAClientError() {
    // distance()'s unit is validated by SQLFunctionGeoDistance with an IllegalArgumentException, which the HTTP layer
    // maps to 400 on its own. Asserted here so the widened arity cannot start hiding an internal error later.
    assertThatThrownBy(() -> consume("RETURN distance(point({latitude: 0, longitude: 0}),"
        + " point({latitude: 0, longitude: 1}), 'furlongs') AS r"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("furlongs");
  }

  // ===================== arguments that are still accepted =====================

  @Test
  void nullPropagationIsNotATypeError() {
    assertThat(single("RETURN abs(null) AS r")).isNull();
    assertThat(single("RETURN sqrt(null) AS r")).isNull();
    assertThat(single("RETURN sign(null) AS r")).isNull();
    assertThat(single("RETURN round(null) AS r")).isNull();
    assertThat(single("RETURN isNaN(null) AS r")).isNull();
    assertThat(single("RETURN atan2(null, 1) AS r")).isNull();
  }

  @Test
  void numericArgumentsStillWork() {
    assertThat(single("RETURN abs(-1) AS r")).isEqualTo(1L);
    assertThat(single("RETURN abs(-1.5) AS r")).isEqualTo(1.5d);
    assertThat(single("RETURN ceil(2.1) AS r")).isEqualTo(3.0d);
    assertThat(single("RETURN floor(2.9) AS r")).isEqualTo(2.0d);
    assertThat(single("RETURN sqrt(9) AS r")).isEqualTo(3.0d);
    assertThat(single("RETURN sign(-7) AS r")).isEqualTo(-1L);
    assertThat(single("RETURN round(3.14159, 2) AS r")).isEqualTo(3.14d);
    assertThat(single("RETURN round(3.14159, 2, 'FLOOR') AS r")).isEqualTo(3.14d);
    assertThat(single("RETURN isNaN(1.0) AS r")).isEqualTo(false);
    assertThat((Double) single("RETURN atan2(1, 1) AS r")).isCloseTo(Math.PI / 4, org.assertj.core.data.Offset.offset(1e-9));
  }

  @Test
  void numericPropertiesStillWork() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Issue5484 {name: 'a', age: -42})"));
    assertThat(single("MATCH (n:Issue5484) RETURN abs(n.age) AS r")).isEqualTo(42L);
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
