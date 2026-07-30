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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

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
    for (final String function : new String[] { "ceil", "ceiling", "floor", "sqrt", "sign", "round", "isNaN", "exp", "log",
        "log10", "sin", "cos", "tan", "asin", "acos", "atan", "cot", "coth", "sinh", "cosh", "tanh", "degrees", "radians",
        "haversin" }) {
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
  void namespacedMathFunctionRejectsAStringArgument() {
    // math.* are ArcadeDB extensions but share the same contract: an argument outside the input domain is a client error,
    // not the NumberFormatException-driven 500 they used to answer.
    assertThatThrownBy(() -> consume("RETURN math.sigmoid('hello') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("math.sigmoid()")
        .hasMessageContaining("STRING");
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
