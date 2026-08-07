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
 * Regression test for issue #5798: {@code toUpper()}, {@code toLower()}, {@code trim()}, {@code lTrim()},
 * {@code rTrim()}, {@code split()} and {@code replace()} declare a {@code STRING} input domain but silently
 * converted any value via {@code Object.toString()} instead of rejecting it, so {@code toUpper(5)} and
 * {@code toUpper(toString(5))} were observationally identical. Handing a non-STRING value to one of these
 * functions is the client's mistake and must be a {@link CommandSemanticException} (400), not a silently
 * "successful" textual conversion. Same class of fix as issues #5484 (numeric family) and #5476/#5477
 * (head/last/tail/size).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherStringFunctionArgumentIssue5798Test extends TestHelper {

  // ===================== the reproducers from the issue =====================

  @Test
  void toUpperOfIntegerIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN toUpper(5) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("toUpper()")
        .hasMessageContaining("STRING");
  }

  @Test
  void toUpperOfBooleanIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN toUpper(true) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("toUpper()")
        .hasMessageContaining("BOOLEAN");
  }

  @Test
  void toUpperOfListIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN toUpper([1,2]) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("toUpper()")
        .hasMessageContaining("LIST");
  }

  @Test
  void splitOfNonStringFirstArgumentIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN split(5, ',') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("split()")
        .hasMessageContaining("STRING");
  }

  @Test
  void toLowerOfIntegerIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN toLower(5) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("toLower()")
        .hasMessageContaining("STRING");
  }

  @Test
  void trimOfIntegerIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN trim(5) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("trim()")
        .hasMessageContaining("STRING");
  }

  @Test
  void lTrimOfIntegerIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN lTrim(5) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("lTrim()")
        .hasMessageContaining("STRING");
  }

  @Test
  void rTrimOfIntegerIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN rTrim(5) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("rTrim()")
        .hasMessageContaining("STRING");
  }

  @Test
  void replaceOfNonStringFirstArgumentIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN replace(5, 'a', 'b') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("replace()")
        .hasMessageContaining("STRING");
  }

  @Test
  void theSqlStyleThreeArgumentTrimFormRejectsANonStringSource() {
    // trim(BOTH/LEADING/TRAILING char FROM string) is a separate code path from the 1- and 2-argument
    // forms above (CypherTrimFunction's args.length == 3 branch); exercise it directly since it is not
    // reachable through the 1-argument parse-time static check.
    assertThatThrownBy(() -> consume("RETURN trim(BOTH 'x' FROM 5) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("trim()")
        .hasMessageContaining("STRING");
  }

  // ===================== the alias spellings share the same check =====================

  @Test
  void theAliasSpellingsRejectANonStringArgumentToo() {
    // canonicalStringFunctionName() maps the parser's lower-cased alias to the same spelling the runtime
    // check uses; exercise the aliases directly rather than only through their canonical name.
    assertThatThrownBy(() -> consume("RETURN upper(5) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("toUpper()")
        .hasMessageContaining("STRING");
    assertThatThrownBy(() -> consume("RETURN lower(5) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("toLower()")
        .hasMessageContaining("STRING");
    assertThatThrownBy(() -> consume("RETURN ltrim(5) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("lTrim()")
        .hasMessageContaining("STRING");
    assertThatThrownBy(() -> consume("RETURN rtrim(5) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("rTrim()")
        .hasMessageContaining("STRING");
  }

  // ===================== the primary argument is checked independent of a null secondary one =====================

  @Test
  void thePrimaryArgumentIsCheckedEvenWhenASecondaryArgumentIsNull() {
    // A null secondary argument (delimiter, search text, trim character) must not let an out-of-domain
    // primary argument slip through as a silent null: the type check on the primary argument must run
    // before null propagation decides the answer, mirroring the #5484 convention for MathBinaryFunction
    // and RoundFunction.
    assertThatThrownBy(() -> consume("RETURN replace(5, null, 'b') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("replace()")
        .hasMessageContaining("STRING");
    assertThatThrownBy(() -> consume("RETURN split(5, null) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("split()")
        .hasMessageContaining("STRING");
    assertThatThrownBy(() -> consume("RETURN lTrim(5, null) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("lTrim()")
        .hasMessageContaining("STRING");
    assertThatThrownBy(() -> consume("RETURN rTrim(5, null) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("rTrim()")
        .hasMessageContaining("STRING");
    assertThatThrownBy(() -> consume("RETURN btrim(5, null) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("trim()")
        .hasMessageContaining("STRING");
  }

  // ===================== explicit conversion still works =====================

  @Test
  void explicitToStringConversionStillWorks() {
    // The distinction the issue is about: an explicit conversion is a valid STRING argument.
    assertThat(single("RETURN toUpper(toString(5)) AS r")).isEqualTo("5");
  }

  // ===================== null propagation is not a type error =====================

  @Test
  void nullPropagationIsNotATypeError() {
    assertThat(single("RETURN toUpper(null) AS r")).isNull();
    assertThat(single("RETURN toLower(null) AS r")).isNull();
    assertThat(single("RETURN trim(null) AS r")).isNull();
    assertThat(single("RETURN lTrim(null) AS r")).isNull();
    assertThat(single("RETURN rTrim(null) AS r")).isNull();
    assertThat(single("RETURN split(null, ',') AS r")).isNull();
    assertThat(single("RETURN replace(null, 'a', 'b') AS r")).isNull();
  }

  // ===================== valid STRING arguments still work =====================

  @Test
  void validStringArgumentsStillWork() {
    assertThat(single("RETURN toUpper('abc') AS r")).isEqualTo("ABC");
    assertThat(single("RETURN toLower('ABC') AS r")).isEqualTo("abc");
    assertThat(single("RETURN trim('  abc  ') AS r")).isEqualTo("abc");
    assertThat(single("RETURN lTrim('  abc') AS r")).isEqualTo("abc");
    assertThat(single("RETURN rTrim('abc  ') AS r")).isEqualTo("abc");
    assertThat(single("RETURN split('a,b', ',') AS r").toString()).isEqualTo("[a, b]");
    assertThat(single("RETURN replace('abc', 'b', 'x') AS r")).isEqualTo("axc");
  }

  // ===================== the runtime path, exercised through a property =====================

  @Test
  void aPropertyHoldingAnIntegerIsRejectedAtRuntime() {
    // The type is known only while the query runs, so this exercises the executor itself rather than the
    // statically-known-argument check in the validator.
    database.transaction(() -> database.command("opencypher", "CREATE (:Issue5798 {name: 'a', age: 42})"));
    assertThatThrownBy(() -> consume("MATCH (n:Issue5798) RETURN toUpper(n.age) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("toUpper()")
        .hasMessageContaining("STRING");
  }

  // ===================== the parse-time literal check =====================

  @Test
  void aLiteralArgumentFailsEvenWhenNoRowMatches() {
    // Neo4j rejects an out-of-domain literal before running the query, so it fails even where the function
    // would never be called. Same behaviour as the numeric family (#5484).
    database.transaction(() -> database.command("opencypher", "CREATE (:Issue5798 {name: 'a'})"));
    assertThatThrownBy(() -> consume("MATCH (n:Issue5798) WHERE n.name = 'nobody' RETURN toUpper(5) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("toUpper()")
        .hasMessageContaining("STRING");
    assertThatThrownBy(() -> consume("MATCH (n:Issue5798) WHERE n.name = 'nobody' RETURN toLower(true) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("toLower()")
        .hasMessageContaining("BOOLEAN");
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
