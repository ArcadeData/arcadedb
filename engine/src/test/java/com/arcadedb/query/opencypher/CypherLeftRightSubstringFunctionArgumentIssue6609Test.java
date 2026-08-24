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
 * Regression test for issue #6609: {@code RETURN left('a', [])} (and the same shape for {@code right()} and
 * {@code substring()}) reached an unchecked {@code (Number) args[1]} cast, so the {@code java.lang.ClassCastException}
 * escaped as {@code CommandExecutionException} / HTTP 500 "Error on transaction commit" instead of a client-facing
 * type error. Handing a LIST to a function declared {@code f(..., length :: INTEGER)} is the client's mistake, not an
 * internal fault: Neo4j answers {@code Neo.ClientError.Statement.TypeError} (22N27) and ArcadeDB must answer 400.
 *
 * <p>Same class of fix as issue #5484 (the numeric family) and issue #5798 (the STRING family): #5901's own
 * description names {@code left()}/{@code right()} as a follow-up left out of that fix's scope, and {@code substring()}
 * shares the same unchecked numeric cast path.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherLeftRightSubstringFunctionArgumentIssue6609Test extends TestHelper {

  // ===================== the reproducer =====================

  @Test
  void leftOfListLengthIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN left('a', []) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("left()")
        .hasMessageContaining("LIST");
  }

  @Test
  void rightOfListLengthIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN right('a', []) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("right()")
        .hasMessageContaining("LIST");
  }

  @Test
  void substringOfListLengthIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN substring('aa', 0, []) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("substring()")
        .hasMessageContaining("LIST");
  }

  @Test
  void substringOfListStartIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN substring('aa', []) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("substring()")
        .hasMessageContaining("LIST");
  }

  // ===================== other out-of-domain types =====================

  @Test
  void leftOfBooleanLengthIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN left('a', true) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("left()")
        .hasMessageContaining("BOOLEAN");
  }

  @Test
  void leftOfStringLengthIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN left('a', 'two') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("left()")
        .hasMessageContaining("STRING");
  }

  @Test
  void leftOfMapLengthIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN left('a', {a: 1}) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("left()")
        .hasMessageContaining("MAP");
  }

  // ===================== runtime path (type known only once the query runs) =====================

  @Test
  void leftOfListPropertyIsRejectedAtRuntime() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Issue6609 {name: 'a', tags: [1, 2]})"));
    assertThatThrownBy(() -> consume("MATCH (n:Issue6609) RETURN left(n.name, n.tags) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("left()")
        .hasMessageContaining("LIST");
  }

  // ===================== a literal fails even when no row matches =====================

  @Test
  void aListLiteralFailsEvenWhenNoRowMatches() {
    // Neo4j rejects an out-of-domain literal before running the query, so it fails even where the function would
    // never be called. Same parse-time guarantee already given to the pure-numeric family (issue #5484).
    database.transaction(() -> database.command("opencypher", "CREATE (:Issue6609 {name: 'a'})"));
    assertThatThrownBy(() -> consume("MATCH (n:Issue6609) WHERE n.name = 'nobody' RETURN left('a', [1,2]) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("left()")
        .hasMessageContaining("LIST");
    assertThatThrownBy(() -> consume("MATCH (n:Issue6609) WHERE n.name = 'nobody' RETURN right('a', [1,2]) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("right()")
        .hasMessageContaining("LIST");
    assertThatThrownBy(() -> consume("MATCH (n:Issue6609) WHERE n.name = 'nobody' RETURN substring('aa', 0, [1]) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("substring()")
        .hasMessageContaining("LIST");
  }

  // ===================== the primary STRING argument is untouched by this fix =====================

  @Test
  void thePrimaryStringArgumentIsStillNotTypeChecked() {
    // Out of scope for #6609 (and for #5798/#5901 before it): left()/right()/substring()'s primary text argument
    // still converts any value via toString() rather than rejecting it. Locked down so a future fix to that separate
    // gap is a deliberate, visible change to this test rather than a silent behavior drift.
    assertThat(single("RETURN left(5, 1) AS r")).isEqualTo("5");
  }

  // ===================== arguments that are still accepted =====================

  @Test
  void nullPropagationIsNotATypeError() {
    assertThat(single("RETURN left('a', null) AS r")).isNull();
    assertThat(single("RETURN right('a', null) AS r")).isNull();
    assertThat(single("RETURN substring('a', null) AS r")).isNull();
    assertThat(single("RETURN substring('a', 0, null) AS r")).isNull();
  }

  @Test
  void normalArgumentsStillWork() {
    assertThat(single("RETURN left('hello', 3) AS r")).isEqualTo("hel");
    assertThat(single("RETURN right('hello', 3) AS r")).isEqualTo("llo");
    assertThat(single("RETURN substring('hello', 1, 3) AS r")).isEqualTo("ell");
    assertThat(single("RETURN substring('hello', 1) AS r")).isEqualTo("ello");
  }

  @Test
  void negativeLengthIsStillAClientErrorNotATypeError() {
    // Regression guard: a well-typed but out-of-range Number (issue #5296/#5793) must still be reported the way it
    // was before this fix, distinct from the type-mismatch message this issue adds.
    assertThatThrownBy(() -> consume("RETURN left('a', -1) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("negative length")
        .hasMessageNotContaining("Type mismatch");
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
