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
 * Regression test for issue #5294: passing an unsupported argument type to a Cypher scalar conversion
 * function ({@code toFloat}, {@code toInteger}, {@code toBoolean}) must fail as a client-side type error
 * ({@link CommandSemanticException}, mapped to HTTP 400), never as an unhandled {@code CommandExecutionException}
 * (mapped to HTTP 500). This mirrors the toString() fix from issue #5203.
 *
 * <p>Behaviour matches the Neo4j reference implementation:
 * <ul>
 *   <li>{@code toFloat()} accepts only STRING/INTEGER/FLOAT, so a BOOLEAN argument is a type error.</li>
 *   <li>{@code toInteger()} additionally accepts BOOLEAN (true -> 1, false -> 0).</li>
 *   <li>{@code toBoolean()} rejects FLOAT.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5294ToFloatBooleanTest extends TestHelper {

  // ===================== toFloat() =====================

  @Test
  void toFloatWithBooleanTrueThrowsSemanticError() {
    // Issue #5294: RETURN toFloat(true) used to crash with a 500 CommandExecutionException.
    assertThatThrownBy(() -> database.command("opencypher", "RETURN toFloat(true) AS r").stream().toList())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("toFloat")
        .hasMessageContaining("Boolean");
  }

  @Test
  void toFloatWithBooleanFalseThrowsSemanticError() {
    assertThatThrownBy(() -> database.command("opencypher", "RETURN toFloat(false) AS r").stream().toList())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("toFloat");
  }

  @Test
  void toFloatWithListThrowsSemanticError() {
    assertThatThrownBy(() -> database.command("opencypher", "RETURN toFloat([1, 2, 3]) AS r").stream().toList())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("toFloat");
  }

  @Test
  void toFloatStillConvertsNumbersAndStrings() {
    try (final ResultSet rs = database.command("opencypher", "RETURN toFloat(1) AS a, toFloat(0) AS b, toFloat('11.5') AS c")) {
      assertThat(rs.hasNext()).isTrue();
      final var row = rs.next();
      assertThat(((Number) row.getProperty("a")).doubleValue()).isEqualTo(1.0);
      assertThat(((Number) row.getProperty("b")).doubleValue()).isEqualTo(0.0);
      assertThat(((Number) row.getProperty("c")).doubleValue()).isEqualTo(11.5);
    }
  }

  @Test
  void toFloatOrNullStillReturnsNullForBoolean() {
    // The OrNull variant swallows the type error into null, matching Neo4j's toFloatOrNull(true) -> null.
    try (final ResultSet rs = database.command("opencypher", "RETURN toFloatOrNull(true) AS r")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("r")).isNull();
    }
  }

  // ===================== toInteger() =====================

  @Test
  void toIntegerStillConvertsBoolean() {
    // Neo4j (and ArcadeDB) DO accept boolean for toInteger: true -> 1, false -> 0.
    try (final ResultSet rs = database.command("opencypher", "RETURN toInteger(true) AS t, toInteger(false) AS f")) {
      assertThat(rs.hasNext()).isTrue();
      final var row = rs.next();
      assertThat(((Number) row.getProperty("t")).longValue()).isEqualTo(1L);
      assertThat(((Number) row.getProperty("f")).longValue()).isEqualTo(0L);
    }
  }

  @Test
  void toIntegerWithListThrowsSemanticError() {
    assertThatThrownBy(() -> database.command("opencypher", "RETURN toInteger([1, 2, 3]) AS r").stream().toList())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("toInteger");
  }

  // ===================== toBoolean() =====================

  @Test
  void toBooleanWithFloatThrowsSemanticError() {
    assertThatThrownBy(() -> database.command("opencypher", "RETURN toBoolean(1.5) AS r").stream().toList())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("toBoolean");
  }

  @Test
  void toBooleanWithListThrowsSemanticError() {
    assertThatThrownBy(() -> database.command("opencypher", "RETURN toBoolean([1, 2, 3]) AS r").stream().toList())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("toBoolean");
  }
}
