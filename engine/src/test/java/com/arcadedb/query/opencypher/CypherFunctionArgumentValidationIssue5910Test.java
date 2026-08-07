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
 * Regression test for issue #5910: follow-up to #5794/#5909. That fix converted the four
 * argument-validation paths named in #5794's reproduction steps from a bare
 * {@link com.arcadedb.exception.CommandExecutionException} (HTTP 500) to {@link CommandSemanticException}
 * (HTTP 400). The same defect class was left in these sibling paths, outside #5794's named steps:
 * {@code localdatetime()}, {@code localtime()}, {@code time()}, the "expects a string, map, or temporal
 * argument" branch of {@code date()}/{@code datetime()} (distinct from the parse-failure branch already
 * fixed in #5909), and the remaining structural-validation branches of {@code point()}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherFunctionArgumentValidationIssue5910Test extends TestHelper {

  @Test
  void localdatetimeWithAnInvalidStringIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN localdatetime('not-a-date') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("localdatetime()")
        .hasMessageContaining("not-a-date");
  }

  @Test
  void localdatetimeWithAnUnsupportedArgumentTypeIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN localdatetime(true) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("localdatetime()")
        .hasMessageContaining("expects");
  }

  @Test
  void localtimeWithAnInvalidStringIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN localtime('not-a-time') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("localtime()")
        .hasMessageContaining("not-a-time");
  }

  @Test
  void localtimeWithAnUnsupportedArgumentTypeIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN localtime(true) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("localtime()")
        .hasMessageContaining("expects");
  }

  @Test
  void timeWithAnInvalidStringIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN time('not-a-time') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("time()")
        .hasMessageContaining("not-a-time");
  }

  @Test
  void timeWithAnUnsupportedArgumentTypeIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN time(true) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("time()")
        .hasMessageContaining("expects");
  }

  @Test
  void dateWithAnUnsupportedArgumentTypeIsAClientError() {
    // Distinct from the parse-failure branch already fixed for #5794/#5909: this exercises the
    // "expects a string, map, or temporal argument" branch, reached only when the argument is
    // not a String at all (so no parse is even attempted).
    assertThatThrownBy(() -> consume("RETURN date(true) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("date()")
        .hasMessageContaining("expects");
  }

  @Test
  void datetimeWithAnUnsupportedArgumentTypeIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN datetime(true) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("datetime()")
        .hasMessageContaining("expects");
  }

  @Test
  void pointWithANonMapArgumentIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN point('not-a-map') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("point()")
        .hasMessageContaining("map");
  }

  @Test
  void pointWithoutRecognizedCoordinateKeysIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN point({foo: 1}) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("point()")
        .hasMessageContaining("x/y");
  }

  @Test
  void pointWithANonNumericSridIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN point({x: 1, y: 1, srid: 'abc'}) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("point()")
        .hasMessageContaining("srid");
  }

  // ===================== valid-input controls: confirm the fix does not disturb success paths =====================

  @Test
  void validInputsStillSucceed() {
    assertThat(single("RETURN localdatetime('2026-08-03T12:00:00') AS r")).isNotNull();
    assertThat(single("RETURN localtime('12:00:00') AS r")).isNotNull();
    assertThat(single("RETURN time('12:00:00Z') AS r")).isNotNull();
    assertThat(single("RETURN date('2026-08-03') AS r")).isNotNull();
    assertThat(single("RETURN datetime('2026-08-03T12:00:00Z') AS r")).isNotNull();
    assertThat(single("RETURN point({x: 1, y: 1, srid: 4326}).srid AS r")).isEqualTo(4326);
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
