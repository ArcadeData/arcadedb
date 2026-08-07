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
 * Regression test for issue #5794: a systematic function x invalid-input scan found four argument-validation
 * paths that still threw a bare {@link com.arcadedb.exception.CommandExecutionException}, which the HTTP layer
 * turns into a misleading 500 "internal server error" instead of a 400 client error. Every failure below is
 * determined entirely by the supplied query arguments and cannot succeed when retried unchanged, so it belongs
 * to the same class of client-caused failure fixed for the neighboring functions in issues #5484 (abs()), #5296
 * (left()/right()) and #5545 (arithmetic errors).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherFunctionArgumentValidationIssue5794Test extends TestHelper {

  @Test
  void rangeWithZeroStepIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN range(1, 5, 0) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("range()")
        .hasMessageContaining("zero");
  }

  @Test
  void dateWithAnInvalidStringIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN date('not-a-date') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("date()")
        .hasMessageContaining("not-a-date");
  }

  @Test
  void datetimeWithAnInvalidStringIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN datetime('not-a-date') AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("datetime()")
        .hasMessageContaining("not-a-date");
  }

  @Test
  void pointWithANonNumericStringCoordinateIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN point({x: 'a', y: 1}) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("point()")
        .hasMessageContaining("x");
  }

  @Test
  void pointWithANonNumericNonStringCoordinateIsAClientError() {
    // Same coerceCoordinate() guard, the other branch: a value that is neither a Number nor a numeric-looking
    // String (e.g. a boolean) must be reported the same way as the string case above.
    assertThatThrownBy(() -> consume("RETURN point({x: true, y: 1}) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("point()")
        .hasMessageContaining("x");
  }

  // ===================== valid-input controls: confirm the fix does not disturb success paths =====================

  @Test
  void validInputsStillSucceed() {
    assertThat(single("RETURN range(1, 5, 1) AS r").toString()).contains("1", "5");
    assertThat(single("RETURN date('2026-08-03') AS r")).isNotNull();
    assertThat(single("RETURN datetime('2026-08-03T12:00:00Z') AS r")).isNotNull();
    assertThat(single("RETURN point({x: 1, y: 1}).x AS r")).isEqualTo(1.0d);
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
