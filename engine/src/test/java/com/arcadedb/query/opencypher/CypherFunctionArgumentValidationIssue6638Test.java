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
 * Regression test for issue #6638: the truncate family ({@code date.truncate()}, {@code time.truncate()},
 * {@code localtime.truncate()}, {@code datetime.truncate()}, {@code localdatetime.truncate()}) answered HTTP 500
 * via a bare {@link com.arcadedb.exception.CommandExecutionException} when the second argument was not a
 * compatible temporal value, instead of the client-facing {@link CommandSemanticException} (HTTP 400) that
 * sibling argument-validation paths already returned since #5794/#5909 and #5910.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherFunctionArgumentValidationIssue6638Test extends TestHelper {

  @Test
  void dateTruncateWithATimeValueIsAClientError() {
    // A TIME value has no DATE component: date.truncate() cannot truncate it.
    assertThatThrownBy(() -> consume("RETURN date.truncate('hour', time('12:31:14Z')) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("date.truncate()")
        .hasMessageContaining("date");
  }

  @Test
  void dateTruncateWithANonTemporalArgumentIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN date.truncate('year', 42) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("date.truncate()");
  }

  @Test
  void timeTruncateWithADateValueIsAClientError() {
    // A DATE value has no TIME component: time.truncate() cannot truncate it.
    assertThatThrownBy(() -> consume("RETURN time.truncate('hour', date('1984-10-11')) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("time.truncate()")
        .hasMessageContaining("time");
  }

  @Test
  void localTimeTruncateWithADateValueIsAClientError() {
    // A DATE value has no TIME component: localtime.truncate() cannot truncate it. (A DATETIME value IS
    // accepted here, its time-of-day extracted, the same way date.truncate() accepts a DATETIME/LOCALDATETIME
    // and extracts its date - see the "still succeeds" control below.)
    assertThatThrownBy(() -> consume("RETURN localtime.truncate('hour', date('1984-10-11')) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("localtime.truncate()")
        .hasMessageContaining("time");
  }

  @Test
  void dateTimeTruncateWithALocalTimeValueIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN datetime.truncate('hour', localtime('12:31:14')) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("datetime.truncate()");
  }

  @Test
  void localDateTimeTruncateWithANonTemporalArgumentIsAClientError() {
    assertThatThrownBy(() -> consume("RETURN localdatetime.truncate('hour', 42) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("localdatetime.truncate()");
  }

  // ===================== valid-input controls: confirm the fix does not disturb success paths =====================

  @Test
  void validInputsStillSucceed() {
    assertThat(single("RETURN date.truncate('year', date('1984-10-11')) AS r").toString()).isEqualTo("1984-01-01");
    assertThat(single("RETURN date.truncate('year', date('1984-10-11'), null) AS r")).isNull();
    assertThat(single("RETURN time.truncate('hour', time('12:31:14Z')) AS r")).isNotNull();
    assertThat(single("RETURN localtime.truncate('hour', localtime('12:31:14')) AS r")).isNotNull();
    assertThat(single("RETURN datetime.truncate('hour', datetime('1984-10-11T12:31:14Z')) AS r")).isNotNull();
    assertThat(single("RETURN localdatetime.truncate('hour', localdatetime('1984-10-11T12:31:14')) AS r")).isNotNull();
    // A DATETIME carries a time-of-day, so localtime.truncate() accepts it and extracts that component,
    // consistent with date.truncate() accepting a DATETIME/LOCALDATETIME and extracting its date.
    assertThat(single("RETURN localtime.truncate('hour', datetime('1984-10-11T12:31:14Z')) AS r").toString()).isEqualTo("12:00");
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
