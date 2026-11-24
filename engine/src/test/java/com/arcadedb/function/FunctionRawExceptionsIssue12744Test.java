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
package com.arcadedb.function;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.function.text.TrimFunction;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for #12744: values outside the census of #11589 reached the client as a raw JDK or library exception (HTTP 500).
 */
class FunctionRawExceptionsIssue12744Test extends TestHelper {

  @Test
  void aBadValueIsAClientErrorNotAServerFault() {
    // the exact category, not only "not SERVER": a regex or a shape the caller got wrong is VALIDATION, an instant past the
    // java.time range is ARITHMETIC, as the date + duration range errors of #12706 are
    for (final Object[] statement : new Object[][] {
        { "opencypher", "RETURN text.regexReplace('abc', 'b', '$2') AS v", ErrorCategory.VALIDATION },
        { "sql", "SELECT text.regexReplace('abc', 'b', '$2') AS v", ErrorCategory.VALIDATION },
        { "opencypher", "RETURN text.regexReplace('abc', 'b', 'x$') AS v", ErrorCategory.VALIDATION },
        { "opencypher", "RETURN datetime.fromepoch(9223372036854775807, 1) AS v", ErrorCategory.ARITHMETIC },
        { "opencypher", "RETURN datetime.fromepoch(9223372036854775807, -1) AS v", ErrorCategory.ARITHMETIC },
        { "opencypher", "RETURN datetime.fromepoch(-9223372036854775808, 0) AS v", ErrorCategory.ARITHMETIC },
        { "sql", "SELECT circle(-1, -1, -1) AS v", ErrorCategory.VALIDATION },
        { "sql", "SELECT circle(200, 0, 1) AS v", ErrorCategory.VALIDATION },
        { "sql", "SELECT rectangle(200, 0, 201, 1) AS v", ErrorCategory.VALIDATION },
        { "sql", "SELECT rectangle(0, 100, 1, 0) AS v", ErrorCategory.VALIDATION } }) {
      final String language = (String) statement[0];
      final String text = (String) statement[1];
      final Throwable failure = failureOf(language, text);
      assertThat(failure).as(text).isNotNull();
      assertThat(ErrorCategory.of(failure)).as(text + " -> " + failure).isEqualTo(statement[2]);
    }
  }

  @Test
  void aNullModeOrValueAnswersNull() {
    // null in, null out, as the other arguments of the same functions already do
    assertThat(single("opencypher", "RETURN trim(null, 'x', 'abc') AS v")).isNull();
    assertThat(single("opencypher", "RETURN btrim(null, 'x', 'abc') AS v")).isNull();
    assertThat(single("opencypher", "RETURN date.add(1000, null, 'ms') AS v")).isNull();
    // openCypher trim()/btrim() resolve to CypherTrimFunction; the TrimFunction it reuses the strip helpers of answers the same
    assertThat(new TrimFunction().execute(new Object[] { null, "x", "abc" }, null)).isNull();
    assertThat(single("sql", "SELECT date.add(1000, null, 'ms') AS v")).isNull();
  }

  @Test
  void theValuesInsideTheRangeStillAnswer() {
    assertThat(single("opencypher", "RETURN text.regexReplace('abc', '(b)', '[$1]') AS v")).isEqualTo("a[b]c");
    assertThat(single("opencypher", "RETURN trim('LEADING', 'x', 'xxabcxx') AS v")).isEqualTo("abcxx");
    assertThat(single("opencypher", "RETURN date.add(1000, 5, 'ms') AS v")).isEqualTo(1005L);
    assertThat(single("opencypher", "RETURN datetime.fromepoch(0, 1500000000).epochMillis AS v")).isEqualTo(1500L);
    assertThat(single("sql", "SELECT circle(10, 20, 5) AS v")).isNotNull();
    // the try/catch around the shape factory keeps the Y ordering rectangle() already applied: a top-left and a bottom-right
    // corner (the larger Y first) is the same rectangle as the lower-left and upper-right one
    assertThat(single("sql", "SELECT rectangle(-1, 1, 1, -1) AS v")).isNotNull();
    assertThat(String.valueOf(single("sql", "SELECT rectangle(-1, 1, 1, -1) AS v")))
        .isEqualTo(String.valueOf(single("sql", "SELECT rectangle(-1, -1, 1, 1) AS v")));
  }

  private Object single(final String language, final String statement) {
    try (final ResultSet rs = database.command(language, statement)) {
      return rs.next().getProperty("v");
    }
  }

  private Throwable failureOf(final String language, final String statement) {
    try (final ResultSet rs = database.command(language, statement)) {
      while (rs.hasNext())
        rs.next().toJSON();
      return null;
    } catch (final Throwable e) {
      if (database.isTransactionActive())
        database.rollback();
      return e;
    }
  }
}
