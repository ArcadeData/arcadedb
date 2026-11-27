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
package com.arcadedb.query.sql.method;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12836: {@code append()}, {@code indexOf()}, {@code lastIndexOf()} and {@code normalize()} removed one pair of
 * surrounding quotes from the VALUE of their argument, which the SQL parser had already unquoted: text that legitimately starts
 * and ends with a quote character was silently changed ({@code 'x'.append("'q'")} was {@code xq}).
 */
class Issue12836MethodArgumentQuotesTest extends TestHelper {
  @Test
  void appendKeepsTheQuotesOfItsArgument() {
    assertThat(value("SELECT 'x'.append(\"'q'\") AS v")).isEqualTo("x'q'");
    assertThat(value("SELECT 'x'.append('a', \"\\\"b\\\"\") AS v")).isEqualTo("xa\"b\"");
    // the same value reaches the method as a bound parameter
    assertThat(value("SELECT 'x'.append(:s) AS v", Map.of("s", "`q`"))).isEqualTo("x`q`");
    // the siblings that never stripped
    assertThat(value("SELECT 'x'.prefix(\"'q'\") AS v")).isEqualTo("'q'x");
  }

  @Test
  void indexOfAndLastIndexOfSearchTheExactText() {
    assertThat(value("SELECT \"'q'\".indexOf(\"'q'\") AS v")).isEqualTo(0);
    assertThat(value("SELECT \"'q'\".lastIndexOf(\"'q'\") AS v")).isEqualTo(0);
    assertThat(value("SELECT \"it's 'x'\".indexOf(:n) AS v", Map.of("n", "'x'"))).isEqualTo(5);
    assertThat(value("SELECT \"it's 'x'\".lastIndexOf(:n) AS v", Map.of("n", "'x'"))).isEqualTo(5);
    // a text that is not quoted is searched as before
    assertThat(value("SELECT 'abcabc'.indexOf('c') AS v")).isEqualTo(2);
    assertThat(value("SELECT 'abcabc'.lastIndexOf('c') AS v")).isEqualTo(5);
  }

  @Test
  void normalizeReadsItsRemovalPatternAsGiven() {
    // the pattern 'b' (quotes included) matches nothing in "abc"
    assertThat(value("SELECT 'abc'.normalize('NFC', \"'b'\") AS v")).isEqualTo("abc");
    assertThat(value("SELECT 'abc'.normalize('NFC', 'b') AS v")).isEqualTo("ac");
    assertThat(value("SELECT 'é'.normalize('NFD') AS v")).isEqualTo("e");
  }

  private Object value(final String sql) {
    return value(sql, Map.of());
  }

  private Object value(final String sql, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      return rs.next().getProperty("v");
    }
  }
}
