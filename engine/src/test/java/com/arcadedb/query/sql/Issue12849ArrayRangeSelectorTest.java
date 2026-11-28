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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12849: the array range selector answered the last element for a range that starts at the list's length, wrapped an
 * inclusive end of {@code Integer.MAX_VALUE} to null, threw for an end below zero, and threw a NullPointerException for the open
 * ranges {@code [1 ..]} and {@code [ .. 2]}.
 */
class Issue12849ArrayRangeSelectorTest extends TestHelper {
  @Test
  void aRangeOutsideTheListIsEmpty() {
    assertThat(value("SELECT [10, 20][2..3] AS v")).isEqualTo(List.of());
    assertThat(value("SELECT [10, 20][2..9] AS v")).isEqualTo(List.of());
    assertThat(value("SELECT [10, 20][3..9] AS v")).isEqualTo(List.of());
    assertThat(value("SELECT [10, 20][2...3] AS v")).isEqualTo(List.of());
    assertThat(value("SELECT [10, 20, 30][-3..-1] AS v")).isEqualTo(List.of());
    assertThat(value("SELECT [10, 20, 30][-3..0] AS v")).isEqualTo(List.of());
  }

  @Test
  void aRangeInsideOrAcrossTheEndsKeepsItsElements() {
    assertThat(value("SELECT [10, 20][1..3] AS v")).isEqualTo(List.of(20));
    assertThat(value("SELECT [10, 20, 30][1..2] AS v")).isEqualTo(List.of(20));
    assertThat(value("SELECT [10, 20, 30][1...2] AS v")).isEqualTo(List.of(20, 30));
    assertThat(value("SELECT [10, 20, 30][-1..5] AS v")).isEqualTo(List.of(10, 20, 30));
    assertThat(value("SELECT [10, 20, 30][1..1] AS v")).isEqualTo(List.of());
  }

  @Test
  void anInclusiveEndAtIntegerMaxValueDoesNotWrap() {
    assertThat(value("SELECT [10, 20, 30][0...2147483647] AS v")).isEqualTo(List.of(10, 20, 30));
    assertThat(value("SELECT [10, 20, 30][1...2147483647] AS v")).isEqualTo(List.of(20, 30));
  }

  @Test
  void aReversedRangeIsStillNull() {
    assertThat(value("SELECT [10, 20, 30][3..1] AS v")).isNull();
    assertThat(value("SELECT [10, 20, 30][0..-1] AS v")).isNull();
  }

  @Test
  void anOpenRangeRunsFromTheStartOrToTheEnd() {
    assertThat(value("SELECT [10, 20, 30][1 ..] AS v")).isEqualTo(List.of(20, 30));
    assertThat(value("SELECT [10, 20, 30][ .. 2] AS v")).isEqualTo(List.of(10, 20));
    assertThat(value("SELECT [10, 20, 30][ ... 1] AS v")).isEqualTo(List.of(10, 20));
    assertThat(value("SELECT [10, 20, 30][1 ...] AS v")).isEqualTo(List.of(20, 30));
    assertThat(value("SELECT [10, 20, 30][ .. ] AS v")).isEqualTo(List.of(10, 20, 30));
    // the bounds are expressions as well as literals
    assertThat(value("SELECT [10, 20, 30][1 .. (1 + 1)] AS v")).isEqualTo(List.of(20));
    assertThat(value("SELECT [10, 20, 30][(2 - 1) ..] AS v")).isEqualTo(List.of(20, 30));
  }

  @Test
  void emptyAndNonCollectionReceiversAreUnchanged() {
    assertThat(value("SELECT [][0..1].size() AS v")).isEqualTo(0);
    assertThat(value("SELECT null[0..1] AS v")).isNull();
  }

  @Test
  void aParameterBoundIsReadAtRuntime() {
    assertThat(value("SELECT [10, 20, 30][?..?] AS v", 1, 3)).isEqualTo(List.of(20, 30));
    assertThat(value("SELECT [10, 20, 30][?...?] AS v", 0, 1)).isEqualTo(List.of(10, 20));
    assertThat(value("SELECT [10, 20, 30][:from..:to] AS v", Map.of("from", 2, "to", 9))).isEqualTo(List.of(30));
    assertThat(value("SELECT [10, 20, 30][:from..:to] AS v", Map.of("from", 3, "to", 9))).isEqualTo(List.of());
    assertThat(value("SELECT [10, 20, 30][:from..:to] AS v", Map.of("from", -3, "to", -1))).isEqualTo(List.of());
    assertThat(value("SELECT [10, 20, 30][0 ... :to] AS v", Map.of("to", Integer.MAX_VALUE))).isEqualTo(List.of(10, 20, 30));
    assertThat(value("SELECT [10, 20, 30][:from ..] AS v", Map.of("from", 1))).isEqualTo(List.of(20, 30));
    assertThat(value("SELECT [10, 20, 30][ .. :to] AS v", Map.of("to", 2))).isEqualTo(List.of(10, 20));
    assertThat(value("SELECT [10, 20, 30][:from..:to] AS v", Map.of("from", 2, "to", 1))).isNull();
  }

  /**
   * A bound that evaluates to null is the open side, as a bound that is not written: a null FROM always read as 0, a null TO
   * threw a NullPointerException.
   */
  @Test
  void aNullBoundIsTheOpenSide() {
    final Map<String, Object> params = new HashMap<>();
    params.put("from", null);
    params.put("to", 2);
    assertThat(value("SELECT [10, 20, 30][:from..:to] AS v", params)).isEqualTo(List.of(10, 20));
    params.put("from", 1);
    params.put("to", null);
    assertThat(value("SELECT [10, 20, 30][:from..:to] AS v", params)).isEqualTo(List.of(20, 30));
    assertThat(value("SELECT [10, 20, 30][:from...:to] AS v", params)).isEqualTo(List.of(20, 30));
    assertThat(value("SELECT [10, 20, 30][1 .. null] AS v")).isEqualTo(List.of(20, 30));
  }

  /**
   * A bound above the int range saturates: narrowed to its low 32 bits, 4294967297 was read as 1 and 4294967296 as 0.
   */
  @Test
  void aBoundBeyondTheIntRangeDoesNotWrap() {
    assertThat(value("SELECT [10, 20, 30][0 .. :to] AS v", Map.of("to", 4294967297L))).isEqualTo(List.of(10, 20, 30));
    assertThat(value("SELECT [10, 20, 30][0 ... :to] AS v", Map.of("to", Long.MAX_VALUE))).isEqualTo(List.of(10, 20, 30));
    assertThat(value("SELECT [10, 20, 30][:from ..] AS v", Map.of("from", 4294967296L))).isEqualTo(List.of());
    // a start past an end of 5 is a reversed range, where it answered the whole list
    assertThat(value("SELECT [10, 20, 30][:from..5] AS v", Map.of("from", 4294967296L))).isNull();
    assertThat(value("SELECT [10, 20, 30][:from..5] AS v", Map.of("from", -4294967295L))).isEqualTo(List.of(10, 20, 30));
  }

  /**
   * The rendering of a statement is parsed again (a cached or copied statement, the default alias of a projection): a digit
   * glued to the operator lexes as a float, so {@code [1 ..]} rendered as {@code [1..]} did not parse.
   */
  @Test
  void theRenderingOfARangeParsesBackToTheSameRange() {
    for (final String range : List.of("[1 ..]", "[1 ...]", "[ .. 2]", "[ ... 1]", "[ .. ]", "[1 .. (1 + 1)]", "[(2 - 1) ..]",
        "[(2 - 1) .. 3]", "[2 - 1 .. 3]", "[2 - 1 ..]", "[1..2]", "[0...1]", "[-3..-1]")) {
      final String sql = "SELECT [10, 20, 30]" + range + " AS v";
      final String rendered = new SQLAntlrParser(null).parse(sql).toString();
      assertThat(value(rendered)).as("%s rendered as %s", sql, rendered).isEqualTo(value(sql));
      assertThat(new SQLAntlrParser(null).parse(rendered).toString()).as("re-render of %s", sql).isEqualTo(rendered);
    }
  }

  private Object value(final String sql, final Object... params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      return rs.next().getProperty("v");
    }
  }

  private Object value(final String sql, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      return rs.next().getProperty("v");
    }
  }

  private Object value(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().getProperty("v");
    }
  }
}
