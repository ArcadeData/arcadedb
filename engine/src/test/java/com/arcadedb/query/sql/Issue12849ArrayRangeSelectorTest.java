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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

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

  private Object value(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().getProperty("v");
    }
  }
}
