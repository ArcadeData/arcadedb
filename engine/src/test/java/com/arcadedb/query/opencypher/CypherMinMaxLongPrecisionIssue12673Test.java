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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The openCypher order of two numbers is the order of their values. {@code min()}, {@code max()}, {@code apoc.coll.min/max/sort}
 * and {@code ORDER BY} over an INTEGER and a FLOAT read both as {@code double}, which holds no integer past 2^53: three
 * identifiers 1500000000000000001..3 were one value to them, so {@code max()} and {@code min()} answered the first row each.
 */
class CypherMinMaxLongPrecisionIssue12673Test extends TestHelper {
  private static final String IDS = "[1500000000000000003, 1500000000000000001, 1500000000000000002]";

  @Test
  void minAndMaxOfLongsPastTheDoublePrecision() {
    assertThat(single("UNWIND " + IDS + " AS x RETURN max(x) AS v")).isEqualTo(1500000000000000003L);
    assertThat(single("UNWIND " + IDS + " AS x RETURN min(x) AS v")).isEqualTo(1500000000000000001L);
    // whatever the row order
    assertThat(single("UNWIND [1500000000000000001, 1500000000000000003, 1500000000000000002] AS x RETURN max(x) AS v"))
        .isEqualTo(1500000000000000003L);
    assertThat(single("UNWIND [1500000000000000002, 1500000000000000003, 1500000000000000001] AS x RETURN min(x) AS v"))
        .isEqualTo(1500000000000000001L);
  }

  @Test
  void minAndMaxOfStoredLongs() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (:Id {n: 1500000000000000002}), (:Id {n: 1500000000000000003}), (:Id {n: 1500000000000000001})"));
    assertThat(single("MATCH (i:Id) RETURN max(i.n) AS v")).isEqualTo(1500000000000000003L);
    assertThat(single("MATCH (i:Id) RETURN min(i.n) AS v")).isEqualTo(1500000000000000001L);
  }

  @Test
  void minAndMaxOfALongAndAFloatThatDoubleCannotTellApart() {
    // 2^53 + 1 is above the float 2^53 although both are the same double
    assertThat(single("UNWIND [9007199254740993, 9007199254740992.0] AS x RETURN max(x) AS v")).isEqualTo(9007199254740993L);
    assertThat(single("UNWIND [9007199254740992.0, 9007199254740993] AS x RETURN max(x) AS v")).isEqualTo(9007199254740993L);
    assertThat(single("UNWIND [9007199254740993, 9007199254740992.0] AS x RETURN min(x) AS v")).isEqualTo(9007199254740992.0);
    assertThat(single("UNWIND [9007199254740992.0, 9007199254740993] AS x RETURN min(x) AS v")).isEqualTo(9007199254740992.0);
  }

  @Test
  void orderByOfALongAndAFloatThatDoubleCannotTellApart() {
    assertThat(column("UNWIND [9007199254740993, 9007199254740992.0] AS x RETURN x AS v ORDER BY x"))
        .containsExactly(9007199254740992.0, 9007199254740993L);
    assertThat(column("UNWIND [9007199254740992.0, 9007199254740993] AS x RETURN x AS v ORDER BY x"))
        .containsExactly(9007199254740992.0, 9007199254740993L);
    assertThat(column("UNWIND [9007199254740992.0, 9007199254740993] AS x RETURN x AS v ORDER BY x DESC"))
        .containsExactly(9007199254740993L, 9007199254740992.0);
    assertThat(column("UNWIND [9007199254740993, 9007199254740992.0] AS x RETURN x AS v ORDER BY x DESC"))
        .containsExactly(9007199254740993L, 9007199254740992.0);
  }

  @Test
  void orderByTiesNegativeZeroWithZeroWhateverTheirClass() {
    // -0.0, 0 and 0.0 are one value to ORDER BY as they are to min() / max() and to DISTINCT: the stable sort keeps them in the
    // order they arrived in. Two doubles were ordered by Double.compareTo (-0.0 below 0.0) while an INTEGER against either of them
    // tied, so -0.0 = 0 = 0.0 but -0.0 < 0.0, an order the sort could not follow
    assertThat(column("UNWIND [0.0, 0, -0.0] AS x RETURN x AS v ORDER BY x")).containsExactly(0.0, 0L, -0.0);
    assertThat(column("UNWIND [-0.0, 0, 0.0] AS x RETURN x AS v ORDER BY x")).containsExactly(-0.0, 0L, 0.0);
    assertThat(column("UNWIND [0.0, -0.0] AS x RETURN x AS v ORDER BY x")).containsExactly(0.0, -0.0);
    assertThat(column("UNWIND [0.0, 0, -0.0] AS x RETURN x AS v ORDER BY x DESC")).containsExactly(0.0, 0L, -0.0);

    final List<Object> zeros = new ArrayList<>();
    final StringBuilder list = new StringBuilder("[");
    for (int i = 0; i < 96; i++) {
      final int kind = (i * 7 + i / 5) % 3;
      zeros.add(kind == 0 ? (Object) 0.0 : kind == 1 ? (Object) 0L : (Object) (-0.0));
      list.append(i > 0 ? ", " : "").append(kind == 0 ? "0.0" : kind == 1 ? "0" : "-0.0");
    }
    list.append(']');
    assertThat(column("UNWIND " + list + " AS x RETURN x AS v ORDER BY x")).isEqualTo(zeros);
  }

  @Test
  void orderByPutsNaNAboveEveryNumberAndKeepsTheLongsApart() {
    assertThat(column("UNWIND [1500000000000000003, 0.0 / 0.0, 1500000000000000001, 1.5, 1500000000000000002] AS x RETURN x AS v ORDER BY x"))
        .containsExactly(1.5, 1500000000000000001L, 1500000000000000002L, 1500000000000000003L, Double.NaN);
    assertThat(single("UNWIND [1500000000000000003, 0.0 / 0.0, 1500000000000000001] AS x RETURN min(x) AS v")).isEqualTo(1500000000000000001L);
  }

  @Test
  void percentileDiscOfLongsPastTheDoublePrecision() {
    assertThat(single("UNWIND " + IDS + " AS x RETURN percentileDisc(x, 0.5) AS v")).isEqualTo(1500000000000000002L);
    assertThat(single("UNWIND " + IDS + " AS x RETURN percentileDisc(x, 0.0) AS v")).isEqualTo(1500000000000000001L);
    assertThat(single("UNWIND " + IDS + " AS x RETURN percentileDisc(x, 1.0) AS v")).isEqualTo(1500000000000000003L);
  }

  @Test
  void collectionMinMaxAndSortOfLongsPastTheDoublePrecision() {
    assertThat(single("RETURN apoc.coll.max(" + IDS + ") AS v")).isEqualTo(1500000000000000003L);
    assertThat(single("RETURN apoc.coll.min(" + IDS + ") AS v")).isEqualTo(1500000000000000001L);
    assertThat(single("RETURN apoc.coll.sort(" + IDS + ") AS v")).isEqualTo(List.of(1500000000000000001L, 1500000000000000002L, 1500000000000000003L));
  }

  @Test
  void theValueOrderOfSmallNumbersIsUnchanged() {
    assertThat(single("UNWIND [3, 1.5, 2, -1, 2.5] AS x RETURN max(x) AS v")).isEqualTo(3L);
    assertThat(single("UNWIND [3, 1.5, 2, -1, 2.5] AS x RETURN min(x) AS v")).isEqualTo(-1L);
    assertThat(single("UNWIND [0.0, -0.0, 0] AS x RETURN count(DISTINCT x) AS v")).isEqualTo(1L);
    assertThat(single("UNWIND [1, 1.0] AS x RETURN max(x) AS v")).isIn(1L, 1.0);
    assertThat(column("UNWIND [2, 1.5, 1, 2.5] AS x RETURN x AS v ORDER BY x")).containsExactly(1L, 1.5, 2L, 2.5);
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.command("opencypher", query)) {
      return rs.next().getProperty("v");
    }
  }

  private List<Object> column(final String query) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", query)) {
      while (rs.hasNext())
        values.add(rs.next().getProperty("v"));
    }
    return values;
  }
}
