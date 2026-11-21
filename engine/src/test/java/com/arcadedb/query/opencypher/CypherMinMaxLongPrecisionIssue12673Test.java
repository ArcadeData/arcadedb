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
