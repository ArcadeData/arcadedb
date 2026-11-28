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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code percentileDisc(DISTINCT x, p)} and {@code percentileCont(DISTINCT x, p)} ignored DISTINCT: the factory built the plain
 * function whatever the flag, so a repeated value counted every time it appeared. The expected values are Neo4j 5.26's answers.
 */
class CypherPercentileDistinctTest extends TestHelper {

  @Test
  void percentileDiscOverDistinctValues() {
    // distinct values 1,2,3,4,5: the nearest rank of 0.5 is the 3rd
    assertThat(single("UNWIND [1, 1, 2, 3, 4, 5] AS x RETURN percentileDisc(DISTINCT x, 0.5) AS v")).isEqualTo(3L);
    // distinct values 1,2,3: the nearest rank of 0.5 is the 2nd, not the 1st of the seven rows
    assertThat(single("UNWIND [1, 1, 1, 1, 2, 2, 3] AS x RETURN percentileDisc(DISTINCT x, 0.5) AS v")).isEqualTo(2L);
    assertThat(single("UNWIND [3.0, 3.0, 2.5, 1.0, 5.0e-324] AS x RETURN percentileDisc(DISTINCT x, 0.5) AS v")).isEqualTo(1.0);
  }

  @Test
  void percentileContOverDistinctValues() {
    assertThat(single("UNWIND [1, 1, 2, 3, 4, 5] AS x RETURN percentileCont(DISTINCT x, 0.5) AS v")).isEqualTo(3.0);
    assertThat(single("UNWIND [3.0, 3.0, 2.5, 1.0, 5.0e-324] AS x RETURN percentileCont(DISTINCT x, 0.5) AS v")).isEqualTo(1.75);
  }

  @Test
  void distinctTreatsAnIntegerAndItsFloatAsOneValue() {
    // 1 and 1.0 are one value for DISTINCT, as for count(DISTINCT x): the distinct values are 1, 2, 3
    assertThat(single("UNWIND [1, 1.0, 2, 3] AS x RETURN percentileDisc(DISTINCT x, 0.5) AS v")).isEqualTo(2L);
  }

  @Test
  void distinctIsPerGroup() {
    try (final ResultSet rs = database.query("opencypher",
        "UNWIND [[0, 1], [0, 1], [0, 2], [0, 3], [1, 10], [1, 10], [1, 10], [1, 20]] AS row "
            + "RETURN row[0] AS g, percentileDisc(DISTINCT row[1], 0.5) AS d, percentileCont(DISTINCT row[1], 0.5) AS c ORDER BY g")) {
      assertThat(rs.hasNext()).isTrue();
      Object[] r = row(rs);
      assertThat(r[0]).isEqualTo(0L);
      assertThat(r[1]).isEqualTo(2L);
      assertThat(r[2]).isEqualTo(2.0);
      r = row(rs);
      assertThat(r[0]).isEqualTo(1L);
      assertThat(r[1]).isEqualTo(10L);
      assertThat(r[2]).isEqualTo(15.0);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void withoutDistinctEveryRowCounts() {
    assertThat(single("UNWIND [1, 1, 2, 3, 4, 5] AS x RETURN percentileDisc(x, 0.5) AS v")).isEqualTo(2L);
    assertThat(single("UNWIND [1, 1, 2, 3, 4, 5] AS x RETURN percentileCont(x, 0.5) AS v")).isEqualTo(2.5);
  }

  @Test
  void distinctOverNothingAndNulls() {
    assertThat(single("UNWIND [] AS x RETURN percentileDisc(DISTINCT x, 0.5) AS v")).isNull();
    assertThat(single("UNWIND [null, null, 4, 4] AS x RETURN percentileCont(DISTINCT x, 0.5) AS v")).isEqualTo(4.0);
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().getProperty("v");
    }
  }

  private static Object[] row(final ResultSet rs) {
    final var r = rs.next();
    return new Object[] { r.getProperty("g"), r.getProperty("d"), r.getProperty("c") };
  }
}
