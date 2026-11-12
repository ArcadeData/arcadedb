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

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #11626: a list index or slice bound beyond the int range was narrowed with intValue(), which kept
 * its low 32 bits, so [1,2,3][4294967296] wrapped round to the first element instead of answering null. The extreme longs
 * are the ones Neo4j's ExtremeValuesLibrary tests.
 */
class ListIndexBeyondIntRangeIssue11626Test extends TestHelper {

  @Test
  void anIndexBeyondTheIntRangeIsOutOfRange() {
    for (final String index : new String[] { "4294967296", "4294967297", "-4294967296", "9223372036854775807",
        "-9223372036854775808", "2147483648", "-2147483649" })
      assertThat(cypher("RETURN [1, 2, 3][" + index + "] AS r", Map.of())).as(index).isNull();
    assertThat(cypher("RETURN [1, 2, 3][$i] AS r", Map.of("i", 4294967296L))).isNull();
    assertThat(cypher("RETURN $l[$i] AS r", Map.of("l", List.of(1, 2, 3), "i", -4294967296L))).isNull();
    // in range, and from the end, unchanged
    assertThat(cypher("RETURN [1, 2, 3][2] AS r", Map.of())).isEqualTo(3L);
    assertThat(cypher("RETURN [1, 2, 3][-3] AS r", Map.of())).isEqualTo(1L);
  }

  @Test
  void aSliceBoundBeyondTheIntRangeIsCutAtTheEndOfTheList() {
    assertThat(cypher("RETURN [1, 2, 3][1..9223372036854775807] AS r", Map.of())).isEqualTo(List.of(2L, 3L));
    assertThat(cypher("RETURN [1, 2, 3][0..4294967298] AS r", Map.of())).isEqualTo(List.of(1L, 2L, 3L));
    assertThat(cypher("RETURN [1, 2, 3][..4294967297] AS r", Map.of())).isEqualTo(List.of(1L, 2L, 3L));
    assertThat(cypher("RETURN [1, 2, 3][-4294967295..] AS r", Map.of())).isEqualTo(List.of(1L, 2L, 3L));
    assertThat(cypher("RETURN [1, 2, 3][4294967296..] AS r", Map.of())).isEqualTo(List.of());
    assertThat(cypher("RETURN 'abc'[1..9223372036854775807] AS r", Map.of())).isEqualTo("bc");
  }

  @Test
  void anSqlIndexBeyondTheIntRangeIsOutOfRange() {
    for (final String list : new String[] { "[1, 2, 3]", "list(1, 2, 3)" })
      for (final String index : new String[] { "4294967296", "4294967297", "9223372036854775807" })
        try (final ResultSet rs = database.query("sql", "SELECT " + list + "[" + index + "] AS r")) {
          assertThat(rs.next().<Object>getProperty("r")).as(list + "[" + index + "]").isNull();
        }
  }

  private Object cypher(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      return rs.next().getProperty("r");
    }
  }
}
