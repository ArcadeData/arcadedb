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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A literal list on the right of {@code IN} is turned into one membership probe per execution, but the decision "this right-hand
 * side is constant" was re-taken for every row by walking the whole list from the parse tree ({@code isLiteral} over every item),
 * so a scan cost rows x listSize before a single comparison was made (#12702): 10,000 literals over 100,000 rows took 7.5 s,
 * against 39 ms for the same list bound as a parameter.
 * <p>
 * The cost itself is pinned without a clock by {@code InConditionRightHandSideWalkTest}, which counts the walks. This test runs the
 * shape end to end and checks that the answers did not move: {@code IN} and {@code NOT IN} over a long literal list, on a scan,
 * against the same list bound as a parameter.
 */
class InLiteralListPerRowCostTest extends TestHelper {
  private static final int ROWS = 5_000;

  @Test
  void aLongLiteralInListAnswersLikeTheSameListAsAParameter() {
    database.getSchema().createDocumentType("InLit");
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newDocument("InLit").set("id", i).save();
    });

    // Every even id, plus values no row holds
    final StringBuilder list = new StringBuilder();
    final Integer[] parameter = new Integer[ROWS];
    for (int i = 0; i < ROWS; i++) {
      list.append(i * 2).append(i + 1 < ROWS ? "," : "");
      parameter[i] = i * 2;
    }

    assertThat(count("SELECT id FROM InLit WHERE id IN [" + list + "]")).isEqualTo(ROWS / 2);
    assertThat(count("SELECT id FROM InLit WHERE id NOT IN [" + list + "]")).isEqualTo(ROWS / 2);
    assertThat(count("SELECT id FROM InLit WHERE id IN ?", (Object) parameter)).isEqualTo(ROWS / 2);
    assertThat(count("SELECT id FROM InLit WHERE id NOT IN ?", (Object) parameter)).isEqualTo(ROWS / 2);

    // Only even ids may match the IN
    try (final ResultSet result = database.query("sql", "SELECT id FROM InLit WHERE id IN [" + list + "]")) {
      while (result.hasNext())
        assertThat(result.next().<Integer>getProperty("id") % 2).isEqualTo(0);
    }
  }

  private long count(final String query, final Object... parameters) {
    long matched = 0;
    try (final ResultSet result = database.query("sql", query, parameters)) {
      while (result.hasNext()) {
        result.next();
        matched++;
      }
    }
    return matched;
  }
}
