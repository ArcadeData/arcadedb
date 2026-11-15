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
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #11661: ORDER BY evaluated its expressions on every comparison, so ORDER BY rand() gave a row a new key each
 * time the sort looked at it and the sort failed with "Comparison method violates its general contract!" in a few percent of runs.
 */
class CypherOrderByRandIssue11661Test extends TestHelper {

  /** At a failure rate of 4% a run, 300 runs all passing has a probability below 1 in 100,000 on the broken code. */
  @Test
  void orderByRandShufflesEveryRowAndNeverFails() {
    final List<Long> all = LongStream.rangeClosed(1, 100).boxed().toList();
    for (int run = 0; run < 300; run++) {
      assertThat(values("UNWIND range(1, 100) AS i RETURN i AS v ORDER BY rand()")).containsExactlyInAnyOrderElementsOf(all);
      assertThat(values("UNWIND range(1, 100) AS i RETURN i AS v ORDER BY i % 7, rand()")).containsExactlyInAnyOrderElementsOf(all);
    }
  }

  /** With a LIMIT the step keeps a top-K heap of rows with their keys: ascending, descending and mixed directions. */
  @Test
  void orderByWithLimitKeepsTheTopRowsByTheirKeys() {
    assertThat(values("UNWIND range(1, 100) AS i RETURN i AS v ORDER BY i % 7, i LIMIT 10")).containsExactly(7L, 14L, 21L,
        28L, 35L, 42L, 49L, 56L, 63L, 70L);
    assertThat(values("UNWIND range(1, 100) AS i RETURN i AS v ORDER BY i % 7 DESC, i DESC LIMIT 10")).containsExactly(97L,
        90L, 83L, 76L, 69L, 62L, 55L, 48L, 41L, 34L);
    assertThat(values("UNWIND range(1, 100) AS i RETURN i AS v ORDER BY i % 7 DESC, i LIMIT 10")).containsExactly(6L, 13L,
        20L, 27L, 34L, 41L, 48L, 55L, 62L, 69L);
    assertThat(values("UNWIND range(1, 100) AS i RETURN i AS v ORDER BY i % 7, i SKIP 12 LIMIT 4")).containsExactly(91L, 98L,
        1L, 8L);
    // a heap as large as the input keeps every row
    assertThat(values("UNWIND range(1, 20) AS i RETURN i AS v ORDER BY -i LIMIT 50")).containsExactlyElementsOf(
        LongStream.rangeClosed(1, 20).map(i -> 21 - i).boxed().toList());
  }

  /** ORDER BY rand() with a LIMIT: the heap keeps K distinct rows of the input. */
  @Test
  void orderByRandWithLimitKeepsDistinctRows() {
    final List<Long> all = LongStream.rangeClosed(1, 100).boxed().toList();
    for (int run = 0; run < 50; run++) {
      final List<Long> top = values("UNWIND range(1, 100) AS i RETURN i AS v ORDER BY rand() LIMIT 10");
      assertThat(top).hasSize(10).doesNotHaveDuplicates();
      assertThat(all).containsAll(top);
      assertThat(values("UNWIND range(1, 100) AS i RETURN i AS v ORDER BY i % 7, rand() LIMIT 14"))
          .containsExactlyInAnyOrder(7L, 14L, 21L, 28L, 35L, 42L, 49L, 56L, 63L, 70L, 77L, 84L, 91L, 98L);
    }
  }

  private List<Long> values(final String query) {
    final List<Long> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      rs.forEachRemaining(r -> values.add(r.<Number>getProperty("v").longValue()));
    }
    return values;
  }
}
