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

  private List<Long> values(final String query) {
    final List<Long> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      rs.forEachRemaining(r -> values.add(r.<Number>getProperty("v").longValue()));
    }
    return values;
  }
}
