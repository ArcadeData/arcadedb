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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5284: Cypher chained comparisons (e.g. {@code a < b < c}) must be
 * evaluated as the conjunction of adjacent comparisons ({@code a < b AND b < c}), matching the
 * OpenCypher specification and Neo4j behavior. Previously only the first comparison in the chain
 * was evaluated, silently yielding wrong results.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5284ChainedComparisonTest extends TestHelper {

  private boolean scalarBoolean(final String cypher) {
    final ResultSet rs = database.query("opencypher", cypher);
    assertThat(rs.hasNext()).isTrue();
    final Result r = rs.next();
    return r.<Boolean>getProperty("result");
  }

  @Test
  void scalarChainedComparisons() {
    assertThat(scalarBoolean("RETURN 1 < 2 > 3 AS result")).isFalse();
    assertThat(scalarBoolean("RETURN 1 < 2 < 1 AS result")).isFalse();
    assertThat(scalarBoolean("RETURN 3 > 2 > 3 AS result")).isFalse();

    // fully-true chain still works
    assertThat(scalarBoolean("RETURN 1 < 2 < 3 AS result")).isTrue();

    // explicit AND controls
    assertThat(scalarBoolean("RETURN 1 < 2 AND 2 > 3 AS result")).isFalse();
    assertThat(scalarBoolean("RETURN 3 > 2 AND 2 > 3 AS result")).isFalse();
  }

  @Test
  void whereChainedComparison() {
    database.command("opencypher", "CREATE (:BugA {value: 15}), (:BugA {value: 100})");
    try {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n:BugA) WHERE 10 < n.value < 20 RETURN n.value AS value ORDER BY value");
      final List<Long> values = new ArrayList<>();
      while (rs.hasNext())
        values.add(rs.next().<Number>getProperty("value").longValue());

      assertThat(values).containsExactly(15L);
    } finally {
      database.command("opencypher", "MATCH (n:BugA) DETACH DELETE n");
    }
  }
}
