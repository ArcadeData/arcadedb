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

import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #11584: a grouped one-hop count whose relationship carries an inline predicate,
 * {@code MATCH (a)-[r:R WHERE r.w = 1]->(b) RETURN a.k, count(b)}, was answered by the per-node count shortcut, which
 * counts every edge of the anchor and drops the predicate. Each answer is checked against the same question written
 * with a MATCH-level WHERE, which the shortcut does not claim.
 */
class CypherGroupedCountInlineRelWhereIssue11584Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:A {k: 1}), (b1:B {k: 2}), (b2:B {k: 3}), (c:A {k: 4}), (b3:B {k: 5}), "
            + "(a)-[:R {w: 1}]->(b1), (a)-[:R {w: 2}]->(b2), (c)-[:R {w: 1}]->(b3), (b1)-[:R {w: 1}]->(a)"));
  }

  private Map<Long, Long> grouped(final String query) {
    final Map<Long, Long> out = new TreeMap<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.put(((Number) r.getProperty("k")).longValue(), ((Number) r.getProperty("n")).longValue());
      }
    }
    return out;
  }

  @Test
  void countOfTheCountedNode() {
    final Map<Long, Long> expected = grouped("MATCH (a:A)-[r:R]->(b) WHERE r.w = 1 RETURN a.k AS k, count(b) AS n");
    assertThat(expected).isEqualTo(Map.of(1L, 1L, 4L, 1L));
    assertThat(grouped("MATCH (a:A)-[r:R WHERE r.w = 1]->(b) RETURN a.k AS k, count(b) AS n")).isEqualTo(expected);
  }

  @Test
  void countStar() {
    assertThat(grouped("MATCH (a:A)-[r:R WHERE r.w = 1]->(b) RETURN a.k AS k, count(*) AS n"))
        .isEqualTo(Map.of(1L, 1L, 4L, 1L));
  }

  @Test
  void countFromTheTargetSideAndBothDirections() {
    assertThat(grouped("MATCH (b)<-[r:R WHERE r.w = 2]-(a:A) RETURN a.k AS k, count(b) AS n")).isEqualTo(Map.of(1L, 1L));
    assertThat(grouped("MATCH (a:A)-[r:R WHERE r.w = 1]-(b) RETURN a.k AS k, count(b) AS n")).isEqualTo(Map.of(1L, 2L, 4L, 1L));
  }

  @Test
  void predicateThatMatchesNothing() {
    assertThat(grouped("MATCH (a:A)-[r:R WHERE r.w = 99]->(b) RETURN a.k AS k, count(b) AS n")).isEmpty();
  }
}
