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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10254: a list kept next to an aggregate in a WITH ({@code WITH r1, collect(DISTINCT b) AS r2}) is the very same
 * object on every row, and the grouping canonicalized and hashed it again for each of them, so the query was quadratic.
 * The cost is pinned by counting the element reads of a list passed as a parameter, which reaches every row as the same
 * object: a per-row walk reads it once per row, the memory of the row before reads it a bounded number of times.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10254ListGroupingKeyTest extends TestHelper {
  private static final int ROWS   = 1_000;
  private static final int LENGTH = 500;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE A");
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex("A").set("i", i).set("g", i % 3).save();
    });
  }

  @Test
  void issueShapesAnswerTheSame() {
    final String list = "MATCH (a:A) WITH collect(a) AS r1 MATCH (b:A) WITH r1, collect(DISTINCT b) AS r2 RETURN size(r1) AS s1, size(r2) AS s2";
    final String scalar = "MATCH (a:A) WITH count(a) AS c1 MATCH (b:A) WITH c1, collect(DISTINCT b) AS r2 RETURN c1 AS s1, size(r2) AS s2";
    for (final String query : new String[] { list, scalar }) {
      final Result row = single(query, Map.of());
      assertThat(row.<Number>getProperty("s1").intValue()).isEqualTo(ROWS);
      assertThat(row.<Number>getProperty("s2").intValue()).isEqualTo(ROWS);
    }

    // the chain of the discussion that found it: each WITH keeps the lists of the WITH before as keys
    final Result chained = single("MATCH (a:A) WITH collect(a) AS r1 MATCH (b:A) WITH r1, collect(DISTINCT b) AS r2 "
        + "MATCH (c:A) WITH r1, r2, collect(DISTINCT c) AS r3 RETURN size(r1) AS s1, size(r2) AS s2, size(r3) AS s3", Map.of());
    assertThat(chained.<Number>getProperty("s3").intValue()).isEqualTo(ROWS);
  }

  @Test
  void singleListKeyIsNotWalkedPerRow() {
    final CountingList key = new CountingList(LENGTH);
    final Result row = single("MATCH (b:A) WITH $l AS r1, b WITH r1, collect(DISTINCT b) AS r2 RETURN size(r1) AS s1, size(r2) AS s2",
        Map.of("l", key));
    assertThat(row.<Number>getProperty("s1").intValue()).isEqualTo(LENGTH);
    assertThat(row.<Number>getProperty("s2").intValue()).isEqualTo(ROWS);
    assertBounded(key);
  }

  @Test
  void listKeyNextToAVaryingKeyIsNotWalkedPerRow() {
    final CountingList key = new CountingList(LENGTH);
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (b:A) WITH $l AS r1, b WITH r1, b.g AS g, count(b) AS n RETURN size(r1) AS s, g, n ORDER BY g", Map.of("l", key))) {
      final List<Long> counts = new ArrayList<>();
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.<Number>getProperty("s").intValue()).isEqualTo(LENGTH);
        counts.add(row.<Number>getProperty("n").longValue());
      }
      assertThat(counts).containsExactly(334L, 333L, 333L);
    }
    assertBounded(key);
  }

  @Test
  void listKeyOfAReturnAggregationIsNotWalkedPerRow() {
    // grouped straight off the label scan, which can aggregate in the workers of a parallel scan
    final CountingList key = new CountingList(LENGTH);
    final Result row = single("MATCH (b:A) RETURN $l AS r1, count(b) AS n", Map.of("l", key));
    assertThat(row.<Number>getProperty("n").longValue()).isEqualTo(ROWS);
    assertBounded(key);
  }

  @Test
  void distinctOverAListIsNotWalkedPerRow() {
    final CountingList key = new CountingList(LENGTH);
    final Result with = single("MATCH (b:A) WITH DISTINCT $l AS r1, b.g AS g RETURN count(*) AS n", Map.of("l", key));
    assertThat(with.<Number>getProperty("n").longValue()).isEqualTo(3L);
    assertBounded(key);

    final CountingList returned = new CountingList(LENGTH);
    try (final ResultSet rs = database.query("opencypher", "MATCH (b:A) RETURN DISTINCT $l AS r1, b.g AS g", Map.of("l", returned))) {
      int rows = 0;
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
      assertThat(rows).isEqualTo(3);
    }
    assertBounded(returned);
  }

  @Test
  void equalListsOfDifferentObjectsStillShareTheirGroup() {
    // [1] and [1.0] are equal under Cypher's = (issue #8561), and two literals are two objects
    try (final ResultSet rs = database.query("opencypher",
        "UNWIND [[1], [1.0], [2], [1], [2, 3], [2]] AS l RETURN l, count(*) AS n ORDER BY n DESC, size(l)")) {
      final List<Long> counts = new ArrayList<>();
      while (rs.hasNext())
        counts.add(rs.next().<Number>getProperty("n").longValue());
      assertThat(counts).containsExactly(3L, 2L, 1L);
    }
    try (final ResultSet rs = database.query("opencypher",
        "UNWIND [null, 1, null, 1.0, null] AS x WITH x, 'k' AS k RETURN x, k, count(*) AS n ORDER BY n DESC")) {
      final List<Long> counts = new ArrayList<>();
      while (rs.hasNext())
        counts.add(rs.next().<Number>getProperty("n").longValue());
      assertThat(counts).containsExactly(3L, 2L);
    }
    try (final ResultSet rs = database.query("opencypher", "UNWIND [[1], [1.0], [1], [2]] AS l RETURN DISTINCT l")) {
      int rows = 0;
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
      assertThat(rows).isEqualTo(2);
    }
  }

  /** A per-row walk reads the list ROWS times over; a bounded number of walks is what the memory of the row before costs. */
  private static void assertBounded(final CountingList key) {
    assertThat(key.reads).as("element reads of a %d-long list over %d rows", LENGTH, ROWS).isLessThan(20L * LENGTH);
  }

  private Result single(final String query, final Map<String, Object> parameters) {
    try (final ResultSet rs = database.query("opencypher", query, parameters)) {
      return rs.next();
    }
  }

  /** A list of distinct longs that counts how many times its elements are read. */
  private static final class CountingList extends AbstractList<Object> {
    private final int  size;
    private       long reads;

    CountingList(final int size) {
      this.size = size;
    }

    @Override
    public Object get(final int index) {
      if (index < 0 || index >= size)
        throw new IndexOutOfBoundsException(index);
      reads++;
      return (long) index;
    }

    @Override
    public int size() {
      return size;
    }
  }
}
