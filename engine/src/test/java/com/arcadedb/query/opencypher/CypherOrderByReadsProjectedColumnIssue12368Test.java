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
import com.arcadedb.function.java.JavaClassFunctionLibraryDefinition;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12368: an openCypher ORDER BY key that repeats an expression the RETURN (or WITH) just projected under an alias, as
 * {@code RETURN n.external_id AS external_id ... ORDER BY n.external_id}, evaluated it again for every row - for a property, a
 * record read - although the row already held the value. The sort now reads such a key, and any projected expression inside a
 * key, from the projected column, unless a projected name shadows a variable the expression reads, where the two would differ.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherOrderByReadsProjectedColumnIssue12368Test extends TestHelper {

  /** A function that counts how many times it is evaluated. */
  public static class CountingKey {
    static final AtomicInteger CALLS = new AtomicInteger();

    public static long key(final long value) {
      CALLS.incrementAndGet();
      return value % 13;
    }
  }

  @Override
  protected void beginTest() {
    try {
      database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("counting", CountingKey.class));
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  /** The direct statement of the fix: the key the RETURN projected is evaluated by the projection only, not again by the sort. */
  @Test
  void aKeyThatRepeatsAProjectedExpressionIsNotEvaluatedAgain() {
    CountingKey.CALLS.set(0);
    final List<Long> all = longs("UNWIND range(1, 500) AS i RETURN counting.key(i) AS k, i AS v ORDER BY counting.key(i), i", "v");
    assertThat(all).hasSize(500).startsWith(13L, 26L, 39L);
    assertThat(CountingKey.CALLS.get()).isEqualTo(500);

    // the top-k heap of a LIMIT reads the same column
    CountingKey.CALLS.set(0);
    assertThat(longs("UNWIND range(1, 500) AS i RETURN counting.key(i) AS k, i AS v ORDER BY counting.key(i) DESC, i LIMIT 2", "v"))
        .containsExactly(12L, 25L);
    assertThat(CountingKey.CALLS.get()).isEqualTo(500);
  }

  /** A key that only contains a projected expression reads that part from its column: coalesce(rel.confidence, -1.0). */
  @Test
  void aProjectedExpressionInsideAKeyIsReadFromItsColumn() {
    CountingKey.CALLS.set(0);
    assertThat(longs("UNWIND range(1, 500) AS i RETURN counting.key(i) AS k, i AS v ORDER BY coalesce(counting.key(i), -1) DESC, i LIMIT 2",
        "v")).containsExactly(12L, 25L);
    assertThat(CountingKey.CALLS.get()).isEqualTo(500);
  }

  @Test
  void theOrderByOfAWithReadsTheColumnsOfThatWith() {
    CountingKey.CALLS.set(0);
    assertThat(longs("UNWIND range(1, 500) AS i WITH i, counting.key(i) AS k ORDER BY counting.key(i), i LIMIT 3 RETURN i AS v", "v"))
        .containsExactly(13L, 26L, 39L);
    assertThat(CountingKey.CALLS.get()).isEqualTo(500);
  }

  /**
   * The key read from the column IS the projected value: with a non-deterministic expression, a key evaluated again would order
   * the rows by values that are not the ones returned.
   */
  @Test
  void theKeyIsTheValueTheProjectionReturned() {
    for (int run = 0; run < 5; run++) {
      final List<Double> projected = doubles("UNWIND range(1, 200) AS i RETURN rand() AS r ORDER BY rand()", "r");
      assertThat(projected).hasSize(200).isSorted();
      final List<Double> inside = doubles("UNWIND range(1, 200) AS i RETURN rand() AS r ORDER BY coalesce(rand(), 0) DESC", "r");
      assertThat(inside).hasSize(200).isSortedAccordingTo((a, b) -> Double.compare(b, a));
    }
  }

  /** The reporter's neighbourhood read: the property spelling and the alias spelling answer the same rows in the same order. */
  @Test
  void thePropertySpellingAndTheAliasSpellingAgree() {
    database.command("sql", "CREATE VERTEX TYPE Entity");
    database.command("sql", "CREATE EDGE TYPE RELATES_TO");
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(0, 9) AS a CREATE (:Entity {external_id: 'A' + a})");
      database.command("opencypher", "UNWIND range(0, 199) AS b CREATE (:Entity {external_id: 'N' + (1000 + b)})");
      database.command("opencypher", """
          MATCH (e:Entity), (n:Entity) WHERE e.external_id STARTS WITH 'A' AND n.external_id STARTS WITH 'N'
            AND toInteger(substring(n.external_id, 1)) % 10 = toInteger(substring(e.external_id, 1))
          CREATE (e)-[:RELATES_TO {confidence: CASE WHEN toInteger(substring(n.external_id, 1)) % 3 = 0 THEN null
                                                   ELSE (toInteger(substring(n.external_id, 1)) % 7) / 10.0 END}]->(n)""");
    });

    final String match = "MATCH (e:Entity)-[rel:RELATES_TO]->(n:Entity) WHERE e.external_id IN $ids "
        + "RETURN e.external_id AS anchor_id, n.external_id AS external_id, rel.confidence AS confidence ";
    final Map<String, Object> params = Map.of("ids", List.of("A1", "A2", "A3"), "edge_limit", 25);
    final List<String> byProperty = rows(
        match + "ORDER BY coalesce(rel.confidence, -1.0) DESC, n.external_id ASC LIMIT $edge_limit", params);
    final List<String> byAlias = rows(match + "ORDER BY coalesce(confidence, -1.0) DESC, external_id ASC LIMIT $edge_limit", params);
    assertThat(byProperty).hasSize(25).isEqualTo(byAlias);
    assertThat(byProperty.getFirst()).startsWith("0.6");
  }

  /**
   * A projected name that shadows a variable changes what the key means, so there the key is evaluated as written. After
   * {@code RETURN y AS x, x AS y} the {@code x} of the ORDER BY is the column x, which holds the old y: the projected expression
   * {@code x} (the old x, now the column y) must not be substituted into {@code ORDER BY x + 0}.
   */
  @Test
  void aShadowedNameIsNotReadFromTheColumnOfTheVariableItShadows() {
    final String with = "UNWIND [1, 2, 3] AS v WITH v AS x, -v AS y ";
    assertThat(longs(with + "RETURN y AS x, x AS y ORDER BY x", "y")).containsExactly(3L, 2L, 1L);
    assertThat(longs(with + "RETURN y AS x, x AS y ORDER BY x + 0", "y")).containsExactly(3L, 2L, 1L);
    assertThat(longs(with + "RETURN y AS x, x AS y ORDER BY y + 0", "y")).containsExactly(1L, 2L, 3L);
  }

  /**
   * An ORDER BY name that is a projected output name is that column, also when an earlier item projects a variable of the same
   * name: in {@code RETURN DISTINCT b AS x, a AS b ORDER BY b} the key is the column b (the old a), not the column x.
   */
  @Test
  void anOutputNameWinsOverAnEarlierItemProjectingTheVariableOfThatName() {
    final String with = "UNWIND [[1, 30], [2, 20], [3, 10]] AS p WITH p[0] AS a, p[1] AS b ";
    assertThat(longs(with + "RETURN DISTINCT b AS x, a AS b ORDER BY b", "x")).containsExactly(30L, 20L, 10L);
    assertThat(longs(with + "RETURN b AS x, a AS b ORDER BY b", "x")).containsExactly(30L, 20L, 10L);
  }

  private List<Long> longs(final String query, final String column) {
    final List<Long> values = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", query)) {
      rs.forEachRemaining(r -> values.add(r.<Number>getProperty(column).longValue()));
    }
    return values;
  }

  private List<Double> doubles(final String query, final String column) {
    final List<Double> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      rs.forEachRemaining(r -> values.add(r.<Number>getProperty(column).doubleValue()));
    }
    return values;
  }

  private List<String> rows(final String query, final Map<String, Object> params) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.add(row.getProperty("confidence") + "|" + row.getProperty("external_id") + "|" + row.getProperty("anchor_id"));
      }
    }
    return rows;
  }
}
