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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8724: {@code ORDER BY v.x LIMIT k} over a whole label read the index in order for one case only (ascending, no
 * WHERE, default null strategy) and scanned and sorted the label in the cases next to it. The index order now answers:
 * both directions when no vertex can lack a key ({@code MANDATORY} and {@code NOTNULL}) or the WHERE is
 * {@code v.x IS NOT NULL}, and both directions of an index with {@code NULL_STRATEGY INDEX}, whose null keys are read
 * from the end of the index openCypher puts them at (last ascending, first descending).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8724CypherIndexOrderWholeLabelTest extends TestHelper {
  private static final int VERTICES   = 3_000;
  private static final int NULL_EVERY = 50;

  private final List<Integer> mandatoryValues = new ArrayList<>();
  private final List<Integer> nullableValues  = new ArrayList<>();

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE M");
    database.command("sql", "CREATE PROPERTY M.x INTEGER (mandatory true, notnull true)");
    database.command("sql", "CREATE INDEX ON M (x) NOTUNIQUE");
    database.command("sql", "CREATE VERTEX TYPE D");
    database.command("sql", "CREATE PROPERTY D.x INTEGER");
    database.command("sql", "CREATE INDEX ON D (x) NOTUNIQUE");
    database.command("sql", "CREATE VERTEX TYPE I");
    database.command("sql", "CREATE PROPERTY I.x INTEGER");
    database.command("sql", "CREATE INDEX ON I (x) NOTUNIQUE NULL_STRATEGY INDEX");

    final Random random = new Random(7);
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++) {
        final int value = random.nextInt(1_000_000);
        database.newVertex("M").set("x", value).set("seq", i).save();
        mandatoryValues.add(value);
        final boolean isNull = i % NULL_EVERY == 0;
        nullableValues.add(isNull ? null : value);
        for (final String type : new String[] { "D", "I" }) {
          final var vertex = database.newVertex(type).set("seq", i);
          if (!isNull)
            vertex.set("x", value);
          vertex.save();
        }
      }
    });
  }

  @Test
  void mandatoryNotNullReadsTheIndexInBothDirections() {
    for (final boolean ascending : new boolean[] { true, false }) {
      final String query = "MATCH (m:M) RETURN m.x AS x ORDER BY m.x" + (ascending ? "" : " DESC") + " LIMIT 10";
      assertThat(column(query)).containsExactlyElementsOf(expected(mandatoryValues, ascending, 0, 10));
      final String profile = profile(query);
      assertThat(profile).contains("NodeIndexRangeScan").contains("index order").doesNotContain("OrderByStep");
      assertThat(profile.contains("descending")).isEqualTo(!ascending);
    }
  }

  @Test
  void skipWalksTheWholeIndex() {
    assertThat(column("MATCH (m:M) RETURN m.x AS x ORDER BY m.x DESC SKIP 2990 LIMIT 20"))
        .containsExactlyElementsOf(expected(mandatoryValues, false, 2990, 20));
  }

  @Test
  void isNotNullReadsTheIndexInBothDirections() {
    for (final String type : new String[] { "D", "I" })
      for (final boolean ascending : new boolean[] { true, false }) {
        final String query = "MATCH (v:" + type + ") WHERE v.x IS NOT NULL RETURN v.x AS x ORDER BY v.x" + (ascending ? "" : " DESC")
            + " LIMIT 10";
        assertThat(column(query)).as(query).containsExactlyElementsOf(expected(nonNull(nullableValues), ascending, 0, 10));
        assertThat(profile(query)).as(query).contains("NodeIndexRangeScan").contains("index order").doesNotContain("OrderByStep");
      }
  }

  @Test
  void isNotNullWalksToTheEndOfTheIndex() {
    final List<Integer> nonNull = nonNull(nullableValues);
    for (final String type : new String[] { "D", "I" }) {
      final String query = "MATCH (v:" + type + ") WHERE v.x IS NOT NULL RETURN v.x AS x ORDER BY v.x DESC SKIP " + (nonNull.size() - 5)
          + " LIMIT 50";
      assertThat(column(query)).as(query).containsExactlyElementsOf(expected(nonNull, false, nonNull.size() - 5, 50));
    }
  }

  @Test
  void nullStrategyIndexPlacesTheNullKeysAtTheEndOpenCypherPutsThem() {
    final int nulls = (int) nullableValues.stream().filter(v -> v == null).count();

    // Ascending: the values, then the nulls
    final String ascending = "MATCH (i:I) RETURN i.x AS x ORDER BY i.x LIMIT 10";
    assertThat(column(ascending)).containsExactlyElementsOf(expected(nullableValues, true, 0, 10));
    assertThat(profile(ascending)).contains("NodeIndexRangeScan").contains("index order").doesNotContain("OrderByStep");
    assertThat(column("MATCH (i:I) RETURN i.x AS x ORDER BY i.x SKIP " + (VERTICES - nulls - 5) + " LIMIT 20"))
        .containsExactlyElementsOf(expected(nullableValues, true, VERTICES - nulls - 5, 20));

    // Descending: the nulls, then the values from the top
    final String descending = "MATCH (i:I) RETURN i.x AS x ORDER BY i.x DESC LIMIT 10";
    assertThat(column(descending)).containsExactlyElementsOf(expected(nullableValues, false, 0, 10));
    assertThat(profile(descending)).contains("NodeIndexRangeScan").contains("index order, descending").doesNotContain("OrderByStep");
    assertThat(column("MATCH (i:I) RETURN i.x AS x ORDER BY i.x DESC SKIP " + (nulls - 3) + " LIMIT 20"))
        .containsExactlyElementsOf(expected(nullableValues, false, nulls - 3, 20));

    // The whole index, both directions
    assertThat(column("MATCH (i:I) RETURN i.x AS x ORDER BY i.x LIMIT " + VERTICES))
        .containsExactlyElementsOf(expected(nullableValues, true, 0, VERTICES));
    assertThat(column("MATCH (i:I) RETURN i.x AS x ORDER BY i.x DESC LIMIT " + VERTICES))
        .containsExactlyElementsOf(expected(nullableValues, false, 0, VERTICES));
  }

  @Test
  void aNullableDefaultIndexKeepsTheSortDescending() {
    // The index holds no null keys and openCypher puts the nulls first descending: the answer has to come from a scan
    final String query = "MATCH (d:D) RETURN d.x AS x ORDER BY d.x DESC LIMIT 10";
    assertThat(column(query)).containsExactlyElementsOf(expected(nullableValues, false, 0, 10));
    assertThat(profile(query)).contains("OrderByStep");
    // and ascending still reads the index, then the vertices with no key
    assertThat(column("MATCH (d:D) RETURN d.x AS x ORDER BY d.x LIMIT " + VERTICES))
        .containsExactlyElementsOf(expected(nullableValues, true, 0, VERTICES));
  }

  @Test
  void otherPredicatesKeepTheSort() {
    final String query = "MATCH (v:D) WHERE v.x IS NOT NULL AND v.seq >= 10 RETURN v.x AS x ORDER BY v.x DESC LIMIT 5";
    final List<Integer> values = new ArrayList<>();
    for (int i = 10; i < VERTICES; i++)
      if (nullableValues.get(i) != null)
        values.add(nullableValues.get(i));
    assertThat(column(query)).containsExactlyElementsOf(expected(values, false, 0, 5));
  }

  @Test
  void theNullKeysSpanMoreThanOneFetchBatch() {
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE PROPERTY N.x INTEGER");
    database.command("sql", "CREATE INDEX ON N (x) NOTUNIQUE NULL_STRATEGY INDEX");
    final List<Integer> values = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++) {
        final Integer value = i % 3 == 0 ? null : i;
        values.add(value);
        final var vertex = database.newVertex("N");
        if (value != null)
          vertex.set("x", value);
        vertex.save();
      }
    });
    assertThat(column("MATCH (n:N) RETURN n.x AS x ORDER BY n.x DESC LIMIT 1000")).containsExactlyElementsOf(expected(values, false, 0, 1000));
    assertThat(column("MATCH (n:N) RETURN n.x AS x ORDER BY n.x LIMIT 1000")).containsExactlyElementsOf(expected(values, true, 0, 1000));
    assertThat(column("MATCH (n:N) RETURN n.x AS x ORDER BY n.x DESC SKIP 150 LIMIT 300")).containsExactlyElementsOf(expected(values, false, 150, 300));
  }

  private static List<Integer> nonNull(final List<Integer> values) {
    return values.stream().filter(v -> v != null).toList();
  }

  /** The window of the values sorted the way openCypher sorts them: null last ascending, first descending. */
  private static List<Object> expected(final List<Integer> values, final boolean ascending, final int skip, final int limit) {
    final List<Integer> sorted = new ArrayList<>(values);
    final Comparator<Integer> nullsLast = Comparator.nullsLast(Comparator.<Integer>naturalOrder());
    sorted.sort(ascending ? nullsLast : nullsLast.reversed());
    return new ArrayList<>(sorted.subList(Math.min(skip, sorted.size()), Math.min(skip + limit, sorted.size())));
  }

  private List<Object> column(final String query) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", query, Map.of())) {
      while (rs.hasNext())
        values.add(rs.next().getProperty("x"));
    }
    return values;
  }

  private String profile(final String query) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, Map.of())) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }
}
