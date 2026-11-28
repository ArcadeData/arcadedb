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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The aggregation the workers of a parallel label scan run on partial states ({@code ParallelAggregation}) over the values the existing
 * parallel tests leave out: grouping keys that are missing, an integer and a float of one value as keys, floats whose sum depends on the
 * order they are added in unless it is exact, numbers of different types under one min()/max(), and a group cap that only the merged groups
 * exceed. Every answer is checked against the same query with parallel scans disabled, and against a model computed here.
 */
class CypherParallelAggregationAwkwardValuesTest extends TestHelper {
  private static final int         ROWS    = 6_000;
  private static final double[]    FLOATS  = { 0.1, 0.2, 0.3, 1e15, -1e15, 3.3, 1e-7, 123456.789, -0.5 };
  private final        List<Object[]> data = new ArrayList<>();

  @Override
  protected void beginTest() {
    // Small units, so the fixture's pages are cut in many of them and several workers meet the same key
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    database.getSchema().createVertexType("Awkward", 4);
    final Random random = new Random(7);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++) {
        final var vertex = database.newVertex("Awkward");
        vertex.set("id", i, "seq", i / 60, "f", FLOATS[random.nextInt(FLOATS.length)]);
        // k: missing for one row in ten
        final Object k = random.nextInt(10) == 0 ? null : (Object) ("k" + random.nextInt(8));
        if (k != null)
          vertex.set("k", k);
        // m: the same number as an integer or as a float, a string, or missing
        final Object m = switch (random.nextInt(6)) {
          case 0 -> 1L;
          case 1 -> 1.0;
          case 2 -> 2L;
          case 3 -> 2.0;
          case 4 -> "a";
          default -> null;
        };
        if (m != null)
          vertex.set("m", m);
        // v: an integer or a float that never ties with another of the other type (the floats end in .5), or missing
        final Object v = switch (random.nextInt(3)) {
          case 0 -> (long) random.nextInt(1000) - 500;
          case 1 -> random.nextInt(1000) - 500 + 0.5;
          default -> null;
        };
        if (v != null)
          vertex.set("v", v);
        vertex.save();
        data.add(new Object[] { k, m, v, vertex.getDouble("f") });
      }
    });
  }

  @Test
  void missingKeysAreOneGroupAndNullsAreSkipped() {
    final String query = "MATCH (n:Awkward) RETURN n.k AS k, count(*) AS c, count(n.v) AS cv, count(n.m) AS cm";
    assertThat(profile(query)).contains("(parallel:");
    final List<Map<String, Object>> parallel = rows(query);
    assertThat(parallel).isEqualTo(sequential(query));

    final Map<Object, long[]> model = new HashMap<>();
    for (final Object[] row : data) {
      final long[] counts = model.computeIfAbsent(row[0], key -> new long[3]);
      counts[0]++;
      if (row[2] != null)
        counts[1]++;
      if (row[1] != null)
        counts[2]++;
    }
    assertThat(parallel).hasSize(model.size()).hasSize(9);
    for (final Map<String, Object> group : parallel) {
      final long[] counts = model.get(group.get("k"));
      assertThat(counts).as("group " + group.get("k")).isNotNull();
      assertThat(group.get("c")).isEqualTo(counts[0]);
      assertThat(group.get("cv")).isEqualTo(counts[1]);
      assertThat(group.get("cm")).isEqualTo(counts[2]);
    }
  }

  @Test
  void anIntegerAndAFloatOfOneValueAreOneGroup() {
    final String query = "MATCH (n:Awkward) RETURN n.m AS m, count(*) AS c, sum(n.id) AS s, min(n.id) AS lo, max(n.id) AS hi";
    assertThat(profile(query)).contains("(parallel:");
    final List<Map<String, Object>> parallel = rows(query);

    // 1 and 1.0, 2 and 2.0, 'a', missing: four groups, in the parallel scan as in the sequential one
    assertThat(parallel).hasSize(4);
    assertThat(sequential(query)).hasSize(4);
    final Map<String, long[]> model = new HashMap<>();
    for (int i = 0; i < data.size(); i++) {
      final Object m = data.get(i)[1];
      final String key = m == null ? "null" : m instanceof Number number ? "n" + number.doubleValue() : m.toString();
      final long[] g = model.computeIfAbsent(key, x -> new long[] { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE });
      g[0]++;
      g[1] += i;
      g[2] = Math.min(g[2], i);
      g[3] = Math.max(g[3], i);
    }
    final Map<String, long[]> got = new HashMap<>();
    for (final Map<String, Object> group : parallel) {
      final Object m = group.get("m");
      final String key = m == null ? "null" : m instanceof Number number ? "n" + number.doubleValue() : m.toString();
      assertThat(got.put(key, new long[] { ((Number) group.get("c")).longValue(), ((Number) group.get("s")).longValue(), ((Number) group.get("lo")).longValue(),
          ((Number) group.get("hi")).longValue() }))
          .as("one group for " + key).isNull();
    }
    assertThat(got.keySet()).isEqualTo(model.keySet());
    for (final String key : model.keySet())
      assertThat(got.get(key)).as(key).containsExactly(model.get(key));
  }

  @Test
  void aSumOfFloatsDoesNotDependOnTheOrderTheWorkersAddThemIn() {
    BigDecimal exact = BigDecimal.ZERO;
    for (final Object[] row : data)
      exact = exact.add(new BigDecimal((Double) row[3]));
    final String query = "MATCH (n:Awkward) RETURN sum(n.f) AS s, avg(n.f) AS a, count(n.f) AS c";
    assertThat(profile(query)).contains("(parallel:");
    // the same on every run: the workers meet the units in a different order each time
    final List<Map<String, Object>> first = rows(query);
    for (int run = 0; run < 5; run++)
      assertThat(rows(query)).isEqualTo(first);
    assertThat(first).isEqualTo(sequential(query));
    assertThat(first.getFirst().get("s")).isEqualTo(exact.doubleValue());
    assertThat(first.getFirst().get("c")).isEqualTo((long) ROWS);

    final String grouped = "MATCH (n:Awkward) RETURN n.seq % 7 AS g, sum(n.f) AS s, avg(n.f) AS a";
    assertThat(profile(grouped)).contains("(parallel:");
    assertThat(rows(grouped)).isEqualTo(sequential(grouped));
  }

  @Test
  void minAndMaxOfIntegersAndFloatsTogether() {
    double lo = Double.MAX_VALUE;
    double hi = -Double.MAX_VALUE;
    for (final Object[] row : data)
      if (row[2] != null) {
        lo = Math.min(lo, ((Number) row[2]).doubleValue());
        hi = Math.max(hi, ((Number) row[2]).doubleValue());
      }
    final String query = "MATCH (n:Awkward) RETURN min(n.v) AS lo, max(n.v) AS hi, count(n.v) AS c";
    assertThat(profile(query)).contains("(parallel:");
    final List<Map<String, Object>> parallel = rows(query);
    assertThat(parallel).isEqualTo(sequential(query));
    assertThat(((Number) parallel.getFirst().get("lo")).doubleValue()).isEqualTo(lo);
    assertThat(((Number) parallel.getFirst().get("hi")).doubleValue()).isEqualTo(hi);

    final String grouped = "MATCH (n:Awkward) RETURN n.k AS k, min(n.v) AS lo, max(n.v) AS hi";
    assertThat(rows(grouped)).isEqualTo(sequential(grouped));
  }

  /**
   * The cap on the groups is checked on each worker's own groups while it scans and once more on the merged ones: 100 groups of 60
   * consecutive rows are met by a worker in runs, so with the cap at 90 a worker is under it where the merged groups are over.
   */
  @Test
  void theGroupCapAppliesToTheMergedGroups() {
    final String query = "MATCH (n:Awkward) RETURN n.seq AS g, count(*) AS c";
    assertThat(rows(query)).hasSize(100);
    final long previous = database.getConfiguration().getValueAsLong(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 90);
    try {
      assertThatThrownBy(() -> rows(query)).isInstanceOf(CommandExecutionException.class);
      assertThatThrownBy(() -> sequential(query)).isInstanceOf(CommandExecutionException.class);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, previous);
    }
    assertThat(rows(query)).hasSize(100);
  }

  private List<Map<String, Object>> sequential(final String query) {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    try {
      return rows(query);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
  }

  private List<Map<String, Object>> rows(final String query) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final Map<String, Object> map = new HashMap<>();
        for (final String name : row.getPropertyNames())
          map.put(name, row.getProperty(name));
        rows.add(map);
      }
    }
    return rows;
  }

  private String profile(final String query) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }
}
