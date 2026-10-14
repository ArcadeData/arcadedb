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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for #9914: {@code algo.kShortestPaths} only followed outgoing edges and took no direction, so on a
 * graph stored in one direction and walked in both (roads) its first path was not the one {@code algo.dijkstra} returns
 * for the same arguments, or there was none at all. It now takes {@code direction} with the same {@code BOTH} default.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9914KShortestPathsDirectionTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE City");
    database.command("sql", "CREATE EDGE TYPE ROAD");
  }

  @Test
  void walksBothDirectionsByDefaultLikeDijkstra() {
    reporterGraph();
    assertThat(dijkstraWeight("A", "C")).isEqualTo(3.0);

    final List<Result> paths = kShortest("A", "C", 3, "");
    assertThat(paths).hasSize(1);
    assertThat(paths.get(0).<Number>getProperty("weight").doubleValue()).isEqualTo(3.0);
    assertThat(paths.get(0).<Number>getProperty("rank").intValue()).isEqualTo(1);
  }

  @Test
  void theDirectionArgumentRestrictsTheWalk() {
    reporterGraph();
    assertThat(kShortest("A", "C", 3, ", 'OUT'")).isEmpty();
    assertThat(kShortest("A", "C", 3, ", 'IN'")).isEmpty();
    assertThat(kShortest("A", "B", 3, ", 'OUT'")).hasSize(1);
    assertThat(kShortest("B", "A", 3, ", 'IN'")).hasSize(1);
    assertThat(kShortest("A", "C", 3, ", 'BOTH'")).hasSize(1);
  }

  @Test
  void theYieldedPathCarriesItsRelationships() {
    reporterGraph();
    final TraversalPath path = kShortest("A", "C", 1, "").get(0).getProperty("path");
    final List<Object> names = new ArrayList<>();
    for (final Vertex node : path.getVertices())
      names.add(node.getString("name"));
    assertThat(names).containsExactly("A", "B", "C");
    assertThat(path.getEdges()).hasSize(2);
    assertThat(path.length()).isEqualTo(2);
  }

  /**
   * Rank 1 must be the path {@code algo.dijkstra} finds, in every direction, on a graph with edges stored both ways,
   * parallel edges and edges that are not walkable.
   */
  @Test
  void rankOneAlwaysEqualsDijkstra() {
    final Random random = new Random(9914);
    final int nodes = 30;
    database.transaction(() -> {
      final List<MutableVertex> cities = new ArrayList<>();
      for (int i = 0; i < nodes; i++)
        cities.add(database.newVertex("City").set("name", "N" + i).save());
      for (int i = 0; i < 90; i++) {
        final MutableVertex from = cities.get(random.nextInt(nodes));
        final MutableVertex to = cities.get(random.nextInt(nodes));
        from.newEdge("ROAD", to, "km", 1 + random.nextInt(20)).save();
      }
    });

    for (int i = 0; i < 20; i++) {
      final String from = "N" + random.nextInt(nodes);
      final String to = "N" + random.nextInt(nodes);
      if (from.equals(to))
        continue;
      for (final String direction : new String[] { "BOTH", "OUT", "IN" }) {
        final Double dijkstra = dijkstraWeight(from, to, direction);
        final List<Result> paths = kShortest(from, to, 4, ", '" + direction + "'");
        if (dijkstra == null) {
          assertThat(paths).as(from + "->" + to + " " + direction).isEmpty();
          continue;
        }
        assertThat(paths).as(from + "->" + to + " " + direction).isNotEmpty();
        assertThat(paths.get(0).<Number>getProperty("weight").doubleValue()).as(from + "->" + to + " " + direction)
            .isEqualTo(dijkstra);
        double previous = 0;
        for (final Result path : paths) {
          final double weight = path.<Number>getProperty("weight").doubleValue();
          assertThat(weight).isGreaterThanOrEqualTo(previous);
          previous = weight;
        }
      }
    }
  }

  @Test
  void edgeCases() {
    reporterGraph();
    // the start is the end: one path, no relationship, weight 0
    final List<Result> self = kShortest("A", "A", 3, "");
    assertThat(self).hasSize(1);
    assertThat(self.get(0).<Number>getProperty("weight").doubleValue()).isZero();
    // more paths asked than exist: as many as exist
    assertThat(kShortest("A", "C", 50, "")).hasSize(1);
    // a direction and a type filter together
    assertThat(kShortest("B", "A", 3, ", 'IN'")).hasSize(1);
    assertThat(kShortest("B", "C", 3, ", 'IN'")).hasSize(1);
    assertThat(kShortest("C", "B", 3, ", 'IN'")).isEmpty();
    // a direction that is none of the three is refused, not read as a default
    assertThatThrownBy(() -> kShortest("A", "C", 3, ", 'SIDEWAYS'")).hasStackTraceContaining("unknown direction 'SIDEWAYS'");
  }

  @Test
  @SuppressWarnings("unchecked")
  void lightweightEdgesAreWalkedAndReported() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("City").set("name", "A").save();
      final MutableVertex b = database.newVertex("City").set("name", "B").save();
      final MutableVertex c = database.newVertex("City").set("name", "C").save();
      a.newLightEdge("ROAD", b);
      c.newLightEdge("ROAD", b);
    });
    final List<Result> paths = kShortest("A", "C", 2, "", "");
    assertThat(paths).hasSize(1);
    assertThat(paths.get(0).<Number>getProperty("weight").doubleValue()).isEqualTo(2.0);
    assertThat(paths.get(0).<TraversalPath>getProperty("path").getEdges()).hasSize(2);
    // the path algo.dijkstra yields used to fail building on a lightweight edge, which has no record to look up
    assertThat(dijkstraWeight("A", "C")).isEqualTo(2.0);
  }

  private void reporterGraph() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO City SET name = 'A'");
      database.command("sql", "INSERT INTO City SET name = 'B'");
      database.command("sql", "INSERT INTO City SET name = 'C'");
      database.command("sql",
          "CREATE EDGE ROAD FROM (SELECT FROM City WHERE name = 'A') TO (SELECT FROM City WHERE name = 'B') SET km = 1");
      database.command("sql",
          "CREATE EDGE ROAD FROM (SELECT FROM City WHERE name = 'C') TO (SELECT FROM City WHERE name = 'B') SET km = 2");
    });
  }

  private Double dijkstraWeight(final String from, final String to) {
    return dijkstraWeight(from, to, "BOTH");
  }

  private Double dijkstraWeight(final String from, final String to, final String direction) {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:City {name: $from}), (b:City {name: $to}) CALL algo.dijkstra(a, b, 'ROAD', 'km', $dir) YIELD weight RETURN weight",
        Map.of("from", from, "to", to, "dir", direction))) {
      return rs.hasNext() ? rs.next().<Number>getProperty("weight").doubleValue() : null;
    }
  }

  private List<Result> kShortest(final String from, final String to, final int k, final String direction) {
    return kShortest(from, to, k, ", 'km'", direction);
  }

  private List<Result> kShortest(final String from, final String to, final int k, final String weight, final String direction) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:City {name: $from}), (b:City {name: $to}) CALL algo.kShortestPaths(a, b, " + k + ", 'ROAD'" + weight + direction
            + ") YIELD path, weight, rank RETURN path, weight, rank", Map.of("from", from, "to", to))) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }
}
