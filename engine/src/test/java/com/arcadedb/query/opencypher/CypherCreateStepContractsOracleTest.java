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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.InvalidPropertyTypeException;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The CREATE write step (CreateStep) against Neo4j: the SET it takes over, the parameter map of a relationship and the edge CREATE of the batched loader. Every expected row and statistic is what Neo4j 5.26 returns for the same data and statement. A row is rendered
 * {@code k=v, k=v} with the keys sorted, integers without a fraction.
 */
class CypherCreateStepContractsOracleTest extends TestHelper {
  private QueryStatistics lastStatistics;

  /** Runs one statement in its own transaction and renders its rows. */
  private List<String> run(final String statement) {
    return run(statement, Map.of());
  }

  private List<String> run(final String statement, final Map<String, Object> params) {
    final List<String> rows = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", statement, params)) {
        while (rs.hasNext())
          rows.add(render(rs.next()));
        lastStatistics = rs.getStatistics().orElse(null);
      }
    });
    return rows;
  }

  private void assertStats(final int propertiesSet, final int nodesCreated, final int relationshipsCreated, final int labelsAdded) {
    assertThat(lastStatistics).isNotNull();
    assertThat(lastStatistics.getPropertiesSet()).as("properties set").isEqualTo(propertiesSet);
    assertThat(lastStatistics.getNodesCreated()).as("nodes created").isEqualTo(nodesCreated);
    assertThat(lastStatistics.getRelationshipsCreated()).as("relationships created").isEqualTo(relationshipsCreated);
    assertThat(lastStatistics.getLabelsAdded()).as("labels added").isEqualTo(labelsAdded);
  }

  private static Map<String, Object> map(final Object... keysAndValues) {
    final Map<String, Object> m = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2)
      m.put((String) keysAndValues[i], keysAndValues[i + 1]);
    return m;
  }

  private static List<Object> list(final Object... values) {
    return new ArrayList<>(Arrays.asList(values));
  }

  private static String render(final Result row) {
    final Map<String, Object> sorted = new TreeMap<>();
    for (final String name : row.getPropertyNames())
      sorted.put(name, row.getProperty(name));
    final StringBuilder sb = new StringBuilder();
    for (final Map.Entry<String, Object> e : sorted.entrySet()) {
      if (sb.length() > 0)
        sb.append(", ");
      sb.append(e.getKey()).append('=').append(value(e.getValue()));
    }
    return sb.toString();
  }

  private static String value(final Object o) {
    if (o == null)
      return "null";
    if (o instanceof Number n)
      return n.doubleValue() == Math.rint(n.doubleValue()) && !(o instanceof Double || o instanceof Float) ? String.valueOf(n.longValue())
          : String.valueOf(n.doubleValue());
    if (o instanceof Map<?, ?> m) {
      final StringBuilder sb = new StringBuilder("{");
      for (final Map.Entry<?, ?> e : new TreeMap<>(m).entrySet())
        sb.append(sb.length() > 1 ? ", " : "").append(e.getKey()).append(": ").append(value(e.getValue()));
      return sb.append('}').toString();
    }
    if (o instanceof Collection<?> c) {
      final List<String> parts = new ArrayList<>();
      for (final Object x : c)
        parts.add(value(x));
      return parts.toString();
    }
    return String.valueOf(o);
  }

  @Test
  void twoSetClausesAfterACreateBothApply() {
    assertThat(run("CREATE (n:X) SET n.a = 1 SET n.b = 2 RETURN n.a AS a, n.b AS b")).
        containsExactly("a=1, b=2");
    assertStats(2, 1, 0, 1);
    assertThat(run("UNWIND [1, 2] AS i CREATE (n:Y {i: i}) SET n.j = i * 2 SET n.k = n.j + 1 RETURN n.i AS i, n.j AS j, n.k AS k ORDER BY i")).
        containsExactly("i=1, j=2, k=3", "i=2, j=4, k=5");
    assertStats(6, 2, 0, 2);
  }

  @Test
  void aSetThatReadsARelationshipOfAnotherPatternOfTheCreateSeesIt() {
    assertThat(run("CREATE (a:A), (b:B)-[r:R]->(c:C) SET a.t = type(r) RETURN a.t AS t")).
        containsExactly("t=R");
    assertStats(1, 3, 1, 3);
  }

  @Test
  void aSetThatReadsAPathOfAnotherPatternOfTheCreateSeesIt() {
    assertThat(run("CREATE (a:A), p = (b:B)-[:R]->(c:C) SET a.len = length(p) RETURN a.len AS len")).
        containsExactly("len=1");
    assertStats(1, 3, 1, 3);
  }

  @Test
  void aSetThatReadsAnotherNodeOfTheCreateSeesItsProperties() {
    assertThat(run("CREATE (a:A {x: 1}), (b:B) SET b.v = a.x RETURN b.v AS v")).
        containsExactly("v=1");
    assertStats(2, 2, 0, 2);
  }

  @Test
  void aSetOnTheEndOfALongerPathStillApplies() {
    assertThat(run("CREATE (a:A)-[:R]->(b:B) SET a.x = 1, b.y = 2 RETURN a.x AS x, b.y AS y")).
        containsExactly("x=1, y=2");
    assertStats(2, 2, 1, 2);
  }

  @Test
  void aSetOnANodeThatReadsItsOwnMissingPropertyStoresNothing() {
    assertThat(run("CREATE (a:A) SET a.l = a.x RETURN a.l AS l")).
        containsExactly("l=null");
    assertStats(0, 1, 0, 1);
  }

  @Test
  void aRelationshipTakesItsPropertiesFromAMapParameterAndSkipsNulls() {
    assertThat(run("CREATE (a:A)-[r:R $p]->(b:B) RETURN r.x AS x, r.y AS y, r.z AS z, r.w AS w", map("p", map("x", 1, "y", null, "z", "s", "w", list(1, 2))))).
        containsExactly("w=[1, 2], x=1, y=null, z=s");
    assertStats(3, 2, 1, 2);
  }

  @Test
  void aMapParameterOfARelationshipIsReadOncePerRow() {
    assertThat(run("UNWIND $rows AS row CREATE (a:A {k: row})-[r:R $p]->(b:B) RETURN count(r) AS c, sum(r.x) AS s", map("p", map("x", 1), "rows", list(1, 2, 3)))).
        containsExactly("c=3, s=3");
    assertStats(6, 6, 3, 6);
  }

  @Test
  void aRelationshipParameterWithAMapValueIsRefused() {
    assertThatThrownBy(() -> run("CREATE (a:A)-[r:R $p]->(b:B) RETURN r.x AS x", map("p", map("x", map("m", 1)))))
        .isInstanceOf(InvalidPropertyTypeException.class).hasMessageContaining("Property values can only be of primitive types").hasMessageContaining("property 'x'");
  }

  @Test
  void aNodeNamedAgainByASecondCreateIsRefused() {
    assertThatThrownBy(() -> run("CREATE (n:X) CREATE (n) RETURN n"))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("Variable 'n' already defined");
  }

  @Test
  void aBatchedEdgeCreateWithTheSetOfTheLoaderStoresEveryPropertyOfTheNewEdge() {
    // The properties_set statistic is not asserted: Neo4j counts a null SET that precedes a SET += of the same clause as one more
    // property set, although nothing is stored for it
    run("CREATE (:Entity {external_id: 'a'}), (:Entity {external_id: 'b'}), (:Entity {external_id: 'c'})");
    assertThat(run("UNWIND $rows AS row MATCH (a:Entity {external_id: row.from_id}) MATCH (b:Entity {external_id: row.to_id}) CREATE (a)-[e:RELATES_TO]->(b) SET e.predicate = row.predicate, e.confidence = row.confidence, e.graph_name = row.graph_name, e.source_record = row.source_record, e.is_asserted = row.is_asserted, e += row.cascade, e.created_at = row.now RETURN a.external_id AS from_id, b.external_id AS to_id ORDER BY from_id", map("rows", list(map("from_id", "a", "to_id", "b", "predicate", "p1", "confidence", 0.5, "graph_name", "g", "source_record", null, "is_asserted", true, "cascade", map("f1", 1, "f2", null), "now", 100), map("from_id", "b", "to_id", "c", "predicate", "p2", "confidence", null, "graph_name", "g", "source_record", "r1", "is_asserted", false, "cascade", map("f3", "x"), "now", 101))))).
        containsExactly("from_id=a, to_id=b", "from_id=b, to_id=c");
    assertThat(run("MATCH ()-[e:RELATES_TO]->() RETURN properties(e) AS p ORDER BY p.predicate")).
        containsExactly("p={confidence: 0.5, created_at: 100, f1: 1, graph_name: g, is_asserted: true, predicate: p1}", "p={created_at: 101, f3: x, graph_name: g, is_asserted: false, predicate: p2, source_record: r1}");
  }
}
