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
 * CREATE in the shapes of a batched graph loader (nodes with labels, lists and vectors from UNWIND rows, null properties skipped, edges with properties between matched nodes, a path, a SET folded into the CREATE). Every expected row and statistic is what Neo4j 5.26 returns for the same data and statement. A row is rendered
 * {@code k=v, k=v} with the keys sorted, integers without a fraction.
 */
class CypherCreateShapesOracleTest extends TestHelper {
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
  void aNodeIsCreatedWithLabelsAndPropertiesAndCountedOnce() {
    assertThat(run("CREATE (n:Chunk:Indexed {external_id: 'c1', text: 'hello', n: 3, f: 0.5, ok: true}) RETURN n.external_id AS id, n.n AS n, n.f AS f, n.ok AS ok, size(labels(n)) AS l")).containsExactly("f=0.5, id=c1, l=2, n=3, ok=true");
    assertStats(5, 1, 0, 2);
    assertThat(run("MATCH (n:Chunk) RETURN properties(n) AS p, size(labels(n)) AS l")).containsExactly("l=2, p={external_id: c1, f: 0.5, n: 3, ok: true, text: hello}");
  }

  @Test
  void nullPropertiesAreNotStoredNorCounted() {
    assertThat(run("CREATE (n:Chunk {external_id: 'c1', gone: null, kept: 1}) RETURN properties(n) AS p")).containsExactly("p={external_id: c1, kept: 1}");
    assertStats(2, 1, 0, 1);
    assertThat(run("CREATE (n:Chunk $p) RETURN properties(n) AS p", map("p", map("a", 1, "b", null, "c", "x")))).containsExactly("p={a: 1, c: x}");
    assertStats(2, 1, 0, 1);
    assertThat(run("CREATE (n:Chunk {a: $p.a, b: $p.b, c: $p.c}) RETURN properties(n) AS p", map("p", map("a", 1, "b", null, "c", "x")))).containsExactly("p={a: 1, c: x}");
    assertStats(2, 1, 0, 1);
  }

  @Test
  void aBatchOfRowsCreatesOneNodePerRowWithItsListsAndVectors() {
    assertThat(run("UNWIND $rows AS row CREATE (c:Chunk {external_id: row.eid, embedding: row.vec, tags: row.tags, n: row.n}) RETURN c.external_id AS id, c.embedding AS v, c.tags AS t, c.n AS n ORDER BY id", map("rows", list(map("eid", "c1", "vec", list(0.5, 0.25), "tags", list("a", "b"), "n", 1), map("eid", "c2", "vec", list(1.5, 2.5), "tags", list(), "n", 2), map("eid", "c3", "vec", null, "tags", list("z"), "n", null))))).containsExactly("id=c1, n=1, t=[a, b], v=[0.5, 0.25]", "id=c2, n=2, t=[], v=[1.5, 2.5]", "id=c3, n=null, t=[z], v=null");
    assertStats(10, 3, 0, 3);
    assertThat(run("MATCH (c:Chunk) RETURN c.external_id AS id, size(c.tags) AS ts, c.n AS n ORDER BY id")).containsExactly("id=c1, n=1, ts=2", "id=c2, n=2, ts=0", "id=c3, n=null, ts=1");
  }

  @Test
  void anEdgeIsCreatedWithPropertiesBetweenMatchedNodesAndCountedWithItsProperties() {
    run("CREATE (:Chunk {external_id:'c1'}), (:Entity {external_id:'e1'})");
    assertThat(run("MATCH (c:Chunk {external_id:'c1'}), (e:Entity {external_id:'e1'}) CREATE (c)-[r:MENTIONS {weight: 0.5, kind: 'x', none: null}]->(e) RETURN properties(r) AS p, type(r) AS t")).containsExactly("p={kind: x, weight: 0.5}, t=MENTIONS");
    assertStats(2, 0, 1, 0);
    assertThat(run("MATCH (c:Chunk {external_id:'c1'}), (e:Entity {external_id:'e1'}) CREATE (c)-[r:MENTIONS]->(e), (e)-[s:MENTIONED_BY {w: 1}]->(c) RETURN type(r) AS r, type(s) AS s")).containsExactly("r=MENTIONS, s=MENTIONED_BY");
    assertStats(1, 0, 2, 0);
    assertThat(run("MATCH (c:Chunk)-[r:MENTIONS]->(e:Entity) RETURN count(r) AS c")).containsExactly("c=2");
  }

  @Test
  void aWholePathIsCreatedInOneClauseAndAnIncomingEdgeIsStoredTheOtherWayRound() {
    assertThat(run("CREATE (a:A {id: 1})-[:R {w: 1}]->(b:B {id: 2})<-[:S]-(c:C {id: 3})")).isEmpty();
    assertStats(4, 3, 2, 3);
    assertThat(run("MATCH (a:A)-[:R]->(b:B)<-[:S]-(c:C) RETURN a.id AS a, b.id AS b, c.id AS c")).containsExactly("a=1, b=2, c=3");
    assertThat(run("MATCH (b:B {id: 2}) CREATE (b)<-[r:T {k: 1}]-(d:D {id: 4}) RETURN d.id AS d, startNode(r).id AS s, endNode(r).id AS e")).containsExactly("d=4, e=2, s=4");
    assertStats(2, 1, 1, 1);
  }

  @Test
  void aCreateFollowedBySetFoldsTheAssignmentsAndStillCountsThem() {
    assertThat(run("CREATE (n:Chunk {external_id: 'c1'}) SET n.a = 1, n += {b: 2}, n:Extra RETURN properties(n) AS p, size(labels(n)) AS l")).containsExactly("l=2, p={a: 1, b: 2, external_id: c1}");
    assertStats(3, 1, 0, 2);
    assertThat(run("CREATE (n:Chunk {external_id: 'c2'}) SET n = {only: 1} RETURN properties(n) AS p")).containsExactly("p={only: 1}");
    assertStats(3, 1, 0, 1);
    assertThat(run("CREATE (n:Chunk {external_id: 'c3'}) SET n += $p, n.y = n.external_id RETURN properties(n) AS p", map("p", map("x", 1)))).containsExactly("p={external_id: c3, x: 1, y: c3}");
    assertStats(3, 1, 0, 1);
  }

  @Test
  void aCreateOnTopOfAMatchRunsOncePerRowAndSharesTheEndpoint() {
    run("CREATE (:Doc {id:1}), (:Doc {id:2})");
    assertThat(run("MATCH (d:Doc) CREATE (d)-[:HAS]->(p:Part {of: d.id}) RETURN d.id AS d, p.of AS p ORDER BY d")).containsExactly("d=1, p=1", "d=2, p=2");
    assertStats(2, 2, 2, 2);
    assertThat(run("MATCH (d:Doc) WITH collect(d) AS ds CREATE (x:Summary {n: size(ds)}) RETURN x.n AS n")).containsExactly("n=2");
    assertStats(1, 1, 0, 1);
    assertThat(run("MATCH (p:Part) RETURN count(p) AS c")).containsExactly("c=2");
  }

  @Test
  void aDuplicateRelationshipCreateIsNotDeduplicated() {
    run("CREATE (:N {id:1}), (:N {id:2})");
    assertThat(run("MATCH (a:N {id:1}), (b:N {id:2}) CREATE (a)-[:R]->(b)")).isEmpty();
    assertStats(0, 0, 1, 0);
    assertThat(run("MATCH (a:N {id:1}), (b:N {id:2}) CREATE (a)-[:R]->(b)")).isEmpty();
    assertStats(0, 0, 1, 0);
    assertThat(run("MATCH (:N)-[r:R]->(:N) RETURN count(r) AS c")).containsExactly("c=2");
  }

  @Test
  void aCreateReturningNothingAndCreateWithADynamicPropertyExpression() {
    assertThat(run("CREATE (n:Entity {normalized_name: toLower($k), len: size($k), joined: 'x' + $k, nested: [1, 2, 3][1]}) RETURN n.normalized_name AS nn, n.len AS len, n.joined AS j, n.nested AS nested", map("k", "alpha"))).containsExactly("j=xalpha, len=5, nested=2, nn=alpha");
    assertStats(4, 1, 0, 1);
    assertThat(run("UNWIND range(1, 3) AS i CREATE (:Num {i: i, sq: i * i})")).isEmpty();
    assertStats(6, 3, 0, 3);
    assertThat(run("MATCH (n:Num) RETURN n.i AS i, n.sq AS sq ORDER BY i")).containsExactly("i=1, sq=1", "i=2, sq=4", "i=3, sq=9");
  }
}
