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
 * MERGE in the shapes of a batched graph loader (a node merged on a UNIQUE key with ON CREATE / ON MATCH SET, a directed edge merged between two matched nodes, duplicate keys and pairs inside one UNWIND batch, a Graphiti RELATES_TO merge with SET e = edge, the entity upsert page). Every expected row and statistic is what Neo4j 5.26 returns for the same data and statement. A row is rendered
 * {@code k=v, k=v} with the keys sorted, integers without a fraction.
 */
class CypherMergeShapesOracleTest extends TestHelper {
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
  void aNodeMergeOnAUniqueKeyCreatesOnceAndMatchesAfterwards() {
    run("CREATE CONSTRAINT FOR (n:Entity) REQUIRE n.normalized_name IS UNIQUE");
    assertThat(run("MERGE (n:Entity {normalized_name: $k}) RETURN n.normalized_name AS nn", map("k", "alpha"))).containsExactly("nn=alpha");
    assertStats(1, 1, 0, 1);
    assertThat(run("MERGE (n:Entity {normalized_name: $k}) RETURN n.normalized_name AS nn", map("k", "alpha"))).containsExactly("nn=alpha");
    assertStats(0, 0, 0, 0);
    assertThat(run("MERGE (n:Entity {normalized_name: $k}) RETURN n.normalized_name AS nn", map("k", "beta"))).containsExactly("nn=beta");
    assertStats(1, 1, 0, 1);
    assertThat(run("MATCH (n:Entity) RETURN n.normalized_name AS nn ORDER BY nn")).containsExactly("nn=alpha", "nn=beta");
  }

  @Test
  void onCreateAndOnMatchSetRunOnTheirOwnBranchAndCountTheirProperties() {
    run("CREATE CONSTRAINT FOR (n:Entity) REQUIRE n.normalized_name IS UNIQUE");
    assertThat(run("MERGE (n:Entity {normalized_name: $k}) ON CREATE SET n.created_at = $now, n.seen = 1 ON MATCH SET n.seen = n.seen + 1, n.updated_at = $now RETURN properties(n) AS p", map("k", "alpha", "now", "t1"))).containsExactly("p={created_at: t1, normalized_name: alpha, seen: 1}");
    assertStats(3, 1, 0, 1);
    assertThat(run("MERGE (n:Entity {normalized_name: $k}) ON CREATE SET n.created_at = $now, n.seen = 1 ON MATCH SET n.seen = n.seen + 1, n.updated_at = $now RETURN properties(n) AS p", map("k", "alpha", "now", "t2"))).containsExactly("p={created_at: t1, normalized_name: alpha, seen: 2, updated_at: t2}");
    assertStats(2, 0, 0, 0);
    assertThat(run("MERGE (n:Entity {normalized_name: $k}) ON CREATE SET n.created_at = $now, n.seen = 1 ON MATCH SET n.seen = n.seen + 1, n.updated_at = $now RETURN properties(n) AS p", map("k", "alpha", "now", "t2"))).containsExactly("p={created_at: t1, normalized_name: alpha, seen: 3, updated_at: t2}");
    assertStats(2, 0, 0, 0);
    assertThat(run("MERGE (n:Entity {normalized_name: $k}) ON CREATE SET n.created_at = $now, n.seen = 1 ON MATCH SET n.seen = n.seen + 1, n.updated_at = $now RETURN properties(n) AS p", map("k", "beta", "now", "t3"))).containsExactly("p={created_at: t3, normalized_name: beta, seen: 1}");
    assertStats(3, 1, 0, 1);
  }

  @Test
  void aSameValueOnMatchSetIsCountedAndLeavesTheValue() {
    run("CREATE (:Entity {normalized_name:'alpha', title:'T', other:1})");
    assertThat(run("MERGE (n:Entity {normalized_name:'alpha'}) ON MATCH SET n.title = 'T' RETURN properties(n) AS p")).containsExactly("p={normalized_name: alpha, other: 1, title: T}");
    assertStats(1, 0, 0, 0);
    assertThat(run("MERGE (n:Entity {normalized_name:'alpha'}) ON MATCH SET n.title = 'U', n.other = 1 RETURN properties(n) AS p")).containsExactly("p={normalized_name: alpha, other: 1, title: U}");
    assertStats(2, 0, 0, 0);
    assertThat(run("MERGE (n:Entity {normalized_name:'alpha'}) ON MATCH SET n.title = null RETURN properties(n) AS p")).containsExactly("p={normalized_name: alpha, other: 1}");
    assertStats(1, 0, 0, 0);
    assertThat(run("MERGE (n:Entity {normalized_name:'alpha'}) ON MATCH SET n.title = null RETURN properties(n) AS p")).containsExactly("p={normalized_name: alpha, other: 1}");
    assertStats(0, 0, 0, 0);
  }

  @Test
  void aMapOnMatchSetMergesAndNullRemoves() {
    run("CREATE (:Entity {normalized_name:'alpha', title:'T', other:1})");
    assertThat(run("MERGE (n:Entity {normalized_name:'alpha'}) ON MATCH SET n += $props RETURN properties(n) AS p", map("props", map("title", "U", "other", null, "new", 2.5)))).containsExactly("p={new: 2.5, normalized_name: alpha, title: U}");
    assertStats(3, 0, 0, 0);
    assertThat(run("MERGE (n:Entity {normalized_name:'alpha'}) ON MATCH SET n += $props RETURN properties(n) AS p", map("props", map("title", "U", "new", 2.5)))).containsExactly("p={new: 2.5, normalized_name: alpha, title: U}");
    assertStats(2, 0, 0, 0);
    assertThat(run("MERGE (n:Entity {normalized_name:'beta'}) ON CREATE SET n += $props ON MATCH SET n.hit = true RETURN properties(n) AS p", map("props", map("title", "V")))).containsExactly("p={normalized_name: beta, title: V}");
    assertStats(2, 1, 0, 1);
    assertThat(run("MERGE (n:Entity {normalized_name:'beta'}) ON CREATE SET n += $props ON MATCH SET n.hit = true RETURN properties(n) AS p", map("props", map("title", "V")))).containsExactly("p={hit: true, normalized_name: beta, title: V}");
    assertStats(1, 0, 0, 0);
  }

  @Test
  void aLabelIsAddedByOnCreateAndOnMatch() {
    run("CREATE (:Entity {normalized_name:'alpha'})");
    assertThat(run("MERGE (n:Entity {normalized_name:'alpha'}) ON MATCH SET n:Seen RETURN labels(n) AS l")).containsExactly("l=[Entity, Seen]");
    assertStats(0, 0, 0, 1);
    assertThat(run("MERGE (n:Entity {normalized_name:'gamma'}) ON CREATE SET n:Fresh RETURN size(labels(n)) AS l")).containsExactly("l=2");
    assertStats(1, 1, 0, 2);
    assertThat(run("MATCH (n:Fresh) RETURN n.normalized_name AS nn")).containsExactly("nn=gamma");
    assertThat(run("MATCH (n:Seen) RETURN n.normalized_name AS nn")).containsExactly("nn=alpha");
  }

  @Test
  void duplicateKeysInOneUnwindBatchCreateOneNodeAndMatchTheRest() {
    run("CREATE CONSTRAINT FOR (n:Entity) REQUIRE n.normalized_name IS UNIQUE");
    assertThat(run("UNWIND $rows AS row MERGE (n:Entity {normalized_name: row.norm}) ON CREATE SET n.external_id = row.eid, n.hits = 1 ON MATCH SET n.hits = n.hits + 1 RETURN n.normalized_name AS nn, n.external_id AS eid, n.hits AS hits", map("rows", list(map("norm", "a", "eid", "e1"), map("norm", "b", "eid", "e2"), map("norm", "a", "eid", "e3"), map("norm", "b", "eid", "e4"), map("norm", "a", "eid", "e5"))))).containsExactlyInAnyOrder("eid=e1, hits=3, nn=a", "eid=e2, hits=2, nn=b", "eid=e1, hits=3, nn=a", "eid=e2, hits=2, nn=b", "eid=e1, hits=3, nn=a");
    assertStats(9, 2, 0, 2);
    assertThat(run("MATCH (n:Entity) RETURN n.normalized_name AS nn, n.external_id AS eid, n.hits AS hits ORDER BY nn")).containsExactly("eid=e1, hits=3, nn=a", "eid=e2, hits=2, nn=b");
  }

  @Test
  void aNullKeyInAMergeIsRefusedAndNothingIsWritten() {
    run("CREATE CONSTRAINT FOR (n:Entity) REQUIRE n.normalized_name IS UNIQUE");
    assertThatThrownBy(() -> run("UNWIND $rows AS row MERGE (n:Entity {normalized_name: row.norm}) RETURN n.normalized_name AS nn", map("rows", list(map("norm", "a"), map("norm", null))))).isInstanceOf(Exception.class);
    assertThat(run("MATCH (n:Entity) RETURN count(n) AS c")).containsExactly("c=0");
  }

  @Test
  void aMergeOnTwoPropertiesMatchesOnlyBothTogether() {
    run("CREATE (:T {a:1, b:'x'}), (:T {a:1, b:'y'})");
    assertThat(run("MERGE (n:T {a:1, b:'y'}) RETURN n.a AS a, n.b AS b")).containsExactly("a=1, b=y");
    assertStats(0, 0, 0, 0);
    assertThat(run("MERGE (n:T {a:1, b:'z'}) RETURN n.a AS a, n.b AS b")).containsExactly("a=1, b=z");
    assertStats(2, 1, 0, 1);
    assertThat(run("MERGE (n:T {a:1}) RETURN n.a AS a, n.b AS b")).containsExactlyInAnyOrder("a=1, b=z", "a=1, b=x", "a=1, b=y");
    assertStats(0, 0, 0, 0);
    assertThat(run("MATCH (n:T) RETURN n.a AS a, n.b AS b ORDER BY a, b")).containsExactly("a=1, b=x", "a=1, b=y", "a=1, b=z");
  }

  @Test
  void aMergeOnSeveralLabelsMatchesOnlyANodeCarryingAllOfThem() {
    run("CREATE (:A {k:1}), (:A:B {k:2})");
    assertThat(run("MERGE (n:A:B {k:1}) RETURN n.k AS k")).containsExactly("k=1");
    assertStats(1, 1, 0, 2);
    assertThat(run("MERGE (n:A:B {k:2}) RETURN n.k AS k")).containsExactly("k=2");
    assertStats(0, 0, 0, 0);
    assertThat(run("MATCH (n:A:B) RETURN n.k AS k ORDER BY k")).containsExactly("k=1", "k=2");
  }

  @Test
  void aDirectedEdgeMergeCreatesTheEdgeOnceAndMatchesItAfterwards() {
    run("CREATE (:Chunk {external_id:'c1'}), (:Chunk {external_id:'c2'}), (:Entity {external_id:'e1'}), (:Entity {external_id:'e2'})");
    assertThat(run("UNWIND $rows AS row MATCH (c:Chunk {external_id: row.cid}) MATCH (e:Entity {external_id: row.eid}) MERGE (c)-[:MENTIONS]->(e) RETURN c.external_id AS cid, e.external_id AS external_id", map("rows", list(map("cid", "c1", "eid", "e1"), map("cid", "c1", "eid", "e2"), map("cid", "c2", "eid", "e1"), map("cid", "cX", "eid", "e1"))))).containsExactlyInAnyOrder("cid=c1, external_id=e1", "cid=c1, external_id=e2", "cid=c2, external_id=e1");
    assertStats(0, 0, 3, 0);
    assertThat(run("UNWIND $rows AS row MATCH (c:Chunk {external_id: row.cid}) MATCH (e:Entity {external_id: row.eid}) MERGE (c)-[:MENTIONS]->(e) RETURN c.external_id AS cid, e.external_id AS external_id", map("rows", list(map("cid", "c1", "eid", "e1"), map("cid", "c1", "eid", "e2"), map("cid", "c2", "eid", "e2"))))).containsExactlyInAnyOrder("cid=c1, external_id=e1", "cid=c1, external_id=e2", "cid=c2, external_id=e2");
    assertStats(0, 0, 1, 0);
    assertThat(run("MATCH (c:Chunk)-[r:MENTIONS]->(e:Entity) RETURN c.external_id AS c, e.external_id AS e ORDER BY c, e")).containsExactly("c=c1, e=e1", "c=c1, e=e2", "c=c2, e=e1", "c=c2, e=e2");
  }

  @Test
  void duplicatePairsInOneBatchCreateOneEdge() {
    run("CREATE (:Chunk {external_id:'c1'}), (:Entity {external_id:'e1'})");
    assertThat(run("UNWIND $rows AS row MATCH (c:Chunk {external_id: row.cid}) MATCH (e:Entity {external_id: row.eid}) MERGE (c)-[:MENTIONS]->(e) RETURN count(*) AS n", map("rows", list(map("cid", "c1", "eid", "e1"), map("cid", "c1", "eid", "e1"), map("cid", "c1", "eid", "e1"))))).containsExactly("n=3");
    assertStats(0, 0, 1, 0);
    assertThat(run("MATCH (:Chunk)-[r:MENTIONS]->(:Entity) RETURN count(r) AS n")).containsExactly("n=1");
  }

  @Test
  void aDirectedMergeDoesNotMatchTheOppositeDirectionButAnUndirectedOneDoes() {
    run("CREATE (a:N {id:1}), (b:N {id:2}), (a)-[:R]->(b)");
    assertThat(run("MATCH (a:N {id:2}), (b:N {id:1}) MERGE (a)-[r:R]->(b) RETURN a.id AS a, b.id AS b")).containsExactly("a=2, b=1");
    assertStats(0, 0, 1, 0);
    assertThat(run("MATCH (a:N {id:1}), (b:N {id:2}) MERGE (a)-[r:R]-(b) RETURN a.id AS a, b.id AS b")).containsExactlyInAnyOrder("a=1, b=2", "a=1, b=2");
    assertStats(0, 0, 0, 0);
    assertThat(run("MATCH (a:N {id:2}), (b:N {id:1}) MERGE (a)-[r:R]-(b) RETURN a.id AS a, b.id AS b")).containsExactlyInAnyOrder("a=2, b=1", "a=2, b=1");
    assertStats(0, 0, 0, 0);
    assertThat(run("MATCH (a:N)-[r:R]->(b:N) RETURN a.id AS a, b.id AS b ORDER BY a, b")).containsExactly("a=1, b=2", "a=2, b=1");
  }

  @Test
  void anUndirectedMergeBetweenUnconnectedNodesCreatesAnOutgoingEdge() {
    run("CREATE (:N {id:1}), (:N {id:2})");
    assertThat(run("MATCH (a:N {id:2}), (b:N {id:1}) MERGE (a)-[r:R]-(b) RETURN startNode(r).id AS s, endNode(r).id AS e")).containsExactly("e=1, s=2");
    assertStats(0, 0, 1, 0);
    assertThat(run("MATCH (a:N {id:1}), (b:N {id:2}) MERGE (a)-[r:R]-(b) RETURN startNode(r).id AS s, endNode(r).id AS e")).containsExactly("e=1, s=2");
    assertStats(0, 0, 0, 0);
  }

  @Test
  void aMergeOfAnEdgeOfAnotherTypeCreatesASecondEdgeBetweenTheSamePair() {
    run("CREATE (a:N {id:1}), (b:N {id:2}), (a)-[:R]->(b)");
    assertThat(run("MATCH (a:N {id:1}), (b:N {id:2}) MERGE (a)-[r:S]->(b) RETURN type(r) AS t")).containsExactly("t=S");
    assertStats(0, 0, 1, 0);
    assertThat(run("MATCH (a:N {id:1}), (b:N {id:2}) MERGE (a)-[r:S]->(b) RETURN type(r) AS t")).containsExactly("t=S");
    assertStats(0, 0, 0, 0);
    assertThat(run("MATCH (:N)-[r]->(:N) RETURN type(r) AS t ORDER BY t")).containsExactly("t=R", "t=S");
  }

  @Test
  void aSelfLoopMergeCreatesOneLoop() {
    run("CREATE (:N {id:1})");
    assertThat(run("MATCH (a:N {id:1}) MERGE (a)-[r:R]->(a) RETURN startNode(r).id AS s, endNode(r).id AS e")).containsExactly("e=1, s=1");
    assertStats(0, 0, 1, 0);
    assertThat(run("MATCH (a:N {id:1}) MERGE (a)-[r:R]->(a) RETURN startNode(r).id AS s, endNode(r).id AS e")).containsExactly("e=1, s=1");
    assertStats(0, 0, 0, 0);
    assertThat(run("MATCH (a:N)-[r:R]->(b:N) RETURN count(r) AS c")).containsExactly("c=1");
  }

  @Test
  void anEdgeMergeMatchesOnItsPropertiesAndCreatesANewEdgeForOtherValues() {
    run("CREATE (a:N {id:1}), (b:N {id:2}), (a)-[:R {uuid:'e1', w:1}]->(b)");
    assertThat(run("MATCH (a:N {id:1}), (b:N {id:2}) MERGE (a)-[r:R {uuid:'e1'}]->(b) RETURN r.uuid AS u, r.w AS w")).containsExactly("u=e1, w=1");
    assertStats(0, 0, 0, 0);
    assertThat(run("MATCH (a:N {id:1}), (b:N {id:2}) MERGE (a)-[r:R {uuid:'e2'}]->(b) RETURN r.uuid AS u, r.w AS w")).containsExactly("u=e2, w=null");
    assertStats(1, 0, 1, 0);
    assertThat(run("MATCH (a:N {id:1}), (b:N {id:2}) MERGE (a)-[r:R {uuid:'e1', w:2}]->(b) RETURN r.uuid AS u, r.w AS w")).containsExactly("u=e1, w=2");
    assertStats(2, 0, 1, 0);
    assertThat(run("MATCH (:N)-[r:R]->(:N) RETURN r.uuid AS u, r.w AS w ORDER BY u, w")).containsExactly("u=e1, w=1", "u=e1, w=2", "u=e2, w=null");
  }

  @Test
  void aRelatesToMergeWithSetOfAMapReplacesThePropertiesOfAnExistingEdge() {
    run("CREATE (a:Entity {uuid:'a'}), (b:Entity {uuid:'b'}), (c:Entity {uuid:'c'}), (a)-[:RELATES_TO {uuid:'e1', fact:'old', extra:1}]->(b)");
    assertThat(run("UNWIND $edges AS edge MATCH (s:Entity {uuid: edge.src}) MATCH (t:Entity {uuid: edge.dst}) MERGE (s)-[e:RELATES_TO {uuid: edge.uuid}]->(t) SET e = edge RETURN edge.uuid AS uuid", map("edges", list(map("uuid", "e1", "src", "a", "dst", "b", "fact", "new", "weight", 0.5, "gone", null), map("uuid", "e2", "src", "a", "dst", "c", "fact", "second", "weight", 1.5, "gone", null))))).containsExactlyInAnyOrder("uuid=e1", "uuid=e2");
    assertStats(12, 0, 1, 0);
    assertThat(run("MATCH (s:Entity)-[e:RELATES_TO]->(t:Entity) RETURN s.uuid AS s, t.uuid AS t, properties(e) AS p ORDER BY s, t")).containsExactly("p={dst: b, fact: new, src: a, uuid: e1, weight: 0.5}, s=a, t=b", "p={dst: c, fact: second, src: a, uuid: e2, weight: 1.5}, s=a, t=c");
  }

  @Test
  void aMergeEdgeWithOnCreateAndOnMatchSetCountsEachBranch() {
    run("CREATE (a:N {id:1}), (b:N {id:2})");
    assertThat(run("MATCH (a:N {id:1}), (b:N {id:2}) MERGE (a)-[r:R]->(b) ON CREATE SET r.created = $now, r.n = 1 ON MATCH SET r.n = r.n + 1, r.seen = $now RETURN properties(r) AS p", map("now", "t1"))).containsExactly("p={created: t1, n: 1}");
    assertStats(2, 0, 1, 0);
    assertThat(run("MATCH (a:N {id:1}), (b:N {id:2}) MERGE (a)-[r:R]->(b) ON CREATE SET r.created = $now, r.n = 1 ON MATCH SET r.n = r.n + 1, r.seen = $now RETURN properties(r) AS p", map("now", "t2"))).containsExactly("p={created: t1, n: 2, seen: t2}");
    assertStats(2, 0, 0, 0);
    assertThat(run("MATCH (a:N {id:1}), (b:N {id:2}) MERGE (a)-[r:R]->(b) ON CREATE SET r.created = $now, r.n = 1 ON MATCH SET r.n = r.n + 1, r.seen = $now RETURN properties(r) AS p", map("now", "t3"))).containsExactly("p={created: t1, n: 3, seen: t3}");
    assertStats(2, 0, 0, 0);
  }

  @Test
  void aMergeOfAWholePathCreatesBothEndsAndTheEdgeOrMatchesThemAll() {
    run("CREATE (:Doc {k:1})");
    assertThat(run("MERGE (a:Doc {k:1})-[r:HAS]->(b:Part {k:2}) RETURN a.k AS a, b.k AS b")).containsExactly("a=1, b=2");
    assertStats(2, 2, 1, 2);
    assertThat(run("MERGE (a:Doc {k:1})-[r:HAS]->(b:Part {k:2}) RETURN a.k AS a, b.k AS b")).containsExactly("a=1, b=2");
    assertStats(0, 0, 0, 0);
    assertThat(run("MERGE (a:Doc {k:3})-[r:HAS]->(b:Part {k:4}) RETURN a.k AS a, b.k AS b")).containsExactly("a=3, b=4");
    assertStats(2, 2, 1, 2);
    assertThat(run("MATCH (a:Doc)-[:HAS]->(b:Part) RETURN a.k AS a, b.k AS b ORDER BY a, b")).containsExactly("a=1, b=2", "a=3, b=4");
  }

  @Test
  void aMergeFromABoundNodeToANewFarNodeCreatesItOnceAndRunsTheSetActions() {
    run("CREATE (:Chunk {external_id:'c1'}), (:Chunk {external_id:'c2'})");
    assertThat(run("MATCH (c:Chunk) MERGE (c)-[r:MENTIONS]->(e:Entity {name:'x'}) ON CREATE SET e.made = 1 ON MATCH SET e.hit = coalesce(e.hit, 0) + 1 RETURN c.external_id AS c, e.made AS made, e.hit AS hit ORDER BY c")).containsExactly("c=c1, hit=null, made=1", "c=c2, hit=null, made=1");
    assertStats(4, 2, 2, 2);
    assertThat(run("MATCH (e:Entity) RETURN count(e) AS c")).containsExactly("c=2");
    assertThat(run("MATCH (:Chunk)-[r:MENTIONS]->(e:Entity) RETURN count(r) AS c")).containsExactly("c=2");
  }

  @Test
  void aMergeWithTheFarEndBoundByAnEarlierClauseAndAnIncomingEdge() {
    run("CREATE (:Chunk {external_id:'c1'}), (:Entity {external_id:'e1'})");
    assertThat(run("MATCH (c:Chunk), (e:Entity) MERGE (e)<-[r:MENTIONS]-(c) RETURN type(r) AS t, startNode(r).external_id AS s")).containsExactly("s=c1, t=MENTIONS");
    assertStats(0, 0, 1, 0);
    assertThat(run("MATCH (c:Chunk), (e:Entity) MERGE (e)<-[r:MENTIONS]-(c) RETURN type(r) AS t, startNode(r).external_id AS s")).containsExactly("s=c1, t=MENTIONS");
    assertStats(0, 0, 0, 0);
  }

  @Test
  void theMergeOfAnEdgeAfterARemoveInTheSameStatementLinksTheChunk() {
    run("CREATE (:Chunk {external_id:'c1', deleted_at:'x'}), (:Chunk {external_id:'c2'}), (:Record {external_id:'r1'})");
    assertThat(run("UNWIND $eids AS eid MATCH (c:Chunk {external_id: eid}) REMOVE c.deleted_at WITH c MATCH (r:Record {external_id: 'r1'}) MERGE (c)-[:OF_RECORD]->(r) RETURN r.deleted_at IS NOT NULL AS dead, c.external_id AS id", map("eids", list("c1", "c2", "cX")))).containsExactlyInAnyOrder("dead=false, id=c1", "dead=false, id=c2");
    assertStats(1, 0, 2, 0);
    assertThat(run("UNWIND $eids AS eid MATCH (c:Chunk {external_id: eid}) REMOVE c.deleted_at WITH c MATCH (r:Record {external_id: 'r1'}) MERGE (c)-[:OF_RECORD]->(r) RETURN r.deleted_at IS NOT NULL AS dead, c.external_id AS id", map("eids", list("c1", "c2", "cX")))).containsExactlyInAnyOrder("dead=false, id=c1", "dead=false, id=c2");
    assertStats(0, 0, 0, 0);
  }

  @Test
  void anEntityPageCreatesThenReplaysThenRevivesATombstone() {
    run("CREATE CONSTRAINT FOR (n:Entity) REQUIRE n.normalized_name IS UNIQUE");
    assertThat(run("UNWIND $rows AS row MERGE (n:Entity {normalized_name: row.norm}) WITH n, row, (n.embedding IS NULL OR n.embedding_model IS NULL OR n.embedding_model <> row.model OR row.force = true) AS vector_stale WITH n, row, vector_stale, (vector_stale OR (n.created_at IS NULL OR n.external_id IS NULL OR n.deleted_at IS NOT NULL OR n.deleted IS NULL OR n.deleted <> false OR any(k IN keys(row.props) WHERE NOT coalesce(n[k] = row.props[k], false)))) AS wrote FOREACH (_ IN CASE WHEN wrote THEN [1] ELSE [] END | SET n += row.props, n.external_id = CASE WHEN n.deleted_at IS NULL THEN coalesce(n.external_id, row.eid) ELSE row.eid END, n.created_at = coalesce(n.created_at, row.now), n.deleted = false REMOVE n.deleted_at) RETURN n.normalized_name AS normalized_name, n.external_id AS external_id, n.graph_name AS graph_name, n.aliases AS aliases, n.type_name AS type_name, vector_stale, wrote", map("rows", list(map("norm", "a", "eid", "e1", "now", "t1", "model", "m1", "force", false, "props", map("graph_name", "g", "type_name", "T", "aliases", list("x"))), map("norm", "b", "eid", "e2", "now", "t1", "model", "m1", "force", false, "props", map("graph_name", "g", "type_name", "T")))))).containsExactlyInAnyOrder("aliases=[x], external_id=e1, graph_name=g, normalized_name=a, type_name=T, vector_stale=true, wrote=true", "aliases=null, external_id=e2, graph_name=g, normalized_name=b, type_name=T, vector_stale=true, wrote=true");
    assertStats(13, 2, 0, 2);
    run("MATCH (n:Entity {normalized_name:'a'}) SET n.embedding = $v, n.embedding_model = 'm1'", map("v", list(0.5, 0.25)));
    run("MATCH (n:Entity {normalized_name:'b'}) SET n.embedding = $v, n.embedding_model = 'm1'", map("v", list(0.5, 0.25)));
    assertThat(run("UNWIND $rows AS row MERGE (n:Entity {normalized_name: row.norm}) WITH n, row, (n.embedding IS NULL OR n.embedding_model IS NULL OR n.embedding_model <> row.model OR row.force = true) AS vector_stale WITH n, row, vector_stale, (vector_stale OR (n.created_at IS NULL OR n.external_id IS NULL OR n.deleted_at IS NOT NULL OR n.deleted IS NULL OR n.deleted <> false OR any(k IN keys(row.props) WHERE NOT coalesce(n[k] = row.props[k], false)))) AS wrote FOREACH (_ IN CASE WHEN wrote THEN [1] ELSE [] END | SET n += row.props, n.external_id = CASE WHEN n.deleted_at IS NULL THEN coalesce(n.external_id, row.eid) ELSE row.eid END, n.created_at = coalesce(n.created_at, row.now), n.deleted = false REMOVE n.deleted_at) RETURN n.normalized_name AS normalized_name, n.external_id AS external_id, n.graph_name AS graph_name, n.aliases AS aliases, n.type_name AS type_name, vector_stale, wrote", map("rows", list(map("norm", "a", "eid", "e9", "now", "t2", "model", "m1", "force", false, "props", map("graph_name", "g", "type_name", "T", "aliases", list("x"))), map("norm", "b", "eid", "e9", "now", "t2", "model", "m2", "force", false, "props", map("graph_name", "g", "type_name", "U")))))).containsExactlyInAnyOrder("aliases=[x], external_id=e1, graph_name=g, normalized_name=a, type_name=T, vector_stale=false, wrote=false", "aliases=null, external_id=e2, graph_name=g, normalized_name=b, type_name=U, vector_stale=true, wrote=true");
    assertStats(5, 0, 0, 0);
    run("MATCH (n:Entity {normalized_name:'a'}) SET n.deleted_at = 't3', n.deleted = true");
    assertThat(run("UNWIND $rows AS row MERGE (n:Entity {normalized_name: row.norm}) WITH n, row, (n.embedding IS NULL OR n.embedding_model IS NULL OR n.embedding_model <> row.model OR row.force = true) AS vector_stale WITH n, row, vector_stale, (vector_stale OR (n.created_at IS NULL OR n.external_id IS NULL OR n.deleted_at IS NOT NULL OR n.deleted IS NULL OR n.deleted <> false OR any(k IN keys(row.props) WHERE NOT coalesce(n[k] = row.props[k], false)))) AS wrote FOREACH (_ IN CASE WHEN wrote THEN [1] ELSE [] END | SET n += row.props, n.external_id = CASE WHEN n.deleted_at IS NULL THEN coalesce(n.external_id, row.eid) ELSE row.eid END, n.created_at = coalesce(n.created_at, row.now), n.deleted = false REMOVE n.deleted_at) RETURN n.normalized_name AS normalized_name, n.external_id AS external_id, n.graph_name AS graph_name, n.aliases AS aliases, n.type_name AS type_name, vector_stale, wrote", map("rows", list(map("norm", "a", "eid", "e7", "now", "t4", "model", "m1", "force", false, "props", map("graph_name", "g", "type_name", "T", "aliases", list("x"))))))).containsExactly("aliases=[x], external_id=e7, graph_name=g, normalized_name=a, type_name=T, vector_stale=false, wrote=true");
    assertStats(7, 0, 0, 0);
    assertThat(run("MATCH (n:Entity) RETURN n.normalized_name AS nn, properties(n) AS p ORDER BY nn")).containsExactly("nn=a, p={aliases: [x], created_at: t1, deleted: false, embedding: [0.5, 0.25], embedding_model: m1, external_id: e7, graph_name: g, normalized_name: a, type_name: T}", "nn=b, p={created_at: t1, deleted: false, embedding: [0.5, 0.25], embedding_model: m1, external_id: e2, graph_name: g, normalized_name: b, type_name: U}");
  }

  @Test
  void aNodeMergeWithoutAnyIndexScansAndAnIndexedNonUniqueKeyStillMatchesOnce() {
    run("CREATE (:Plain {k:'a'}), (:Plain {k:'a'}), (:Indexed {k:'a'}), (:Indexed {k:'a'})");
    run("CREATE INDEX FOR (n:Indexed) ON (n.k)");
    assertThat(run("UNWIND ['a', 'b', 'b'] AS v MERGE (n:Plain {k: v}) RETURN n.k AS k")).containsExactlyInAnyOrder("k=a", "k=a", "k=b", "k=b");
    assertStats(1, 1, 0, 1);
    assertThat(run("UNWIND ['a', 'b', 'b'] AS v MERGE (n:Indexed {k: v}) RETURN n.k AS k")).containsExactlyInAnyOrder("k=a", "k=a", "k=b", "k=b");
    assertStats(1, 1, 0, 1);
    assertThat(run("MATCH (n:Plain) RETURN count(n) AS c")).containsExactly("c=3");
    assertThat(run("MATCH (n:Indexed) RETURN count(n) AS c")).containsExactly("c=3");
  }

  @Test
  void aCompositeUniqueKeyMergeMatchesOnTheWholeKey() {
    run("CREATE CONSTRAINT FOR (n:Chunk) REQUIRE (n.record_id, n.position) IS UNIQUE");
    assertThat(run("UNWIND $rows AS row MERGE (c:Chunk {record_id: row.r, position: row.p}) ON CREATE SET c.new = true ON MATCH SET c.new = false RETURN c.record_id AS r, c.position AS p, c.new AS n", map("rows", list(map("r", "r1", "p", 0), map("r", "r1", "p", 1), map("r", "r1", "p", 0), map("r", "r2", "p", 0))))).containsExactlyInAnyOrder("n=false, p=0, r=r1", "n=true, p=1, r=r1", "n=false, p=0, r=r1", "n=true, p=0, r=r2");
    assertStats(10, 3, 0, 3);
    assertThat(run("MATCH (c:Chunk) RETURN c.record_id AS r, c.position AS p, c.new AS n ORDER BY r, p")).containsExactly("n=false, p=0, r=r1", "n=true, p=1, r=r1", "n=true, p=0, r=r2");
  }

  @Test
  void aMergeKeyTakenFromANestedParameterMapAndAListElement() {
    run("CREATE CONSTRAINT FOR (n:Entity) REQUIRE n.normalized_name IS UNIQUE");
    assertThat(run("MERGE (a:Entity {normalized_name: $m.k}) MERGE (b:Entity {normalized_name: $l[0]}) MERGE (c:Entity {normalized_name: toLower('GAMMA')}) RETURN a.normalized_name AS a, b.normalized_name AS b, c.normalized_name AS c", map("m", map("k", "alpha"), "l", list("beta", "gamma")))).containsExactly("a=alpha, b=beta, c=gamma");
    assertStats(3, 3, 0, 3);
    assertThat(run("MERGE (a:Entity {normalized_name: $m.k}) MERGE (b:Entity {normalized_name: $l[0]}) MERGE (c:Entity {normalized_name: toLower('GAMMA')}) RETURN a.normalized_name AS a, b.normalized_name AS b, c.normalized_name AS c", map("m", map("k", "alpha"), "l", list("beta", "gamma")))).containsExactly("a=alpha, b=beta, c=gamma");
    assertStats(0, 0, 0, 0);
    assertThat(run("MATCH (n:Entity) RETURN count(n) AS c")).containsExactly("c=3");
  }

  @Test
  void aMergeThatMatchesSeveralNodesReturnsEveryOneAndRunsOnMatchOnEach() {
    run("CREATE (:M {k:1, n:0}), (:M {k:1, n:5}), (:M {k:2, n:7})");
    assertThat(run("MERGE (m:M {k:1}) ON MATCH SET m.n = m.n + 1 RETURN m.n AS n ORDER BY n")).containsExactly("n=1", "n=6");
    assertStats(2, 0, 0, 0);
    assertThat(run("MERGE (m:M {k:3}) ON CREATE SET m.n = -1 RETURN m.n AS n")).containsExactly("n=-1");
    assertStats(2, 1, 0, 1);
    assertThat(run("MATCH (m:M) RETURN m.k AS k, m.n AS n ORDER BY k, n")).containsExactly("k=1, n=1", "k=1, n=6", "k=2, n=7", "k=3, n=-1");
  }

  @Test
  void aMergeBoundToAnEarlierMatchedNodeDoesNotCreateAFarEndTwice() {
    run("CREATE (:Doc {id:1}), (:Doc {id:2})");
    assertThat(run("MATCH (d:Doc) MERGE (d)-[:HAS]->(p:Part {name: 'intro'}) RETURN d.id AS id, p.name AS n ORDER BY id")).containsExactly("id=1, n=intro", "id=2, n=intro");
    assertStats(2, 2, 2, 2);
    assertThat(run("MATCH (d:Doc) MERGE (d)-[:HAS]->(p:Part {name: 'intro'}) RETURN d.id AS id, p.name AS n ORDER BY id")).containsExactly("id=1, n=intro", "id=2, n=intro");
    assertStats(0, 0, 0, 0);
    assertThat(run("MATCH (p:Part) RETURN count(p) AS c")).containsExactly("c=2");
  }

  @Test
  void aMergeWithAnEdgeInTheOppositeDirectionBoundByTheFarEnd() {
    run("CREATE (:Doc {id:1}), (:Part {name:'p'})");
    assertThat(run("MATCH (p:Part) MERGE (p)<-[:HAS]-(d:Doc {id: 1}) RETURN d.id AS id")).containsExactly("id=1");
    assertStats(1, 1, 1, 1);
    assertThat(run("MATCH (p:Part) MERGE (p)<-[:HAS]-(d:Doc {id: 2}) RETURN d.id AS id")).containsExactly("id=2");
    assertStats(1, 1, 1, 1);
    assertThat(run("MATCH (d:Doc)-[:HAS]->(p:Part) RETURN d.id AS id ORDER BY id")).containsExactly("id=1", "id=2");
  }

  @Test
  void aMergeSetsTheRelationshipPropertiesFromTheMergePatternAndFromAnOnCreateMap() {
    run("CREATE (:N {id:1}), (:N {id:2})");
    assertThat(run("MATCH (a:N {id:1}), (b:N {id:2}) MERGE (a)-[r:R {k: 'one'}]->(b) ON CREATE SET r += $m RETURN properties(r) AS p", map("m", map("a", 1, "c", "x")))).containsExactly("p={a: 1, c: x, k: one}");
    assertStats(3, 0, 1, 0);
    assertThat(run("MATCH (a:N {id:1}), (b:N {id:2}) MERGE (a)-[r:R {k: 'one'}]->(b) ON CREATE SET r += $m ON MATCH SET r.hit = true RETURN properties(r) AS p", map("m", map("a", 2)))).containsExactly("p={a: 1, c: x, hit: true, k: one}");
    assertStats(1, 0, 0, 0);
  }

  @Test
  void aTypeLessMergeIsRefusedAndAnEmptyRelationshipTypeIsNotCreated() {
    assertThatThrownBy(() -> run("MATCH (a:N), (b:N) MERGE (a)-[r]->(b)")).isInstanceOf(Exception.class);
  }
}
