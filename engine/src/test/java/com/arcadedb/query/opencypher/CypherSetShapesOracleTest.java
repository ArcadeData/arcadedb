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
 * SET in the shapes of a batched graph loader (a map merged with {@code SET n += $p}, a vector assigned from a row, a timestamp from a parameter, a dynamic key, a label, an expression target, a same-value self assignment). Every expected row and statistic is what Neo4j 5.26 returns for the same data and statement. A row is rendered
 * {@code k=v, k=v} with the keys sorted, integers without a fraction.
 */
class CypherSetShapesOracleTest extends TestHelper {
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
  void aMapSetOnAMatchedEntityMergesTheMapAndNullRemovesAProperty() {
    run("CREATE (:Entity {normalized_name:'a', keep:1, drop_me:'x', n:1})");
    assertThat(run("MATCH (n:Entity {normalized_name:'a'}) SET n += $p RETURN properties(n) AS p", map("p", map("title", "T", "drop_me", null, "n", 2, "nothing", null, "score", 0.5)))).containsExactly("p={keep: 1, n: 2, normalized_name: a, score: 0.5, title: T}");
    assertStats(4, 0, 0, 0);
    assertThat(run("MATCH (n:Entity) RETURN properties(n) AS p")).containsExactly("p={keep: 1, n: 2, normalized_name: a, score: 0.5, title: T}");
  }

  @Test
  void aReplaceMapDropsEveryOtherPropertyAndCountsTheRemovedOnes() {
    run("CREATE (:Entity {normalized_name:'a', keep:1, drop_me:'x', n:1})");
    assertThat(run("MATCH (n:Entity {normalized_name:'a'}) SET n = $p RETURN properties(n) AS p", map("p", map("title", "T", "drop_me", null, "normalized_name", "a")))).containsExactly("p={normalized_name: a, title: T}");
    assertStats(5, 0, 0, 0);
    assertThat(run("MATCH (n:Entity) RETURN properties(n) AS p")).containsExactly("p={normalized_name: a, title: T}");
  }

  @Test
  void aReplaceMapWithAnEmptyMapClearsTheNodeAndAMergeMapWithAnEmptyMapChangesNothing() {
    run("CREATE (:Entity {normalized_name:'a', k:1, j:2})");
    assertThat(run("MATCH (n:Entity) SET n += $p RETURN properties(n) AS p", map("p", map()))).containsExactly("p={j: 2, k: 1, normalized_name: a}");
    assertStats(0, 0, 0, 0);
    assertThat(run("MATCH (n:Entity) SET n = $p RETURN properties(n) AS p", map("p", map()))).containsExactly("p={}");
    assertStats(3, 0, 0, 0);
  }

  @Test
  void aNodeIsCopiedOntoAnotherByReplaceAndByMerge() {
    run("CREATE (:S {id:1, a:1, b:'x'}), (:T {id:2, c:3, a:9}), (:U {id:3, c:3, a:9})");
    assertThat(run("MATCH (s:S), (t:T) SET t = s RETURN properties(t) AS p")).containsExactly("p={a: 1, b: x, id: 1}");
    assertStats(4, 0, 0, 0);
    assertThat(run("MATCH (s:S), (u:U) SET u += s RETURN properties(u) AS p")).containsExactly("p={a: 1, b: x, c: 3, id: 1}");
    assertStats(3, 0, 0, 0);
  }

  @Test
  void aRelationshipPropertyMapReplacesTheRelationshipProperties() {
    run("CREATE (a:Entity {uuid:'a'}), (b:Entity {uuid:'b'}), (a)-[:RELATES_TO {uuid:'e1', fact:'old', extra:1}]->(b)");
    assertThat(run("MATCH (a:Entity {uuid:'a'})-[e:RELATES_TO {uuid:'e1'}]->(b:Entity {uuid:'b'}) SET e = $edge RETURN properties(e) AS p", map("edge", map("uuid", "e1", "fact", "new", "weight", 0.25, "gone", null)))).containsExactly("p={fact: new, uuid: e1, weight: 0.25}");
    assertStats(4, 0, 0, 0);
    assertThat(run("MATCH ()-[e:RELATES_TO]->() RETURN properties(e) AS p")).containsExactly("p={fact: new, uuid: e1, weight: 0.25}");
  }

  @Test
  void aPropertyIsSetFromAParameterAndASameValueAssignmentStillCounts() {
    run("CREATE (:Entity {normalized_name:'a', updated_at:'t0'})");
    assertThat(run("MATCH (n:Entity {normalized_name:'a'}) SET n.updated_at = $now RETURN n.updated_at AS u", map("now", "t1"))).containsExactly("u=t1");
    assertStats(1, 0, 0, 0);
    assertThat(run("MATCH (n:Entity {normalized_name:'a'}) SET n.updated_at = $now RETURN n.updated_at AS u", map("now", "t1"))).containsExactly("u=t1");
    assertStats(1, 0, 0, 0);
    assertThat(run("MATCH (n:Entity {normalized_name:'a'}) SET n.updated_at = $now, n.created_at = coalesce(n.created_at, $now) RETURN n.updated_at AS u, n.created_at AS c", map("now", "t2"))).containsExactly("c=t2, u=t2");
    assertStats(2, 0, 0, 0);
  }

  @Test
  void settingAPropertyToNullRemovesItAndCountsOnlyAnExistingProperty() {
    run("CREATE (:Entity {normalized_name:'a', deleted_at:'x', other:1})");
    assertThat(run("MATCH (n:Entity) SET n.deleted_at = null RETURN properties(n) AS p")).containsExactly("p={normalized_name: a, other: 1}");
    assertStats(1, 0, 0, 0);
    assertThat(run("MATCH (n:Entity) SET n.deleted_at = null RETURN properties(n) AS p")).containsExactly("p={normalized_name: a, other: 1}");
    assertStats(0, 0, 0, 0);
    assertThat(run("MATCH (n:Entity) SET n.other = null, n.fresh = null RETURN properties(n) AS p")).containsExactly("p={normalized_name: a}");
    assertStats(1, 0, 0, 0);
  }

  @Test
  void aVectorIsStoredAsAListOfFloatsOrIntegersAndAnEmptyListIsStored() {
    run("CREATE (:Chunk {external_id:'c1'}), (:Chunk {external_id:'c2'}), (:Chunk {external_id:'c3'})");
    assertThat(run("UNWIND $rows AS row MATCH (c:Chunk {external_id: row.eid}) SET c.embedding = row.vec RETURN c.external_id AS id, c.embedding AS v ORDER BY id", map("rows", list(map("eid", "c1", "vec", list(0.5, 0.25, -1.5)), map("eid", "c2", "vec", list(1, 2, 3)), map("eid", "c3", "vec", list()))))).containsExactly("id=c1, v=[0.5, 0.25, -1.5]", "id=c2, v=[1, 2, 3]", "id=c3, v=[]");
    assertStats(3, 0, 0, 0);
    assertThat(run("MATCH (c:Chunk) RETURN c.external_id AS id, size(c.embedding) AS n ORDER BY id")).containsExactly("id=c1, n=3", "id=c2, n=3", "id=c3, n=0");
  }

  @Test
  void aVectorSetToNullThroughARowRemovesTheStoredVector() {
    run("CREATE (:Chunk {external_id:'c1', embedding:[0.5,0.25]}), (:Chunk {external_id:'c2', embedding:[1.5,2.5]})");
    assertThat(run("UNWIND $rows AS row MATCH (c:Chunk {external_id: row.eid}) SET c.embedding = row.vec RETURN c.external_id AS id, c.embedding AS v ORDER BY id", map("rows", list(map("eid", "c1", "vec", null), map("eid", "c2", "vec", list(9.5, 8.5)))))).containsExactly("id=c1, v=null", "id=c2, v=[9.5, 8.5]");
    assertStats(2, 0, 0, 0);
  }

  @Test
  void aMapRightHandSideInsideAMapSetIsRefused() {
    run("CREATE (:Entity {normalized_name:'a'})");
    assertThatThrownBy(() -> run("MATCH (n:Entity) SET n += $p", map("p", map("a", map("b", 1))))).isInstanceOf(Exception.class);
    assertThatThrownBy(() -> run("MATCH (n:Entity) SET n = $p", map("p", map("a", map("b", 1))))).isInstanceOf(Exception.class);
    assertThatThrownBy(() -> run("MATCH (n:Entity) SET n += 5")).isInstanceOf(Exception.class);
    assertThatThrownBy(() -> run("MATCH (n:Entity) SET n = 'x'")).isInstanceOf(Exception.class);
    assertThat(run("MATCH (n:Entity) RETURN properties(n) AS p")).containsExactly("p={normalized_name: a}");
  }

  @Test
  void numericKindsOfParametersAreStoredAsIntegersOrFloats() {
    run("CREATE (:N {id:1})");
    assertThat(run("MATCH (n:N) SET n.i = $i, n.big = $big, n.f = $f, n.whole = $whole, n.neg = $neg RETURN n.i AS i, n.big AS big, n.f AS f, n.whole AS whole, n.neg AS neg", map("i", 7, "big", 5000000000L, "f", 2.5, "whole", 3.0, "neg", -1))).containsExactly("big=5000000000, f=2.5, i=7, neg=-1, whole=3.0");
    assertStats(5, 0, 0, 0);
    assertThat(run("MATCH (n:N) RETURN n.i + 1 AS i1, n.big + 1 AS b1, n.whole + 1 AS w1, n.f * 2 AS f2, toString(n.whole) AS ws")).containsExactly("b1=5000000001, f2=5.0, i1=8, w1=4.0, ws=3.0");
  }

  @Test
  void aRowOfSeveralItemsIsEvaluatedAgainstThePreClauseState() {
    run("CREATE (:P {id:1, x:1, y:2}), (:P {id:2, x:5, y:6})");
    assertThat(run("MATCH (a:P {id:1}), (b:P {id:2}) SET a.x = a.y, a.y = a.x, b.x = b.y, b.y = b.x RETURN a.x AS ax, a.y AS ay, b.x AS bx, b.y AS by")).containsExactly("ax=2, ay=1, bx=6, by=5");
    assertStats(4, 0, 0, 0);
    assertThat(run("MATCH (a:P {id:1}) SET a.x = 10, a.y = a.x RETURN a.x AS ax, a.y AS ay")).containsExactly("ax=10, ay=2");
    assertStats(2, 0, 0, 0);
  }

  @Test
  void aFanoutOverTheSameNodeReadsThroughEarlierRowWrites() {
    run("CREATE (:C {id:1}), (:C {id:2})");
    assertThat(run("UNWIND [1,2,3] AS i MATCH (c:C) SET c.n = coalesce(c.n, 0) + i RETURN c.id AS id, c.n AS n ORDER BY id, n")).containsExactly("id=1, n=6", "id=1, n=6", "id=1, n=6", "id=2, n=6", "id=2, n=6", "id=2, n=6");
    assertStats(6, 0, 0, 0);
    assertThat(run("MATCH (c:C) RETURN c.id AS id, c.n AS n ORDER BY id")).containsExactly("id=1, n=6", "id=2, n=6");
  }

  @Test
  void aDynamicKeyWritesTheNamedProperty() {
    run("CREATE (:K {id:1})");
    assertThat(run("MATCH (n:K) SET n['a'] = 1 RETURN properties(n) AS p")).containsExactly("p={a: 1, id: 1}");
    assertStats(1, 0, 0, 0);
    assertThat(run("MATCH (n:K) SET n[$k] = 'x' RETURN properties(n) AS p", map("k", "b"))).containsExactly("p={a: 1, b: x, id: 1}");
    assertStats(1, 0, 0, 0);
    assertThat(run("MATCH (n:K) SET n[$k] = null RETURN properties(n) AS p", map("k", "b"))).containsExactly("p={a: 1, id: 1}");
    assertStats(1, 0, 0, 0);
    assertThat(run("MATCH (n:K) UNWIND $ks AS k SET n[k] = k RETURN properties(n) AS p", map("ks", list("p", "q", "p")))).containsExactlyInAnyOrder("p={a: 1, id: 1, p: p, q: q}", "p={a: 1, id: 1, p: p, q: q}", "p={a: 1, id: 1, p: p, q: q}");
    assertStats(3, 0, 0, 0);
  }

  @Test
  void aLabelIsAddedOnceAndAnExistingOneIsNotCounted() {
    run("CREATE (:A {id:1}), (:A:B {id:2})");
    assertThat(run("MATCH (n {id:1}) SET n:B WITH n UNWIND labels(n) AS l RETURN l ORDER BY l")).containsExactly("l=A", "l=B");
    assertStats(0, 0, 0, 1);
    assertThat(run("MATCH (n {id:1}) SET n:B WITH n UNWIND labels(n) AS l RETURN l ORDER BY l")).containsExactly("l=A", "l=B");
    assertStats(0, 0, 0, 0);
    assertThat(run("MATCH (n {id:2}) SET n:A:B:C WITH n UNWIND labels(n) AS l RETURN l ORDER BY l")).containsExactly("l=A", "l=B", "l=C");
    assertStats(0, 0, 0, 1);
    assertThat(run("MATCH (n) UNWIND labels(n) AS l RETURN n.id AS id, l ORDER BY id, l")).containsExactly("id=1, l=A", "id=1, l=B", "id=2, l=A", "id=2, l=B", "id=2, l=C");
    assertThat(run("MATCH (n:C) RETURN n.id AS id")).containsExactly("id=2");
  }

  @Test
  void aLabelAndAPropertyAreSetInOneClauseAndTheNodeKeepsItsRelationships() {
    run("CREATE (a:A {id:1})-[:R {w:1}]->(b:B {id:2})");
    assertThat(run("MATCH (a:A)-[r:R]->(b:B) SET a:Extra, a.flag = true, b:Extra RETURN labels(a) AS la, a.flag AS f, labels(b) AS lb, r.w AS w")).containsExactly("f=true, la=[A, Extra], lb=[B, Extra], w=1");
    assertStats(1, 0, 0, 2);
    assertThat(run("MATCH (a:Extra)-[r:R]->(b:Extra) RETURN a.id AS a, b.id AS b, a.flag AS f")).containsExactly("a=1, b=2, f=true");
  }

  @Test
  void aLabelSetOnANullNodeIsANoOp() {
    assertThat(run("OPTIONAL MATCH (n:Nope) SET n:X, n.p = 1 RETURN n")).containsExactly("n=null");
    assertStats(0, 0, 0, 0);
    assertThat(run("OPTIONAL MATCH (n:Nope) SET n += {a: 1} RETURN n")).containsExactly("n=null");
    assertStats(0, 0, 0, 0);
  }

  @Test
  void anExpressionTargetWritesTheChosenNodeOnly() {
    run("CREATE (:T {id:1, on:true}), (:T {id:2, on:false})");
    assertThat(run("MATCH (n:T) SET (CASE WHEN n.on THEN n END).flag = 1 RETURN n.id AS id, n.flag AS f ORDER BY id")).containsExactly("f=1, id=1", "f=null, id=2");
    assertStats(1, 0, 0, 0);
    assertThat(run("MATCH (n:T) SET (CASE WHEN n.on THEN n END).flag = null RETURN n.id AS id, n.flag AS f ORDER BY id")).containsExactly("f=null, id=1", "f=null, id=2");
    assertStats(1, 0, 0, 0);
  }

  @Test
  void aSetAfterWithAndAggregationWritesEachGroupedNode() {
    run("CREATE (:G {id:1})-[:R]->(:H), (:G {id:2})-[:R]->(:H), (:G {id:2})-[:R]->(:H)");
    assertThat(run("MATCH (g:G)-[:R]->(h:H) WITH g, count(h) AS c SET g.deg = c RETURN g.id AS id, g.deg AS d ORDER BY id, d")).containsExactly("d=1, id=1", "d=1, id=2", "d=1, id=2");
    assertStats(3, 0, 0, 0);
  }

  @Test
  void aForeachGatedByACaseWritesOnlyTheFlaggedRows() {
    run("CREATE (:Entity {normalized_name:'a', created_at:'t0'}), (:Entity {normalized_name:'b'})");
    assertThat(run("UNWIND $rows AS row MATCH (n:Entity {normalized_name: row.n}) WITH n, row FOREACH (_ IN CASE WHEN row.w THEN [1] ELSE [] END | SET n += row.props, n.created_at = coalesce(n.created_at, row.now), n.deleted = false REMOVE n.deleted_at) RETURN n.normalized_name AS nn, n.title AS t, n.created_at AS c, n.deleted AS d ORDER BY nn", map("rows", list(map("n", "a", "w", false, "now", "t1", "props", map("title", "A")), map("n", "b", "w", true, "now", "t1", "props", map("title", "B", "opt", null)))))).containsExactly("c=t0, d=null, nn=a, t=null", "c=t1, d=false, nn=b, t=B");
    assertStats(3, 0, 0, 0);
    assertThat(run("MATCH (n:Entity) RETURN n.normalized_name AS nn, properties(n) AS p ORDER BY nn")).containsExactly("nn=a, p={created_at: t0, normalized_name: a}", "nn=b, p={created_at: t1, deleted: false, normalized_name: b, title: B}");
  }

  @Test
  void aSameValueSelfAssignmentIsCountedAndLeavesTheValue() {
    run("CREATE (:Chunk {external_id:'c1', t:'x'}), (:Chunk {external_id:'c2'})");
    assertThat(run("UNWIND $rows AS row MATCH (c:Chunk {external_id: row.eid}) SET c.external_id = c.external_id RETURN c.external_id AS id ORDER BY id", map("rows", list(map("eid", "c1"), map("eid", "c2"), map("eid", "cX"))))).containsExactly("id=c1", "id=c2");
    assertStats(2, 0, 0, 0);
  }

  @Test
  void aSetFromAnotherRecordCopiesItsDeletedStateAndANullComparison() {
    run("CREATE (:Record {external_id:'r1', deleted_at:'t9'}), (:Record {external_id:'r2'}), (:Fact {external_id:'f1', deleted_at:'old'}), (:Fact {external_id:'f2', deleted_at:'old2'})");
    assertThat(run("UNWIND $rows AS row MATCH (f:Fact {external_id: row.eid}) MATCH (r:Record {external_id: row.rid}) SET f.deleted_at = r.deleted_at, f.deleted = r.deleted_at IS NOT NULL RETURN f.external_id AS id, f.deleted_at AS da, f.deleted AS d ORDER BY id", map("rows", list(map("eid", "f1", "rid", "r1"), map("eid", "f2", "rid", "r2"))))).containsExactly("d=true, da=t9, id=f1", "d=false, da=null, id=f2");
    assertStats(4, 0, 0, 0);
  }

  @Test
  void aReplaceMapKeepsInternalLabelsAndRelationships() {
    run("CREATE (a:Entity {uuid:'a', name:'x'})-[:R]->(:Entity {uuid:'b'})");
    assertThat(run("MATCH (a:Entity {uuid:'a'}) SET a = $p RETURN properties(a) AS p, labels(a) AS l", map("p", map("uuid", "a", "name", "y", "summary", "s")))).containsExactly("l=[Entity], p={name: y, summary: s, uuid: a}");
    assertStats(3, 0, 0, 0);
    assertThat(run("MATCH (a:Entity {uuid:'a'})-[:R]->(b) RETURN b.uuid AS b")).containsExactly("b=b");
  }

  @Test
  void aSetOfAListPropertyIsReadBackAndReplaced() {
    run("CREATE (:Entity {uuid:'a'})");
    assertThat(run("MATCH (n:Entity) SET n.aliases = $v RETURN n.aliases AS a", map("v", list("x", "y")))).containsExactly("a=[x, y]");
    assertStats(1, 0, 0, 0);
    assertThat(run("MATCH (n:Entity) SET n.aliases = $v + n.aliases RETURN n.aliases AS a", map("v", list("z")))).containsExactly("a=[z, x, y]");
    assertStats(1, 0, 0, 0);
    assertThat(run("MATCH (n:Entity) SET n.aliases = [] RETURN n.aliases AS a, size(n.aliases) AS s")).containsExactly("a=[], s=0");
    assertStats(1, 0, 0, 0);
  }

  @Test
  void relationshipPropertiesAreSetOneByOneAndMergedFromAMap() {
    run("CREATE (a:N {id:1})-[:R {w:1}]->(b:N {id:2})");
    assertThat(run("MATCH (:N)-[r:R]->(:N) SET r.w = r.w + 1, r.t = 'x' RETURN properties(r) AS p")).containsExactly("p={t: x, w: 2}");
    assertStats(2, 0, 0, 0);
    assertThat(run("MATCH (:N)-[r:R]->(:N) SET r += $m RETURN properties(r) AS p", map("m", map("w", null, "z", 3)))).containsExactly("p={t: x, z: 3}");
    assertStats(2, 0, 0, 0);
    assertThat(run("MATCH (:N)-[r:R]->(:N) SET r.t = null RETURN properties(r) AS p")).containsExactly("p={z: 3}");
    assertStats(1, 0, 0, 0);
  }

  @Test
  void aListPropertyIsExtendedAndAUnicodeAndALongStringAreStoredUnchanged() {
    run("CREATE (:L {id:1, tags:['a']})");
    assertThat(run("MATCH (n:L) SET n.tags = n.tags + ['b', 'c'], n.s = $s RETURN n.tags AS t, n.s AS s, size(n.s) AS len", map("s", "héllo 世界"))).containsExactly("len=8, s=héllo 世界, t=[a, b, c]");
    assertStats(2, 0, 0, 0);
    assertThat(run("MATCH (n:L) SET n.long = $long RETURN size(n.long) AS len", map("long", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))).containsExactly("len=215");
    assertStats(1, 0, 0, 0);
  }

  @Test
  void aBooleanAndStringConditionalSetOnlyWritesWhereTheConditionHolds() {
    run("CREATE (:Chunk {external_id:'c1', embedding_model:'m1'}), (:Chunk {external_id:'c2', embedding_model:'m2'}), (:Chunk {external_id:'c3'})");
    assertThat(run("UNWIND $rows AS row MATCH (c:Chunk {external_id: row.eid}) WHERE c.embedding IS NULL OR c.embedding_model IS NULL OR c.embedding_model <> row.model SET c.external_id = c.external_id RETURN c.external_id AS id ORDER BY id", map("rows", list(map("eid", "c1", "model", "m1"), map("eid", "c2", "model", "m1"), map("eid", "c3", "model", "m1"))))).containsExactly("id=c1", "id=c2", "id=c3");
    assertStats(3, 0, 0, 0);
  }

  @Test
  void aSetInsideACaseDrivenForeachOverTheRowsLeavesTheOtherRowsUntouched() {
    run("CREATE (:Entity {normalized_name:'a', v:1}), (:Entity {normalized_name:'b', v:1})");
    assertThat(run("UNWIND $rows AS row MATCH (n:Entity {normalized_name: row.n}) FOREACH (_ IN CASE WHEN row.w THEN [1] ELSE [] END | SET n.v = 2) RETURN n.normalized_name AS nn, n.v AS v ORDER BY nn", map("rows", list(map("n", "a", "w", true), map("n", "b", "w", false))))).containsExactly("nn=a, v=2", "nn=b, v=1");
    assertStats(1, 0, 0, 0);
    assertThat(run("MATCH (n:Entity) RETURN n.normalized_name AS nn, n.v AS v ORDER BY nn")).containsExactly("nn=a, v=2", "nn=b, v=1");
  }

  @Test
  void aSetFollowedByAWithAndAMatchReadsTheWrittenValue() {
    run("CREATE (:A {id:1, v:1}), (:B {id:1})");
    assertThat(run("MATCH (a:A {id:1}) SET a.v = a.v + 10 WITH a MATCH (b:B {id: a.id}) SET b.copy = a.v RETURN a.v AS av, b.copy AS bc")).containsExactly("av=11, bc=11");
    assertStats(2, 0, 0, 0);
    assertThat(run("MATCH (b:B) RETURN b.copy AS c")).containsExactly("c=11");
  }

  @Test
  void aSetOfADurationOrTemporalValueFromAFunctionIsStoredAndReadBack() {
    run("CREATE (:T {id:1})");
    assertThat(run("MATCH (n:T) SET n.d = date('2024-03-05'), n.dt = datetime('2024-03-05T10:20:30Z') RETURN toString(n.d) AS d, n.dt.year AS y")).containsExactly("d=2024-03-05, y=2024");
    assertStats(2, 0, 0, 0);
  }

  @Test
  void aSecondAliasOfTheWrittenNodeSeesEveryFormOfSet() {
    run("CREATE (:T {id:1, x:0})");
    assertThat(run("MATCH (n:T) WITH n, n AS m SET n.x = 5 RETURN m.x AS mx, n.x AS nx")).containsExactly("mx=5, nx=5");
    assertStats(1, 0, 0, 0);
    assertThat(run("MATCH (n:T) WITH n, n AS m SET n += {x: 6} RETURN m.x AS mx, n.x AS nx")).containsExactly("mx=6, nx=6");
    assertStats(1, 0, 0, 0);
    assertThat(run("MATCH (n:T) WITH n, n AS m SET n = {id: 1, x: 7} RETURN m.x AS mx, n.x AS nx")).containsExactly("mx=7, nx=7");
    assertStats(2, 0, 0, 0);
    assertThat(run("MATCH (n:T) WITH n, n AS m SET n.x = null RETURN m.x AS mx, n.x AS nx")).containsExactly("mx=null, nx=null");
    assertStats(1, 0, 0, 0);
  }

  @Test
  void aListHoldingTheWrittenNodesSeesEveryFormOfSet() {
    run("CREATE (:T {id:1}), (:T {id:2})");
    assertThat(run("MATCH (n:T) WITH collect(n) AS ns UNWIND ns AS x SET x.p = x.id RETURN reduce(s = 0, m IN ns | s + m.p) AS total LIMIT 1")).containsExactly("total=3");
    assertStats(2, 0, 0, 0);
    assertThat(run("MATCH (n:T) WITH collect(n) AS ns UNWIND ns AS x SET x += {q: x.id} RETURN reduce(s = 0, m IN ns | s + m.q) AS total LIMIT 1")).containsExactly("total=3");
    assertStats(2, 0, 0, 0);
    assertThat(run("MATCH (n:T) WITH collect(n) AS ns UNWIND ns AS x SET x = {id: x.id, r: 9} RETURN reduce(s = 0, m IN ns | s + m.r) AS total LIMIT 1")).containsExactly("total=18");
    assertStats(8, 0, 0, 0);
  }

  @Test
  void aLabelWrittenBeforeAPropertyInLaterClausesAndAcrossFannedOutRows() {
    run("CREATE (:T {id:1})");
    assertThat(run("UNWIND [1,2,3] AS i MATCH (n:T) SET n:X SET n.c = coalesce(n.c, 0) + i RETURN n.c AS c, labels(n) AS l ORDER BY c")).containsExactly("c=6, l=[T, X]", "c=6, l=[T, X]", "c=6, l=[T, X]");
    assertStats(3, 0, 0, 1);
    assertThat(run("MATCH (n:T) SET n:Y WITH n SET n.p = 1 RETURN n.p AS p, size(labels(n)) AS l")).containsExactly("l=3, p=1");
    assertStats(1, 0, 0, 1);
    assertThat(run("MATCH (n:T) SET n.p = 2, n:Z, n.q = 3 RETURN n.p AS p, n.q AS q, size(labels(n)) AS l")).containsExactly("l=4, p=2, q=3");
    assertStats(2, 0, 0, 1);
    assertThat(run("MATCH (n:Z) RETURN n.p AS p, n.q AS q, n.c AS c")).containsExactly("c=6, p=2, q=3");
  }

  @Test
  void aReplaceMapOnANullNodeIsANoOp() {
    assertThat(run("OPTIONAL MATCH (n:Nope) SET n = {a: 1} RETURN n")).containsExactly("n=null");
    assertStats(0, 0, 0, 0);
  }

  @Test
  void anIncomingPropertyValueIsNotNeededWhenTheRowHasNoRows() {
    assertThat(run("MATCH (n:Nope) SET n.p = 1, n += {q: 2}, n:X RETURN n")).isEmpty();
    assertStats(0, 0, 0, 0);
  }

  @Test
  void aSetOfAnExpressionTargetOnANodeFromAnotherClauseWritesTheNode() {
    run("CREATE (:T {id:1}), (:U {id:1})");
    assertThat(run("MATCH (t:T), (u:U) SET (CASE WHEN t.id = u.id THEN u END).matched = true RETURN t.id AS t, u.matched AS m")).containsExactly("m=true, t=1");
    assertStats(1, 0, 0, 0);
    assertThat(run("MATCH (t:T) WITH [t] AS ts UNWIND ts AS x SET x.listed = 1 RETURN x.listed AS l")).containsExactly("l=1");
    assertStats(1, 0, 0, 0);
  }

  @Test
  void aListCollectedBeforeAnotherMatchWritesTheNodesSeesEveryFormOfSet() {
    run("CREATE (:T {id:1}), (:T {id:2})");
    assertThat(run("MATCH (n:T) WITH collect(n) AS ns MATCH (x:T) SET x.p = x.id RETURN reduce(s = 0, m IN ns | s + m.p) AS total LIMIT 1")).containsExactly("total=3");
    assertStats(2, 0, 0, 0);
    assertThat(run("MATCH (n:T) WITH collect(n) AS ns MATCH (x:T) SET x += {q: x.id * 10} RETURN reduce(s = 0, m IN ns | s + m.q) AS total LIMIT 1")).containsExactly("total=30");
    assertStats(2, 0, 0, 0);
    assertThat(run("MATCH (n:T) WITH collect(n) AS ns MATCH (x:T) SET x = {id: x.id, r: 9} RETURN reduce(s = 0, m IN ns | s + m.r) AS total, reduce(s = 0, m IN ns | s + coalesce(m.p, 0)) AS gone LIMIT 1")).containsExactly("gone=0, total=18");
    assertStats(8, 0, 0, 0);
  }

  @Test
  void aSecondAliasFollowsALabelWriteAndLaterPropertyWritesOnFannedOutRows() {
    run("CREATE (:T {id:1}), (:T {id:2})");
    assertThat(run("MATCH (n:T) WITH n, n AS m SET n:X RETURN size(labels(m)) AS l, n.id AS id ORDER BY id")).containsExactly("id=1, l=2", "id=2, l=2");
    assertStats(0, 0, 0, 2);
    assertThat(run("UNWIND [1,2] AS i MATCH (n:T) WITH i, n, n AS m SET n:Y SET n.c = i RETURN m.c AS c, size(labels(m)) AS l ORDER BY c")).containsExactly("c=2, l=3", "c=2, l=3", "c=2, l=3", "c=2, l=3");
    assertStats(4, 0, 0, 2);
  }

  @Test
  void aListHoldingTheTargetVariableSeesAMapFormOfSetInTheSameClause() {
    run("CREATE (:T {id:1, v:1})");
    assertThat(run("MATCH (n:T) WITH n, [n] AS ns SET n += {v: 10} RETURN head(ns).v AS v, n.v AS nv")).containsExactly("nv=10, v=10");
    assertStats(1, 0, 0, 0);
    assertThat(run("MATCH (n:T) WITH n, [n] AS ns SET n = {id: 1, v: 20} RETURN head(ns).v AS v, n.v AS nv")).containsExactly("nv=20, v=20");
    assertStats(2, 0, 0, 0);
    assertThat(run("MATCH (n:T) WITH n, [n] AS ns SET n.v = 30 RETURN head(ns).v AS v, n.v AS nv")).containsExactly("nv=30, v=30");
    assertStats(1, 0, 0, 0);
    assertThat(run("MATCH (n:T) WITH n, {k: n} AS mp SET n += {v: 40} RETURN mp.k.v AS v")).containsExactly("v=40");
    assertStats(1, 0, 0, 0);
    assertThat(run("MATCH (n:T) WITH n, collect(n) AS ns SET n:Lab, n += {v: 50} RETURN head(ns).v AS v, size(labels(head(ns))) AS l")).containsExactly("l=2, v=50");
    assertStats(1, 0, 0, 1);
  }
}
