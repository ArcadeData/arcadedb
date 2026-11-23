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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * REMOVE, FOREACH and UNWIND in the shapes of a batched graph loader (a tombstone cleared by {@code REMOVE c.deleted_at}, a
 * conditional write behind {@code FOREACH (_ IN CASE WHEN wrote THEN [1] ELSE [] END | ...)}, an upsert page behind
 * {@code UNWIND $rows AS row MERGE ... SET n += row.props}), each with the rows and the statement statistics Neo4j 5.26 gives for
 * the same data. A row is rendered {@code k=v, k=v} with the keys sorted, integers without a fraction.
 */
class CypherRemoveForeachUnwindShapesOracleTest extends TestHelper {
  private QueryStatistics lastStatistics;

  /** Runs one statement in its own transaction and renders its rows. */
  private List<String> run(final String statement) {
    final List<String> rows = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", statement)) {
        while (rs.hasNext())
          rows.add(render(rs.next()));
        lastStatistics = rs.getStatistics().orElse(null);
      }
    });
    return rows;
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

  // ---- REMOVE

  @Test
  void aTombstoneIsClearedAndTheChunkLinkedToItsRecordInOneStatement() {
    run("CREATE (c:Chunk {external_id:'c1', deleted_at:'x', questions_text:'q', context_line:'l', keep:1}), (c2:Chunk {external_id:'c2', keep:2}),"
        + " (r:Record {external_id:'r1'})");

    final List<String> rows = run("UNWIND ['c1','c2','cX'] AS eid MATCH (c:Chunk {external_id: eid}) REMOVE c.deleted_at WITH c"
        + " MATCH (r:Record {external_id:'r1'}) MERGE (c)-[:OF_RECORD]->(r) RETURN r.deleted_at IS NOT NULL AS dead, c.external_id AS id");

    assertThat(rows).containsExactlyInAnyOrder("dead=false, id=c1", "dead=false, id=c2");
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(1);
    assertThat(lastStatistics.getRelationshipsCreated()).isEqualTo(2);
    assertThat(run("MATCH (c:Chunk) RETURN c.external_id AS id, c.deleted_at AS d, c.keep AS k ORDER BY id")).containsExactly(
        "d=null, id=c1, k=1", "d=null, id=c2, k=2");
  }

  @Test
  void severalPropertiesAreRemovedAndAnAbsentOneIsNotCounted() {
    run("CREATE (:Chunk {external_id:'c1', questions_text:'q', context_line:'l', keep:1})");

    assertThat(run("MATCH (c:Chunk {external_id:'c1'}) REMOVE c.questions_text, c.context_line, c.enrichment_error RETURN properties(c) AS p"))
        .containsExactly("p={external_id: c1, keep: 1}");
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(2);

    assertThat(run("MATCH (c:Chunk {external_id:'c1'}) REMOVE c.keep, c.keep RETURN properties(c) AS p")).containsExactly(
        "p={external_id: c1}");
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(1);
  }

  @Test
  void removingFromANullNodeIsANoOp() {
    assertThat(run("OPTIONAL MATCH (n:Nope) REMOVE n.x RETURN n")).containsExactly("n=null");
    assertThat(run("OPTIONAL MATCH (n:Nope) REMOVE n:Foo RETURN n")).containsExactly("n=null");
  }

  @Test
  void labelsAreRemovedOneAtATimeOrSeveralAndAnAbsentOneIsIgnored() {
    run("CREATE (a:A:B:C {id:1}), (b:A {id:2})");

    assertThat(run("MATCH (n {id:1}) REMOVE n:B RETURN labels(n) AS l")).containsExactly("l=[A, C]");
    assertThat(lastStatistics.getLabelsRemoved()).isEqualTo(1);

    assertThat(run("MATCH (n {id:1}) REMOVE n:A:C RETURN labels(n) AS l")).containsExactly("l=[]");
    assertThat(lastStatistics.getLabelsRemoved()).isEqualTo(2);

    assertThat(run("MATCH (n {id:2}) REMOVE n:Z RETURN labels(n) AS l")).containsExactly("l=[A]");
    assertThat(lastStatistics.getLabelsRemoved()).isZero();

    assertThat(run("MATCH (n {id:2}) REMOVE n:A RETURN labels(n) AS l")).containsExactly("l=[]");
    assertThat(run("MATCH (n) RETURN n.id AS id, labels(n) AS l ORDER BY id")).containsExactly("id=1, l=[]", "id=2, l=[]");
  }

  @Test
  void aPropertyIsRemovedByADynamicKeyAndAMissingKeyIsIgnored() {
    run("CREATE (:L {id:1, p:1, q:2})");

    assertThat(run("MATCH (a:L {id:1}) REMOVE a['p'] RETURN properties(a) AS p")).containsExactly("p={id: 1, q: 2}");
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(1);

    assertThat(run("UNWIND ['q', 'nope'] AS k MATCH (a:L {id:1}) REMOVE a[k] RETURN properties(a) AS p")).containsExactly("p={id: 1}",
        "p={id: 1}");
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(1);
  }

  @Test
  void relationshipPropertiesAreRemovedBehindAPropertyFilter() {
    run("CREATE (a:L {id:1}), (b:L {id:2}), (a)-[:R {x:1, y:2, is_asserted:true}]->(b)");

    assertThat(run("MATCH (a)-[e:R]->(b) WHERE e.is_asserted = true REMOVE e.is_asserted, e.y RETURN properties(e) AS p")).containsExactly(
        "p={x: 1}");
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(2);
  }

  @Test
  void aRemovedPropertyReadsNullInTheSameStatementAndIsCountedOncePerRecord() {
    run("CREATE (:L {id:1}), (:L {id:2})");

    assertThat(run("MATCH (a:L {id:1}) REMOVE a.id RETURN a.id AS id")).containsExactly("id=null");
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(1);
    assertThat(run("MATCH (a:L) REMOVE a.id RETURN count(a) AS c")).containsExactly("c=2");
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(1);
  }

  @Test
  void aSetToNullAndARemoveInOneStatementBothDropTheProperty() {
    run("CREATE (:L {id:1, k:'a'})");

    assertThat(run("MATCH (a:L {id:1}) SET a.k = 'z', a.id = null RETURN properties(a) AS p")).containsExactly("p={k: z}");
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(2);
  }

  // ---- FOREACH

  @Test
  void aForeachRemovesAPropertyFromEveryCollectedNode() {
    run("CREATE (:L {id:1, p:1}), (:L {id:2, p:2})");

    run("MATCH (a:L) WITH collect(a) AS ns FOREACH (n IN ns | REMOVE n.p)");

    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(2);
    assertThat(run("MATCH (a:L) RETURN a.id AS id, a.p AS p ORDER BY id")).containsExactly("id=1, p=null", "id=2, p=null");
  }

  @Test
  void aForeachOverARangeNullEmptyOrScalarList() {
    run("FOREACH (i IN range(1, 3) | CREATE (:L {id: i}))");
    assertThat(run("MATCH (n:L) RETURN count(n) AS c")).containsExactly("c=3");

    run("FOREACH (x IN null | CREATE (:Z))");
    run("FOREACH (x IN [] | CREATE (:Z))");
    assertThat(run("MATCH (n:Z) RETURN count(n) AS c")).containsExactly("c=0");

    run("FOREACH (x IN [1, null, 2] | CREATE (:Z {v: x}))");
    assertThat(lastStatistics.getNodesCreated()).isEqualTo(3);
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(2);
    assertThat(run("MATCH (n:Z) RETURN n.v AS v ORDER BY v")).containsExactly("v=1", "v=2", "v=null");

    run("FOREACH (x IN 5 | CREATE (:Y))");
    assertThat(lastStatistics.getNodesCreated()).isEqualTo(1);
  }

  @Test
  void nestedForeachBodiesSeeBothVariables() {
    run("FOREACH (i IN [1, 2] | FOREACH (j IN [10, 20] | CREATE (:N {i: i, j: j})))");

    assertThat(lastStatistics.getNodesCreated()).isEqualTo(4);
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(8);
    assertThat(run("MATCH (n:N) RETURN n.i AS i, n.j AS j ORDER BY i, j")).containsExactly("i=1, j=10", "i=1, j=20", "i=2, j=10",
        "i=2, j=20");
  }

  @Test
  void aForeachKeepsTheRowsAndAccumulatesItsWrites() {
    run("CREATE (:L {id:1}), (:L {id:2}), (:L {id:3})");

    assertThat(run("MATCH (n:L) FOREACH (x IN [1,2] | SET n.s = coalesce(n.s, 0) + x) RETURN n.id AS id, n.s AS s ORDER BY id"))
        .containsExactly("id=1, s=3", "id=2, s=3", "id=3, s=3");
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(6);
  }

  @Test
  void aConditionalForeachWritesOnlyTheRowsThatAskForIt() {
    run("CREATE (:L {id:1}), (:L {id:2}), (:L {id:3})");

    assertThat(run("UNWIND [{id:1, w:true}, {id:2, w:false}, {id:3, w:null}, {id:9, w:true}] AS row MATCH (n:L {id: row.id})"
        + " FOREACH (_ IN CASE WHEN row.w THEN [1] ELSE [] END | SET n.touched = true) RETURN n.id AS id, n.touched AS t"))
        .containsExactly("id=1, t=true", "id=2, t=null", "id=3, t=null");
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(1);
  }

  @Test
  void theEntityUpsertShapeWritesOnlyWhenTheGateSaysSo() {
    run("CREATE (:L {id:1, touched:true}), (:L {id:2}), (:L {id:3})");
    run("MATCH (n:L {id:1}) SET n.s = 3");

    assertThat(run("UNWIND [{id:1, w:true}, {id:7, w:true}] AS row MERGE (n:L {id: row.id}) WITH n, row, (n.s IS NULL OR n.s <> 3) AS x"
        + " WITH n, row, x, (x OR row.id = 7) AS wrote FOREACH (_ IN CASE WHEN wrote THEN [1] ELSE [] END |"
        + " SET n += {m: row.id}, n.created = coalesce(n.created, 'now') REMOVE n.touched) RETURN n.id AS id, wrote"))
        .containsExactly("id=1, wrote=false", "id=7, wrote=true");
    assertThat(lastStatistics.getNodesCreated()).isEqualTo(1);
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(3);
    assertThat(run("MATCH (n:L) RETURN n.id AS id, n.m AS m, n.created AS c, n.touched AS t ORDER BY id")).containsExactly(
        "c=null, id=1, m=null, t=true", "c=null, id=2, m=null, t=null", "c=null, id=3, m=null, t=null", "c=now, id=7, m=7, t=null");
  }

  @Test
  void aForeachVariableShadowsAnOuterOneAndANestedOneShadowsItsParent() {
    run("CREATE (:L {id:1})");

    run("MATCH (n:L {id:1}) FOREACH (n2 IN [1] | SET n.shadow = n2)");
    assertThat(run("MATCH (n:L {id:1}) RETURN n.shadow AS s")).containsExactly("s=1");

    run("MATCH (n:L {id:1}) FOREACH (x IN [1] | FOREACH (x IN [2] | SET n.sh2 = x))");
    assertThat(run("MATCH (n:L {id:1}) RETURN n.sh2 AS s")).containsExactly("s=2");
  }

  @Test
  void aForeachListMayBeReadFromTheOuterRowAndWritesMergeAndSetMaps() {
    run("CREATE (:L {id:1}), (:L {id:2})");

    run("UNWIND [[1,2],[3]] AS l FOREACH (x IN l | CREATE (:F {x: x}))");
    assertThat(run("MATCH (n:F) RETURN n.x AS x ORDER BY x")).containsExactly("x=1", "x=2", "x=3");

    run("FOREACH (k IN ['a', 'b', 'a'] | MERGE (:M {k: k}))");
    assertThat(run("MATCH (n:M) RETURN n.k AS k ORDER BY k")).containsExactly("k=a", "k=b");

    run("MATCH (n:L {id:1}) FOREACH (m IN [{p: 1}, {p: 2, q: 3}] | SET n += m)");
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(3);
    assertThat(run("MATCH (n:L {id:1}) RETURN n.p AS p, n.q AS q")).containsExactly("p=2, q=3");

    run("MATCH (a:L {id:1}), (b:L {id:2}) FOREACH (i IN range(1, 3) | CREATE (a)-[:R {i: i}]->(b))");
    assertThat(lastStatistics.getRelationshipsCreated()).isEqualTo(3);
    assertThat(run("MATCH ()-[r:R]->() RETURN r.i AS i ORDER BY i")).containsExactly("i=1", "i=2", "i=3");

    run("MATCH (n:L) WITH collect(n) AS ns FOREACH (x IN ns | SET x.flag = size(ns))");
    assertThat(run("MATCH (n:L) RETURN n.id AS id, n.flag AS f ORDER BY id")).containsExactly("f=2, id=1", "f=2, id=2");
  }

  @Test
  void aForeachInsideAMultiRowStatementRunsOncePerRowAndNeverWithoutRows() {
    run("UNWIND [1, 2] AS i FOREACH (x IN [1, 2, 3] | CREATE (:Cnt {i: i, x: x}))");
    assertThat(lastStatistics.getNodesCreated()).isEqualTo(6);
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(12);

    run("MATCH (n:Nope) FOREACH (x IN [1] | CREATE (:Never))");
    assertThat(run("MATCH (n:Never) RETURN count(n) AS c")).containsExactly("c=0");

    run("CREATE (:L {id:1})");
    assertThat(run("MATCH (n:L {id:1}) FOREACH (x IN [1] | SET n.fx = x) RETURN n.fx AS fx")).containsExactly("fx=1");
  }

  @Test
  void aMergeInsideAForeachCreatesThenMatches() {
    final String statement = "FOREACH (k IN ['p', 'q'] | MERGE (m:MM {k: k}) ON CREATE SET m.c = 1 ON MATCH SET m.c = 2)";

    run(statement);
    assertThat(lastStatistics.getNodesCreated()).isEqualTo(2);
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(4);

    run(statement);
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(2);
    assertThat(run("MATCH (m:MM) RETURN m.k AS k, m.c AS c ORDER BY k")).containsExactly("c=2, k=p", "c=2, k=q");
  }

  // ---- UNWIND

  @Test
  void unwindOfNullEmptyScalarAndMixedLists() {
    assertThat(run("UNWIND null AS x RETURN x")).isEmpty();
    assertThat(run("UNWIND [] AS x RETURN x")).isEmpty();
    assertThat(run("UNWIND [1, null, 'a', [2], {k: 1}] AS x RETURN x")).containsExactly("x=1", "x=null", "x=a", "x=[2]", "x={k: 1}");
    assertThat(run("UNWIND 5 AS x RETURN x")).containsExactly("x=5");
    assertThat(run("UNWIND 'abc' AS x RETURN x")).containsExactly("x=abc");
    assertThat(run("UNWIND {a: 1} AS x RETURN x")).containsExactly("x={a: 1}");
  }

  @Test
  void unwindOfRangesOrdersAndLimits() {
    assertThat(run("UNWIND range(1, 5) AS x RETURN x ORDER BY x DESC LIMIT 2")).containsExactly("x=5", "x=4");
    assertThat(run("UNWIND range(5, 1, -2) AS x RETURN x")).containsExactly("x=5", "x=3", "x=1");
    assertThat(run("UNWIND range(1, 0) AS x RETURN count(*) AS c")).containsExactly("c=0");
    assertThat(run("UNWIND [1, 2, 2, 3] AS x RETURN DISTINCT x ORDER BY x")).containsExactly("x=1", "x=2", "x=3");
    assertThat(run("UNWIND [1, 2, 3] AS x RETURN x SKIP 1")).containsExactly("x=2", "x=3");
    assertThat(run("UNWIND [1, 2, 3] AS x RETURN x SKIP 1 LIMIT 1")).containsExactly("x=2");
  }

  @Test
  void nestedAndDependentUnwinds() {
    assertThat(run("UNWIND [[1,2],[3]] AS l UNWIND l AS x RETURN l, x")).containsExactly("l=[1, 2], x=1", "l=[1, 2], x=2", "l=[3], x=3");
    assertThat(run("UNWIND [1, 2, 3] AS x UNWIND [x, x * 10] AS y RETURN x, y")).containsExactly("x=1, y=1", "x=1, y=10", "x=2, y=2",
        "x=2, y=20", "x=3, y=3", "x=3, y=30");
    assertThat(run("UNWIND [1, 2, 3] AS x WITH sum(x) AS s UNWIND range(1, s) AS y RETURN count(*) AS c")).containsExactly("c=6");
    assertThat(run("UNWIND range(1, 3) AS x WITH x, collect(x) AS c UNWIND c AS y RETURN x, y")).containsExactly("x=1, y=1", "x=2, y=2",
        "x=3, y=3");
    assertThat(run("UNWIND [[]] AS l UNWIND l AS x RETURN count(*) AS c")).containsExactly("c=0");
    assertThat(run("UNWIND [{a: 1, p: {x: 1, y: null}}] AS row UNWIND keys(row.p) AS k RETURN k")).containsExactlyInAnyOrder("k=x", "k=y");
  }

  @Test
  void unwindOfAMapFieldThatIsEmptyNullOrMissingYieldsNoRow() {
    assertThat(run("UNWIND [{a: 1, items: [10, 20]}, {a: 2, items: []}, {a: 3, items: null}] AS row UNWIND row.items AS it"
        + " RETURN row.a AS a, it")).containsExactly("a=1, it=10", "a=1, it=20");
    assertThat(run("UNWIND [{a: 1, items: [10, 20]}, {a: 2, items: []}, {a: 3}] AS row OPTIONAL MATCH (n:Nope) UNWIND row.items AS it"
        + " RETURN row.a AS a, it")).containsExactly("a=1, it=10", "a=1, it=20");
    assertThat(run("UNWIND [{a: 1, b: null}, {a: 2}] AS row RETURN row.a AS a, row.b AS b, row.c AS c")).containsExactly("a=1, b=null, c=null",
        "a=2, b=null, c=null");
    assertThat(run("UNWIND [{a: {b: 1}}, {a: null}, {}] AS row RETURN row.a.b AS b")).containsExactly("b=1", "b=null", "b=null");
    assertThat(run("UNWIND [{a: {b: 1}}, {a: null}, {}] AS row RETURN row['a']['b'] AS b")).containsExactly("b=1", "b=null", "b=null");
    assertThat(run("UNWIND [{a: [1,2,3]}, {a: null}] AS row RETURN row.a[1] AS x, size(row.a) AS s, row.a[0..2] AS sl")).containsExactly(
        "s=3, sl=[1, 2], x=2", "s=null, sl=null, x=null");
  }

  @Test
  void unwindExpressionsAreEvaluatedPerRow() {
    assertThat(run("UNWIND [1.5, 2, 3] AS x RETURN x + 1 AS y")).containsExactly("y=2.5", "y=3", "y=4");
    assertThat(run("UNWIND ['a', 'b'] AS x RETURN x + 'z' AS y")).containsExactly("y=az", "y=bz");
    assertThat(run("UNWIND [true, false, null] AS x RETURN x, x IS NULL AS isn, NOT x AS nx")).containsExactly("isn=false, nx=false, x=true",
        "isn=false, nx=true, x=false", "isn=true, nx=null, x=null");
    assertThat(run("UNWIND [1, 2, 3] AS x RETURN count(x) AS c, sum(x) AS s, avg(x) AS a")).containsExactly("a=2.0, c=3, s=6");
    assertThat(run("UNWIND coalesce(null, [1, 2]) AS x RETURN x")).containsExactly("x=1", "x=2");
    assertThat(run("UNWIND CASE WHEN true THEN [1, 2] ELSE [] END AS x RETURN x")).containsExactly("x=1", "x=2");
    assertThat(run("UNWIND CASE WHEN false THEN [1, 2] END AS x RETURN count(*) AS c")).containsExactly("c=0");
  }

  @Test
  void anUpsertPageMergesBySetsAndNullsAndMapsPerRow() {
    assertThat(run("UNWIND [{k:'a', v:1}, {k:'b', v:2}, {k:'a', v:3}] AS row MERGE (n:E {k: row.k}) SET n.v = row.v RETURN n.k AS k, n.v AS v"))
        .containsExactly("k=a, v=3", "k=b, v=2", "k=a, v=3");
    assertThat(lastStatistics.getNodesCreated()).isEqualTo(2);
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(5);
    assertThat(run("MATCH (n:E) RETURN n.k AS k, n.v AS v ORDER BY k")).containsExactly("k=a, v=3", "k=b, v=2");

    assertThat(run("UNWIND [{k:'a', v:1}, {k:'c', v:null}] AS row MERGE (n:E {k: row.k}) SET n.v = row.v RETURN n.k AS k, n.v AS v"))
        .containsExactly("k=a, v=1", "k=c, v=null");
    assertThat(lastStatistics.getNodesCreated()).isEqualTo(1);
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(2);

    assertThat(run("UNWIND [{k:'a', props:{x:1, y:null}}, {k:'d', props:{}}] AS row MERGE (n:E {k: row.k}) SET n += row.props"
        + " RETURN n.k AS k, n.x AS x")).containsExactly("k=a, x=1", "k=d, x=null");
    assertThat(run("MATCH (n:E) RETURN n.k AS k, n.v AS v, n.x AS x ORDER BY k")).containsExactly("k=a, v=1, x=1", "k=b, v=2, x=null",
        "k=c, v=null, x=null", "k=d, v=null, x=null");

    assertThat(run("UNWIND [{k:'a', t:['x','y']}, {k:'e', t:[]}] AS row MERGE (n:E {k: row.k}) SET n.t = row.t RETURN n.t AS t"))
        .containsExactly("t=[x, y]", "t=[]");
  }

  @Test
  void unwindMergeOfANullKeyIsRefused() {
    run("CREATE (:E {k:'a'})");

    assertThatThrownBy(() -> run("UNWIND [{k:'a'}, {k:null}] AS row MERGE (n:E {k: row.k}) RETURN n.k AS k"))
        .hasStackTraceContaining("property value that is null");
  }

  @Test
  void unwindCreateAndMatchWriteOncePerRowAndNothingForNullOrEmpty() {
    assertThat(run("UNWIND [1, 2, 3] AS i CREATE (:W {i: i}) RETURN count(*) AS c")).containsExactly("c=3");
    assertThat(run("UNWIND [1, 2, 3] AS i CREATE (a:W2 {i: i})-[:R]->(b:W2 {i: i * 10}) RETURN a.i AS a, b.i AS b")).containsExactly("a=1, b=10",
        "a=2, b=20", "a=3, b=30");
    assertThat(lastStatistics.getNodesCreated()).isEqualTo(6);
    assertThat(lastStatistics.getRelationshipsCreated()).isEqualTo(3);

    assertThat(run("UNWIND [] AS i CREATE (:Never2) RETURN count(*) AS c")).containsExactly("c=0");
    assertThat(run("UNWIND null AS i CREATE (:Never2) RETURN count(*) AS c")).containsExactly("c=0");
    assertThat(run("MATCH (n:Never2) RETURN count(n) AS c")).containsExactly("c=0");

    assertThat(run("UNWIND [1, 2, 3] AS i MATCH (n:W {i: i}) SET n.hit = true RETURN i, n.hit AS h")).containsExactly("h=true, i=1", "h=true, i=2",
        "h=true, i=3");
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(3);
    assertThat(run("UNWIND [1, 2, 9] AS i OPTIONAL MATCH (n:W {i: i}) SET n.hit2 = true RETURN i, n.hit2 AS h")).containsExactly("h=true, i=1",
        "h=true, i=2", "h=null, i=9");
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(2);
    assertThat(run("UNWIND ['x', 'y'] AS a UNWIND [1, 2] AS b MERGE (n:X {a: a, b: b}) RETURN count(*) AS c")).containsExactly("c=4");
    assertThat(run("UNWIND range(1, 5) AS i MERGE (n:Cnt {i: i % 2}) RETURN count(*) AS c")).containsExactly("c=5");
    assertThat(lastStatistics.getNodesCreated()).isEqualTo(2);
  }

  @Test
  void aMentionPageMergesRelationshipsOnceAndCountsEachRow() {
    run("CREATE (:Chunk {external_id:'c1'}), (:Chunk {external_id:'c2'}), (:Entity {external_id:'e1'}), (:Entity {external_id:'e2'})");

    assertThat(run("UNWIND [{cid:'c1', eid:'e1'}, {cid:'c1', eid:'e2'}, {cid:'c2', eid:'e1'}, {cid:'c1', eid:'e1'}, {cid:'cx', eid:'e1'}] AS row"
        + " MATCH (c:Chunk {external_id: row.cid}) MATCH (e:Entity {external_id: row.eid}) MERGE (c)-[:MENTIONS]->(e) RETURN row.cid AS c,"
        + " row.eid AS e")).containsExactly("c=c1, e=e1", "c=c1, e=e2", "c=c2, e=e1", "c=c1, e=e1");
    assertThat(lastStatistics.getRelationshipsCreated()).isEqualTo(3);
    assertThat(run("MATCH ()-[m:MENTIONS]->() RETURN count(m) AS c")).containsExactly("c=3");

    assertThat(run("UNWIND [{cid:'c1', eid:'e1'}] AS row MATCH (c:Chunk {external_id: row.cid}) MATCH (e:Entity {external_id: row.eid})"
        + " MERGE (c)-[m:MENTIONS]->(e) ON CREATE SET m.n = 1 ON MATCH SET m.n = coalesce(m.n, 0) + 1 RETURN m.n AS n")).containsExactly("n=1");
  }

  @Test
  void anEnlistingNoOpSetAnswersTheRowsItMatched() {
    run("CREATE (:Chunk {external_id:'c1'}), (:Chunk {external_id:'c2'})");

    assertThat(run("UNWIND [{eid:'c1'}, {eid:'c2'}, {eid:'nope'}] AS row MATCH (c:Chunk {external_id: row.eid}) WHERE c.deleted_at IS NULL"
        + " SET c.external_id = c.external_id RETURN c.external_id AS id")).containsExactly("id=c1", "id=c2");
    assertThat(lastStatistics.getPropertiesSet()).isEqualTo(2);
  }
  // ---- a node held in a list or a map follows every write of the statement (Neo4j answers the entity, not a copy)

  @Test
  void aNodeHeldInAListOrMapSeesTheWritesOfAForeach() {
    run("CREATE (:L {id:1, p:1}), (:L {id:2, p:2})");

    assertThat(run("MATCH (n:L {id:1}) WITH collect(n) AS ns, n FOREACH (_ IN [1] | SET n.p = 5) RETURN ns[0].p AS p, n.p AS q"))
        .containsExactly("p=5, q=5");
    assertThat(run("MATCH (n:L {id:1}) WITH {k: n} AS m, n FOREACH (_ IN [1] | SET n.p = 6) RETURN m.k.p AS p, n.p AS q"))
        .containsExactly("p=6, q=6");
    assertThat(run("MATCH (n:L) WITH n, collect(n) AS ns FOREACH (x IN ns | SET x.p = 7) RETURN n.id AS id, n.p AS p ORDER BY id"))
        .containsExactly("id=1, p=7", "id=2, p=7");
    assertThat(run("MATCH (n:L {id:1}) WITH collect(n) AS ns, n FOREACH (_ IN [1] | REMOVE n.p) RETURN ns[0].p AS p, n.p AS q"))
        .containsExactly("p=null, q=null");
    assertThat(run("MATCH (n:L {id:1}) WITH collect(n) AS ns, n FOREACH (_ IN [1] | SET n:X) RETURN labels(ns[0]) AS l, labels(n) AS m"))
        .containsExactly("l=[L, X], m=[L, X]");
  }

  @Test
  void aNodeHeldInAListSeesTheWritesOfSetRemoveAndMerge() {
    run("CREATE (:L {id:1, p:1}), (:L {id:2, p:2})");

    assertThat(run("MATCH (n:L {id:1}) WITH collect(n) AS ns, n SET n.p = 9 RETURN ns[0].p AS p, n.p AS q")).containsExactly("p=9, q=9");
    assertThat(run("MATCH (n:L {id:1}) WITH collect(n) AS ns, n REMOVE n.p RETURN ns[0].p AS p, n.p AS q")).containsExactly("p=null, q=null");
    assertThat(run("MATCH (n:L {id:1}) WITH collect(n) AS ns, n MERGE (m:L {id:1}) SET m.p = 8 RETURN ns[0].p AS p, n.p AS q, m.p AS r"))
        .containsExactly("p=8, q=8, r=8");
    assertThat(run("MATCH (n:L {id:1}) WITH collect(n) AS ns, n DELETE n RETURN size(ns) AS s")).containsExactly("s=1");
  }

  @Test
  void aCollectedListFollowsTheWritesDoneThroughItsElements() {
    run("CREATE (:L {id:1, p:1}), (:L {id:2, p:2})");

    assertThat(run("MATCH (n:L) WITH n ORDER BY n.id WITH collect(n) AS ns UNWIND ns AS m SET m.p = m.id * 10"
        + " RETURN ns[0].p AS p0, ns[1].p AS p1, m.p AS mp")).containsExactly("mp=10, p0=10, p1=20", "mp=20, p0=10, p1=20");
    assertThat(run("MATCH (n:L) WITH n ORDER BY n.id WITH collect(n) AS ns, collect(n.p) AS ps FOREACH (x IN ns | SET x.p = 0)"
        + " RETURN ps, [x IN ns | x.p] AS now")).containsExactly("now=[0, 0], ps=[10, 20]");
    assertThat(run("MATCH (n:L) WITH n ORDER BY n.id WITH collect(n) AS ns FOREACH (x IN ns | FOREACH (y IN ns | SET y.p = y.p + 1))"
        + " RETURN [x IN ns | x.p] AS now")).containsExactly("now=[2, 2]");
    assertThat(run("UNWIND [1, 2] AS i MATCH (n:L {id: i}) WITH collect(n) AS ns, i FOREACH (x IN ns | SET x.p = x.p + 100)"
        + " RETURN i, [x IN ns | x.p] AS now")).containsExactly("i=1, now=[102]", "i=2, now=[102]");
  }
}
