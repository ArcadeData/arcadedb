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
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * DELETE statement shapes of a batched graph loader (a Chunk or Fact retired by the record it belongs to, an owned edge dropped
 * through a collected {@code hits} list, an optional relationship unlinked, a page of owner rows dropped by key), each with the
 * answer Neo4j 5.26 gives for the same data: the rows returned, the statistics of the statement, and what is left in the graph.
 * The one-statement-per-case layout pins the deletion of a vertex reached twice, of a relationship through a map-held reference,
 * of nothing at all (an empty collect, a null from OPTIONAL MATCH) and the refusal of a connected node.
 */
class CypherDeleteShapesOracleTest extends TestHelper {
  private QueryStatistics lastStatistics;

  private List<Map<String, Object>> run(final String statement) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", statement)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          rows.add(r.toMap());
        }
        lastStatistics = rs.getStatistics().orElse(null);
      }
    });
    return rows;
  }

  private long count(final String statement) {
    try (final ResultSet rs = database.query("opencypher", statement)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  @Test
  void aChunkReachedByTwoRecordsIsDeletedOnceAndCountedPerRow() {
    run("CREATE (r1:Rec {id:'r1'}), (r2:Rec {id:'r2'}), (c1:Chunk {id:'c1'}), (c2:Chunk {id:'c2'}), (c3:Chunk {id:'c3'}), (c4:Chunk {id:'c4'}),"
        + " (c1)-[:OF]->(r1), (c2)-[:OF]->(r1), (c3)-[:OF]->(r2), (c1)-[:OF]->(r2)");

    final List<Map<String, Object>> rows = run(
        "MATCH (x:Chunk)-[:OF]->(r:Rec) WHERE r.id IN ['r1','r2'] DETACH DELETE x RETURN count(x) AS deleted");

    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).get("deleted")).longValue()).isEqualTo(4L);
    assertThat(lastStatistics.getNodesDeleted()).isEqualTo(3);
    assertThat(lastStatistics.getRelationshipsDeleted()).isEqualTo(4);
    assertThat(count("MATCH (c:Chunk) RETURN count(c) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (r:Rec) RETURN count(r) AS c")).isEqualTo(2L);
    assertThat(count("MATCH ()-[e:OF]->() RETURN count(e) AS c")).isZero();
  }

  @Test
  void parallelRelationshipsToOneRecordDeleteTheChunkOnce() {
    run("CREATE (r1:Rec {id:'r1'}), (c1:Chunk {id:'c1'}), (c2:Chunk {id:'c2'}), (c1)-[:OF]->(r1), (c1)-[:OF]->(r1), (c2)-[:OF]->(r1)");

    final List<Map<String, Object>> rows = run(
        "MATCH (x:Chunk)-[:OF]->(r:Rec {id:'r1'}) DETACH DELETE x RETURN count(*) AS rows, count(x) AS cx");

    assertThat(((Number) rows.get(0).get("rows")).longValue()).isEqualTo(3L);
    assertThat(((Number) rows.get(0).get("cx")).longValue()).isEqualTo(3L);
    assertThat(lastStatistics.getNodesDeleted()).isEqualTo(2);
    assertThat(lastStatistics.getRelationshipsDeleted()).isEqualTo(3);
    assertThat(count("MATCH (c:Chunk) RETURN count(c) AS c")).isZero();
    assertThat(count("MATCH (r:Rec) RETURN count(r) AS c")).isEqualTo(1L);
  }

  @Test
  void ownedEdgesAreDeletedThroughACollectedHitsList() {
    run("CREATE (a:Entity {external_id:'a'}), (b:Entity {external_id:'b'}), (c:Entity {external_id:'c'}),"
        + " (a)-[:RELATES_TO {predicate:'p', source_record:''}]->(b), (a)-[:RELATES_TO {predicate:'p', source_record:'r1'}]->(b),"
        + " (a)-[:RELATES_TO {predicate:'q'}]->(b), (a)-[:RELATES_TO {predicate:'p'}]->(c)");

    final List<Map<String, Object>> rows = run(
        "UNWIND [{from_id:'a', to_id:'b', predicate:'p'}, {from_id:'a', to_id:'c', predicate:'p'}, {from_id:'a', to_id:'zz', predicate:'p'}] AS row"
            + " MATCH (a:Entity {external_id: row.from_id})-[e:RELATES_TO]->(b:Entity {external_id: row.to_id})"
            + " WHERE e.predicate = row.predicate AND (e.source_record IS NULL OR e.source_record = '')"
            + " WITH collect({a: a.external_id, b: b.external_id, e: e}) AS hits UNWIND hits AS h DELETE h.e RETURN h.a AS a, h.b AS b");

    assertThat(rows).extracting(r -> r.get("a") + ">" + r.get("b")).containsExactlyInAnyOrder("a>b", "a>c");
    assertThat(lastStatistics.getRelationshipsDeleted()).isEqualTo(2);
    final List<Map<String, Object>> left = run("MATCH ()-[e:RELATES_TO]->() RETURN e.predicate AS p, e.source_record AS s ORDER BY p, s");
    assertThat(left).hasSize(2);
    assertThat(left.get(0).get("p")).isEqualTo("p");
    assertThat(left.get(0).get("s")).isEqualTo("r1");
    assertThat(left.get(1).get("p")).isEqualTo("q");
    assertThat(left.get(1).get("s")).isNull();
  }

  @Test
  void anEmptyHitsListDeletesNothingAndAnswersNoRow() {
    run("CREATE (a:Entity {external_id:'a'}), (b:Entity {external_id:'b'}), (a)-[:RELATES_TO {predicate:'p'}]->(b)");

    final List<Map<String, Object>> rows = run(
        "UNWIND [{from_id:'nope', to_id:'b', predicate:'p'}] AS row MATCH (a:Entity {external_id: row.from_id})-[e:RELATES_TO]->(b:Entity"
            + " {external_id: row.to_id}) WITH collect({a: a.external_id, b: b.external_id, e: e}) AS hits UNWIND hits AS h DELETE h.e"
            + " RETURN h.a AS a, h.b AS b");

    assertThat(rows).isEmpty();
    assertThat(count("MATCH ()-[e:RELATES_TO]->() RETURN count(e) AS c")).isEqualTo(1L);
  }

  @Test
  void anOptionalRelationshipAlternationIsUnlinkedAndCounted() {
    run("CREATE (f:MemoryFact {external_id:'f1', graph_name:'g'}), (s:Subj {id:1}), (o:Obj {id:2}), (e:Ep {id:3}),"
        + " (f)-[:MEM_SUBJECT]->(s), (f)-[:MEM_OBJECT]->(o), (f)-[:MEM_SOURCE_EPISODE]->(e), (f)-[:OTHER]->(s), (s)-[:MEM_SUBJECT]->(f)");
    final String unlink = "MATCH (f:MemoryFact {external_id: 'f1'}) OPTIONAL MATCH (f)-[rel:MEM_SUBJECT|MEM_OBJECT|MEM_SOURCE_EPISODE]->()"
        + " DELETE rel RETURN f.graph_name AS graph_name, count(rel) AS dropped";

    List<Map<String, Object>> rows = run(unlink);

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("graph_name")).isEqualTo("g");
    assertThat(((Number) rows.get(0).get("dropped")).longValue()).isEqualTo(3L);
    assertThat(lastStatistics.getRelationshipsDeleted()).isEqualTo(3);
    final List<Map<String, Object>> left = run("MATCH ()-[r]->() RETURN type(r) AS t ORDER BY t");
    assertThat(left).extracting(r -> r.get("t")).containsExactly("MEM_SUBJECT", "OTHER");

    // nothing left to unlink: the fact is still answered, with nothing dropped (the OPTIONAL MATCH row is a null relationship)
    rows = run("MATCH (f:MemoryFact {external_id: 'f1'}) OPTIONAL MATCH (f)-[rel:MEM_SUBJECT|MEM_OBJECT]->() DELETE rel"
        + " RETURN f.graph_name AS graph_name, count(rel) AS dropped");
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).get("dropped")).longValue()).isZero();
    assertThat(lastStatistics.getRelationshipsDeleted()).isZero();

    // no such fact: no row at all
    assertThat(run("MATCH (f:MemoryFact {external_id: 'nope'}) OPTIONAL MATCH (f)-[rel:MEM_SUBJECT]->() DELETE rel RETURN f.graph_name AS g,"
        + " count(rel) AS dropped")).isEmpty();
  }

  @Test
  void aPageOfOwnerRowsIsDroppedByKeyWithDuplicatesAndMisses() {
    run("UNWIND range(1, 12) AS i CREATE (:NodeOwnership {node_id: 'n' + toString(i)})");

    final List<Map<String, Object>> rows = run(
        "MATCH (o:NodeOwnership) WHERE o.node_id IN ['n1','n2','n3','missing','n3'] DELETE o RETURN count(o) AS dropped");

    assertThat(((Number) rows.get(0).get("dropped")).longValue()).isEqualTo(3L);
    assertThat(lastStatistics.getNodesDeleted()).isEqualTo(3);
    assertThat(count("MATCH (o:NodeOwnership) RETURN count(o) AS c")).isEqualTo(9L);
  }

  @Test
  void aNodeNamedTwiceThroughAnAliasIsDeletedOnce() {
    run("CREATE (a:L {id:1}), (b:L {id:2}), (a)-[:R]->(b)");

    final List<Map<String, Object>> rows = run("MATCH (a:L {id:1}) WITH a, a AS b DETACH DELETE a, b RETURN count(*) AS c");

    assertThat(((Number) rows.get(0).get("c")).longValue()).isEqualTo(1L);
    assertThat(lastStatistics.getNodesDeleted()).isEqualTo(1);
    assertThat(lastStatistics.getRelationshipsDeleted()).isEqualTo(1);
    assertThat(count("MATCH (n:L) RETURN count(n) AS c")).isEqualTo(1L);
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isZero();
  }

  @Test
  void aRelationshipAndItsEndpointsAreDeletedByOneClause() {
    run("CREATE (a:L {id:1}), (b:L {id:2}), (a)-[:R]->(b)");

    run("MATCH (a:L)-[r:R]->(b:L) DELETE r, a, b RETURN count(*) AS c");

    assertThat(lastStatistics.getNodesDeleted()).isEqualTo(2);
    assertThat(lastStatistics.getRelationshipsDeleted()).isEqualTo(1);
    assertThat(count("MATCH (n) RETURN count(n) AS c")).isZero();
  }

  @Test
  void aConnectedNodeIsRefusedAndNothingIsDeleted() {
    run("CREATE (a:L {id:1}), (b:L {id:2}), (a)-[:R]->(b)");

    final Throwable thrown = catchThrowable(() -> run("MATCH (a:L {id:1}) DELETE a"));

    assertThat(thrown).isNotNull();
    assertThat(count("MATCH (n:L) RETURN count(n) AS c")).isEqualTo(2L);
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(1L);
  }

  @Test
  void aPathIsDeletedWithItsNodesAndRelationships() {
    run("CREATE (a:L {id:1}), (b:L {id:2}), (a)-[:R]->(b)");

    run("MATCH p = (a:L {id:1})-[:R]->(b:L {id:2}) DETACH DELETE p");

    assertThat(lastStatistics.getNodesDeleted()).isEqualTo(2);
    assertThat(lastStatistics.getRelationshipsDeleted()).isEqualTo(1);
    assertThat(count("MATCH (n) RETURN count(n) AS c")).isZero();
  }

  @Test
  void collectedAndMapHeldNodesAreDeleted() {
    run("CREATE (:L {id:1}), (:L {id:2}), (:L {id:3})");
    assertThat(((Number) run("MATCH (n:L) WITH collect(n) AS ns UNWIND ns AS m DETACH DELETE m RETURN count(*) AS c").get(0).get("c"))
        .longValue()).isEqualTo(3L);
    assertThat(count("MATCH (n:L) RETURN count(n) AS c")).isZero();

    run("CREATE (:L {id:1}), (:L {id:2})");
    assertThat(((Number) run("MATCH (n:L) WITH {k: n} AS m DETACH DELETE m.k RETURN count(*) AS c").get(0).get("c")).longValue())
        .isEqualTo(2L);
    assertThat(count("MATCH (n:L) RETURN count(n) AS c")).isZero();

    run("CREATE (:L {id:1}), (:L {id:2})");
    final List<Map<String, Object>> sizes = run("MATCH (n:L) WITH collect(n) AS ns DETACH DELETE ns[0] RETURN size(ns) AS s");
    assertThat(((Number) sizes.get(0).get("s")).longValue()).isEqualTo(2L);
    assertThat(lastStatistics.getNodesDeleted()).isEqualTo(1);
    assertThat(count("MATCH (n:L) RETURN count(n) AS c")).isEqualTo(1L);
  }

  @Test
  void aNullFromAnOptionalMatchDeletesNothing() {
    assertThat(((Number) run("OPTIONAL MATCH (n:Nope) DELETE n RETURN count(n) AS c").get(0).get("c")).longValue()).isZero();
    assertThat(run("OPTIONAL MATCH (n:Nope) DETACH DELETE n RETURN n IS NULL AS isnull").get(0).get("isnull")).isEqualTo(true);
    assertThat(lastStatistics.getNodesDeleted()).isZero();
  }

  @Test
  void aDeletedNodeCannotBeReadAfterwardsButCanBeCounted() {
    run("CREATE (:L {id:1})");
    final Throwable thrown = catchThrowable(() -> run("MATCH (a:L {id:1}) DELETE a RETURN a.id AS id"));
    assertThat(thrown).isNotNull();
    assertThat(count("MATCH (n:L) RETURN count(n) AS c")).isEqualTo(1L);

    assertThat(((Number) run("MATCH (a:L {id:1}) DELETE a RETURN count(a) AS c").get(0).get("c")).longValue()).isEqualTo(1L);
    assertThat(count("MATCH (n:L) RETURN count(n) AS c")).isZero();
  }

  @Test
  void aDeleteBehindOrderSkipAndLimitTouchesOnlyTheWindow() {
    run("UNWIND range(1, 10) AS i CREATE (:L {id: i})");

    assertThat(((Number) run("MATCH (n:L) WITH n ORDER BY n.id DESC LIMIT 3 DETACH DELETE n RETURN count(*) AS c").get(0).get("c")).longValue())
        .isEqualTo(3L);
    assertThat(run("MATCH (n:L) RETURN min(n.id) AS lo, max(n.id) AS hi, count(n) AS c").get(0)).containsEntry("lo", 1L)
        .containsEntry("hi", 7L).containsEntry("c", 7L);

    assertThat(((Number) run("MATCH (n:L) WITH n ORDER BY n.id SKIP 2 LIMIT 2 DETACH DELETE n RETURN count(*) AS c").get(0).get("c")).longValue())
        .isEqualTo(2L);
    assertThat(run("MATCH (n:L) WITH n ORDER BY n.id RETURN collect(n.id) AS ids").get(0).get("ids")).isEqualTo(List.of(1L, 2L, 5L, 6L, 7L));

    assertThat(((Number) run("MATCH (n:L) WITH n ORDER BY n.id LIMIT 0 DETACH DELETE n RETURN count(*) AS c").get(0).get("c")).longValue())
        .isZero();
    assertThat(lastStatistics.getNodesDeleted()).isZero();
    assertThat(count("MATCH (n:L) RETURN count(n) AS c")).isEqualTo(5L);
  }

  @Test
  void aDeletedRelationshipKeepsItsPropertyValueReadBeforeTheDelete() {
    run("CREATE (a:L {id:1}), (b:L {id:2}), (a)-[:R {w:5}]->(b)");

    final List<Map<String, Object>> rows = run("MATCH ()-[r:R]->() WITH r, r.w AS w DELETE r RETURN w");

    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).get("w")).longValue()).isEqualTo(5L);
  }

  @Test
  void aForeachDetachDeletesTheNodesOfAPathAndTheirRelationships() {
    run("CREATE (a:L {id:1}), (b:L {id:2}), (c:L {id:3}), (a)-[:R]->(b)-[:R]->(c)");

    run("MATCH p = (a:L {id:1})-[:R*]->(c:L {id:3}) FOREACH (x IN nodes(p) | DETACH DELETE x)");

    assertThat(count("MATCH (n:L) RETURN count(n) AS c")).isZero();
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isZero();
  }

  @Test
  void aForeachDeletesAnIsolatedNodeAndRefusesAConnectedOneUnlessDetached() {
    run("CREATE (a:A {id:1}), (b:B {id:2}), (s:Solo {id:3}), (a)-[:R]->(b)");

    assertThat(catchThrowable(() -> run("MATCH (a:A)-[:R]->(b:B) FOREACH (x IN [a] | DELETE x)"))).isNotNull();
    assertThat(count("MATCH (n) RETURN count(n) AS c")).isEqualTo(3L);

    run("MATCH (s:Solo) FOREACH (x IN [s] | DELETE x)");
    assertThat(count("MATCH (n) RETURN count(n) AS c")).isEqualTo(2L);

    run("MATCH (a:A) FOREACH (x IN [a] | DETACH DELETE x)");
    assertThat(count("MATCH (n) RETURN count(n) AS c")).isEqualTo(1L);
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isZero();
  }

  @Test
  void aForeachDeletesARelationshipBeforeItsEndpoints() {
    run("CREATE (a:A {id:1}), (b:B {id:2}), (a)-[:R]->(b)");

    run("MATCH (a:A)-[r:R]->(b:B) FOREACH (x IN [r, a, b] | DELETE x)");

    assertThat(count("MATCH (n) RETURN count(n) AS c")).isZero();
  }

  @Test
  void aBatchedForeachDetachDeleteWorksPerRow() {
    run("UNWIND range(1, 4) AS i CREATE (:U {id: i})");

    final List<Map<String, Object>> rows = run("UNWIND [1, 2, 2, 9] AS i MATCH (n:U {id: i}) FOREACH (_ IN [1] | DETACH DELETE n) RETURN i");

    assertThat(rows).extracting(r -> ((Number) r.get("i")).longValue()).containsExactly(1L, 2L, 2L);
    assertThat(run("MATCH (n:U) WITH n ORDER BY n.id RETURN collect(n.id) AS ids").get(0).get("ids")).isEqualTo(List.of(3L, 4L));

    run("UNWIND [3, 4] AS i MATCH (n:U {id: i}) FOREACH (_ IN CASE WHEN i = 3 THEN [1] ELSE [] END | DELETE n)");
    assertThat(run("MATCH (n:U) RETURN n.id AS id").get(0).get("id")).isEqualTo(4L);
  }

  @Test
  void parallelRelationshipsWithoutPropertiesAreDeletedOneByOne() {
    run("CREATE (a:A {id:1}), (b:B {id:2})");
    run("MATCH (a:A), (b:B) CREATE (a)-[:R]->(b), (a)-[:R]->(b), (a)-[:R]->(b)");
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(3L);

    run("MATCH (a:A)-[r:R]->(b:B) WITH r LIMIT 1 DELETE r RETURN count(*) AS c");
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(2L);

    run("MATCH (a:A)-[r:R]->(b:B) WITH r SKIP 1 LIMIT 1 DELETE r RETURN count(*) AS c");
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(1L);

    run("MATCH (a:A), (b:B) CREATE (a)-[:R]->(b), (a)-[:R]->(b), (b)-[:R]->(a), (b)-[:R]->(a)");
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(5L);
    run("MATCH (a:A)-[r:R]-(b:B) DELETE r RETURN count(*) AS c");
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isZero();

    run("MATCH (a:A), (b:B) CREATE (a)-[:R]->(b), (a)-[:R]->(b), (a)-[:R]->(b), (a)-[:R]->(b), (a)-[:R]->(b)");
    run("MATCH (a:A)-[r:R]->(b:B) WITH collect(r) AS rs UNWIND rs AS x DELETE x RETURN count(*) AS c");
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isZero();

    run("MATCH (a:A), (b:B) CREATE (a)-[:R]->(b), (a)-[:R]->(b), (a)-[:R]->(b)");
    run("MATCH (a:A)-[r1:R]->(b:B), (a)-[r2:R]->(b) DELETE r1, r2 RETURN count(*) AS c");
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isZero();

    run("MATCH (a:A), (b:B) CREATE (a)-[:R]->(b), (a)-[:R]->(b), (a)-[:R]->(b)");
    run("MATCH (a:A)-[r:R]->(b:B) WITH collect(r) AS rs FOREACH (x IN rs | DELETE x)");
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isZero();

    run("MATCH (a:A), (b:B) CREATE (a)-[:R]->(b), (a)-[:R]->(b), (a)-[:R]->(b)");
    run("MATCH (a:A)-[r:R]->(b:B) WITH collect(r) AS rs FOREACH (x IN rs[0..2] | DELETE x)");
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(1L);

    run("MATCH (a:A) DETACH DELETE a RETURN count(*) AS c");
    assertThat(count("MATCH (n) RETURN count(n) AS c")).isEqualTo(1L);
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isZero();
  }

  @Test
  void aRelationshipHeldByTheRowOutlivesItsDeletedEndpointOnlyForItsType() {
    run("CREATE (a:A {id:1}), (b:B {id:2}), (a)-[:R {w:1}]->(b)");

    final List<Map<String, Object>> rows = run("MATCH (a:A)-[r:R]->(b:B) DETACH DELETE a RETURN type(r) AS t, count(*) AS c");

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0)).containsEntry("t", "R");
    assertThat(((Number) rows.get(0).get("c")).longValue()).isEqualTo(1L);
    assertThat(count("MATCH (n) RETURN count(n) AS c")).isEqualTo(1L);

    run("CREATE (a:A {id:1}), (a)-[:R {w:1}]->(:B {id:3})");
    assertThat(catchThrowable(() -> run("MATCH (a:A)-[r:R]->(b:B {id:3}) DETACH DELETE a RETURN r.w AS w"))).isNotNull();
    assertThat(count("MATCH (n:A) RETURN count(n) AS c")).isEqualTo(1L);
  }
}
