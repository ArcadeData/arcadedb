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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.opencypher.executor.steps.DeleteStep;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for GitHub issue #12548:
 * When a node or relationship is deleted earlier in a statement and is still referenced elsewhere in the row
 * (top-level alias, List, Map, nested List/Map, or TraversalPath), subsequent property/label/key/map-projection
 * access must raise DeletedEntityAccess instead of reading stale state or returning null.
 */
class CypherDeletedEntityInCollectionIssue12548Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-deleted-entity-in-collections-12548").create();
    database.getSchema().createVertexType("A");
    database.getSchema().createVertexType("B");
    database.getSchema().createVertexType("C");
    database.getSchema().createVertexType("Isolated");
    database.getSchema().createVertexType("Hub");
    database.getSchema().createVertexType("Leaf");
    database.getSchema().createEdgeType("R");
    resetGraph();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private void resetGraph() {
    database.transaction(() -> {
      execute("MATCH (n) DETACH DELETE n");
      execute("""
          CREATE (a:A {id: 1, v: 10})
          CREATE (b:B {id: 2, v: 20})
          CREATE (c:C {id: 3, v: 30})
          CREATE (iso:Isolated {id: 4, v: 40})
          CREATE (h:Hub {id: 5, v: 50})
          CREATE (l1:Leaf {id: 6, v: 60})
          CREATE (l2:Leaf {id: 7, v: 70})
          CREATE (a)-[:R {k: 100}]->(b)
          CREATE (b)-[:R {k: 200}]->(c)
          CREATE (h)-[:R {k: 501}]->(l1)
          CREATE (h)-[:R {k: 502}]->(l2)""");
    });
  }

  private void execute(final String query) {
    try (final ResultSet rs = database.command("opencypher", query)) {
      while (rs.hasNext())
        rs.next();
    }
  }

  private void assertDeletedEntityAccess(final String query) {
    assertThatThrownBy(() -> database.transaction(() -> execute(query)))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("DeletedEntityAccess");
  }

  @Test
  void testExactReproIssue12548() {
    assertDeletedEntityAccess("""
        MATCH (a:A)-[r:R]->(b:B)
        WITH a, b, collect(r) AS rs
        DETACH DELETE b
        UNWIND rs AS rr
        RETURN rr.k""");
  }

  @Test
  void testDirectDeleteOfRelationshipWithCollectedList() {
    assertDeletedEntityAccess("""
        MATCH ()-[r:R]->()
        WITH r, collect(r) AS rs
        DELETE r
        UNWIND rs AS rr
        RETURN rr.k""");

    assertDeletedEntityAccess("""
        MATCH ()-[r:R]->()
        WITH r, collect(r) AS rs
        DELETE r
        RETURN rs[0].k""");

    assertDeletedEntityAccess("""
        MATCH ()-[r:R]->()
        WITH r, collect(r) AS rs
        DELETE r
        RETURN rs[0]['k']""");

    assertDeletedEntityAccess("""
        MATCH ()-[r:R]->()
        WITH r, collect(r) AS rs
        DELETE r
        RETURN properties(rs[0])""");

    assertDeletedEntityAccess("""
        MATCH ()-[r:R]->()
        WITH r, collect(r) AS rs
        DELETE r
        RETURN keys(rs[0])""");
  }

  @Test
  void testTypeAndSizeSucceedOnCollectedListWithDeletedRelationships() {
    final List<Object> types = new ArrayList<>();
    final List<Long> sizes = new ArrayList<>();

    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (a:A)-[r:R]->(b:B)
          WITH collect(r) AS rs, b
          DETACH DELETE b
          UNWIND rs AS rr
          RETURN type(rr) AS t, size(rs) AS s""")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          types.add(row.getProperty("t"));
          sizes.add(((Number) row.getProperty("s")).longValue());
        }
      }
    });

    assertThat(types).containsExactly("R");
    assertThat(sizes).containsExactly(1L);
  }

  @Test
  void testDirectDeleteNodeWithCollectionsMapsAndProjections() {
    assertDeletedEntityAccess("""
        MATCH (n:B)
        WITH n, collect(n) AS ns
        DETACH DELETE n
        UNWIND ns AS x
        RETURN x.v""");

    assertDeletedEntityAccess("""
        MATCH (n:B)
        WITH n, {node: n} AS m
        DETACH DELETE n
        RETURN m.node.v""");

    assertDeletedEntityAccess("""
        MATCH (n:B)
        WITH n, {items: [n]} AS m
        DETACH DELETE n
        RETURN m.items[0].v""");

    assertDeletedEntityAccess("""
        MATCH (n:B)
        WITH n, n AS m
        DETACH DELETE n
        RETURN m.v""");

    assertDeletedEntityAccess("""
        MATCH (n:B)
        WITH n, collect(n) AS ns
        DETACH DELETE n
        UNWIND ns AS x
        RETURN x { .v }""");

    assertDeletedEntityAccess("""
        MATCH (n:B)
        WITH n, collect(n) AS ns
        DETACH DELETE n
        UNWIND ns AS x
        RETURN x { .* }""");
  }

  @Test
  void testDeleteMapPropertyAndListIndex() {
    assertDeletedEntityAccess("""
        MATCH (n:B)
        WITH {node: n} AS m
        DETACH DELETE m.node
        RETURN m.node.v""");

    assertDeletedEntityAccess("""
        MATCH (n:B)
        WITH collect(n) AS ns
        DETACH DELETE ns[0]
        RETURN ns[0].v""");

    assertDeletedEntityAccess("""
        MATCH (n:B)
        WITH collect(n) AS ns
        DETACH DELETE ns[0]
        RETURN labels(ns[0])""");
  }

  @Test
  void testForeachDeleteMarksOuterCollections() {
    assertDeletedEntityAccess("""
        MATCH (iso:Isolated)
        WITH collect(iso) AS ns
        FOREACH (x IN ns | DELETE x)
        UNWIND ns AS m
        RETURN m.v""");
  }

  @Test
  void testTraversalPathHoldingDeletedEntities() {
    assertDeletedEntityAccess("""
        MATCH p = (a:A)-[r:R]->(b:B)
        DETACH DELETE b
        UNWIND relationships(p) AS rr
        RETURN rr.k""");

    assertDeletedEntityAccess("""
        MATCH p = (a:A)-[r:R]->(b:B)
        DETACH DELETE b
        UNWIND nodes(p) AS nn
        RETURN nn.v""");

    final List<String> types = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH p = (a:A)-[r:R]->(b:B)
          DETACH DELETE b
          UNWIND relationships(p) AS rr
          RETURN type(rr) AS t""")) {
        while (rs.hasNext())
          types.add(rs.next().getProperty("t"));
      }
    });

    assertThat(types).containsExactly("R");
  }

  @Test
  void testPartialDeletionInMixedCollectionPreservesSurvivingElements() {
    final List<Long> survivorValues = new ArrayList<>();
    final List<Long> listSizes = new ArrayList<>();

    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (a:A), (b:B)
          WITH [a, b] AS ns, b
          DETACH DELETE b
          RETURN ns[0].v AS av, size(ns) AS s""")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          survivorValues.add(((Number) row.getProperty("av")).longValue());
          listSizes.add(((Number) row.getProperty("s")).longValue());
        }
      }
    });

    assertThat(survivorValues).containsExactly(10L);
    assertThat(listSizes).containsExactly(2L);

    resetGraph();

    final List<Long> mapSurvivorValues = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (a:A), (b:B)
          WITH {survivor: a, dead: b} AS m, b
          DETACH DELETE b
          RETURN m.survivor.v AS av""")) {
        while (rs.hasNext())
          mapSurvivorValues.add(((Number) rs.next().getProperty("av")).longValue());
      }
    });

    assertThat(mapSurvivorValues).containsExactly(10L);

    resetGraph();

    assertDeletedEntityAccess("""
        MATCH (a:A), (b:B)
        WITH {survivor: a, dead: b} AS m, b
        DETACH DELETE b
        RETURN m.dead.v""");
  }

  @Test
  void testNonDetachMultiRowDeleteWithPendingVerticesInList() {
    assertDeletedEntityAccess("""
        MATCH (h:Hub)-[r:R]->(l:Leaf)
        WITH [h] AS hs, r, h
        DELETE r, h
        RETURN hs[0].v""");
  }

  @Test
  void testMultiRowBatchDeletionWhereLaterRowDeletesEntityRetainedByEarlierRow() {
    assertDeletedEntityAccess("""
        MATCH (b:B {id: 2})
        WITH b
        UNWIND [null, b] AS target
        WITH target, b, [b] AS refs
        DETACH DELETE target
        RETURN refs[0].v""");
  }

  @Test
  void testParallelLightweightEdgesDeletionRetainsSurvivingEdge() {
    database.transaction(() -> {
      execute("""
          CREATE (p1:Leaf {id: 101, v: 1010})
          CREATE (p2:Leaf {id: 102, v: 1020})
          CREATE (p1)-[:R {k: 1}]->(p2)
          CREATE (p1)-[:R {k: 2}]->(p2)""");
    });

    final List<Long> survivingK = new ArrayList<>();
    final List<Object> survivingKeys = new ArrayList<>();

    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (p1:Leaf {id: 101})-[r:R]->(p2:Leaf {id: 102})
          WITH collect(r) AS rs
          DELETE rs[0]
          RETURN rs[1].k AS k, keys(rs[1]) AS ks""")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          survivingK.add(((Number) row.getProperty("k")).longValue());
          survivingKeys.add(row.getProperty("ks"));
        }
      }
    });

    assertThat(survivingK).hasSize(1);
    assertThat(survivingKeys).hasSize(1);

    assertDeletedEntityAccess("""
        MATCH (p1:Leaf {id: 101})-[r:R]->(p2:Leaf {id: 102})
        WITH collect(r) AS rs
        DELETE rs[0]
        RETURN rs[0].k""");
  }

  @Test
  void testNestedForeachDeletionPropagatesToOuterRow() {
    assertDeletedEntityAccess("""
        MATCH (n:Isolated)
        FOREACH (i IN [1] | FOREACH (j IN [1] | DELETE n))
        RETURN n.v""");
  }

  @Test
  void testCopyOnWritePreservesContainerIdentityWhenNothingDeleted() {
    final ResultInternal row = new ResultInternal();
    final List<Object> list = new ArrayList<>(List.of("val1", 42L));
    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("key1", "val1");
    map.put("key2", 42L);

    row.setProperty("list", list);
    row.setProperty("map", map);

    final DeleteStep.DeletedSoFar deleted = new DeleteStep.DeletedSoFar();

    DeleteStep.markDeletedReferences(row, deleted);

    assertThat((Object) row.getProperty("list")).isSameAs(list);
    assertThat((Object) row.getProperty("map")).isSameAs(map);
  }
}
