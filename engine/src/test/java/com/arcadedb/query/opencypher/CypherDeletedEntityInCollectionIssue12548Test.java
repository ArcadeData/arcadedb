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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for GitHub issue #12548:
 * When a node or relationship is deleted earlier in a statement and is still referenced elsewhere in the row
 * (top-level alias, List, Map, nested List/Map, or TraversalPath), subsequent property/label/key/map-projection
 * access must raise DeletedEntityAccess instead of reading stale state or returning null.
 */
class CypherDeletedEntityInCollectionIssue12548Test {
  private static final String DB_PATH = "./target/databases/testopencypher-deleted-entity-in-collections-12548";
  private Database            database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
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
    // Two LIGHTWEIGHT edges of one type over the same ordered pair share their RID: told apart by the entry of the edge list they
    // were read from. Deleting one renumbers the other, so a survivor retained in a collection must keep the number it had
    createTwoParallelLightweightEdges();

    final List<Object> survivingKeys = new ArrayList<>();
    final List<Long> sizes = new ArrayList<>();

    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (p1:Leaf {id: 101})-[r:L]->(p2:Leaf {id: 102})
          WITH collect(r) AS rs
          DELETE rs[0]
          RETURN keys(rs[1]) AS ks, size(rs) AS s""")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          survivingKeys.add(row.getProperty("ks"));
          sizes.add(((Number) row.getProperty("s")).longValue());
        }
      }
    });

    // The twin that was not deleted is read as it is, where a deleted one raises
    assertThat(survivingKeys).hasSize(1);
    assertThat((List<?>) survivingKeys.get(0)).isEmpty();
    assertThat(sizes).containsExactly(2L);

    // ... and the one deleted is read as deleted
    assertDeletedEntityAccess("""
        MATCH (p1:Leaf {id: 101})-[r:L]->(p2:Leaf {id: 102})
        WITH collect(r) AS rs
        DELETE rs[0]
        RETURN keys(rs[0]) AS ks""");
  }

  @Test
  void testParallelLightweightEdgesDeletedTogetherAreBothDeleted() {
    createTwoParallelLightweightEdges();

    assertDeletedEntityAccess("""
        MATCH (p1:Leaf {id: 101})-[r:L]->(p2:Leaf {id: 102})
        WITH collect(r) AS rs
        DELETE rs[0], rs[1]
        RETURN keys(rs[1]) AS ks""");
  }

  private void createTwoParallelLightweightEdges() {
    database.getSchema().createEdgeType("L");
    database.transaction(() -> {
      final MutableVertex p1 = database.newVertex("Leaf").set("id", 101L).save();
      final MutableVertex p2 = database.newVertex("Leaf").set("id", 102L).save();
      p1.newLightEdge("L", p2);
      p1.newLightEdge("L", p2);
    });
  }

  @Test
  void testNestedForeachDeletionPropagatesToOuterRow() {
    assertDeletedEntityAccess("""
        MATCH (n:Isolated)
        FOREACH (i IN [1] | FOREACH (j IN [1] | DELETE n))
        RETURN n.v""");
  }

  @Test
  void testMultiRowForeachBufferedRowSeesLaterRowDeletion() {
    // Row lid = 6 is processed first and deletes nothing; row lid = 7 deletes b. Only the EARLIER row reads the list, so it is
    // the earlier row that has to raise: it was buffered before the node it holds was deleted
    assertDeletedEntityAccess("""
        MATCH (b:B {id: 2}), (l:Leaf)
        WITH b, [b] AS list, l.id AS lid ORDER BY lid
        FOREACH (i IN CASE WHEN lid = 7 THEN [1] ELSE [] END | DETACH DELETE b)
        RETURN CASE WHEN lid = 6 THEN list[0].v ELSE null END AS v""");
  }

  @Test
  void testForeachRowWithANullListIsServedReadingTheDeletionOfALaterRow() {
    // The FOREACH list of row lid = 6 is null: its body never runs, but the row holds a node row lid = 7 deletes
    assertDeletedEntityAccess("""
        MATCH (b:B {id: 2}), (l:Leaf)
        WITH b, [b] AS list, l.id AS lid ORDER BY lid
        FOREACH (i IN CASE WHEN lid = 7 THEN [1] END | DETACH DELETE b)
        RETURN CASE WHEN lid = 6 THEN list[0].v ELSE null END AS v""");
  }

  @Test
  void testForeachBodyClauseAfterTheDeleteReadsTheNestedAliasAsDeleted() {
    // The body's DELETE runs when the iteration ends, but the clause after it reads the node as deleted already, through the
    // list as it does through the variable
    assertDeletedEntityAccess("""
        MATCH (b:B {id: 2}), (a:A)
        WITH a, b, [b] AS refs
        FOREACH (i IN [1] | DETACH DELETE b SET a.c = refs[0].v)""");

    assertDeletedEntityAccess("""
        MATCH (b:B {id: 2}), (a:A)
        WITH a, b
        FOREACH (i IN [1] | DETACH DELETE b SET a.c = b.v)""");
  }

  @Test
  void testForeachBodyReadsTheRelationshipsAnIterationDetachDeletesAsDeleted() {
    // DETACH DELETE a is deferred to the end of the iteration: the relationship of a, held in a list, is read as deleted by the
    // clause after it, as it is once the iteration ends
    assertDeletedEntityAccess("""
        MATCH (a:A)-[r:R]->(b:B)
        WITH a, b, [r] AS rs
        FOREACH (i IN [1] | DETACH DELETE a SET b.c = rs[0].k)""");

    // Without DETACH the relationship is not taken away with the node: the statement fails on the node still connected
    assertThatThrownBy(() -> database.transaction(() -> execute("""
        MATCH (a:A)-[r:R]->(b:B)
        WITH a, b, [r] AS rs
        FOREACH (i IN [1] | DELETE a SET b.c = rs[0].k)""")))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("DeleteConnectedNode");
  }

  @Test
  void testForeachBodyReadsADeferredLightweightEdgeAsDeletedAndItsTwinAsLive() {
    createTwoParallelLightweightEdges();

    // The twin deleted by the iteration is read as deleted by the clause after the DELETE
    assertDeletedEntityAccess("""
        MATCH (p1:Leaf {id: 101})-[r:L]->(p2:Leaf {id: 102}), (a:A)
        WITH a, collect(r) AS rs
        FOREACH (i IN [1] | DELETE rs[0] SET a.c = size(keys(rs[0])))""");

    // ... and the twin that is kept is not
    final List<Long> sizes = new ArrayList<>();
    database.transaction(() -> {
      execute("""
          MATCH (p1:Leaf {id: 101})-[r:L]->(p2:Leaf {id: 102}), (a:A)
          WITH a, collect(r) AS rs
          FOREACH (i IN [1] | DELETE rs[0] SET a.c = size(keys(rs[1])))""");
      try (final ResultSet rs = database.query("opencypher", "MATCH (a:A) RETURN a.c AS c")) {
        sizes.add(((Number) rs.next().getProperty("c")).longValue());
      }
    });
    assertThat(sizes).containsExactly(0L);
  }

  @Test
  void testForeachOverManyIterationsKeepsOneViewOfTheListItDeletesFrom() {
    // Each iteration runs on a plan of its own: a view stacked on the one of the iteration before would be as deep as the loop is
    // long, and the first read of the list would overflow the stack
    final int count = 20_000;
    database.getSchema().createVertexType("Many");
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.newVertex("Many").set("i", (long) i).save();
    });

    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Many)
          WITH collect(n) AS ns
          FOREACH (x IN ns | DELETE x)
          RETURN size(ns) AS s""")) {
        assertThat(((Number) rs.next().getProperty("s")).longValue()).isEqualTo(count);
      }
    });

    assertDeletedEntityAccess("""
        MATCH (n:Isolated)
        WITH collect(n) AS ns
        FOREACH (x IN [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] | FOREACH (y IN ns | DELETE y))
        RETURN ns[0].v""");
  }

  @Test
  void testForeachLaterIterationReadsWhatAnEarlierOneDeleted() {
    assertDeletedEntityAccess("""
        MATCH (n:Isolated), (a:A)
        FOREACH (i IN [1, 2] | SET a.c = n.v DELETE n)""");
  }

  @Test
  void testForeachDeletingTheSameNodeInEveryIterationDeletesItOnce() {
    final List<Long> deleted = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Isolated)
          FOREACH (i IN [1, 2, 3] | DELETE n)
          RETURN count(*) AS c""")) {
        while (rs.hasNext())
          deleted.add(((Number) rs.next().getProperty("c")).longValue());
      }
    });

    assertThat(deleted).containsExactly(1L);
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Isolated) RETURN count(n) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isZero();
    }
  }

  @Test
  void testLabelWriteAfterTheDeleteKeepsThePathReadingTheDeletion() {
    // SET a:Extra replaces the node a under a new RID and rebuilds the path that holds it: the rebuilt path still reads b as deleted
    database.getSchema().createVertexType("Extra");
    assertDeletedEntityAccess("""
        MATCH p = (a:A)-[:R]->(b:B)
        DETACH DELETE b
        SET a:Extra
        UNWIND nodes(p) AS nn
        RETURN nn.v""");
  }

  @Test
  void testEqualityDistinctAndMembershipStillWorkOnCollectionsHoldingDeletedEntities() {
    // What is read of a deleted entity raises, but the collection that holds it is still a value: it can be counted, compared,
    // grouped and searched, as the entity the clause deleted is
    final List<Object> answers = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:B {id: 2})
          WITH n, [n, 3] AS ns
          DETACH DELETE n
          RETURN count(DISTINCT ns) AS d, size(ns) AS s, n IN ns AS has, 3 IN ns AS hasThree, ns = ns AS same""")) {
        final Result row = rs.next();
        answers.add(((Number) row.getProperty("d")).longValue());
        answers.add(((Number) row.getProperty("s")).longValue());
        answers.add(row.getProperty("has"));
        answers.add(row.getProperty("hasThree"));
        answers.add(row.getProperty("same"));
        assertThat(rs.hasNext()).isFalse();
      }
    });

    assertThat(answers).containsExactly(1L, 2L, true, true, true);
  }

  @Test
  void testAPathThatWentThroughADeleteIsUsedByTheClausesAfterIt() {
    // The path is extended nowhere, rebuilt nowhere: it is read as the row carries it, over a plan with a disconnected pattern
    // after the delete
    final List<Long> lengths = new ArrayList<>();
    final List<Long> nodeCounts = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH p = (a:A)-[:R]->(b:B), (l:Leaf)
          DETACH DELETE b
          WITH p, l
          MATCH (h:Hub)
          RETURN length(p) AS len, size(nodes(p)) AS s""")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          lengths.add(((Number) row.getProperty("len")).longValue());
          nodeCounts.add(((Number) row.getProperty("s")).longValue());
        }
      }
    });

    assertThat(lengths).containsExactly(1L, 1L);
    assertThat(nodeCounts).containsExactly(2L, 2L);
  }

  @Test
  void testAnOrdinaryDeleteStillAnswersRowsThatHoldNoDeletedEntity() {
    final List<Long> values = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (a:A), (b:B)
          WITH a, b, [a, 'text', 3] AS mixed, {k: a.v} AS m
          DETACH DELETE b
          RETURN a.v AS av, size(mixed) AS s, mixed[0].v AS mv, m.k AS k""")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          values.add(((Number) row.getProperty("av")).longValue());
          values.add(((Number) row.getProperty("s")).longValue());
          values.add(((Number) row.getProperty("mv")).longValue());
          values.add(((Number) row.getProperty("k")).longValue());
        }
      }
    });
    assertThat(values).containsExactly(10L, 3L, 10L, 10L);
  }

  /**
   * The rows of a DELETE that removes one node per row share the list that holds them all: the cost of a row must not grow with
   * the size of that list (a copy or a walk of it per row made the statement quadratic).
   */
  @Test
  @Tag("slow")
  void testPerRowDeleteOfAnElementOfASharedListIsLinear() {
    final int count = 100_000;
    database.getSchema().createVertexType("Perf");
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.newVertex("Perf").set("i", (long) i).save();
    });

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    final long[] rows = new long[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          MATCH (n:Perf)
          WITH collect(n) AS ns
          UNWIND ns AS x
          DELETE x
          RETURN size(ns) AS s""")) {
        while (rs.hasNext()) {
          assertThat(((Number) rs.next().getProperty("s")).longValue()).isEqualTo(count);
          rows[0]++;
        }
      }
    });

    assertThat(rows[0]).isEqualTo(count);
    // Measured: a couple of seconds for 100,000 rows, where a copy or a walk of the shared list per row took minutes
    stopwatch.assertGaveUpWithin(20_000L, "linear in the rows (seconds) from quadratic in the size of the shared list (minutes)");
  }
}
