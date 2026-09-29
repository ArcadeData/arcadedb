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
package com.arcadedb.graph;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8625: a pattern over an edge type whose edges carry no incoming pointers returned 0 rows, with no error,
 * whenever a planner walked it from the target end. Every query below must answer what a loop over the OUT edges
 * answers, whatever direction the pattern is written in and whichever end the planner prefers.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8625UnidirectionalEdgeDirectionTest extends TestHelper {
  private static final int QUESTIONS = 400;
  private static final int TAGS      = 20;
  private static final int EXPECTED  = QUESTIONS * 2;

  @Test
  void newEdgeOnAUnidirectionalTypeIsFoundFromEitherEnd() {
    createSchema(false);
    loadWithNewEdge();
    assertEveryPatternFindsEveryEdge();
  }

  @Test
  void graphBatchOnAUnidirectionalTypeIsFoundFromEitherEnd() {
    createSchema(false);
    loadWithGraphBatch(false);
    assertEveryPatternFindsEveryEdge();
  }

  @Test
  void graphBatchOnAUnidirectionalTypeFollowsTheSchemaEvenWhenLeftBidirectional() {
    createSchema(false);
    loadWithGraphBatch(true);
    assertEveryPatternFindsEveryEdge();

    for (final var it = database.iterateType("Tag", false); it.hasNext(); )
      assertThat(it.next().asVertex().countEdges(Vertex.DIRECTION.IN, "TAGGED_WITH"))
          .as("the type is unidirectional, so the batch must not write the incoming side")
          .isZero();
  }

  @Test
  void graphBatchRefusesAUnidirectionalEdgeOnABidirectionalType() {
    createSchema(true);
    final List<RID> tags = createTags();
    final RID[] question = new RID[1];
    database.transaction(() -> question[0] = database.newVertex("Question").set("qid", 0).save().getIdentity());

    try (final GraphBatch batch = database.batch().withBidirectional(false).build()) {
      assertThatThrownBy(() -> batch.newEdge(question[0], "TAGGED_WITH", tags.getFirst()))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Edge type 'TAGGED_WITH' is bidirectional");
    }
  }

  @Test
  void bidirectionalTypeWithNewEdgeIsFoundFromEitherEnd() {
    createSchema(true);
    loadWithNewEdge();
    assertEveryPatternFindsEveryEdge();
  }

  @Test
  void newEdgeMessageNamesTheRealMismatch() {
    createSchema(true);
    final List<RID> tags = createTags();
    database.transaction(() -> {
      final MutableVertex q = database.newVertex("Question").set("qid", 0).save();
      assertThatThrownBy(() -> q.newEdge("TAGGED_WITH", tags.getFirst(), false, (Object[]) null))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Edge type 'TAGGED_WITH' is bidirectional");
    });
  }

  @Test
  void lightweightUnidirectionalTypeIsFoundFromEitherEnd() {
    database.getSchema().createVertexType("Question");
    database.getSchema().createVertexType("Tag");
    database.getSchema().buildEdgeType().withName("TAGGED_WITH").withBidirectional(false).withLightweight(true).create();
    loadWithNewEdge();
    assertEveryPatternFindsEveryEdge();
  }

  @Test
  void unidirectionalSubtypeIsFoundThroughItsSupertype() {
    database.getSchema().createVertexType("Question");
    database.getSchema().createVertexType("Tag");
    database.getSchema().createEdgeType("LINKED");
    database.getSchema().buildEdgeType().withName("TAGGED_WITH").withBidirectional(false).create().addSuperType("LINKED");
    loadWithNewEdge();

    assertThat(sum("opencypher", "MATCH (t:Tag)<-[:LINKED]-(q:Question) RETURN count(*) AS n")).isEqualTo(EXPECTED);
    assertThat(sum("opencypher", "MATCH (t:Tag {name: 't3'})<-[:LINKED]-(q) RETURN count(q) AS n")).isEqualTo(tagDegree("t3"));
    assertThat(sum("opencypher", "MATCH (t:Tag {name: 't3'})<--(q) RETURN count(q) AS n")).isEqualTo(tagDegree("t3"));
    assertThat(sum("sql", "SELECT count(*) AS n FROM (MATCH {type: Tag, where: (name = 't3')}.in('LINKED'){as: q} RETURN q)")).isEqualTo(tagDegree("t3"));
    assertThat(sum("sql", "SELECT count(*) AS n FROM (MATCH {type: Tag, where: (name = 't3')}.in(){as: q} RETURN q)")).isEqualTo(tagDegree("t3"));
  }

  @Test
  void everyTraversalShapeAnswersTheIncomingSide() {
    createSchema(false);
    loadWithNewEdge();
    final long degree = tagDegree("t3");

    // Undirected, from the target
    assertThat(sum("opencypher", "MATCH (t:Tag {name: 't3'})-[:TAGGED_WITH]-(q) RETURN count(q) AS n")).isEqualTo(degree);
    // Both ends bound: expand-into
    assertThat(sum("opencypher",
        "MATCH (t:Tag), (q:Question) WHERE t.name = 't3' WITH t, q MATCH (t)<-[:TAGGED_WITH]-(q) RETURN count(*) AS n"))
        .isEqualTo(degree);
    assertThat(sum("opencypher",
        "MATCH (t:Tag), (q:Question) WHERE t.name = 't3' WITH t, q MATCH (t)-[:TAGGED_WITH]-(q) RETURN count(*) AS n"))
        .isEqualTo(degree);
    // Both ends bound on the legacy path (OPTIONAL MATCH), filtered on the neighbour pointer
    assertThat(sum("opencypher",
        "MATCH (t:Tag {name: 't3'}), (q:Question {qid: 3}) OPTIONAL MATCH (t)<-[r:TAGGED_WITH]-(q) RETURN count(r) AS n"))
        .isEqualTo(1);
    assertThat(sum("opencypher",
        "MATCH (t:Tag {name: 't3'}), (q:Question {qid: 3}) OPTIONAL MATCH (t)-[r:TAGGED_WITH]-(q) RETURN count(r) AS n"))
        .isEqualTo(1);
    // Variable length, from the target
    assertThat(sum("opencypher", "MATCH (t:Tag {name: 't3'})<-[:TAGGED_WITH*1..2]-(q) RETURN count(q) AS n"))
        .isEqualTo(degree);
    // Pattern predicates
    assertThat(sum("opencypher",
        "MATCH (t:Tag) WHERE t.name = 't3' AND EXISTS { (t)<-[:TAGGED_WITH]-(:Question) } RETURN count(t) AS n")).isEqualTo(1);
    assertThat(sum("opencypher", "MATCH (t:Tag) WHERE (t)<-[:TAGGED_WITH]-(:Question) RETURN count(t) AS n"))
        .isEqualTo(TAGS);
    assertThat(sum("opencypher",
        "MATCH (t:Tag {name: 't3'}), (q:Question) WHERE (t)<-[:TAGGED_WITH]-(q) RETURN count(q) AS n")).isEqualTo(degree);
    // Shortest path from the target
    assertThat(rows("opencypher",
        "MATCH (t:Tag {name: 't3'}), (q:Question {qid: 3}) MATCH p = shortestPath((t)<-[:TAGGED_WITH*]-(q)) RETURN p"))
        .isEqualTo(1);
    // SQL functions and edges
    assertThat(sum("sql", "SELECT count(*) AS n FROM (MATCH {type: Tag, where: (name = 't3')}.in('TAGGED_WITH'){as: q} RETURN q)")).isEqualTo(degree);
    assertThat(sum("sql", "SELECT count(*) AS n FROM (MATCH {type: Tag, where: (name = 't3')}.inE('TAGGED_WITH'){as: e} RETURN e)")).isEqualTo(degree);
    assertThat(sum("sql", "SELECT count(*) AS n FROM (MATCH {type: Tag, where: (name = 't3')}.both('TAGGED_WITH'){as: q} RETURN q)")).isEqualTo(degree);
    assertThat(sum("sql", "SELECT count(*) AS n FROM (MATCH {type: Tag, where: (name = 't3')}.bothE('TAGGED_WITH'){as: e} RETURN e)")).isEqualTo(degree);
    // SQL functions called on their own read what the vertex stores, as the vertex API does (embedded, remote, Gremlin)
    assertThat(sum("sql", "SELECT in('TAGGED_WITH').size() AS n FROM Tag WHERE name = 't3'")).isZero();
    assertThat(sum("sql", "SELECT inE('TAGGED_WITH').size() AS n FROM Tag WHERE name = 't3'")).isZero();
    assertThat(sum("sql",
        "SELECT count(*) AS n FROM (MATCH {type: Tag, as: t, where: (name = 't3')}-TAGGED_WITH-{as: q} RETURN q, t)"))
        .isEqualTo(degree);

    // The vertex API keeps the contract the type was declared with
    for (final var it = database.iterateType("Tag", false); it.hasNext(); ) {
      final Vertex tag = it.next().asVertex();
      assertThat(tag.countEdges(Vertex.DIRECTION.IN, "TAGGED_WITH")).isZero();
      assertThat(tag.getVertices(Vertex.DIRECTION.IN, "TAGGED_WITH").iterator().hasNext()).isFalse();
    }
  }

  @Test
  void plannersWalkAUnidirectionalHopFromItsSource() {
    createSchema(false);
    loadWithNewEdge();

    assertThat(explain("opencypher", "MATCH (t:Tag)<-[:TAGGED_WITH]-(q:Question) RETURN q.qid AS qid, t.name AS tag"))
        .contains("NodeByLabelScan(q:Question)")
        .contains("ExpandAll(q)-[:TAGGED_WITH]->(t:Tag)");
    assertThat(explain("sql", "MATCH {type: Tag, as: t}<-TAGGED_WITH-{type: Question, as: q} RETURN q, t"))
        .contains("FETCH FROM TYPE Question");
    // An outgoing chain reads only what is stored: the count push-down stays
    assertThat(explain("opencypher", "MATCH (q:Question)-[:TAGGED_WITH]->(t:Tag) RETURN count(*) AS n"))
        .contains("Count Push-Down");
    assertThat(explain("opencypher", "MATCH (t:Tag)<-[:TAGGED_WITH]-(q:Question) RETURN count(*) AS n"))
        .doesNotContain("Count Push-Down");
  }

  @Test
  void aGraphAnalyticalViewAnswersTheIncomingSide() {
    createSchema(false);
    loadWithNewEdge();
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("tagged8625")
        .withVertexTypes("Question", "Tag")
        .withEdgeTypes("TAGGED_WITH")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .build();
    try {
      assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
      assertEveryPatternFindsEveryEdge();
    } finally {
      view.drop();
    }
  }

  @Test
  void theLookupIsBoundedByTheHeapElementsCap() {
    createSchema(false);
    loadWithNewEdge();
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 100L);
    try {
      assertThatThrownBy(() -> sum("opencypher",
          "MATCH (t:Tag) WHERE t.name = 't3' MATCH (t)<-[:TAGGED_WITH]-(q) RETURN count(q) AS n"))
          .hasMessageContaining("incoming-edge lookup over the unidirectional edge type TAGGED_WITH")
          .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, -1L);
    }
  }

  @Test
  void groupedCountsAreRightForEveryTarget() {
    createSchema(false);
    loadWithNewEdge();
    for (final String query : new String[] {
        "MATCH (t:Tag)<-[:TAGGED_WITH]-(q:Question) RETURN t.name AS tag, count(q) AS n",
        "MATCH (q:Question)-[:TAGGED_WITH]->(t:Tag) RETURN t.name AS tag, count(q) AS n",
        "MATCH (t:Tag)-[:TAGGED_WITH]-(q:Question) RETURN t.name AS tag, count(*) AS n" }) {
      long rows = 0;
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          assertThat(((Number) r.getProperty("n")).longValue()).as(query + " for " + r.getProperty("tag"))
              .isEqualTo(tagDegree(r.getProperty("tag")));
          ++rows;
        }
      }
      assertThat(rows).as(query).isEqualTo(TAGS);
    }
  }

  @Test
  void aWalkReachingUnidirectionalAndBidirectionalTypesAnswersBoth() {
    createSchema(false);
    database.getSchema().createEdgeType("FOLLOWS");
    loadWithNewEdge();
    final RID[] follower = new RID[1];
    database.transaction(() -> {
      final Vertex t3 = database.query("sql", "SELECT FROM Tag WHERE name = 't3'").next().getVertex().get();
      final MutableVertex fan = database.newVertex("Question").set("qid", -1).save();
      fan.newEdge("FOLLOWS", t3);
      follower[0] = fan.getIdentity();
    });
    final long degree = tagDegree("t3") + 1;

    assertThat(sum("sql", "SELECT count(*) AS n FROM (MATCH {type: Tag, where: (name = 't3')}.in(){as: q} RETURN q)")).isEqualTo(degree);
    assertThat(sum("sql", "SELECT count(*) AS n FROM (MATCH {type: Tag, where: (name = 't3')}.inE(){as: e} RETURN e)")).isEqualTo(degree);
    assertThat(sum("opencypher", "MATCH (t:Tag {name: 't3'})<--(q) RETURN count(q) AS n")).isEqualTo(degree);
    assertThat(sum("opencypher", "MATCH (t:Tag {name: 't3'})<-[:TAGGED_WITH|FOLLOWS]-(q) RETURN count(q) AS n"))
        .isEqualTo(degree);
    assertThat(sum("opencypher", "MATCH (t:Tag {name: 't3'})--(q) RETURN count(q) AS n")).isEqualTo(degree);
  }

  @Test
  void anIncomingPointerLeftOnAUnidirectionalTypeIsNotCountedTwice() {
    createSchema(false);
    loadWithNewEdge();
    // What a bulk load that did not follow the type left behind: the incoming side of a unidirectional edge
    database.transaction(() -> {
      for (final var it = database.iterateType("Question", false); it.hasNext(); ) {
        final Vertex q = it.next().asVertex();
        for (final Edge e : q.getEdges(Vertex.DIRECTION.OUT, "TAGGED_WITH"))
          ((DatabaseInternal) database).getGraphEngine().connectIncomingEdge(e.getInVertex(), q.getIdentity(), e.getIdentity());
      }
    });
    assertEveryPatternFindsEveryEdge();
  }

  @Test
  void aSelfLoopIsOneRelationship() {
    createSchema(false);
    database.transaction(() -> {
      final MutableVertex tag = database.newVertex("Tag").set("name", "loop").save();
      tag.newEdge("TAGGED_WITH", tag);
    });
    assertThat(sum("opencypher", "MATCH (t:Tag {name: 'loop'})-[r:TAGGED_WITH]-(x) RETURN count(r) AS n")).isEqualTo(1);
    assertThat(sum("opencypher", "MATCH (t:Tag {name: 'loop'})<-[r:TAGGED_WITH]-(x) RETURN count(r) AS n")).isEqualTo(1);
    assertThat(sum("opencypher", "MATCH (t:Tag {name: 'loop'})-[:TAGGED_WITH]-(x) RETURN count(x) AS n")).isEqualTo(1);
  }

  @Test
  void anonymousNodesAndBoundTargetsAcrossClauses() {
    createSchema(false);
    loadWithNewEdge();
    final long degree = tagDegree("t3");
    assertThat(sum("opencypher", "MATCH (:Tag {name: 't3'})<-[:TAGGED_WITH]-(q:Question) RETURN count(q) AS n"))
        .isEqualTo(degree);
    assertThat(sum("opencypher", "MATCH (t:Tag)<-[:TAGGED_WITH]-(:Question) RETURN count(t) AS n")).isEqualTo(EXPECTED);
    assertThat(sum("opencypher",
        "MATCH (t:Tag {name: 't3'}) MATCH (q:Question) WHERE q.qid < 100 MATCH (t)<-[:TAGGED_WITH]-(q) RETURN count(q) AS n"))
        .isEqualTo(tagDegreeBelow("t3", 100));
    assertThat(sum("opencypher",
        "MATCH (t:Tag {name: 't3'}) WITH t MATCH (t)<-[:TAGGED_WITH]-(q:Question)-[:TAGGED_WITH]->(other:Tag) "
            + "RETURN count(other) AS n")).isEqualTo(degree); // relationship uniqueness: the other tag of each question
  }

  @Test
  void shortestPathFunctionsWalkTheIncomingSide() {
    createSchema(false);
    loadWithNewEdge();
    // The SQL function called on its own walks what the vertices store, as the vertex API does: from the source only
    assertThat(sum("sql",
        "SELECT shortestPath((SELECT FROM Question WHERE qid = 3), (SELECT FROM Tag WHERE name = 't3'), 'OUT', 'TAGGED_WITH').size() AS n "
            + "FROM Tag LIMIT 1")).isEqualTo(2);
    assertThat(sum("sql",
        "SELECT shortestPath((SELECT FROM Tag WHERE name = 't3'), (SELECT FROM Question WHERE qid = 3), 'IN', 'TAGGED_WITH').size() AS n "
            + "FROM Tag LIMIT 1")).isZero();
    assertThat(rows("opencypher",
        "MATCH (t:Tag {name: 't3'}), (q:Question {qid: 3}) MATCH p = allShortestPaths((t)<-[:TAGGED_WITH*]-(q)) RETURN p"))
        .isEqualTo(1);
    assertThat(rows("opencypher",
        "MATCH (t:Tag {name: 't3'}), (q:Question {qid: 3}) "
            + "MATCH p = shortestPath((t)<-[r:TAGGED_WITH* WHERE r.missing IS NULL]-(q)) RETURN p")).isEqualTo(1);
  }

  @Test
  void mergeFindsTheEdgeFromItsTarget() {
    createSchema(false);
    loadWithNewEdge();
    database.transaction(() -> {
      database.command("opencypher", "MATCH (t:Tag {name: 't3'}), (q:Question {qid: 3}) MERGE (t)<-[:TAGGED_WITH]-(q)");
      database.command("opencypher", "MATCH (t:Tag {name: 't3'}), (q:Question {qid: 3}) MERGE (t)-[:TAGGED_WITH]-(q)");
      database.command("opencypher", "MATCH (t:Tag {name: 't3'}) MERGE (t)<-[:TAGGED_WITH]-(q:Question {qid: 3})");
    });
    assertThat(groundTruth()).as("MERGE must match the existing edges, not create new ones").isEqualTo(EXPECTED);
  }

  @Test
  void writesInTheQueryTransactionAreOverlaidWithoutANewScan() {
    createSchema(false);
    loadWithNewEdge();
    final long degree = tagDegree("t3");
    database.transaction(() -> {
      // A MERGE per row that creates and then reads the incoming side: the scan taken by the first row is kept
      final long scansBefore = IncomingEdgeLookup.getScansTaken();
      database.command("opencypher",
          "UNWIND range(1000, 1099) AS i CREATE (q:Question {qid: i}) WITH q MATCH (t:Tag {name: 't3'}) "
              + "MERGE (t)<-[:TAGGED_WITH]-(q) WITH t RETURN COUNT { (t)<-[:TAGGED_WITH]-() } AS n");
      assertThat(IncomingEdgeLookup.getScansTaken() - scansBefore).as("one scan for the whole query").isEqualTo(1);
      assertThat(sum("opencypher", "MATCH (t:Tag {name: 't3'})<-[:TAGGED_WITH]-(q) RETURN count(q) AS n"))
          .isEqualTo(degree + 100);
      database.command("sql", "DELETE FROM TAGGED_WITH WHERE @out IN (SELECT FROM Question WHERE qid >= 1050)");
      assertThat(sum("sql", "SELECT count(*) AS n FROM (MATCH {type: Tag, where: (name = 't3')}.in('TAGGED_WITH'){as: q} RETURN q)")).isEqualTo(degree + 50);
    });
    assertThat(sum("opencypher", "MATCH (t:Tag {name: 't3'})<-[:TAGGED_WITH]-(q) RETURN count(q) AS n"))
        .isEqualTo(degree + 50);
  }

  @Test
  void aRolledBackOrDeletedEdgeIsNotAnsweredAnymore() {
    createSchema(false);
    loadWithNewEdge();
    final long degree = tagDegree("t3");

    // Rollback: the edges created in the rolled back transaction are gone for the next read
    database.begin();
    database.command("sql", "CREATE EDGE TAGGED_WITH FROM (SELECT FROM Question WHERE qid = 0) TO (SELECT FROM Tag WHERE name = 't3')");
    assertThat(sum("sql", "SELECT count(*) AS n FROM (MATCH {type: Tag, where: (name = 't3')}.in('TAGGED_WITH'){as: q} RETURN q)")).isEqualTo(degree + 1);
    database.rollback();
    assertThat(sum("sql", "SELECT count(*) AS n FROM (MATCH {type: Tag, where: (name = 't3')}.in('TAGGED_WITH'){as: q} RETURN q)")).isEqualTo(degree);

    // A deleted source vertex takes its edges with it, within the same query
    database.transaction(() -> {
      assertThat(sumCommand("opencypher",
          "MATCH (t:Tag {name: 't3'}) WITH t, COUNT { (t)<-[:TAGGED_WITH]-() } AS before "
              + "MATCH (q:Question {qid: 3}) DETACH DELETE q "
              + "WITH t, before RETURN before - COUNT { (t)<-[:TAGGED_WITH]-() } AS n")).isEqualTo(1);
    });
    assertThat(sum("sql", "SELECT count(*) AS n FROM (MATCH {type: Tag, where: (name = 't3')}.in('TAGGED_WITH'){as: q} RETURN q)")).isEqualTo(degree - 1);
  }

  @Test
  void aLightweightTypeOverlaysTheWritesOfItsTransaction() {
    database.getSchema().createVertexType("Question");
    database.getSchema().createVertexType("Tag");
    database.getSchema().buildEdgeType().withName("TAGGED_WITH").withBidirectional(false).withLightweight(true).create();
    loadWithNewEdge();
    final long degree = tagDegree("t3");
    database.transaction(() -> {
      assertThat(sum("sql", "SELECT count(*) AS n FROM (MATCH {type: Tag, where: (name = 't3')}.in('TAGGED_WITH'){as: q} RETURN q)")).isEqualTo(degree);
      assertThat(sumCommand("opencypher",
          "MATCH (t:Tag {name: 't3'}) WITH t, COUNT { (t)<-[:TAGGED_WITH]-() } AS before "
              + "CREATE (t)<-[:TAGGED_WITH]-(:Question {qid: -5}) "
              + "WITH t, before RETURN COUNT { (t)<-[:TAGGED_WITH]-() } - before AS n")).isEqualTo(1);
      assertThat(sumCommand("opencypher",
          "MATCH (t:Tag {name: 't3'}) WITH t, COUNT { (t)<-[:TAGGED_WITH]-() } AS before "
              + "MATCH (t)<-[r:TAGGED_WITH]-(:Question {qid: -5}) DELETE r "
              + "WITH t, before RETURN before - COUNT { (t)<-[:TAGGED_WITH]-() } AS n")).isEqualTo(1);
    });
    assertThat(sum("sql", "SELECT count(*) AS n FROM (MATCH {type: Tag, where: (name = 't3')}.in('TAGGED_WITH'){as: q} RETURN q)")).isEqualTo(degree);
  }

  @Test
  void aTransactionThatNeverReadsTheIncomingSideRecordsNothing() {
    createSchema(false);
    final List<RID> tags = createTags();
    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++)
        database.newVertex("Question").set("qid", i).save().newEdge("TAGGED_WITH", tags.get(i % TAGS));
      final UnidirectionalEdgeChanges changes = ((DatabaseInternal) database).getTransaction().getUnidirectionalEdgeChangesIfAny();
      assertThat(changes == null || changes.size() == 0).as("a bulk write keeps no change nothing will read").isTrue();
    });
  }

  @Test
  void manyDistinctTargetsInAnyOrderAreAllFound() {
    createSchema(false);
    final int count = 5_000;
    database.transaction(() -> {
      final List<MutableVertex> tags = new ArrayList<>(count);
      for (int i = 0; i < count; i++)
        tags.add(database.newVertex("Tag").set("name", "x" + i).save());
      final MutableVertex q = database.newVertex("Question").set("qid", 0).save();
      // Ascending, then descending targets: the orders a naive quicksort degenerates on
      for (int i = 0; i < count; i++)
        q.newEdge("TAGGED_WITH", tags.get(i));
      for (int i = count - 1; i >= 0; i--)
        q.newEdge("TAGGED_WITH", tags.get(i));
    });
    assertThat(sum("opencypher", "MATCH (t:Tag)<-[:TAGGED_WITH]-(q:Question) WITH t, count(q) AS c WHERE c = 2 RETURN count(t) AS n"))
        .isEqualTo(count);
    assertThat(sum("sql", "SELECT count(*) AS n FROM (MATCH {type: Tag, where: (name = 'x4321')}.in('TAGGED_WITH'){as: q} RETURN q)")).isEqualTo(2);
  }

  @Test
  void aScriptCommittingStatementByStatementReadsWhatItWrote() {
    createSchema(false);
    database.transaction(() -> {
      database.newVertex("Tag").set("name", "s").save();
      database.newVertex("Question").set("qid", 1).save();
      database.newVertex("Question").set("qid", 2).save();
    });
    assertThat(runScriptReadingBeforeAndAfterACommit()).isEqualTo(2);
  }

  @Test
  void aScriptWhoseFirstStatementReadsOnAFreshThreadStillSeesItsLaterWrites() throws Exception {
    createSchema(false);
    database.transaction(() -> {
      database.newVertex("Tag").set("name", "s").save();
      database.newVertex("Question").set("qid", 1).save();
      database.newVertex("Question").set("qid", 2).save();
    });
    final long[] result = new long[1];
    final Thread thread = new Thread(() -> result[0] = runScriptReadingBeforeAndAfterACommit());
    thread.start();
    thread.join();
    assertThat(result[0]).isEqualTo(2);
  }

  /**
   * A read that takes the scan, one committed write, and a second read on the same script context: the second read
   * must see the written edge. Plain statements rather than LET, which makes the script run a CREATE EDGE twice (#8633).
   */
  private long runScriptReadingBeforeAndAfterACommit() {
    try (final ResultSet rs = database.command("sqlscript", """
        SELECT count(*) AS n FROM (MATCH {type: Tag, where: (name = 's')}.in('TAGGED_WITH'){as: q} RETURN q);
        BEGIN;
        CREATE EDGE TAGGED_WITH FROM (SELECT FROM Question WHERE qid = 1) TO (SELECT FROM Tag WHERE name = 's');
        CREATE EDGE TAGGED_WITH FROM (SELECT FROM Question WHERE qid = 2) TO (SELECT FROM Tag WHERE name = 's');
        COMMIT;
        SELECT count(*) AS n FROM (MATCH {type: Tag, where: (name = 's')}.in('TAGGED_WITH'){as: q} RETURN q);
        """)) {
      final long n = ((Number) rs.next().getProperty("n")).longValue();
      assertThat(sum("sql", "SELECT count(*) AS n FROM TAGGED_WITH")).as("the script wrote two edges").isEqualTo(2);
      return n;
    }
  }

  @Test
  void aScriptSeesTheEdgesItWritesBetweenReads() {
    createSchema(false);
    database.transaction(() -> {
      database.newVertex("Tag").set("name", "s").save();
      database.newVertex("Question").set("qid", 1).save();
      database.newVertex("Question").set("qid", 2).save();
    });
    final List<Object> counts = new ArrayList<>();
    database.transaction(() -> {
      final ResultSet rs = database.command("sqlscript", """
        CREATE EDGE TAGGED_WITH FROM (SELECT FROM Question WHERE qid = 1) TO (SELECT FROM Tag WHERE name = 's');
        LET a = SELECT count(*) AS n FROM (MATCH {type: Tag, where: (name = 's')}.in('TAGGED_WITH'){as: q} RETURN q);
        CREATE EDGE TAGGED_WITH FROM (SELECT FROM Question WHERE qid = 2) TO (SELECT FROM Tag WHERE name = 's');
        LET b = SELECT count(*) AS n FROM (MATCH {type: Tag, where: (name = 's')}.in('TAGGED_WITH'){as: q} RETURN q);
        DELETE FROM TAGGED_WITH WHERE @out IN (SELECT FROM Question WHERE qid = 1);
        LET c = SELECT count(*) AS n FROM (MATCH {type: Tag, where: (name = 's')}.in('TAGGED_WITH'){as: q} RETURN q);
        RETURN [$a[0].n, $b[0].n, $c[0].n];
        """);
      while (rs.hasNext())
        counts.add(rs.next().getProperty("value"));
    });
    assertThat(counts.toString()).isEqualTo("[1, 2, 1]");
  }

  private void assertEveryPatternFindsEveryEdge() {
    assertThat(groundTruth()).isEqualTo(EXPECTED);

    assertThat(sum("opencypher", "MATCH (q:Question)-[:TAGGED_WITH]->(t:Tag) RETURN count(*) AS n")).isEqualTo(EXPECTED);
    assertThat(sum("opencypher", "MATCH (q:Question)-[:TAGGED_WITH]->(t:Tag) RETURN t.name AS tag, count(q) AS n"))
        .isEqualTo(EXPECTED);
    assertThat(rows("opencypher", "MATCH (q:Question)-[:TAGGED_WITH]->(t:Tag) RETURN q.qid AS qid, t.name AS tag"))
        .isEqualTo(EXPECTED);
    assertThat(sum("opencypher", "MATCH (t:Tag)<-[:TAGGED_WITH]-(q:Question) RETURN count(*) AS n")).isEqualTo(EXPECTED);
    assertThat(rows("opencypher", "MATCH (t:Tag)<-[:TAGGED_WITH]-(q:Question) RETURN q.qid AS qid, t.name AS tag"))
        .isEqualTo(EXPECTED);
    assertThat(sum("opencypher", "MATCH (t:Tag)<-[:TAGGED_WITH]-(q:Question) RETURN t.name AS tag, count(q) AS n"))
        .isEqualTo(EXPECTED);
    // Anchored on the target: only one end is bound, and it is the end without pointers
    assertThat(sum("opencypher", "MATCH (t:Tag {name: 't3'})<-[:TAGGED_WITH]-(q:Question) RETURN count(q) AS n"))
        .isEqualTo(tagDegree("t3"));
    assertThat(sum("opencypher", "MATCH (t:Tag) WHERE t.name = 't3' MATCH (t)<-[:TAGGED_WITH]-(q:Question) RETURN count(q) AS n"))
        .isEqualTo(tagDegree("t3"));

    assertThat(sum("sql", "SELECT count(*) AS n FROM (MATCH {type: Question, as: q}-TAGGED_WITH->{type: Tag, as: t} RETURN q, t)"))
        .isEqualTo(EXPECTED);
    assertThat(sum("sql", "SELECT count(*) AS n FROM (MATCH {type: Tag, as: t}<-TAGGED_WITH-{type: Question, as: q} RETURN q, t)"))
        .isEqualTo(EXPECTED);
    assertThat(sum("sql", "SELECT count(*) AS n FROM (MATCH {type: Tag, as: t, where: (name = 't3')}<-TAGGED_WITH-{type: Question, as: q} RETURN q, t)"))
        .isEqualTo(tagDegree("t3"));
    assertThat(sum("sql", "SELECT count(*) AS n FROM (MATCH {type: Tag, as: t, where: (name = 't3')}.in('TAGGED_WITH'){as: q} RETURN q, t)"))
        .isEqualTo(tagDegree("t3"));
  }

  private void createSchema(final boolean bidirectional) {
    database.getSchema().createVertexType("Question");
    database.getSchema().createVertexType("Tag");
    database.getSchema().buildEdgeType().withName("TAGGED_WITH").withBidirectional(bidirectional).create();
  }

  private List<RID> createTags() {
    final List<RID> tags = new ArrayList<>();
    database.transaction(() -> {
      for (int t = 0; t < TAGS; t++)
        tags.add(database.newVertex("Tag").set("name", "t" + t).save().getIdentity());
    });
    return tags;
  }

  private void loadWithNewEdge() {
    final List<RID> tags = createTags();
    database.transaction(() -> {
      for (int i = 0; i < QUESTIONS; i++) {
        final MutableVertex q = database.newVertex("Question").set("qid", i).save();
        q.newEdge("TAGGED_WITH", tags.get(i % TAGS));
        q.newEdge("TAGGED_WITH", tags.get((i * 7 + 3) % TAGS));
      }
    });
  }

  private void loadWithGraphBatch(final boolean batchBidirectional) {
    final List<RID> tags = createTags();
    final List<RID> questions = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < QUESTIONS; i++)
        questions.add(database.newVertex("Question").set("qid", i).save().getIdentity());
    });
    try (final GraphBatch batch = database.batch().withBidirectional(batchBidirectional).build()) {
      for (int i = 0; i < QUESTIONS; i++) {
        batch.newEdge(questions.get(i), "TAGGED_WITH", tags.get(i % TAGS));
        batch.newEdge(questions.get(i), "TAGGED_WITH", tags.get((i * 7 + 3) % TAGS));
      }
    }
  }

  private long groundTruth() {
    long n = 0;
    for (final var it = database.iterateType("Question", false); it.hasNext(); )
      n += it.next().asVertex().countEdges(Vertex.DIRECTION.OUT, "TAGGED_WITH");
    return n;
  }

  private long tagDegree(final String name) {
    long n = 0;
    for (final var it = database.iterateType("Question", false); it.hasNext(); )
      for (final Vertex t : it.next().asVertex().getVertices(Vertex.DIRECTION.OUT, "TAGGED_WITH"))
        if (name.equals(t.getString("name")))
          n++;
    return n;
  }

  private long tagDegreeBelow(final String name, final int maxQid) {
    long n = 0;
    for (final var it = database.iterateType("Question", false); it.hasNext(); ) {
      final Vertex q = it.next().asVertex();
      if (q.getInteger("qid") < maxQid)
        for (final Vertex t : q.getVertices(Vertex.DIRECTION.OUT, "TAGGED_WITH"))
          if (name.equals(t.getString("name")))
            n++;
    }
    return n;
  }

  private long sumCommand(final String language, final String query) {
    long n = 0;
    try (final ResultSet rs = database.command(language, query)) {
      while (rs.hasNext())
        n += ((Number) rs.next().getProperty("n")).longValue();
    }
    return n;
  }

  private long sum(final String language, final String query) {
    long n = 0;
    try (final ResultSet rs = database.query(language, query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        n += ((Number) r.getProperty("n")).longValue();
      }
    }
    return n;
  }

  private String explain(final String language, final String query) {
    final StringBuilder plan = new StringBuilder();
    try (final ResultSet rs = database.query(language, "EXPLAIN " + query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final Object text = r.getProperty("executionPlanAsString");
        plan.append(text != null ? text : r.toJSON()).append('\n');
      }
    }
    return plan.toString();
  }

  private long rows(final String language, final String query) {
    long n = 0;
    try (final ResultSet rs = database.query(language, query)) {
      while (rs.hasNext()) {
        rs.next();
        n++;
      }
    }
    return n;
  }
}
