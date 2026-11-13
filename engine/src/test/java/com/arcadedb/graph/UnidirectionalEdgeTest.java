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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests of the UnidirectionalEdge area, one nested class per former stand-alone regression class.
 */
class UnidirectionalEdgeTest {
  /**
   * Issue #8625: a pattern over an edge type whose edges carry no incoming pointers returned 0 rows, with no error,
   * whenever a planner walked it from the target end. Every query below must answer what a loop over the OUT edges
   * answers, whatever direction the pattern is written in and whichever end the planner prefers.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class UnidirectionalEdgeDirectionTest extends TestHelper {
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
    void graphBatchNamesAnUnknownEdgeType() {
      createSchema(false);
      final List<RID> tags = createTags();
      try (final GraphBatch batch = database.batch().build()) {
        assertThatThrownBy(() -> batch.newEdge(tags.get(0), "NoSuchEdge", tags.get(1)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Edge type 'NoSuchEdge' not found");
        assertThatThrownBy(() -> batch.newEdge(tags.get(0), "Tag", tags.get(1)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Edge type 'Tag' not found");
      }
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
      // A pattern hop consumed lazily (a while: traversal cut by LIMIT) keeps the incoming side: the choice is taken when
      // the hop is called, and the iterable it returns holds it
      assertThat(rows("sql",
          "MATCH {type: Tag, where: (name = 't3')}.in('TAGGED_WITH'){as: q, while: ($depth < 1)} RETURN q LIMIT 3"))
          .isEqualTo(3);
      // An in() in a MATCH where: condition is an expression, not a hop: it answers as the function does anywhere else
      assertThat(sum("sql",
          "SELECT count(*) AS n FROM (MATCH {type: Tag, as: t, where: (name = 't3' AND in('TAGGED_WITH').size() > 0)} RETURN t)"))
          .isZero();
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
    void aChangeLogPastItsCapIsDroppedAndTheScanTakenAgain() {
      createSchema(false);
      final List<RID> tags = createTags();
      final int writes = UnidirectionalEdgeChanges.MAX_CHANGES + 10;
      database.transaction(() -> {
        final String count = "SELECT count(*) AS n FROM (MATCH {type: Tag, where: (name = 't3')}.in('TAGGED_WITH'){as: q} RETURN q)";
        assertThat(sum("sql", count)).isZero();
        final MutableVertex source = database.newVertex("Question").set("qid", 0).save();
        for (int i = 0; i < writes; i++)
          source.newEdge("TAGGED_WITH", tags.get(3));
        final UnidirectionalEdgeChanges changes = ((DatabaseInternal) database).getTransaction().getUnidirectionalEdgeChangesIfAny();
        assertThat(changes.getOverflows()).as("the log overflowed and was dropped").isEqualTo(1);
        assertThat(changes.size()).isLessThanOrEqualTo(UnidirectionalEdgeChanges.MAX_CHANGES);
        assertThat(sum("sql", count)).as("the next read scans again and sees every edge").isEqualTo(writes);
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
    void anotherThreadWritingDuringAReadDoesNotMakeItScanAgain() throws Exception {
      createSchema(false);
      loadWithNewEdge();
      final RID tag = database.query("sql", "SELECT FROM Tag WHERE name = 't3'").next().getIdentity().get();
      final long scansBefore = IncomingEdgeLookup.getScansTaken();
      long rows = 0;
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (t:Tag) MATCH (t)<-[:TAGGED_WITH]-(q) RETURN t.name AS tag, q.qid AS qid")) {
        while (rs.hasNext()) {
          rs.next();
          if (++rows == 1) {
            // A writer on another thread commits more edges of the type while the read is running
            final Thread writer = new Thread(() -> database.transaction(() -> {
              for (int i = 0; i < 10; i++)
                database.newVertex("Question").set("qid", 10_000 + i).save().newEdge("TAGGED_WITH", tag);
            }));
            writer.start();
            writer.join();
          }
        }
      }
      assertThat(IncomingEdgeLookup.getScansTaken() - scansBefore).as("other threads' writes never invalidate the scan")
          .isEqualTo(1);
      assertThat(rows).isGreaterThanOrEqualTo(EXPECTED);
    }

    @Test
    void aHeapCapHitOnTheSecondTypeOfAWalkFailsCleanly() {
      createSchema(false);
      database.getSchema().buildEdgeType().withName("FOLLOWS_ONE_WAY").withBidirectional(false).create();
      loadWithNewEdge();
      database.transaction(() -> {
        final Vertex t3 = database.query("sql", "SELECT FROM Tag WHERE name = 't3'").next().getVertex().get();
        for (int i = 0; i < 5; i++)
          database.newVertex("Question").set("qid", -100 - i).save().newEdge("FOLLOWS_ONE_WAY", t3);
      });
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 100L);
      try {
        // FOLLOWS_ONE_WAY (5 edges) fits, TAGGED_WITH (800) does not: the whole walk fails, loudly
        assertThatThrownBy(() -> sum("opencypher",
            "MATCH (t:Tag) WHERE t.name = 't3' MATCH (t)<-[:FOLLOWS_ONE_WAY|TAGGED_WITH]-(q) RETURN count(q) AS n"))
            .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, -1L);
      }
      // And the next query, with no cap, answers both types
      assertThat(sum("opencypher",
          "MATCH (t:Tag) WHERE t.name = 't3' MATCH (t)<-[:FOLLOWS_ONE_WAY|TAGGED_WITH]-(q) RETURN count(q) AS n"))
          .isEqualTo(tagDegree("t3") + 5);
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

  /**
   * Issue #8629: an edge type declared UNIDIRECTIONAL stores only the outgoing pointer, and #8625 made the query
   * languages answer its incoming side. The graph algorithms, the path-finding functions and the path and refactor
   * procedures still read the incoming adjacency straight off the vertices, so over such a type they saw half of an
   * undirected graph, nothing at all when walking IN, and the refactor procedures dropped the incoming edges of the node
   * they absorbed or cloned.
   * <p>
   * An algorithm is a question about the graph, not about what a vertex stores, so the same graph must give the same
   * answer whichever way its edge type is stored. Every check runs the same statement against two databases holding the
   * same graph - built in the same order, so even the RIDs match - whose edge type is bidirectional in one and
   * unidirectional in the other, and compares the answers by the vertex names. A Graph Analytical View already answered
   * the incoming side (its CSR is built from the outgoing lists), so it must agree with both.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class UnidirectionalGraphAlgorithmsTest extends TestHelper {
    private Database uni;

    @Override
    protected void beginTest() {
      buildGraph(database, true);
      uni = createDatabase(getDatabasePath() + "_uni");
      buildGraph(uni, false);
    }

    @Override
    protected void endTest() {
      if (uni != null) {
        uni.drop();
        uni = null;
      }
    }

    @Test
    void sqlPathFindingFunctions() {
      for (final String direction : new String[] { "IN", "BOTH" }) {
        assertSameFromTo("SELECT dijkstra(?, ?, 'w', '" + direction + "') AS p", "n5", "n0");
        assertSameFromTo("SELECT astar(?, ?, 'w', {direction: '" + direction + "'}) AS p", "n5", "n0");
        assertSameFromTo("SELECT bellmanFord(?, ?, 'w', '" + direction + "') AS p", "n5", "n0");
        assertSameFromTo("SELECT cchShortestPath(?, ?, 'w', '" + direction + "') AS p", "n5", "n0");
        assertSameFromTo("SELECT duanSSSP(?, ?, 'w', '" + direction + "') AS p", "n5", "n0");
      }
      // the backward half of a bidirectional search reads IN from the target even when the walk follows the edges forward
      assertSameFromTo("SELECT cchShortestPath(?, ?, 'w', 'OUT') AS p", "n0", "n5");
    }

    @Test
    void cypherPathFindingProcedures() {
      final String ends = "MATCH (a:N {name: 'n5'}), (b:N {name: 'n0'}) ";
      for (final String direction : new String[] { "IN", "BOTH" }) {
        assertSame("opencypher", ends + "CALL algo.dijkstra(a, b, 'R', 'w', '" + direction + "') YIELD path, weight RETURN path, weight",
            true);
        assertSame("opencypher", "MATCH (a:N {name: 'n5'}) CALL algo.dijkstra.singleSource(a, 'R', 'w', '" + direction
            + "') YIELD node, cost RETURN node, cost", true);
      }
      assertSame("opencypher", ends + "CALL algo.astar(a, b, 'R', 'w') YIELD path, weight RETURN path, weight", true);
      assertSame("opencypher", ends + "CALL algo.allSimplePaths(a, b, 'R', 6) YIELD path RETURN path", true);
    }

    @Test
    void wholeGraphAlgorithms() {
      assertSame("opencypher", "CALL algo.degree(null, 'BOTH') YIELD node, inDegree, outDegree, degree RETURN node, inDegree, outDegree, degree",
          true);
      assertSame("opencypher", "CALL algo.degree(null, 'IN') YIELD node, inDegree, degree RETURN node, inDegree, degree", true);
      assertSame("opencypher", "CALL algo.triangleCount('R') YIELD node, triangles RETURN node, triangles", true);
      assertSame("opencypher", "CALL algo.pageRank({direction: 'BOTH'}) YIELD node, score RETURN node, score", true);
      assertSame("opencypher", "CALL algo.pageRank({direction: 'IN'}) YIELD node, score RETURN node, score", true);
      assertSame("opencypher", "CALL algo.pageRank({direction: 'BOTH', weightProperty: 'w'}) YIELD node, score RETURN node, score", true);
      assertSame("opencypher", "CALL algo.kcore('R') YIELD node, coreNumber RETURN node, coreNumber", true);
      assertSame("opencypher", "CALL algo.closeness('R', 'BOTH') YIELD node, score RETURN node, score", true);

      assertSamePartition("CALL algo.wcc('R') YIELD node, componentId RETURN node.name AS name, componentId AS g", 2);
      assertSamePartition("CALL algo.scc('R') YIELD node, componentId RETURN node.name AS name, componentId AS g", 10);
      assertSamePartition("CALL algo.labelPropagation() YIELD node, communityId RETURN node.name AS name, communityId AS g", 0);
      assertSamePartition("CALL algo.louvain() YIELD node, communityId RETURN node.name AS name, communityId AS g", 0);
    }

    @Test
    void pathProcedures() {
      final String start = "MATCH (a:N {name: 'n5'}) ";
      assertSame("opencypher", start + "CALL path.expand(a, 'R', null, 1, 3) YIELD path RETURN path", true);
      assertSame("opencypher", start + "CALL path.expandConfig(a, {relationshipFilter: 'R', maxLevel: 3}) YIELD path RETURN path", true);
      assertSame("opencypher", start + "CALL path.expandConfig(a, {relationshipFilter: 'R', maxLevel: 3, bfs: false}) YIELD path RETURN path",
          true);
      assertSame("opencypher", start + "CALL path.subgraphNodes(a, {relationshipFilter: 'R'}) YIELD node RETURN node", true);
      assertSame("opencypher", start + "CALL path.subgraphAll(a, {relationshipFilter: 'R'}) YIELD nodes, relationships "
          + "RETURN size(nodes) AS nodes, size(relationships) AS relationships", true);
      // a spanning tree is any tree over the reachable vertices: which edges it takes depends on the order the edges are met
      assertSame("opencypher", start + "CALL path.spanningTree(a, {relationshipFilter: 'R'}) YIELD path "
          + "RETURN count(path) AS paths", true);
    }

    @Test
    void nodeFunctions() {
      assertSame("opencypher", "MATCH (n:N) RETURN n, node.degree(n, 'R', 'IN') AS d", true);
      assertSame("opencypher", "MATCH (n:N) RETURN n, node.degree(n, 'R') AS d", true);
      assertSame("opencypher", "MATCH (n:N) RETURN n, node.degree.in(n, 'R') AS d", true);
      assertSame("opencypher", "MATCH (n:N) RETURN n, node.relationship.exists(n, 'R', 'IN') AS d", true);
      assertSame("opencypher", "MATCH (n:N) RETURN n, node.relationship.types(n, 'IN') AS d", true);
    }

    @Test
    void refactorMergeNodesRewiresIncomingEdges() {
      final String query = "MATCH (a:N {name: 'n1'}), (b:N {name: 'n3'}) CALL refactor.mergeNodes([a, b]) YIELD node RETURN node";
      for (final Database db : new Database[] { database, uni })
        db.transaction(() -> db.command("opencypher", query).close());
      assertSameAfterWrite();
    }

    @Test
    void refactorCloneNodesKeepsIncomingEdges() {
      final String query = "MATCH (a:N {name: 'n3'}) CALL refactor.cloneNodesWithRelationships([a]) YIELD output "
          + "SET output.name = 'clone' RETURN output";
      for (final Database db : new Database[] { database, uni })
        db.transaction(() -> db.command("opencypher", query).close());
      assertSameAfterWrite();
      assertThat(rows(uni, "opencypher", "MATCH (x)-[:R]->(c {name: 'clone'}) RETURN count(*) AS n")).containsExactly("{n=3}");
    }

    @Test
    void labelChangeKeepsIncomingEdges() {
      final String query = "MATCH (a:N {name: 'n3'}) SET a:Extra RETURN a";
      for (final Database db : new Database[] { database, uni })
        db.transaction(() -> db.command("opencypher", query).close());
      assertSameAfterWrite();
      assertThat(rows(uni, "opencypher", "MATCH (x)-[:R]->(c {name: 'n3'}) RETURN count(*) AS n")).containsExactly("{n=3}");
    }

    /**
     * A self loop on the node a write moves is both an outgoing edge and one that ends in it, so the edges a unidirectional
     * type completes from the scan meet it twice, as both lists of a bidirectional type do: it must be migrated, cloned and
     * rewired once, not duplicated or lost.
     */
    @Test
    void selfLoopOnTheMovedNodeIsKeptOnce() {
      for (final Database db : new Database[] { database, uni })
        db.transaction(() -> db.command("opencypher", "MATCH (a:N {name: 'n3'}) CREATE (a)-[:R {w: 0.5}]->(a)").close());
      assertSameAfterWrite();
      // an algorithm reads the self loop as the bidirectional type stores it: once per list it sits in, so twice in a
      // BOTH degree, as a graph degree counts it
      assertSame("opencypher", "CALL algo.degree(null, 'BOTH') YIELD node, inDegree, outDegree, degree "
          + "RETURN node, inDegree, outDegree, degree", true);
      assertSame("opencypher", "CALL algo.triangleCount('R') YIELD node, triangles RETURN node, triangles", true);
      assertSame("opencypher", "CALL algo.wcc() YIELD node, componentId RETURN count(DISTINCT componentId) AS c", true);

      for (final Database db : new Database[] { database, uni })
        db.transaction(() -> db.command("opencypher", "MATCH (a:N {name: 'n3'}) SET a:Extra RETURN a").close());
      assertSameAfterWrite();
      assertThat(rows(uni, "opencypher", "MATCH (a {name: 'n3'})-[:R]->(a) RETURN count(*) AS n")).containsExactly("{n=1}");

      for (final Database db : new Database[] { database, uni })
        db.transaction(() -> db.command("opencypher", "MATCH (a {name: 'n3'}) CALL refactor.cloneNodesWithRelationships([a]) "
            + "YIELD output SET output.name = 'clone' RETURN output").close());
      assertSameAfterWrite();

      // the merge keeps every relationship: the self loop of the absorbed node becomes one of the survivor, and so does
      // each edge joining the two merged nodes
      final long selfLoopsBefore = count(uni, "MATCH (a)-[:R]->(a) RETURN count(*) AS n");
      final long joining = count(uni, "MATCH (a {name: 'n1'})-[:R]-(b {name: 'n3'}) RETURN count(*) AS n");
      assertThat(selfLoopsBefore).isPositive();
      assertThat(joining).isPositive();
      for (final Database db : new Database[] { database, uni })
        db.transaction(() -> db.command("opencypher",
            "MATCH (a:N {name: 'n1'}), (b {name: 'n3'}) CALL refactor.mergeNodes([a, b]) YIELD node RETURN node").close());
      assertSameAfterWrite();
      assertThat(count(uni, "MATCH (a)-[:R]->(a) RETURN count(*) AS n")).isEqualTo(selfLoopsBefore + joining);
    }

    private static long count(final Database db, final String query) {
      try (final ResultSet rs = db.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }

    /**
     * A view over the unidirectional graph agrees with the record path over both graphs. PageRank is left out: its view
     * kernel runs a fixed number of iterations while the record path stops on a tolerance, so the two differ on any graph.
     */
    @Test
    void aViewAgreesWithTheRecordPath() throws InterruptedException {
      final String[] queries = {
          "CALL algo.degree(null, 'BOTH') YIELD node, inDegree, outDegree, degree RETURN node, inDegree, outDegree, degree",
          "CALL algo.triangleCount('R') YIELD node, triangles RETURN node, triangles",
          "MATCH (a:N {name: 'n5'}) CALL algo.dijkstra.singleSource(a, 'R', 'w', 'BOTH') YIELD node, cost RETURN node, cost" };
      final List<List<String>> withoutView = new ArrayList<>();
      for (final String query : queries) {
        withoutView.add(rows(uni, "opencypher", query));
        assertThat(withoutView.getLast()).as(query).isEqualTo(rows(database, "opencypher", query));
      }

      uni.command("sql", "CREATE GRAPH ANALYTICAL VIEW algoView VERTEX TYPES (N) EDGE TYPES (R) PROPERTIES (w) UPDATE MODE OFF");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(uni, "algoView");
      final long deadline = System.currentTimeMillis() + 60_000;
      while (!view.isReady() && System.currentTimeMillis() < deadline)
        Thread.sleep(20);
      assertThat(view.isReady()).isTrue();
      try {
        for (int i = 0; i < queries.length; i++)
          assertThat(rows(uni, "opencypher", queries[i])).as("with a view: %s", queries[i]).isEqualTo(withoutView.get(i));
      } finally {
        uni.command("sql", "DROP GRAPH ANALYTICAL VIEW algoView");
      }
    }

    /**
     * A bulk load that stored the incoming pointers of a unidirectional type: the incoming side is answered once, from the
     * edges that end in the vertex, and the stored pointers are not added on top of it.
     */
    @Test
    void incomingPointersStoredByABulkLoadAreNotCountedTwice() {
      final Database loaded = createDatabase(getDatabasePath() + "_batch");
      try {
        buildGraph(loaded, false, true);
        final String[] queries = {
            "CALL algo.degree(null, 'BOTH') YIELD node, inDegree, outDegree, degree RETURN node, inDegree, outDegree, degree",
            "CALL algo.triangleCount('R') YIELD node, triangles RETURN node, triangles",
            "CALL algo.kcore('R') YIELD node, coreNumber RETURN node, coreNumber",
            "MATCH (a:N {name: 'n5'}) CALL algo.dijkstra.singleSource(a, 'R', 'w', 'BOTH') YIELD node, cost RETURN node, cost",
            "MATCH (a:N {name: 'n5'}) CALL path.expand(a, 'R', null, 1, 2) YIELD path RETURN path",
            "MATCH (a:N {name: 'n5'}) CALL path.subgraphNodes(a, {relationshipFilter: 'R'}) YIELD node RETURN node",
            "MATCH (n:N) RETURN n, node.degree(n, 'R') AS d" };
        for (final String query : queries)
          assertThat(rows(loaded, "opencypher", query)).as(query).isEqualTo(rows(database, "opencypher", query));
      } finally {
        loaded.drop();
      }
    }

    private static void buildGraph(final Database db, final boolean bidirectional) {
      buildGraph(db, bidirectional, false);
    }

    /**
     * Two clusters of five vertices - the first with every edge pointing up, the second down, and one back edge closing a
     * cycle - joined by a bridge, plus an isolated pair. Every weight is a distinct power of two, so every path has a weight
     * of its own and a shortest path is never a tie.
     *
     * @param batchWithIncomingPointers load the edges with a bidirectional graph batch, which stores the incoming pointer of
     *                                  every edge whatever its type declares
     */
    private static void buildGraph(final Database db, final boolean bidirectional, final boolean batchWithIncomingPointers) {
      db.getSchema().createVertexType("N");
      db.getSchema().buildEdgeType().withName("R").withBidirectional(bidirectional).create();
      final List<RID> v = new ArrayList<>();
      db.transaction(() -> {
        for (int i = 0; i < 12; i++)
          v.add(db.newVertex("N").set("name", "n" + i).save().getIdentity());
      });
      final List<int[]> edges = new ArrayList<>();
      for (int i = 0; i < 5; i++)
        for (int j = i + 1; j < 5; j++)
          edges.add(new int[] { i, j });
      edges.add(new int[] { 2, 0 });
      for (int i = 5; i < 10; i++)
        for (int j = i + 1; j < 10; j++)
          edges.add(new int[] { j, i });
      edges.add(new int[] { 4, 5 });
      edges.add(new int[] { 10, 11 });

      if (batchWithIncomingPointers) {
        try (final GraphBatch batch = db.batch().withBidirectional(true).build()) {
          double weight = 1;
          for (final int[] edge : edges) {
            batch.newEdge(v.get(edge[0]), "R", v.get(edge[1]), "w", weight);
            weight *= 2;
          }
        }
        return;
      }
      db.transaction(() -> {
        double weight = 1;
        for (final int[] edge : edges) {
          v.get(edge[0]).asVertex().modify().newEdge("R", v.get(edge[1]), "w", weight);
          weight *= 2;
        }
      });
    }

    /** The statement answers the same on both graphs, and (when asked) something rather than nothing. */
    private void assertSame(final String language, final String query, final boolean notEmpty) {
      final List<String> expected = rows(database, language, query);
      if (notEmpty)
        assertThat(expected).as("bidirectional: %s", query).isNotEmpty().anyMatch(r -> !r.contains("=null") && !r.contains("=[]"));
      assertThat(rows(uni, language, query)).as(query).isEqualTo(expected);
    }

    /** A path function from one named vertex to another: the same path on both graphs, and a path rather than none. */
    private void assertSameFromTo(final String query, final String from, final String to) {
      final List<String> expected = rows(database, "sql", query, vertex(database, from), vertex(database, to));
      assertThat(expected).as("bidirectional: %s", query).hasSize(1).noneMatch(r -> r.contains("=null") || r.contains("=[]"));
      assertThat(rows(uni, "sql", query, vertex(uni, from), vertex(uni, to))).as(query).isEqualTo(expected);
    }

    private static RID vertex(final Database db, final String name) {
      try (final ResultSet rs = db.query("sql", "SELECT FROM N WHERE name = ?", name)) {
        return rs.next().getIdentity().get();
      }
    }

    /** The same grouping of the vertices, whatever ids the groups got; {@code groups} > 0 also checks how many there are. */
    private void assertSamePartition(final String query, final int groups) {
      final List<String> expected = partition(database, query);
      if (groups > 0)
        assertThat(expected).as("bidirectional: %s", query).hasSize(groups);
      assertThat(partition(uni, query)).as(query).isEqualTo(expected);
    }

    /** The whole graph, edges included, after a write. */
    private void assertSameAfterWrite() {
      final String edges = "MATCH (a)-[r:R]->(b) RETURN a.name AS a, b.name AS b, r.w AS w";
      assertThat(rows(uni, "opencypher", edges)).isEqualTo(rows(database, "opencypher", edges));
      final String perVertex = "MATCH (n) RETURN n.name AS n, labels(n) AS l, node.degree(n, 'R', 'IN') AS i, node.degree(n, 'R', 'OUT') AS o";
      assertThat(rows(uni, "opencypher", perVertex)).isEqualTo(rows(database, "opencypher", perVertex));
      final long stored = ((Number) uni.query("sql", "SELECT count(*) AS n FROM R").next().getProperty("n")).longValue();
      assertThat(rows(uni, "opencypher", "MATCH ()-[r:R]->() RETURN count(r) AS n")).containsExactly("{n=" + stored + "}");
    }

    private static List<String> partition(final Database db, final String query) {
      final Map<String, TreeSet<String>> groups = new HashMap<>();
      try (final ResultSet rs = db.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          groups.computeIfAbsent(String.valueOf((Object) row.getProperty("g")), k -> new TreeSet<>()).add(row.getProperty("name"));
        }
      }
      final List<String> result = new ArrayList<>();
      for (final TreeSet<String> members : groups.values())
        result.add(members.toString());
      result.sort(null);
      return result;
    }

    /** The rows of a statement, each one canonical (vertices by name, edges by their ends, numbers rounded), sorted. */
    private static List<String> rows(final Database db, final String language, final String query, final Object... args) {
      final List<String> result = new ArrayList<>();
      try (final ResultSet rs = db.query(language, query, args)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final Map<String, Object> canonical = new TreeMap<>();
          for (final String name : row.getPropertyNames())
            canonical.put(name, canonical(db, row.getProperty(name)));
          result.add(canonical.toString());
        }
      }
      result.sort(null);
      return result;
    }

    private static Object canonical(final Database db, final Object value) {
      if (value instanceof Edge edge)
        return "(" + name(db, edge.getOut()) + ")-[" + edge.getTypeName() + " " + edge.get("w") + "]->(" + name(db, edge.getIn()) + ")";
      if (value instanceof Document document && document.has("name"))
        return document.getString("name");
      if (value instanceof RID rid)
        return name(db, rid);
      if (value instanceof Double d)
        return String.format(Locale.ROOT, "%.6f", d);
      if (value instanceof Float f)
        return String.format(Locale.ROOT, "%.6f", f.doubleValue());
      if (value instanceof Map<?, ?> map) {
        final Map<String, Object> result = new TreeMap<>();
        for (final Map.Entry<?, ?> entry : map.entrySet())
          result.put(String.valueOf(entry.getKey()), canonical(db, entry.getValue()));
        return result;
      }
      if (value instanceof Collection<?> collection) {
        final List<Object> result = new ArrayList<>(collection.size());
        for (final Object element : collection)
          result.add(canonical(db, element));
        return result;
      }
      return value;
    }

    private static String name(final Database db, final Identifiable identifiable) {
      return db.lookupByRID(identifiable.getIdentity(), true).asDocument().getString("name");
    }
  }

  /**
   * Issue #8676: deleting the TARGET vertex of a unidirectional edge walked only the target's own lists, which hold no
   * trace of the edge, so the edge record survived and the source kept a pointer to a deleted vertex.
   *
   * The tests that count scans read JVM-wide counters, so they assume the test classes of a JVM run one after the other.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DeleteTargetOfUnidirectionalEdgeTest extends TestHelper {
    private RID a;
    private RID b;

    @Test
    void cypherDetachDeleteOfTheTargetRemovesTheEdge() {
      createSchema(false);
      createPair("U8");
      database.transaction(() -> database.command("opencypher", "MATCH (b:V8 {n: 'b'}) DETACH DELETE b").close());
      assertEdgeGone("U8");
    }

    @Test
    void vertexDeleteOfTheTargetRemovesTheEdge() {
      createSchema(false);
      createPair("U8");
      database.transaction(() -> b.asVertex().delete());
      assertEdgeGone("U8");
    }

    @Test
    void sqlDeleteVertexOfTheTargetRemovesTheEdge() {
      createSchema(false);
      createPair("U8");
      database.transaction(() -> database.command("sql", "DELETE VERTEX FROM V8 WHERE n = 'b'").close());
      assertEdgeGone("U8");
    }

    @Test
    void lightweightUnidirectionalEdgeIsRemovedToo() {
      createSchema(true);
      createPair("U8");
      assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "U8")).isEqualTo(1);
      database.transaction(() -> b.asVertex().delete());
      assertEdgeGone("U8");
    }

    @Test
    void plainCypherDeleteOfAConnectedTargetIsRefused() {
      createSchema(false);
      createPair("U8");
      assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "MATCH (b:V8 {n: 'b'}) DELETE b").close()))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("still has relationships");
      assertThat(database.existsRecord(b)).isTrue();
      assertThat(database.countType("U8", false)).isEqualTo(1);
    }

    @Test
    void edgeCreatedInTheSameTransactionIsRemoved() {
      createSchema(false);
      database.transaction(() -> {
        a = database.newVertex("V8").set("n", "a").save().getIdentity();
        b = database.newVertex("V8").set("n", "b").save().getIdentity();
        a.asVertex().newEdge("U8", b);
        b.asVertex().delete();
      });
      assertEdgeGone("U8");
    }

    @Test
    void manyTargetsInOneTransactionShareOneScan() {
      createSchema(false);
      final int n = 50;
      final RID[] targets = new RID[n];
      database.transaction(() -> {
        a = database.newVertex("V8").set("n", "a").save().getIdentity();
        for (int i = 0; i < n; i++) {
          targets[i] = database.newVertex("V8").set("n", "t" + i).save().getIdentity();
          a.asVertex().newEdge("U8", targets[i]);
        }
      });
      final long before = IncomingEdgeLookup.getScansTaken();
      database.transaction(() -> {
        for (final RID target : targets)
          target.asVertex().delete();
      });
      assertThat(IncomingEdgeLookup.getScansTaken() - before).isEqualTo(1L);
      assertThat(database.countType("U8", false)).isZero();
      assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "U8")).isZero();
    }

    @Test
    void typeTooLargeToIndexInHeapIsScannedForTheVertexAlone() {
      createSchema(false);
      createPair("U8");
      // A CAP OF ONE ELEMENT IS EXCEEDED BY THE SECOND EDGE OF THE TYPE
      database.transaction(() -> database.newVertex("V8").set("n", "c").save().asVertex().newEdge("U8", b));
      final long cap = GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();
      final long fallbacksBefore = IncomingEdgeLookup.getFallbackScans();
      try {
        GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(1L);
        database.transaction(() -> b.asVertex().delete());
        assertThat(IncomingEdgeLookup.getFallbackScans() - fallbacksBefore).as("the streaming scan answered").isEqualTo(1L);
      } finally {
        GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(cap);
      }
      assertThat(database.countType("U8", false)).isZero();
    }

    @Test
    void aTypeTooLargeToIndexIsNotTriedAgainByTheNextDeletesOfTheTransaction() {
      createSchema(false);
      final int n = 5;
      final RID[] targets = new RID[n];
      database.transaction(() -> {
        a = database.newVertex("V8").set("n", "a").save().getIdentity();
        for (int i = 0; i < n; i++) {
          targets[i] = database.newVertex("V8").set("n", "t" + i).save().getIdentity();
          a.asVertex().newEdge("U8", targets[i]);
        }
      });
      final long cap = GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();
      final long scansBefore = IncomingEdgeLookup.getScansTaken();
      final long fallbacksBefore = IncomingEdgeLookup.getFallbackScans();
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(1L);
      try {
        database.transaction(() -> {
          for (final RID target : targets)
            target.asVertex().delete();
        });
      } finally {
        GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(cap);
      }
      assertThat(IncomingEdgeLookup.getScansTaken() - scansBefore).as("one failed attempt to index, then none").isEqualTo(1L);
      assertThat(IncomingEdgeLookup.getFallbackScans() - fallbacksBefore).isEqualTo(n);
      assertThat(database.countType("U8", false)).isZero();
    }

    @Test
    void cypherDeleteDegradesToAScanWhenTheTypeIsTooLargeToIndex() {
      createSchema(false);
      createPair("U8");
      database.transaction(() -> database.newVertex("V8").set("n", "c").save().asVertex().newEdge("U8", b));
      final long cap = GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(1L);
      try {
        assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "MATCH (b:V8 {n: 'b'}) DELETE b").close()))
            .isInstanceOf(CommandExecutionException.class).hasMessageContaining("still has relationships");
        database.transaction(() -> database.command("opencypher", "MATCH (b:V8 {n: 'b'}) DETACH DELETE b").close());
      } finally {
        GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(cap);
      }
      assertThat(database.countType("U8", false)).isZero();
    }

    @Test
    void cypherDetachDeleteCountsTheIncomingEdgeOfAUnidirectionalType() {
      createSchema(false);
      createPair("U8");
      database.transaction(() -> {
        final ResultSet rs = database.command("opencypher", "MATCH (b:V8 {n: 'b'}) DETACH DELETE b");
        while (rs.hasNext())
          rs.next();
        final QueryStatistics stats = rs.getStatistics().orElseThrow();
        assertThat(stats.getNodesDeleted()).isEqualTo(1);
        assertThat(stats.getRelationshipsDeleted()).isEqualTo(1);
      });
      assertEdgeGone("U8");
    }

    @Test
    void aRegularAndALightweightUnidirectionalTypeAreBothCleaned() {
      createSchema(false);
      database.getSchema().buildEdgeType().withName("L8").withBidirectional(false).withLightweight(true).create();
      createPair("U8");
      database.transaction(() -> a.asVertex().newEdge("L8", b));
      assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "L8")).isEqualTo(1);

      database.transaction(() -> b.asVertex().delete());
      assertEdgeGone("U8");
      assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "L8")).isZero();
    }

    @Test
    void anEdgeDeletedEarlierInTheTransactionIsNotDeletedAgain() {
      createSchema(false);
      final RID[] c = new RID[1];
      createPair("U8");
      database.transaction(() -> {
        c[0] = database.newVertex("V8").set("n", "c").save().getIdentity();
        a.asVertex().newEdge("U8", c[0]);
      });
      database.transaction(() -> {
        // THE FIRST DELETE TAKES THE SCAN, THEN THE EDGE INTO c IS DELETED BY HAND, THEN c ITSELF
        b.asVertex().delete();
        a.asVertex().getEdges(Vertex.DIRECTION.OUT, "U8").forEach(Edge::delete);
        c[0].asVertex().delete();
      });
      assertThat(database.countType("U8", false)).isZero();
    }

    @Test
    void theIncomingEdgesAreFoundOutsideATransactionToo() {
      createSchema(false);
      createPair("U8");
      assertThat(database.isTransactionActive()).isFalse();
      assertThat(IncomingEdgeLookup.getIncomingUnidirectionalEdges((DatabaseInternal) database, b)).hasSize(1);
    }

    @Test
    void movingTheTargetKeepsItsIncomingUnidirectionalEdges() {
      createSchema(false);
      database.getSchema().createVertexType("V8Other");
      createPair("U8");
      database.transaction(() -> a.asVertex().newEdge("U8", b).set("w", 7).save());

      database.transaction(() -> database.command("sql", "MOVE VERTEX " + b + " TO TYPE:V8Other").close());
      final RID[] moved = new RID[] { database.iterateType("V8Other", false).next().getIdentity() };

      assertThat(database.countType("U8", false)).isEqualTo(2);
      assertThat(a.asVertex().getVertices(Vertex.DIRECTION.OUT, "U8")).extracting(v -> v.getIdentity()).containsOnly(moved[0]);
      assertThat(IncomingEdgeLookup.getIncomingUnidirectionalEdges((DatabaseInternal) database, moved[0])).hasSize(2);
    }

    @Test
    void movingTheTargetKeepsItsIncomingLightweightUnidirectionalEdges() {
      createSchema(false);
      database.getSchema().createVertexType("V8Other");
      database.getSchema().buildEdgeType().withName("L8").withBidirectional(false).withLightweight(true).create();
      createPair("U8");
      database.transaction(() -> a.asVertex().newEdge("L8", b));

      database.transaction(() -> database.command("sql", "MOVE VERTEX " + b + " TO TYPE:V8Other").close());
      final RID moved = database.iterateType("V8Other", false).next().getIdentity();

      assertThat(a.asVertex().getVertices(Vertex.DIRECTION.OUT, "L8")).extracting(v -> v.getIdentity()).containsOnly(moved);
      assertThat(a.asVertex().getVertices(Vertex.DIRECTION.OUT, "U8")).extracting(v -> v.getIdentity()).containsOnly(moved);
    }

    @Test
    void otherEdgesOfTheSourceSurvive() {
      createSchema(false);
      createPair("U8");
      final RID[] c = new RID[1];
      database.transaction(() -> {
        c[0] = database.newVertex("V8").set("n", "c").save().getIdentity();
        a.asVertex().newEdge("U8", c[0]);
      });
      database.transaction(() -> b.asVertex().delete());
      assertThat(database.countType("U8", false)).isEqualTo(1);
      assertThat(a.asVertex().getVertices(Vertex.DIRECTION.OUT, "U8")).extracting(v -> v.getIdentity()).containsExactly(c[0]);
    }

    @Test
    void selfLoopOfAUnidirectionalTypeIsRemoved() {
      createSchema(false);
      database.transaction(() -> {
        a = database.newVertex("V8").set("n", "a").save().getIdentity();
        a.asVertex().newEdge("U8", a);
      });
      database.transaction(() -> a.asVertex().delete());
      assertThat(database.countType("U8", false)).isZero();
    }

    @Test
    void lightweightSelfLoopIsRemoved() {
      createSchema(true);
      database.transaction(() -> {
        a = database.newVertex("V8").set("n", "a").save().getIdentity();
        a.asVertex().newEdge("U8", a);
      });
      assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "U8")).isEqualTo(1);
      database.transaction(() -> a.asVertex().delete());
      assertThat(database.existsRecord(a)).isFalse();
    }

    @Test
    void everySourceOfATargetIsCleaned() {
      createSchema(false);
      createPair("U8");
      final RID[] c = new RID[1];
      database.transaction(() -> {
        c[0] = database.newVertex("V8").set("n", "c").save().getIdentity();
        c[0].asVertex().newEdge("U8", b);
      });
      database.transaction(() -> b.asVertex().delete());
      assertThat(database.countType("U8", false)).isZero();
      assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "U8")).isZero();
      assertThat(c[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "U8")).isZero();
    }

    @Test
    void aRolledBackDeleteLeavesNoStaleScanForTheNextTransaction() {
      createSchema(false);
      createPair("U8");
      database.begin();
      b.asVertex().delete();
      database.rollback();
      assertThat(database.countType("U8", false)).isEqualTo(1);

      database.transaction(() -> b.asVertex().delete());
      assertEdgeGone("U8");
    }

    @Test
    void bidirectionalTypeIsUnchanged() {
      createSchema(false);
      createPair("B8");
      database.transaction(() -> b.asVertex().delete());
      assertEdgeGone("B8");
    }

    private void createSchema(final boolean lightweight) {
      database.getSchema().createVertexType("V8");
      database.getSchema().buildEdgeType().withName("U8").withBidirectional(false).withLightweight(lightweight).create();
      database.getSchema().createEdgeType("B8");
    }

    private void createPair(final String edgeType) {
      database.transaction(() -> {
        a = database.newVertex("V8").set("n", "a").save().getIdentity();
        b = database.newVertex("V8").set("n", "b").save().getIdentity();
        a.asVertex().newEdge(edgeType, b);
      });
    }

    private void assertEdgeGone(final String edgeType) {
      assertThat(database.countType(edgeType, false)).isZero();
      assertThat(a.asVertex().getVertices(Vertex.DIRECTION.OUT, edgeType)).isEmpty();
      assertThat(database.existsRecord(b)).isFalse();
    }
  }

  /**
   * Issue #8986: a unidirectional edge created by a transaction that was still open when its target vertex was deleted
   * (and committed) must not commit: the delete could not find the edge, and the commit had nothing to check the target
   * against, so the edge pointed at a deleted record, and at whatever vertex reused its RID. The same holds in the other
   * order: a delete that scanned for the edges ending in the vertex before another transaction committed one.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class UnidirectionalEdgeToDeletedVertexTest extends TestHelper {
    private RID source;
    private RID target;

    @Override
    public void beginTest() {
      database.getSchema().createVertexType("V");
      database.command("sql", "CREATE EDGE TYPE U UNIDIRECTIONAL");
    }

    private void createVertices() {
      database.transaction(() -> {
        source = database.newVertex("V").set("name", "a").save().getIdentity();
        target = database.newVertex("V").set("name", "b").save().getIdentity();
      });
    }

    @Test
    void edgeCreatedBeforeTheTargetIsDeletedByAnotherTransactionDoesNotCommit() throws Exception {
      createVertices();
      final ExecutorService otherThread = Executors.newSingleThreadExecutor();
      try {
        database.begin();
        database.lookupByRID(source, true).asVertex().newEdge("U", database.lookupByRID(target, true).asVertex());

        otherThread.submit(() -> database.transaction(() -> database.lookupByRID(target, true).asVertex().delete())).get();

        assertThatThrownBy(() -> database.commit()).isInstanceOf(NeedRetryException.class);
      } finally {
        if (database.isTransactionActive())
          database.rollback();
        otherThread.shutdown();
      }

      assertNoDanglingEdge();
    }

    @Test
    void edgeCommittedAfterTheDeleteScannedForItDoesNotLeaveAGhost() throws Exception {
      createVertices();
      final ExecutorService otherThread = Executors.newSingleThreadExecutor();
      final CountDownLatch deleted = new CountDownLatch(1);
      final CountDownLatch edgeCommitted = new CountDownLatch(1);
      try {
        // the deleting transaction scans for the edges ending in the target, then waits for the edge to commit
        final Future<Object> deleter = otherThread.submit(() -> {
          database.begin();
          try {
            database.lookupByRID(target, true).asVertex().delete();
            deleted.countDown();
            assertThat(edgeCommitted.await(30, TimeUnit.SECONDS)).isTrue();
            database.commit();
            return "committed";
          } catch (final NeedRetryException e) {
            return "conflict";
          } finally {
            if (database.isTransactionActive())
              database.rollback();
          }
        });

        assertThat(deleted.await(30, TimeUnit.SECONDS)).isTrue();
        boolean edgeWasCommitted = true;
        try {
          database.transaction(() -> database.lookupByRID(source, true).asVertex()
              .newEdge("U", database.lookupByRID(target, true).asVertex()));
        } catch (final NeedRetryException | RecordNotFoundException e) {
          edgeWasCommitted = false;
        } finally {
          edgeCommitted.countDown();
        }
        final Object outcome = deleter.get(60, TimeUnit.SECONDS);

        if (edgeWasCommitted)
          assertThat(outcome).as("the delete never saw the committed edge, so it must not commit over it").isEqualTo("conflict");
      } finally {
        otherThread.shutdown();
      }

      assertNoDanglingEdge();
    }

    @Test
    void aRetriedDeleteRemovesTheEdgeThatCommittedUnderIt() throws Exception {
      createVertices();
      final ExecutorService otherThread = Executors.newSingleThreadExecutor();
      final CountDownLatch deleted = new CountDownLatch(1);
      final CountDownLatch edgeCommitted = new CountDownLatch(1);
      try {
        final int[] attempts = { 0 };
        final Future<?> deleter = otherThread.submit(() -> database.transaction(() -> {
          database.lookupByRID(target, true).asVertex().delete();
          if (++attempts[0] == 1) {
            deleted.countDown();
            try {
              assertThat(edgeCommitted.await(30, TimeUnit.SECONDS)).isTrue();
            } catch (final InterruptedException e) {
              throw new IllegalStateException(e);
            }
          }
        }, false, 3));

        assertThat(deleted.await(30, TimeUnit.SECONDS)).isTrue();
        database.transaction(() -> database.lookupByRID(source, true).asVertex()
            .newEdge("U", database.lookupByRID(target, true).asVertex()));
        edgeCommitted.countDown();
        deleter.get(60, TimeUnit.SECONDS);

        assertThat(attempts[0]).as("the first attempt must have been refused and run again").isEqualTo(2);
      } finally {
        otherThread.shutdown();
      }

      assertNoDanglingEdge();
      assertThat(database.countType("U", false)).isZero();
    }

    private void assertNoDanglingEdge() {
      try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
        assertThat(rs.next().<Long>getProperty("totalCorruptedRecords")).isZero();
      }
      // every edge left points at a vertex that exists: expanding it must not hit a deleted record
      database.transaction(() -> {
        final Vertex a = database.lookupByRID(source, true).asVertex();
        for (final Edge edge : a.getEdges(Vertex.DIRECTION.OUT, "U"))
          assertThat(database.existsRecord(edge.getIn())).as("the target of " + edge.getIdentity() + " must exist").isTrue();
      });
    }

    /** Vertices and unidirectional edges created in one transaction: the targets are not committed yet, and must not conflict. */
    @Test
    void targetsCreatedInTheSameTransactionCommit() {
      database.transaction(() -> {
        final MutableVertex hub = database.newVertex("V").set("name", "hub").save();
        for (int i = 0; i < 2_000; i++) {
          final MutableVertex v = database.newVertex("V").set("name", "n" + i).save();
          hub.newEdge("U", v);
        }
      });
      assertThat(database.countType("U", false)).isEqualTo(2_000L);
    }

    /** A delete of a vertex in a bucket no unidirectional edge ended in meanwhile is not refused. */
    @Test
    void aDeleteInAnotherBucketIsNotRefusedByAnUnrelatedEdgeCommit() throws Exception {
      database.getSchema().createVertexType("W");
      createVertices();
      final RID[] other = new RID[1];
      database.transaction(() -> other[0] = database.newVertex("W").set("name", "w").save().getIdentity());
      final ExecutorService otherThread = Executors.newSingleThreadExecutor();
      final CountDownLatch deleted = new CountDownLatch(1);
      final CountDownLatch edgeCommitted = new CountDownLatch(1);
      try {
        final Future<Object> deleter = otherThread.submit(() -> {
          database.begin();
          try {
            database.lookupByRID(other[0], true).asVertex().delete();
            deleted.countDown();
            assertThat(edgeCommitted.await(30, TimeUnit.SECONDS)).isTrue();
            database.commit();
            return "committed";
          } finally {
            if (database.isTransactionActive())
              database.rollback();
          }
        });
        assertThat(deleted.await(30, TimeUnit.SECONDS)).isTrue();
        database.transaction(() -> database.lookupByRID(source, true).asVertex()
            .newEdge("U", database.lookupByRID(target, true).asVertex()));
        edgeCommitted.countDown();
        assertThat(deleter.get(60, TimeUnit.SECONDS)).isEqualTo("committed");
      } finally {
        otherThread.shutdown();
      }
    }
  }
}
