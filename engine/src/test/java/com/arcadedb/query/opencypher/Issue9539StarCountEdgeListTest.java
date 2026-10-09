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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.graph.EdgeBucketMask;
import com.arcadedb.graph.EdgeLinkedList;
import com.arcadedb.graph.EdgeSegment;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.StripeDirectory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9539: since #9484 the out-of-view star count ({@code COUNT STAR JOIN}) reads every arm off the edge lists of
 * the central vertices, which is right for light edges but was 2.3x slower than the edge-record scan it replaced: each
 * arm walked the list again and looked up every neighbor to check the far-end label. The count now walks each edge
 * list once for all the arms leaving in its direction and checks the label on the bucket the entry carries.
 * <p>
 * Every count here is checked against the ordinary row pipeline, reached through a {@code WITH *} that the count
 * push-downs decline, so the push-down is compared with the pipeline it stands in for, not with a number
 * worked out by hand.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9539StarCountEdgeListTest extends TestHelper {
  private static final String STAR_PLAN = "COUNT STAR JOIN";

  private static final String Q4 = "MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person), "
      + "(message)<-[:LIKES]-(liker:Person), (message)<-[:REPLY_OF]-(comment:Message)";
  private static final String Q7 = "MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person) "
      + "OPTIONAL MATCH (message)<-[:LIKES]-(liker:Person) OPTIONAL MATCH (message)<-[:REPLY_OF]-(comment:Message)";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Tag");
    database.command("sql", "CREATE VERTEX TYPE SubTag EXTENDS Tag");
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE VERTEX TYPE Forum");
    database.command("sql", "CREATE VERTEX TYPE Message");
    database.command("sql", "CREATE VERTEX TYPE Comment EXTENDS Message");
    database.command("sql", "CREATE EDGE TYPE HAS_TAG");
    database.command("sql", "CREATE EDGE TYPE HAS_CREATOR");
    database.command("sql", "CREATE EDGE TYPE LIKES LIGHTWEIGHT");
    database.command("sql", "CREATE EDGE TYPE REPLY_OF");
    database.command("sql", "CREATE EDGE TYPE KNOWS");
  }

  @Test
  void lsqbShapesMatchThePipeline() {
    buildSocialGraph(400);
    assertStarMatchesPipeline(Q4);
    assertStarMatchesPipeline(Q7);
  }

  /**
   * The cost the issue is about, measured in record lookups rather than in time: the count reads the central vertices
   * and their edge lists, and nothing proportional to the number of edges. Before the fix every arm with a far-end label
   * looked up each neighbor, so the lookups grew with the edges (over 4,000 here) instead of with the vertices.
   */
  @Test
  void theCountLooksUpNoNeighbor() {
    final int messages = 200;
    final int edgesPerArm = 5;
    database.transaction(() -> {
      final List<Vertex> tags = new ArrayList<>();
      final List<Vertex> persons = new ArrayList<>();
      for (int i = 0; i < 20; i++) {
        tags.add(database.newVertex("Tag").save());
        persons.add(database.newVertex("Person").save());
      }
      final List<MutableVertex> created = new ArrayList<>();
      for (int i = 0; i < messages; i++)
        created.add(database.newVertex("Message").save());
      for (int i = 0; i < messages; i++) {
        final MutableVertex m = created.get(i);
        for (int k = 0; k < edgesPerArm; k++) {
          m.newEdge("HAS_TAG", tags.get((i + k) % tags.size()));
          m.newEdge("HAS_CREATOR", persons.get((i + k) % persons.size()));
          persons.get((i * 3 + k) % persons.size()).asVertex().modify().newLightEdge("LIKES", m);
          created.get((i + k + 1) % messages).newEdge("REPLY_OF", m);
        }
      }
    });

    final long expected = (long) messages * edgesPerArm * edgesPerArm * edgesPerArm * edgesPerArm;
    assertThat(starPlan(Q4)).contains(STAR_PLAN);

    final long readsBefore = readRecordStat();
    assertThat(countMatch(Q4)).isEqualTo(expected);
    final long reads = readRecordStat() - readsBefore;

    // 4,000 edges hang off the central vertices: a count that looks any of them up cannot stay under this
    final long edges = (long) messages * 4 * edgesPerArm;
    assertThat(reads).as("records read by the star count, for %s central vertices and %s edges", messages, edges)
        .isLessThan(edges / 2);
    assertThat(count(pipelineQuery(Q4))).isEqualTo(expected);
  }

  @Test
  void farEndLabelsFilterOnTheEntryBucket() {
    // HAS_TAG reaches a Tag, a SubTag (a Tag too) and a Forum (not a Tag); LIKES comes from a Person and from a Forum
    database.transaction(() -> {
      for (int i = 0; i < 6; i++) {
        final MutableVertex m = database.newVertex(i % 2 == 0 ? "Message" : "Comment").save();
        m.newEdge("HAS_TAG", database.newVertex("Tag").save());
        m.newEdge("HAS_TAG", database.newVertex("SubTag").save());
        m.newEdge("HAS_TAG", database.newVertex("Forum").save());
        m.newEdge("HAS_CREATOR", database.newVertex("Person").save());
        database.newVertex("Person").save().newLightEdge("LIKES", m);
        database.newVertex("Forum").save().newLightEdge("LIKES", m);
        database.newVertex("Message").save().newEdge("REPLY_OF", m);
      }
    });
    assertThat(countMatch(Q4)).isEqualTo(6 * 2L);
    assertStarMatchesPipeline(Q4);
    assertStarMatchesPipeline("MATCH (:SubTag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person), "
        + "(message)<-[:LIKES]-(liker:Forum)");
    assertStarMatchesPipeline("MATCH (:Tag)<-[:HAS_TAG]-(message:Comment)-[:HAS_CREATOR]->(creator:Person), "
        + "(message)<-[:LIKES]-(liker)");
    // a far-end label no vertex type carries reaches nothing
    assertThat(countMatch("MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person), "
        + "(message)<-[:LIKES]-(liker:Nobody)")).isZero();
  }

  @Test
  void undirectedAndMultiHopArmsMatchThePipeline() {
    buildSocialGraph(150);
    // a BOTH arm reads the OUT and the IN list of the central vertex
    assertStarMatchesPipeline("MATCH (other:Message)-[:REPLY_OF]-(message:Message)-[:HAS_CREATOR]->(creator:Person), "
        + "(message)-[:HAS_TAG]->(:Tag)");
    assertStarMatchesPipeline("MATCH (other:Message)-[:REPLY_OF]-(message:Message)-[:HAS_CREATOR]->(creator:Person) "
        + "OPTIONAL MATCH (message)<-[:LIKES]-(liker:Person)");
    // a multi-hop arm: its intermediate label is checked before the vertex is looked up, its last hop on raw entries
    assertStarMatchesPipeline("MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person)-[:KNOWS]->(friend:Person), "
        + "(message)<-[:LIKES]-(liker:Person)");
    assertStarMatchesPipeline("MATCH (:Tag)<-[:HAS_TAG]-(message:Message)<-[:REPLY_OF]-(comment:Message)-[:HAS_CREATOR]->(p:Person), "
        + "(message)<-[:LIKES]-(liker:Person)");
    assertStarMatchesPipeline("MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person) "
        + "OPTIONAL MATCH (message)<-[:LIKES]-(liker:Person)-[:KNOWS]-(friend:Person)");
  }

  /**
   * A self loop sits in both edge lists of its vertex, and an undirected relationship matches it once (as Neo4j does,
   * and as the row pipeline does): an undirected arm adding the OUT and the IN degree counted it twice. Checked with no
   * view and with a view covering the whole pattern, on a single-hop arm and on both kinds of hop of a multi-hop arm.
   */
  @Test
  void aSelfLoopOnAnUndirectedArmCountsOnce() throws InterruptedException {
    database.transaction(() -> {
      final MutableVertex m1 = database.newVertex("Message").save();
      final MutableVertex m2 = database.newVertex("Message").save();
      final MutableVertex p1 = database.newVertex("Person").save();
      final MutableVertex p2 = database.newVertex("Person").save();
      m1.newEdge("REPLY_OF", m1);
      m1.newEdge("REPLY_OF", m2);
      m1.newEdge("HAS_CREATOR", p1);
      m2.newEdge("HAS_CREATOR", p2);
      p1.newEdge("KNOWS", p1);
      p1.newEdge("KNOWS", p2);
      p2.newLightEdge("KNOWS", p2);
      p1.newLightEdge("LIKES", m1);
      p2.newLightEdge("LIKES", m1);
      p2.newLightEdge("LIKES", m2);
    });
    // m1 reaches itself (once) and m2, m2 reaches m1
    final String singleHop = "MATCH (other:Message)-[:REPLY_OF]-(message:Message), (message)-[:HAS_CREATOR]->(creator:Person)";
    // the friends of p1 are p1 (once) and p2, those of p2 are p1 and p2 (once): m1 2 x 2, m2 1 x 2
    final String lastHop = "MATCH (other:Message)-[:REPLY_OF]-(message:Message), "
        + "(message)-[:HAS_CREATOR]->(creator:Person)-[:KNOWS]-(friend:Person)";
    // p1 likes m1 and p2 likes m1 and m2: m1 (1 + 2) x 2, m2 (1 + 2) x 1
    final String middleHop = "MATCH (message:Message)-[:HAS_CREATOR]->(creator:Person)-[:KNOWS]-(friend:Person)-[:LIKES]->(liked:Message), "
        + "(message)-[:REPLY_OF]-(other:Message)";

    assertThat(countMatch(singleHop)).isEqualTo(3L);
    assertThat(countMatch(lastHop)).isEqualTo(6L);
    assertThat(countMatch(middleHop)).isEqualTo(9L);
    for (final String match : new String[] { singleHop, lastHop, middleHop })
      assertStarMatchesPipeline(match);

    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW selfLoops VERTEX TYPES (Message, Person) "
        + "EDGE TYPES (REPLY_OF, HAS_CREATOR, KNOWS) UPDATE MODE OFF");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "selfLoops");
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!view.isReady() && System.currentTimeMillis() < deadline)
      Thread.sleep(20);
    assertThat(view.isReady()).isTrue();
    try {
      assertThat(countMatch(singleHop)).as("with a view").isEqualTo(3L);
      assertThat(countMatch(lastHop)).as("with a view").isEqualTo(6L);
      assertThat(countMatch(middleHop)).as("with a view").isEqualTo(9L);
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW selfLoops");
    }
  }

  /**
   * The edge-list primitive itself, below the operator: several filters answered in one walk of a chain of many
   * segments, mixing record and light edges, two edge types, two far-end labels and self loops.
   */
  @Test
  void countIntoAnswersEveryFilterInOneWalkOfAMultiSegmentChain() {
    database.command("sql", "CREATE VERTEX TYPE X");
    database.command("sql", "CREATE VERTEX TYPE Y");
    database.command("sql", "CREATE EDGE TYPE A");
    database.command("sql", "CREATE EDGE TYPE B");

    final long[] expected = new long[5];
    final RID[] hubRid = new RID[1];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("X").save();
      hubRid[0] = hub.getIdentity();
      for (int i = 0; i < 300; i++) {
        final boolean toX = i % 3 == 0;
        final MutableVertex far = database.newVertex(toX ? "X" : "Y").save();
        final String type = i % 4 == 0 ? "B" : "A";
        if (i % 2 == 0)
          hub.newLightEdge(type, far);
        else
          hub.newEdge(type, far);
        if (type.equals("A")) {
          expected[0]++;
          if (toX)
            expected[1]++;
        } else
          expected[2]++;
      }
      for (int i = 0; i < 3; i++) {
        if (i == 0)
          hub.newLightEdge("A", hub);
        else
          hub.newEdge("A", hub);
        // a self loop of A reaches an X (the hub itself)
        expected[0]++;
        expected[1]++;
      }
      // the same filter as the first, leaving the self loops out
      expected[3] = expected[0] - 3;
      // both types at once
      expected[4] = expected[0] + expected[2];
    });

    database.transaction(() -> {
      final DatabaseInternal internal = (DatabaseInternal) database;
      final VertexInternal hub = (VertexInternal) hubRid[0].asVertex(true);
      final EdgeLinkedList out = internal.getGraphEngine().getEdgeHeadChunk(hub, Vertex.DIRECTION.OUT);
      assertThat(((EdgeSegment) database.lookupByRID(hub.getOutEdgesHeadChunk(), true)).getPreviousRID())
          .as("the OUT list spans several segments").isNotNull();

      final EdgeBucketMask a = EdgeBucketMask.of(internal, new String[] { "A" });
      final EdgeBucketMask b = EdgeBucketMask.of(internal, new String[] { "B" });
      final EdgeBucketMask x = EdgeBucketMask.ofBucketIds(database.getSchema().getType("X").getBucketIds(true).stream()
          .mapToInt(Integer::intValue).toArray());
      final long[] counts = new long[5];
      out.countInto(new EdgeBucketMask[] { a, a, b, a, EdgeBucketMask.of(internal, new String[] { "A", "B" }) },
          new EdgeBucketMask[] { null, x, null, null, null }, new boolean[] { false, false, false, true, false }, counts);
      assertThat(counts).containsExactly(expected);

      // the single-filter count behind Vertex.countEdges agrees
      assertThat(hub.countEdges(Vertex.DIRECTION.OUT, "A")).isEqualTo(expected[0]);
      assertThat(hub.countEdges(Vertex.DIRECTION.OUT, "B")).isEqualTo(expected[2]);
      assertThat(hub.countEdges(Vertex.DIRECTION.OUT)).isEqualTo(expected[4]);
      assertThat(hub.countEdges(Vertex.DIRECTION.IN, "A")).isEqualTo(3L);
    });
  }

  @Test
  void anOptionalArmOverAnUndeclaredTypeKeepsTheRow() {
    buildSocialGraph(50);
    final String query = "MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person) "
        + "OPTIONAL MATCH (message)<-[:NOT_A_TYPE]-(x:Person)";
    assertStarMatchesPipeline(query);
    assertThat(countMatch(query)).isEqualTo(countMatch("MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person)"));
  }

  @Test
  void uncommittedEdgesAreCounted() {
    buildSocialGraph(80);
    database.transaction(() -> {
      final MutableVertex m = database.newVertex("Message").save();
      m.newEdge("HAS_TAG", database.newVertex("Tag").save());
      m.newEdge("HAS_CREATOR", database.newVertex("Person").save());
      for (int i = 0; i < 3; i++)
        database.newVertex("Person").save().newLightEdge("LIKES", m);
      database.newVertex("Message").save().newEdge("REPLY_OF", m);
      assertStarMatchesPipeline(Q4);
      assertStarMatchesPipeline(Q7);
    });
  }

  @Test
  void aSuperNodeStripedEdgeListIsCountedOnEveryStripe() {
    final int savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    try {
      final RID[] hub = new RID[1];
      database.transaction(() -> {
        final MutableVertex m = database.newVertex("Message").save();
        m.newEdge("HAS_TAG", database.newVertex("Tag").save());
        m.newEdge("HAS_CREATOR", database.newVertex("Person").save());
        database.newVertex("Message").save().newEdge("REPLY_OF", m);
        hub[0] = m.getIdentity();
      });
      // ONE EDGE PER TRANSACTION, SO THE LIST GROWS CHUNK BY CHUNK AND IS PROMOTED ONCE IT CROSSES THE THRESHOLD
      for (int i = 0; i < 300; i++) {
        final String likerType = i % 3 == 0 ? "Forum" : "Person";
        database.transaction(() -> database.newVertex(likerType).save().newLightEdge("LIKES", hub[0].asVertex()));
      }

      database.transaction(() -> {
        final RID inHead = ((VertexInternal) hub[0].asVertex(true)).getInEdgesHeadChunk();
        assertThat(database.lookupByRID(inHead, true)).isInstanceOf(StripeDirectory.class);
      });
      assertThat(countMatch(Q4)).isEqualTo(200L);
      assertStarMatchesPipeline(Q4);
    } finally {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    }
  }

  /**
   * Tags, persons and messages (a quarter of them comments, a subtype of Message) wired at random, with record and light
   * edges, edges reaching vertices of the wrong label and messages missing one arm or another.
   */
  private void buildSocialGraph(final int messages) {
    final Random random = new Random(9539);
    database.transaction(() -> {
      final List<Vertex> tags = new ArrayList<>();
      final List<Vertex> persons = new ArrayList<>();
      final List<Vertex> forums = new ArrayList<>();
      for (int i = 0; i < 30; i++)
        tags.add(database.newVertex(i % 5 == 0 ? "SubTag" : "Tag").save());
      for (int i = 0; i < 60; i++)
        persons.add(database.newVertex("Person").save());
      for (int i = 0; i < 10; i++)
        forums.add(database.newVertex("Forum").save());
      for (int i = 0; i < 100; i++)
        persons.get(random.nextInt(persons.size())).asVertex().modify().newEdge("KNOWS", persons.get(random.nextInt(persons.size())));

      final List<Vertex> created = new ArrayList<>();
      for (int i = 0; i < messages; i++) {
        final MutableVertex m = database.newVertex(i % 4 == 0 ? "Comment" : "Message").save();
        created.add(m);
        for (int k = random.nextInt(4); k > 0; k--)
          if (random.nextInt(5) == 0)
            m.newEdge("HAS_TAG", forums.get(random.nextInt(forums.size())));
          else if (random.nextBoolean())
            m.newLightEdge("HAS_TAG", tags.get(random.nextInt(tags.size())));
          else
            m.newEdge("HAS_TAG", tags.get(random.nextInt(tags.size())));
        if (random.nextInt(10) > 0)
          m.newEdge("HAS_CREATOR", persons.get(random.nextInt(persons.size())));
        for (int k = random.nextInt(6); k > 0; k--) {
          final List<Vertex> likers = random.nextInt(6) == 0 ? forums : persons;
          likers.get(random.nextInt(likers.size())).asVertex().modify().newLightEdge("LIKES", m);
        }
        if (i > 0 && random.nextBoolean())
          m.newEdge("REPLY_OF", created.get(random.nextInt(i)));
      }
    });
  }

  /** The push-down answers the query and agrees with the row pipeline. */
  private void assertStarMatchesPipeline(final String match) {
    final String pushedDown = match + " RETURN count(*) AS n";
    final String pipeline = pipelineQuery(match);
    assertThat(starPlan(match)).as("plan of %s", pushedDown).contains(STAR_PLAN);
    assertThat(plan(pipeline)).as("plan of %s", pipeline).doesNotContain(STAR_PLAN);
    assertThat(count(pushedDown)).as(pushedDown).isEqualTo(count(pipeline));
  }

  /** The same count through the ordinary row pipeline: the push-down detectors only take a MATCH ... RETURN statement. */
  private static String pipelineQuery(final String match) {
    return match + " WITH * RETURN count(*) AS n";
  }

  private String starPlan(final String match) {
    return plan(match + " RETURN count(*) AS n");
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().map(plan -> plan.prettyPrint(0, 2)).orElse("");
    }
  }

  private long countMatch(final String match) {
    return count(match + " RETURN count(*) AS n");
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  private long readRecordStat() {
    return (long) database.getStats().get("readRecord");
  }
}
