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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * LSQB Q8, {@code (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Comment)-[:HAS_TAG]->(t2:Tag) WHERE NOT (c)-[:HAS_TAG]->(t1) AND
 * t1 <> t2}, takes the COUNT ANTI-JOIN CHAIN push-down again, with an exact count: over random graphs with parallel edges,
 * self-replies, comments that reply to comments, tags that are not {@code Tag}s and messages without tags, the push-down (on the
 * analytical view and on the vertices) must equal the row pipeline (the same WHERE after a WITH). Shapes the formula does not cover
 * must stay on the row pipeline and still answer the same.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AntiJoinChainQ8ShapeTest {
  private static final String DB_PATH = "./target/databases/antijoin-q8-shape";

  private static final String Q8 = "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Comment)-[:HAS_TAG]->(t2:Tag) ";
  private static final String Q8_WHERE = "NOT (c)-[:HAS_TAG]->(t1) AND t1 <> t2";

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    for (final String ddl : new String[] { "CREATE VERTEX TYPE Tag", "CREATE VERTEX TYPE Other", "CREATE VERTEX TYPE Message",
        "CREATE VERTEX TYPE Post EXTENDS Message", "CREATE VERTEX TYPE Comment EXTENDS Message", "CREATE EDGE TYPE HAS_TAG",
        "CREATE EDGE TYPE REPLY_OF", "CREATE EDGE TYPE LIKES" })
      database.command("sql", ddl);
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void handBuiltGraphCountsTheTagsTheCommentLacks() {
    // m has tags {a, b}; c1 replies to m with tags {b, c}; c2 replies to m with tags {a, b}
    database.transaction(() -> {
      final var a = database.newVertex("Tag").save();
      final var b = database.newVertex("Tag").save();
      final var c = database.newVertex("Tag").save();
      final var m = database.newVertex("Post").save();
      final var c1 = database.newVertex("Comment").save();
      final var c2 = database.newVertex("Comment").save();
      m.newEdge("HAS_TAG", a).save();
      m.newEdge("HAS_TAG", b).save();
      c1.newEdge("HAS_TAG", b).save();
      c1.newEdge("HAS_TAG", c).save();
      c2.newEdge("HAS_TAG", a).save();
      c2.newEdge("HAS_TAG", b).save();
      c1.newEdge("REPLY_OF", m).save();
      c2.newEdge("REPLY_OF", m).save();
    });
    // c1: t1 in {a} (b is a tag of c1) x t2 in {b, c} = 2; c2: t1 in {} = 0
    assertThat(count(Q8 + "WHERE " + Q8_WHERE + " RETURN count(*) AS n")).isEqualTo(2);
    assertPushDown(Q8 + "WHERE " + Q8_WHERE + " RETURN count(*) AS n", true);
  }

  @Test
  void randomGraphsAgreeWithTheRowPipelineOnTheVertices() {
    for (long seed = 0; seed < 40; seed++) {
      populate(seed);
      assertAgrees(Q8, "t1, m, c, t2", Q8_WHERE, true);
      database.drop();
      setUp();
    }
  }

  @Test
  void randomGraphsAgreeWithTheRowPipelineOnTheAnalyticalView() {
    for (long seed = 100; seed < 130; seed++) {
      populate(seed);
      createView();
      assertAgrees(Q8, "t1, m, c, t2", Q8_WHERE, true);
      database.drop();
      setUp();
    }
  }

  @Test
  void reversedEdgeDirectionsAreCountedToo() {
    final String reversed = "MATCH (t1:Tag)-[:HAS_TAG]->(m:Message)-[:REPLY_OF]->(c:Comment)<-[:HAS_TAG]-(t2:Tag) ";
    // the tags point at the messages here: rebuild the random graph with the edges flipped
    for (long seed = 200; seed < 215; seed++) {
      populate(seed, true);
      assertAgrees(reversed, "t1, m, c, t2", "NOT (c)<-[:HAS_TAG]-(t1) AND t1 <> t2", true);
      database.drop();
      setUp();
    }
  }

  @Test
  void anUndirectedHopStaysOnTheRowPipeline() {
    populate(7);
    final String undirected = "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)-[:REPLY_OF]-(c:Comment)-[:HAS_TAG]->(t2:Tag) ";
    assertAgrees(undirected, "t1, m, c, t2", Q8_WHERE, false);
  }

  @Test
  void anUnlabelledNodeStaysOnTheRowPipeline() {
    populate(8);
    final String unlabelled = "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Comment)-[:HAS_TAG]->(t2) ";
    assertAgrees(unlabelled, "t1, m, c, t2", Q8_WHERE, false);
  }

  @Test
  void aMiddleHopOfTheSameTypeStaysOnTheRowPipeline() {
    populate(9);
    final String sameType = "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:HAS_TAG]-(c:Comment)-[:HAS_TAG]->(t2:Tag) ";
    assertAgrees(sameType, "t1, m, c, t2", Q8_WHERE, false);
  }

  @Test
  void aNegatedPatternOfAnotherTypeStaysOnTheRowPipeline() {
    populate(10);
    assertAgrees(Q8, "t1, m, c, t2", "NOT (c)-[:LIKES]->(t1) AND t1 <> t2", false);
  }

  @Test
  void noInequalityStaysOnTheRowPipeline() {
    populate(11);
    assertAgrees(Q8, "t1, m, c, t2", "NOT (c)-[:HAS_TAG]->(t1)", false);
  }

  @Test
  void inlinePropertyOnANodeKeepsTheRowPipelineAnswer() {
    populate(12);
    database.transaction(() -> {
      int i = 0;
      for (final var it = database.iterateType("Comment", true); it.hasNext(); )
        if (i++ % 2 == 0)
          it.next().asVertex().modify().set("flag", true).save();
        else
          it.next();
    });
    final String filtered = "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Comment {flag: true})-[:HAS_TAG]->(t2:Tag) ";
    assertThat(count(filtered + "WHERE " + Q8_WHERE + " RETURN count(*) AS n"))
        .isEqualTo(count(filtered + "WITH t1, m, c, t2 WHERE " + Q8_WHERE + " RETURN count(*) AS n"));
  }

  @Test
  void inlinePropertiesOnAnyNodeAnswerLikeTheRowPipeline() {
    for (final boolean view : new boolean[] { false, true }) {
      populate(21);
      database.transaction(() -> {
        int i = 0;
        for (final String type : new String[] { "Tag", "Message", "Comment" })
          for (final var it = database.iterateType(type, true); it.hasNext(); )
            it.next().asVertex().modify().set("flag", i++ % 2 == 0).save();
      });
      if (view)
        createView();
      for (final String chain : new String[] {
          "MATCH (t1:Tag {flag: true})<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Comment)-[:HAS_TAG]->(t2:Tag) ",
          "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message {flag: true})<-[:REPLY_OF]-(c:Comment)-[:HAS_TAG]->(t2:Tag) ",
          "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Comment {flag: false})-[:HAS_TAG]->(t2:Tag) ",
          "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Comment)-[:HAS_TAG]->(t2:Tag {flag: false}) " })
        assertThat(count(chain + "WHERE " + Q8_WHERE + " RETURN count(*) AS n")).as(chain + (view ? " (view)" : ""))
            .isEqualTo(count(chain + "WITH t1, m, c, t2 WHERE " + Q8_WHERE + " RETURN count(*) AS n"));
      database.drop();
      setUp();
    }
  }

  @Test
  void aSubTypeOfTheTagEdgeAnswersLikeTheRowPipeline() {
    database.command("sql", "CREATE EDGE TYPE HAS_TAG_X EXTENDS HAS_TAG");
    for (final boolean view : new boolean[] { false, true }) {
      populate(22);
      database.transaction(() -> {
        final List<com.arcadedb.graph.Vertex> messages = new ArrayList<>(), tags = new ArrayList<>();
        for (final var it = database.iterateType("Message", true); it.hasNext(); )
          messages.add(it.next().asVertex());
        for (final var it = database.iterateType("Tag", true); it.hasNext(); )
          tags.add(it.next().asVertex());
        final Random random = new Random(5);
        for (int i = 0; i < 10; i++)
          messages.get(random.nextInt(messages.size())).newEdge("HAS_TAG_X", tags.get(random.nextInt(tags.size()))).save();
      });
      if (view)
        createView("gavq8x", "HAS_TAG, HAS_TAG_X, REPLY_OF, LIKES");
      assertAgrees(Q8, "t1, m, c, t2", Q8_WHERE, true);
      database.drop();
      setUp();
      database.command("sql", "CREATE EDGE TYPE HAS_TAG_X EXTENDS HAS_TAG");
    }
  }

  @Test
  void aLabelOnTheNegatedPatternAnswersLikeTheRowPipeline() {
    populate(23);
    final String text = Q8 + "WHERE NOT (c)-[:HAS_TAG]->(:Tag {flag: true}) AND t1 <> t2 RETURN count(*) AS n";
    assertThat(count(text)).isEqualTo(count(Q8 + "WITH t1, m, c, t2 WHERE NOT (c)-[:HAS_TAG]->(:Tag {flag: true}) AND t1 <> t2 RETURN count(*) AS n"));
    assertAgrees(Q8, "t1, m, c, t2", "NOT (c)-[:HAS_TAG]->(t1:Tag) AND t1 <> t2", true);
  }

  // ===================================================================================================
  // helpers
  // ===================================================================================================

  private void populate(final long seed) {
    populate(seed, false);
  }

  /**
   * Tags with duplicates (parallel edges), vertices that are not Tags, messages with no tag, self-replies, comments replying to
   * comments and the same reply twice. {@code flipTags} points HAS_TAG from the tag to the message and REPLY_OF from the message to
   * the comment.
   */
  private void populate(final long seed, final boolean flipTags) {
    final Random random = new Random(seed);
    database.transaction(() -> {
      final List<MutableVertex> tags = new ArrayList<>(), others = new ArrayList<>(), messages = new ArrayList<>(),
          comments = new ArrayList<>();
      for (int i = 0; i < 6; i++)
        tags.add(database.newVertex("Tag").save());
      for (int i = 0; i < 2; i++)
        others.add(database.newVertex("Other").save());
      for (int i = 0; i < 5; i++)
        messages.add(database.newVertex("Post").save());
      for (int i = 0; i < 12; i++) {
        final MutableVertex comment = database.newVertex("Comment").save();
        comments.add(comment);
        messages.add(comment);
      }
      for (final MutableVertex message : messages)
        for (int k = random.nextInt(4); k > 0; k--) {
          final MutableVertex tag = random.nextInt(8) == 0 ? others.get(random.nextInt(others.size())) : tags.get(random.nextInt(tags.size()));
          if (flipTags)
            tag.newEdge("HAS_TAG", message).save();
          else
            message.newEdge("HAS_TAG", tag).save();
        }
      for (final MutableVertex comment : comments)
        for (int k = random.nextInt(3); k > 0; k--) {
          final MutableVertex parent = messages.get(random.nextInt(messages.size()));
          if (flipTags)
            parent.newEdge("REPLY_OF", comment).save();
          else
            comment.newEdge("REPLY_OF", parent).save();
        }
    });
  }

  private void createView() {
    createView("gavq8", "HAS_TAG, REPLY_OF, LIKES");
  }

  private void createView(final String name, final String edgeTypes) {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW " + name + " VERTEX TYPES (Tag, Other, Message, Post, Comment) "
        + "EDGE TYPES (" + edgeTypes + ")");
    final var view = GraphAnalyticalViewRegistry.get(database, name);
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!view.isReady() && System.currentTimeMillis() < deadline)
      Thread.onSpinWait();
    assertThat(view.isReady()).isTrue();
  }

  private void assertAgrees(final String chain, final String vars, final String where, final boolean pushedDown) {
    final String text = chain + "WHERE " + where + " RETURN count(*) AS n";
    assertThat(count(text)).as("as written: " + text).isEqualTo(count(chain + "WITH " + vars + " WHERE " + where + " RETURN count(*) AS n"));
    assertPushDown(text, pushedDown);
  }

  private void assertPushDown(final String query, final boolean expected) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      final String plan = rs.getExecutionPlan().map(x -> x.prettyPrint(0, 2)).orElse("");
      assertThat(plan.contains("COUNT ANTI-JOIN CHAIN")).as("push-down in plan: " + plan).isEqualTo(expected);
    }
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
