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
import com.arcadedb.database.Record;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9598: LSQB Q8 written with the other common spelling of "no such edge", an OPTIONAL MATCH whose relationship is then
 * tested for null, ran row by row, 250x slower than the negated pattern the anti-join push-down answers. When the variables
 * the OPTIONAL MATCH introduces are read by nothing but that test, it is the anti-join, and it is now planned as one.
 * <p>
 * Each rewritten query is checked against an oracle the rewrite cannot apply to: the same query also counting the tested
 * variable in its RETURN, which keeps the OPTIONAL MATCH and runs it row by row. The second count of the oracle is 0, which
 * is what the test meant.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9598OptionalMatchAntiJoinTest extends TestHelper {
  private static final String ANTI_JOIN = "COUNT ANTI-JOIN CHAIN";

  private static final String CHAIN = "MATCH (tag1:Tag)<-[:HAS_TAG]-(message:Message)<-[:REPLY_OF]-(comment:Comment)-[:HAS_TAG]->(tag2:Tag) ";
  private static final String Q8    = CHAIN + "WHERE NOT (comment)-[:HAS_TAG]->(tag1) AND tag1 <> tag2 RETURN count(*) AS n";
  private static final String Q8_OPTIONAL = CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) "
      + "WITH tag1, tag2, h WHERE tag1 <> tag2 AND h IS NULL RETURN count(*) AS n";

  @Override
  protected void beginTest() {
    for (final String ddl : new String[] { "CREATE VERTEX TYPE Tag", "CREATE VERTEX TYPE Message",
        "CREATE VERTEX TYPE Post EXTENDS Message", "CREATE VERTEX TYPE Comment EXTENDS Message", "CREATE EDGE TYPE HAS_TAG",
        "CREATE EDGE TYPE REPLY_OF" })
      database.command("sql", ddl);
  }

  @Test
  void theReportedQueryTakesTheAntiJoinPushDown() {
    // m has tags {a, b}; c1 replies to m with tags {b, c}; c2 replies to m with tags {a, b}
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Tag").set("name", "a").save();
      final MutableVertex b = database.newVertex("Tag").set("name", "b").save();
      final MutableVertex c = database.newVertex("Tag").set("name", "c").save();
      final MutableVertex m = database.newVertex("Post").save();
      final MutableVertex c1 = database.newVertex("Comment").save();
      final MutableVertex c2 = database.newVertex("Comment").save();
      m.newEdge("HAS_TAG", a);
      m.newEdge("HAS_TAG", b);
      c1.newEdge("HAS_TAG", b);
      c1.newEdge("HAS_TAG", c);
      c2.newEdge("HAS_TAG", a);
      c2.newEdge("HAS_TAG", b);
      c1.newEdge("REPLY_OF", m);
      c2.newEdge("REPLY_OF", m);
    });
    // c1: tag1 in {a} x tag2 in {b, c} = 2; c2: no tag of m it lacks
    assertThat(plan(Q8_OPTIONAL)).contains(ANTI_JOIN).doesNotContain("OPTIONAL MATCH");
    assertThat(count(Q8_OPTIONAL)).isEqualTo(2L);
    assertThat(count(Q8)).isEqualTo(2L);
    assertAgreesWithTheOptionalMatch(Q8_OPTIONAL, "h");
  }

  @Test
  void randomGraphsAgreeWithTheOptionalMatchOnTheVerticesAndOnAView() {
    for (long seed = 0; seed < 12; seed++) {
      populate(seed, 120);
      assertThat(plan(Q8_OPTIONAL)).contains(ANTI_JOIN);
      assertThat(count(Q8_OPTIONAL)).as("seed %s", seed).isEqualTo(count(Q8));
      assertAgreesWithTheOptionalMatch(Q8_OPTIONAL, "h");

      final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("q8").build();
      try {
        assertThat(count(Q8_OPTIONAL)).as("seed %s, on the view", seed).isEqualTo(count(Q8));
      } finally {
        view.drop();
      }
      database.drop();
      database = factory.create();
      beginTest();
    }
  }

  /** Every way of writing the test for null: a pass-through WITH *, a GQL FILTER, the pattern read from its other end. */
  @Test
  void otherSpellingsOfTheSameTestAreRewrittenToo() {
    populate(42, 150);
    final long expected = count(Q8);
    for (final String query : new String[] {
        CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH * WHERE tag1 <> tag2 AND h IS NULL RETURN count(*) AS n",
        CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) FILTER h IS NULL AND tag1 <> tag2 RETURN count(*) AS n",
        CHAIN + "OPTIONAL MATCH (tag1)<-[h:HAS_TAG]-(comment) WITH tag1, tag2, h WHERE h IS NULL AND tag1 <> tag2 RETURN count(*) AS n",
        CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag2, h, tag1 WHERE h IS NULL AND tag1 <> tag2 RETURN count(*) AS n" }) {
      assertThat(plan(query)).as("plan of %s", query).doesNotContain("OPTIONAL MATCH");
      assertThat(count(query)).as(query).isEqualTo(expected);
    }
    // the pair count, through a DISTINCT that keeps the WITH
    final String distinct = CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) "
        + "WITH DISTINCT tag1, tag2, h WHERE tag1 <> tag2 AND h IS NULL RETURN count(*) AS n";
    assertThat(plan(distinct)).doesNotContain("OPTIONAL MATCH");
    assertAgreesWithTheOptionalMatch(distinct, "h");
  }

  /** Not only counts: the rows the rewritten query returns are the rows the OPTIONAL MATCH kept. */
  @Test
  void theRowsAreTheRowsTheOptionalMatchKept() {
    populate(7, 100);
    final String rewritten = CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h "
        + "WHERE tag1 <> tag2 AND h IS NULL RETURN tag1.name AS a, tag2.name AS b ORDER BY a, b";
    final String oracle = CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h "
        + "WHERE tag1 <> tag2 AND h IS NULL RETURN tag1.name AS a, tag2.name AS b, h AS hh ORDER BY a, b";
    assertThat(plan(rewritten)).doesNotContain("OPTIONAL MATCH");
    assertThat(plan(oracle)).contains("OPTIONAL MATCH");
    assertThat(pairs(rewritten)).isNotEmpty().isEqualTo(pairs(oracle));
  }

  /** A new node tested for null, a longer optional pattern, and a variable-length one. */
  @Test
  void otherPatternShapesAreRewrittenAndAgree() {
    populate(11, 150);
    for (final String query : new String[] {
        // comments without a tag
        "MATCH (c:Comment) OPTIONAL MATCH (c)-[:HAS_TAG]->(t:Tag) WITH c, t WHERE t IS NULL RETURN count(*) AS n",
        // the tags of a comment that the message it replies to lacks, two hops away: an EXISTS subquery
        "MATCH (c:Comment)-[:HAS_TAG]->(t:Tag) OPTIONAL MATCH (c)-[:REPLY_OF]->(:Message)-[h:HAS_TAG]->(t) "
            + "WITH c, t, h WHERE h IS NULL RETURN count(*) AS n",
        // posts a comment does not reach in up to three replies
        "MATCH (c:Comment), (p:Post) OPTIONAL MATCH (c)-[h:REPLY_OF*1..3]->(p) WITH c, p, h WHERE h IS NULL RETURN count(*) AS n",
        // the pattern starting at the new node and ending at the bound one
        "MATCH (t:Tag) OPTIONAL MATCH (c:Comment)-[h:HAS_TAG]->(t) WITH t, h WHERE h IS NULL RETURN count(*) AS n",
        // variable-length hops inside a longer pattern, which runs from its rendered text: the bounds must survive it
        "MATCH (c:Comment)-[:HAS_TAG]->(t:Tag) OPTIONAL MATCH (c)-[:REPLY_OF*2]->(:Message)-[h:HAS_TAG]->(t) "
            + "WITH c, t, h WHERE h IS NULL RETURN count(*) AS n",
        "MATCH (c:Comment)-[:HAS_TAG]->(t:Tag) OPTIONAL MATCH (c)-[:REPLY_OF*..3]->(:Message)-[h:HAS_TAG]->(t) "
            + "WITH c, t, h WHERE h IS NULL RETURN count(*) AS n",
        "MATCH (c:Comment)-[:HAS_TAG]->(t:Tag) OPTIONAL MATCH (c)-[:REPLY_OF*]->(:Message)-[h:HAS_TAG]->(t) "
            + "WITH c, t, h WHERE h IS NULL RETURN count(*) AS n",
        // an undirected optional pattern, and a WITH that renames a shared variable it carries on
        "MATCH (c:Comment), (p:Post) OPTIONAL MATCH (c)-[h:REPLY_OF]-(p) WITH c, p, h WHERE h IS NULL RETURN count(*) AS n",
        "MATCH (c:Comment)-[:HAS_TAG]->(t:Tag) OPTIONAL MATCH (c)-[:REPLY_OF]->(:Message)-[h:HAS_TAG]->(t) "
            + "WITH c AS comment, t.name AS tag, h WHERE h IS NULL RETURN count(*) AS n",
        // a WITH that renames what the RETURN reads is not a pass-through: it stays, without the removed name
        CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1 AS first, tag2, h WHERE first <> tag2 AND h IS NULL "
            + "RETURN count(first) AS n",
        // one relationship type twice: the predicate binds two different relationships, as the OPTIONAL MATCH does
        "MATCH (c:Comment), (p:Post) OPTIONAL MATCH (c)-[:REPLY_OF]->(:Message)-[h:REPLY_OF]->(p) "
            + "WITH c, p, h WHERE h IS NULL RETURN count(*) AS n" }) {
      assertThat(plan(query)).as("plan of %s", query).doesNotContain("OPTIONAL MATCH");
      final String variable = query.contains("t IS NULL") ? "t" : "h";
      assertAgreesWithTheOptionalMatch(query, variable);
    }
  }

  /**
   * A pattern of more than one hop is evaluated by parsing its text again, so the names written in it must survive the trip:
   * a label with a space and a relationship type with a backtick in it.
   */
  @Test
  void namesThatNeedQuotingSurviveTheRenderedPattern() {
    database.getSchema().createVertexType("Odd Tag");
    database.getSchema().createEdgeType("HAS`TAG");
    database.transaction(() -> {
      final List<MutableVertex> tags = new ArrayList<>();
      for (int i = 0; i < 4; i++)
        tags.add(database.newVertex("Odd Tag").save());
      final MutableVertex post = database.newVertex("Post").save();
      post.newEdge("HAS`TAG", tags.get(0));
      post.newEdge("HAS`TAG", tags.get(1));
      for (int i = 0; i < 4; i++) {
        final MutableVertex comment = database.newVertex("Comment").save();
        comment.newEdge("REPLY_OF", post);
        comment.newEdge("HAS`TAG", tags.get(i));
      }
    });
    // the comments tagged with a tag their post does not have: the ones tagged 2 and 3
    final String query = "MATCH (c:Comment)-[:`HAS``TAG`]->(t:`Odd Tag`) "
        + "OPTIONAL MATCH (c)-[:REPLY_OF]->(:Message)-[h:`HAS``TAG`]->(t) WITH c, t, h WHERE h IS NULL RETURN count(*) AS n";
    assertThat(plan(query)).doesNotContain("OPTIONAL MATCH");
    assertThat(count(query)).isEqualTo(2L);
    assertAgreesWithTheOptionalMatch(query, "h");
  }

  /**
   * Shapes where dropping the OPTIONAL MATCH would change the answer stay as written: a variable read again, renamed, or
   * carried to a RETURN *, an endpoint that can be null, a WITH that aggregates or pages, a WHERE on the OPTIONAL MATCH, a
   * test for NOT NULL.
   */
  @Test
  void shapesThatAreNotOnlyATestForAbsenceStayAsWritten() {
    populate(13, 120);
    for (final String query : new String[] {
        CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h WHERE h IS NULL RETURN count(*) AS n, count(h) AS hs",
        CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h AS x WHERE x IS NULL RETURN count(*) AS n",
        CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, h, count(*) AS k WHERE h IS NULL RETURN sum(k) AS n",
        CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h LIMIT 1000000 WHERE h IS NULL RETURN count(*) AS n",
        CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WHERE tag1 <> tag2 WITH tag1, tag2, h WHERE h IS NULL RETURN count(*) AS n",
        CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h WHERE h IS NOT NULL RETURN count(*) AS n",
        // the test only inside another expression: not a conjunct of its own
        CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h WHERE h IS NULL OR tag1 = tag2 RETURN count(*) AS n",
        CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h WHERE NOT (h IS NOT NULL) RETURN count(*) AS n",
        CHAIN + "OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h WHERE coalesce(h, 0) = 0 RETURN count(*) AS n",
        // p is null for a comment that replies to a comment: OPTIONAL MATCH from a null node matches nothing
        "MATCH (c:Comment) OPTIONAL MATCH (c)-[:REPLY_OF]->(p:Post) OPTIONAL MATCH (p)-[h:HAS_TAG]->(:Tag) "
            + "WITH c, h WHERE h IS NULL RETURN count(*) AS n" }) {
      assertThat(plan(query)).as("plan of %s", query).contains("OPTIONAL MATCH");
      assertThat(count(query)).as(query).isEqualTo(count(query.replace("RETURN ", "WITH * SKIP 0 RETURN ")));
    }

    // carried by WITH * to a RETURN *: the column is part of the answer
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (c:Comment) OPTIONAL MATCH (c)-[h:HAS_TAG]->(:Tag) WITH * WHERE h IS NULL RETURN * LIMIT 1")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().getPropertyNames()).contains("c", "h");
    }

    // p is null for a comment that replies to a comment: such a comment is kept whatever the tags of other posts
    long expected = 0;
    for (final Iterator<Record> it = database.iterateType("Comment", true); it.hasNext(); ) {
      final Vertex comment = it.next().asVertex();
      boolean tagged = false;
      for (final Vertex parent : comment.getVertices(Vertex.DIRECTION.OUT, "REPLY_OF"))
        if (parent.getTypeName().equals("Post") && parent.getVertices(Vertex.DIRECTION.OUT, "HAS_TAG").iterator().hasNext())
          tagged = true;
      if (!tagged)
        ++expected;
    }
    assertThat(count("MATCH (c:Comment) OPTIONAL MATCH (c)-[:REPLY_OF]->(p:Post) OPTIONAL MATCH (p)-[h:HAS_TAG]->(:Tag) "
        + "WITH c, h WHERE h IS NULL RETURN count(*) AS n")).isEqualTo(expected);
  }

  /**
   * Relationship uniqueness is scoped to one MATCH clause: the OPTIONAL MATCH may bind the very relationship the MATCH before
   * it bound, and so may the pattern predicate. Here it can bind nothing else, so no row has it missing.
   */
  @Test
  void theOptionalMatchMayBindTheRelationshipAnEarlierMatchBound() {
    populate(29, 100);
    final String query = "MATCH (c:Comment)-[r:HAS_TAG]->(t:Tag) OPTIONAL MATCH (c)-[h:HAS_TAG]->(t) "
        + "WITH c, t, h WHERE h IS NULL RETURN count(*) AS n";
    assertThat(plan(query)).doesNotContain("OPTIONAL MATCH");
    assertThat(count(query)).isZero();
    assertAgreesWithTheOptionalMatch(query, "h");
  }

  /** An OPTIONAL MATCH after another one filters in its own place, since the WHERE of an OPTIONAL MATCH is part of it. */
  @Test
  void afterAnotherOptionalMatchTheTestBecomesAFilter() {
    populate(17, 120);
    final String query = "MATCH (c:Comment) OPTIONAL MATCH (c)-[:REPLY_OF]->(p:Post) OPTIONAL MATCH (c)-[h:HAS_TAG]->(:Tag) "
        + "WITH c, p, h WHERE h IS NULL RETURN count(*) AS n";
    final String plan = plan(query);
    assertThat(plan.indexOf("OPTIONAL MATCH")).isEqualTo(plan.lastIndexOf("OPTIONAL MATCH")).isNotNegative();
    assertAgreesWithTheOptionalMatch(query, "h");
  }

  @Test
  void eachBranchOfAUnionIsRewritten() {
    populate(19, 100);
    final String union = Q8_OPTIONAL + " UNION ALL " + Q8_OPTIONAL.replace("count(*)", "count(*) + 1");
    assertThat(plan(union)).contains("NOT ((comment)-[:HAS_TAG]->(tag1))").doesNotContain("OPTIONAL MATCH");
    final List<Long> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", union)) {
      while (rs.hasNext())
        values.add(((Number) rs.next().getProperty("n")).longValue());
    }
    final long expected = count(Q8);
    assertThat(values).containsExactlyInAnyOrder(expected, expected + 1);
  }

  /** A statement that writes is left as written: its clauses are not the ones the rewrite models. */
  @Test
  void aWritingStatementIsNotRewritten() {
    populate(23, 80);
    database.transaction(() -> database.command("opencypher",
        "MATCH (c:Comment) OPTIONAL MATCH (c)-[h:HAS_TAG]->(:Tag) WITH c, h WHERE h IS NULL SET c.untagged = true"));
    assertThat(count("MATCH (c:Comment) WHERE c.untagged = true RETURN count(*) AS n"))
        .isEqualTo(count("MATCH (c:Comment) WHERE NOT (c)-[:HAS_TAG]->(:Tag) RETURN count(*) AS n"));
  }

  /**
   * The query agrees with the same OPTIONAL MATCH run row by row: the oracle counts the tested variable too, which keeps the
   * OPTIONAL MATCH, and that count is 0.
   */
  private void assertAgreesWithTheOptionalMatch(final String query, final String variable) {
    final String oracle = query.replace(" AS n", " AS n, count(" + variable + ") AS tested");
    assertThat(plan(oracle)).as("plan of %s", oracle).contains("OPTIONAL MATCH");
    try (final ResultSet rs = database.query("opencypher", oracle)) {
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("tested")).longValue()).isZero();
      assertThat(count(query)).as(query).isEqualTo(((Number) row.getProperty("n")).longValue());
    }
  }

  /**
   * Tags, posts and comments replying to posts or to comments, with random tags, some comments untagged, some replying to
   * themselves.
   */
  private void populate(final long seed, final int messages) {
    final Random random = new Random(seed);
    database.transaction(() -> {
      final List<MutableVertex> tags = new ArrayList<>();
      for (int i = 0; i < 12; i++)
        tags.add(database.newVertex("Tag").set("name", "t" + i).save());
      final List<MutableVertex> all = new ArrayList<>();
      for (int i = 0; i < messages; i++) {
        final boolean post = i < messages / 3 || random.nextInt(4) == 0;
        final MutableVertex message = database.newVertex(post ? "Post" : "Comment").save();
        for (int t = random.nextInt(4); t > 0; t--)
          message.newEdge("HAS_TAG", tags.get(random.nextInt(tags.size())));
        if (!post) {
          final MutableVertex parent = random.nextInt(20) == 0 ? message : all.get(random.nextInt(all.size()));
          message.newEdge("REPLY_OF", parent);
        }
        all.add(message);
      }
    });
  }

  private List<String> pairs(final String query) {
    final List<String> pairs = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        pairs.add(row.getProperty("a") + "," + row.getProperty("b"));
      }
    }
    return pairs;
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().map(plan -> plan.prettyPrint(0, 2)).orElse("");
    }
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
