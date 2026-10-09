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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9595: a property predicate on a pattern node - {@code (:Message {kind: 'Post'})} or
 * {@code WHERE m.kind = 'Post'} - took a count-only chain off the count push-down, so LSQB Q1 written with the parent type
 * and a {@code kind} property ran 400x slower than with the sub-type labels. A predicate that reads one node only is a
 * per-vertex filter, and the chain and star count operators now apply it on top of the label.
 * <p>
 * Every count is checked against the row pipeline, reached through {@code RETURN sum(1)} which no count push-down
 * answers, on a graph where the {@code kind} property and the sub-type disagree on purpose: some {@code Comment} vertices
 * say {@code kind: 'Post'}, some plain {@code Message} vertices carry either kind and some carry none.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9595CountPushDownPropertyPredicateTest extends TestHelper {
  private static final String PUSH_DOWN = "Using Count Push-Down";

  private static final String Q1_PROPERTY =
      "MATCH (:TagClass)<-[:HAS_TYPE]-(:Tag)<-[:HAS_TAG]-(:Message {kind: 'Comment'})-[:REPLY_OF]->(:Message {kind: 'Post'})"
          + "<-[:CONTAINER_OF]-(:Forum)-[:HAS_MEMBER]->(:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(:Country)";

  @Override
  protected void beginTest() {
    for (final String type : new String[] { "TagClass", "Tag", "Forum", "Person", "City", "Country", "Message" })
      database.command("sql", "CREATE VERTEX TYPE " + type);
    database.command("sql", "CREATE VERTEX TYPE Post EXTENDS Message");
    database.command("sql", "CREATE VERTEX TYPE Comment EXTENDS Message");
    for (final String type : new String[] { "HAS_TYPE", "HAS_TAG", "REPLY_OF", "CONTAINER_OF", "HAS_MEMBER", "IS_LOCATED_IN",
        "IS_PART_OF", "HAS_CREATOR", "LIKES" })
      database.command("sql", "CREATE EDGE TYPE " + type);
    buildGraph();
  }

  @Test
  void inlinePropertyOnAChainKeepsThePushDown() {
    assertPushedDownAndExact(Q1_PROPERTY);
    assertPushedDownAndExact("MATCH (:Message {kind: 'Post'})<-[:REPLY_OF]-(:Message {kind: 'Comment'})");
    assertPushedDownAndExact("MATCH (:Message {kind: 'Comment'})-[:REPLY_OF]->(:Message {kind: 'Post'})");
    // the property and the label together, and two properties on one node
    assertPushedDownAndExact("MATCH (:Comment {kind: 'Post'})-[:REPLY_OF]->(m:Message {kind: 'Post', cid: 3})");
    assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(m:Message {kind: 'Post'})");
  }

  @Test
  void wherePredicatesOnSingleNodesKeepThePushDown() {
    assertPushedDownAndExact("MATCH (p:Message)<-[:REPLY_OF]-(c:Message) WHERE p.kind = 'Post' AND c.kind = 'Comment'");
    assertPushedDownAndExact("MATCH (p:Message)<-[:REPLY_OF]-(c:Message) WHERE c.kind = 'Comment' AND p.cid > 20");
    assertPushedDownAndExact("MATCH (p:Message)<-[:REPLY_OF]-(c:Message) WHERE c.kind IS NULL");
    // a missing property makes <> null, which filters the row out like = does
    assertPushedDownAndExact("MATCH (p:Message)<-[:REPLY_OF]-(c:Message) WHERE c.kind <> 'Post'");
    assertPushedDownAndExact("MATCH (p:Message)<-[:REPLY_OF]-(c:Message) WHERE NOT (c.kind = 'Post') AND p.kind IS NOT NULL");
    assertPushedDownAndExact("MATCH (p:Message)<-[:REPLY_OF]-(c:Message) WHERE p.kind STARTS WITH 'Po' OR p.cid < 10");
    assertPushedDownAndExact(
        "MATCH (:TagClass)<-[:HAS_TYPE]-(:Tag)<-[:HAS_TAG]-(c:Message)-[:REPLY_OF]->(p:Message)<-[:CONTAINER_OF]-(:Forum) "
            + "WHERE p.kind = 'Post' AND c.kind = 'Comment'");
  }

  @Test
  void parametersAreResolvedPerExecution() {
    for (final String kind : new String[] { "Post", "Comment", "None" }) {
      final Map<String, Object> params = Map.of("kind", kind);
      final String match = "MATCH (:Tag)<-[:HAS_TAG]-(m:Message {kind: $kind})-[:REPLY_OF]->(:Message)";
      assertThat(plan(match + " RETURN count(*) AS n", params)).contains(PUSH_DOWN);
      assertThat(count(match + " RETURN count(*) AS n", params)).as("%s with %s", match, kind)
          .isEqualTo(count(match + " RETURN sum(1) AS n", params));

      final String where = "MATCH (:Tag)<-[:HAS_TAG]-(m:Message)-[:REPLY_OF]->(:Message) WHERE m.kind = $kind";
      assertThat(count(where + " RETURN count(*) AS n", params)).as("%s with %s", where, kind)
          .isEqualTo(count(where + " RETURN sum(1) AS n", params));
    }
  }

  @Test
  void anInequalityAndAPropertyPredicateTogether() {
    assertPushedDownAndExact("MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Message)-[:HAS_TAG]->(t2:Tag) "
        + "WHERE t1 <> t2 AND c.kind = 'Comment'");
    assertPushedDownAndExact("MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message {kind: 'Post'})<-[:REPLY_OF]-(c:Message)-[:HAS_TAG]->(t2:Tag) "
        + "WHERE t1 <> t2");
    assertPushedDownAndExact("MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)-[:HAS_TAG]->(t2:Tag) WHERE t1 <> t2 AND m.kind = 'Post'");
  }

  @Test
  void starArmsAndCentralNodeCarryPredicates() {
    // LSQB Q4 split into three MATCH clauses, with the comment written as a Message of kind 'Comment'
    assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person) "
        + "MATCH (message)<-[:LIKES]-(liker:Person) MATCH (message)<-[:REPLY_OF]-(comment:Message {kind: 'Comment'})");
    assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(message:Message {kind: 'Post'})-[:HAS_CREATOR]->(creator:Person), "
        + "(message)<-[:LIKES]-(liker:Person), (message)<-[:REPLY_OF]-(comment:Message)");
    // Q7 with a filtered optional arm: a message whose replies all fail the filter still keeps its row
    assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person) "
        + "OPTIONAL MATCH (message)<-[:LIKES]-(liker:Person) OPTIONAL MATCH (message)<-[:REPLY_OF]-(comment:Message {kind: 'Comment'})");
    assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person) "
        + "MATCH (message)<-[:REPLY_OF]-(comment:Message) WHERE comment.kind = 'Comment' AND message.cid > 5");
  }

  @Test
  void anOptionalClauseMatchesAsAWhole() {
    // Found while extending the star count: an OPTIONAL clause yielding two arms was counted as two independent optional
    // arms, max(1, d1) * max(1, d2), where the clause contributes one row as soon as either arm reaches nothing
    for (final String match : new String[] {
        "MATCH (m:Message) OPTIONAL MATCH (c:Message)-[:REPLY_OF]->(m)-[:HAS_TAG]->(t:Tag)",
        "MATCH (m:Message) OPTIONAL MATCH (m)-[:HAS_TAG]->(t:Tag), (m)<-[:LIKES]-(p:Person)" }) {
      assertThat(count(match + " RETURN count(*) AS n", Map.of())).as(match)
          .isEqualTo(count(match + " RETURN sum(1) AS n", Map.of()));
    }
  }

  @Test
  void anOptionalClauseFilterIsAConditionOfItsArm() {
    // a WHERE of an OPTIONAL clause on the reached node keeps the row of a message whose replies all fail it
    assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(message:Message) "
        + "OPTIONAL MATCH (message)<-[:REPLY_OF]-(comment:Message) WHERE comment.kind = 'Comment'");
    // on the central node it decides whether the arm matched, not whether the row exists: left to the pipeline
    final String central = "MATCH (:Tag)<-[:HAS_TAG]-(message:Message) "
        + "OPTIONAL MATCH (message)<-[:REPLY_OF]-(comment:Message) WHERE message.kind = 'Post'";
    assertThat(count(central + " RETURN count(*) AS n", Map.of())).isEqualTo(count(central + " RETURN sum(1) AS n", Map.of()));
    final String inline = "MATCH (:Tag)<-[:HAS_TAG]-(message:Message) "
        + "OPTIONAL MATCH (message {kind: 'Post'})<-[:REPLY_OF]-(comment:Message)";
    assertThat(count(inline + " RETURN count(*) AS n", Map.of())).isEqualTo(count(inline + " RETURN sum(1) AS n", Map.of()));
  }

  @Test
  void theSameAnswersOverAGraphAnalyticalView() throws InterruptedException {
    withView(() -> {
      inlinePropertyOnAChainKeepsThePushDown();
      wherePredicatesOnSingleNodesKeepThePushDown();
      parametersAreResolvedPerExecution();
      anInequalityAndAPropertyPredicateTogether();
      starArmsAndCentralNodeCarryPredicates();
      anOptionalClauseMatchesAsAWhole();
      anOptionalClauseFilterIsAConditionOfItsArm();
    });
  }

  @Test
  void aCorrelatedBodyFiltersTheReachedNodes() throws InterruptedException {
    final String query = "MATCH (p:Message) RETURN p.cid AS cid, COUNT { (p)<-[:REPLY_OF]-(:Message {kind: 'Comment'}) } AS n "
        + "ORDER BY cid";
    final String pipeline = "MATCH (p:Message) OPTIONAL MATCH (p)<-[:REPLY_OF]-(c:Message) WITH p, c "
        + "RETURN p.cid AS cid, sum(CASE WHEN c.kind = 'Comment' THEN 1 ELSE 0 END) AS n ORDER BY cid";
    assertThat(rows(query)).isEqualTo(rows(pipeline));
    withView(() -> assertThat(rows(query)).isEqualTo(rows(pipeline)));
  }

  @Test
  void predicatesReadingTwoNodesStayOnThePipeline() {
    final String match = "MATCH (p:Message)<-[:REPLY_OF]-(c:Message) WHERE p.cid < c.cid";
    assertThat(plan(match + " RETURN count(*) AS n", Map.of())).doesNotContain(PUSH_DOWN);
    assertThat(count(match + " RETURN count(*) AS n", Map.of())).isEqualTo(count(match + " RETURN sum(1) AS n", Map.of()));

    // an inline value read off another node of the pattern is not a constant
    final String inline = "MATCH (p:Message)<-[:REPLY_OF]-(c:Message {kind: p.kind})";
    assertThat(plan(inline + " RETURN count(*) AS n", Map.of())).doesNotContain(PUSH_DOWN);
    assertThat(count(inline + " RETURN count(*) AS n", Map.of())).isEqualTo(count(inline + " RETURN sum(1) AS n", Map.of()));

    // a non-deterministic predicate is evaluated per row, not per vertex
    final String random = "MATCH (p:Message)<-[:REPLY_OF]-(c:Message) WHERE rand() < 2";
    assertThat(plan(random + " RETURN count(*) AS n", Map.of())).doesNotContain(PUSH_DOWN);

    // so is a function that may resolve to a DEFINE FUNCTION body, which can read anything on every call
    database.command("sql", "DEFINE FUNCTION kinds.twice \"SELECT :x * 2\" PARAMETERS [x] LANGUAGE sql");
    final String custom = "MATCH (p:Message)<-[:REPLY_OF]-(c:Message) WHERE kinds.twice(p.cid) > 40";
    // run as commands: a call that may reach a DEFINE FUNCTION body is not a read-only query
    try (final ResultSet pushed = database.command("opencypher", "EXPLAIN " + custom + " RETURN count(*) AS n")) {
      assertThat(pushed.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("")).doesNotContain(PUSH_DOWN);
    }
    try (final ResultSet counted = database.command("opencypher", custom + " RETURN count(*) AS n");
        final ResultSet summed = database.command("opencypher", custom + " RETURN sum(1) AS n")) {
      assertThat(((Number) counted.next().getProperty("n")).longValue())
          .isEqualTo(((Number) summed.next().getProperty("n")).longValue());
    }
  }

  @Test
  void aSelfLoopOnAnUndirectedHopIsCountedOnceUnderAPredicate() throws InterruptedException {
    database.transaction(() -> {
      final MutableVertex loop = database.newVertex("Message").set("cid", 1000).set("kind", "Comment").save();
      loop.newEdge("REPLY_OF", loop);
    });
    final String[] matches = { "MATCH (a:Message {kind: 'Comment'})-[:REPLY_OF]-(b:Message)",
        "MATCH (a:Message)-[:REPLY_OF]-(b:Message) WHERE a.kind = 'Comment' AND b.cid >= 1000",
        "MATCH (:Tag)<-[:HAS_TAG]-(a:Message)-[:REPLY_OF]-(b:Message {kind: 'Comment'})" };
    for (final String match : matches)
      assertPushedDownAndExact(match);
    withView(() -> {
      for (final String match : matches)
        assertPushedDownAndExact(match);
    });
  }

  @Test
  void numbersCompareByValueAcrossTypes() {
    // cid is stored as an Integer: an inline Long, a Double and a parameter of another type all compare by value
    assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(m:Message {cid: 3})");
    assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(m:Message {cid: 3.0})");
    assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(m:Message) WHERE m.cid = 3.0");
    for (final Object cid : new Object[] { 3, 3L, 3.0d, 3.0f, "3" }) {
      final String match = "MATCH (:Tag)<-[:HAS_TAG]-(m:Message {cid: $cid})";
      assertThat(count(match + " RETURN count(*) AS n", Map.of("cid", cid))).as("cid %s (%s)", cid, cid.getClass().getSimpleName())
          .isEqualTo(count(match + " RETURN sum(1) AS n", Map.of("cid", cid)));
    }
  }

  @Test
  void uncommittedChangesOfTheTransactionAreSeen() throws InterruptedException {
    // a transaction with changes of its own does not use the view, and the records it reads carry those changes
    final String match = "MATCH (:Tag)<-[:HAS_TAG]-(m:Message {kind: 'Post'})-[:REPLY_OF]->(:Message)";
    final Runnable check = () -> {
      database.begin();
      try {
        final long before = count(match + " RETURN count(*) AS n", Map.of());
        database.command("opencypher", "MATCH (m:Message {kind: 'Post'}) SET m.kind = 'Moved'");
        assertThat(count(match + " RETURN count(*) AS n", Map.of())).isZero()
            .isEqualTo(count(match + " RETURN sum(1) AS n", Map.of()));
        database.command("opencypher", "MATCH (m:Message {kind: 'Moved'}) SET m.kind = 'Post'");
        assertThat(count(match + " RETURN count(*) AS n", Map.of())).isEqualTo(before).isPositive();
      } finally {
        database.rollback();
      }
    };
    check.run();
    withView(check);
  }

  @Test
  void anInlineNullMatchesNothing() {
    // {kind: null} means kind = null, which is never true, not "kind is missing"
    assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(m:Message {kind: null})");
    assertThat(count("MATCH (:Tag)<-[:HAS_TAG]-(m:Message {kind: null}) RETURN count(*) AS n", Map.of())).isZero();
    final String parameter = "MATCH (:Tag)<-[:HAS_TAG]-(m:Message {kind: $kind}) RETURN count(*) AS n";
    final Map<String, Object> nullKind = new HashMap<>();
    nullKind.put("kind", null);
    assertThat(count(parameter, nullKind)).isZero();
  }

  @Test
  void thePredicateIsEvaluatedOncePerDistinctVertex() throws InterruptedException {
    // Every Comment-kind message is reached once per tag it carries; the filter reads it once
    final String match = "MATCH (:Tag)<-[:HAS_TAG]-(c:Message {kind: 'Comment'})-[:REPLY_OF]->(:Message)";
    final long messages = count("MATCH (m:Message) RETURN count(m) AS n", Map.of());
    withView(() -> {
      final long before = readRecordStat();
      count(match + " RETURN count(*) AS n", Map.of());
      assertThat(readRecordStat() - before).isLessThanOrEqualTo(messages);
    });
  }

  /**
   * A small social graph whose {@code kind} property does not follow the sub-types: the filter has to read the
   * property, not the type.
   */
  private void buildGraph() {
    final Random random = new Random(9595);
    database.transaction(() -> {
      final List<Vertex> tagClasses = create("TagClass", 4);
      final List<Vertex> tags = create("Tag", 25);
      final List<Vertex> forums = create("Forum", 8);
      final List<Vertex> persons = create("Person", 30);
      final List<Vertex> cities = create("City", 6);
      final List<Vertex> countries = create("Country", 3);
      for (final Vertex tag : tags)
        tag.modify().newEdge("HAS_TYPE", pick(random, tagClasses));
      for (final Vertex city : cities)
        city.modify().newEdge("IS_PART_OF", pick(random, countries));
      for (final Vertex person : persons)
        person.modify().newEdge("IS_LOCATED_IN", pick(random, cities));
      for (final Vertex forum : forums)
        for (int k = random.nextInt(6); k > 0; k--)
          forum.modify().newEdge("HAS_MEMBER", pick(random, persons));

      final List<Vertex> messages = new ArrayList<>();
      for (int i = 0; i < 300; i++) {
        final int shape = random.nextInt(10);
        final String type = shape < 4 ? "Post" : shape < 8 ? "Comment" : "Message";
        final MutableVertex m = database.newVertex(type);
        m.set("cid", i);
        if (shape < 3)
          m.set("kind", "Post");
        else if (shape == 3)
          m.set("kind", "Comment"); // a Post saying it is a comment
        else if (shape < 7)
          m.set("kind", "Comment");
        else if (shape == 7)
          m.set("kind", "Post"); // a Comment saying it is a post
        else if (shape == 8)
          m.set("kind", random.nextBoolean() ? "Post" : "Comment");
        // shape 9: no kind at all
        m.save();
        messages.add(m);
        for (int k = random.nextInt(3); k > 0; k--)
          m.newEdge("HAS_TAG", pick(random, tags));
        if (random.nextInt(8) > 0)
          m.newEdge("HAS_CREATOR", pick(random, persons));
        for (int k = random.nextInt(4); k > 0; k--)
          pick(random, persons).modify().newEdge("LIKES", m);
        if (i > 0 && random.nextInt(4) > 0)
          m.newEdge("REPLY_OF", messages.get(random.nextInt(i)));
        if (random.nextInt(3) == 0)
          pick(random, forums).modify().newEdge("CONTAINER_OF", m);
      }
    });
  }

  private List<Vertex> create(final String type, final int count) {
    final List<Vertex> vertices = new ArrayList<>(count);
    for (int i = 0; i < count; i++)
      vertices.add(database.newVertex(type).set("cid", i).save());
    return vertices;
  }

  private static Vertex pick(final Random random, final List<Vertex> vertices) {
    return vertices.get(random.nextInt(vertices.size()));
  }

  private void assertPushedDownAndExact(final String match) {
    final String pushedDown = match + " RETURN count(*) AS n";
    assertThat(plan(pushedDown, Map.of())).as("plan of %s", pushedDown).contains(PUSH_DOWN);
    final long expected = count(match + " RETURN sum(1) AS n", Map.of());
    assertThat(count(pushedDown, Map.of())).as(pushedDown).isEqualTo(expected);
  }

  private void withView(final Runnable check) throws InterruptedException {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW social VERTEX TYPES (TagClass, Tag, Forum, Person, City, Country, "
        + "Message, Post, Comment) EDGE TYPES (HAS_TYPE, HAS_TAG, REPLY_OF, CONTAINER_OF, HAS_MEMBER, IS_LOCATED_IN, "
        + "IS_PART_OF, HAS_CREATOR, LIKES) UPDATE MODE OFF");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "social");
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!view.isReady() && System.currentTimeMillis() < deadline)
      Thread.sleep(20);
    assertThat(view.isReady()).isTrue();
    try {
      check.run();
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW social");
    }
  }

  private String plan(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query, params)) {
      return rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
    }
  }

  private long count(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      final Object value = rs.next().getProperty("n");
      return value == null ? 0L : ((Number) value).longValue();
    }
  }

  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final var row = rs.next();
        rows.add(row.getProperty("cid") + ":" + ((Number) row.getProperty("n")).longValue());
      }
    }
    return rows;
  }

  private long readRecordStat() {
    return (long) database.getStats().get("readRecord");
  }
}
