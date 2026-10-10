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
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9608: a property predicate on a pattern node, {@code (:Message {kind: 'Comment'})}, made the LSQB Q2 (pair join) and
 * Q8 (anti-join) shapes fall back to the row pipeline, in every order and every spelling of the negation, while the label
 * spelling was a push-down. Both operators now apply a per-vertex predicate on top of the label of each position, the way the
 * chain and star operators do since issue #9595.
 * <p>
 * Every count is checked against the row pipeline, reached through a {@code sum(1)} that no push-down takes, with and
 * without a Graph Analytical View, since the operators have a CSR path and a record path.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9608PropertyPredicatePairAndAntiJoinTest extends TestHelper {
  private static final String Q2_LABELS     = "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
      + "(person1)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_CREATOR]->(person2)";
  private static final String Q2_PROPERTIES = "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
      + "(person1)<-[:HAS_CREATOR]-(comment:Message {kind: 'Comment'})-[:REPLY_OF]->(post:Message {kind: 'Post'})-[:HAS_CREATOR]->(person2)";
  private static final String Q2_REORDERED  = "MATCH (post:Message {kind: 'Post'})-[:HAS_CREATOR]->(person2:Person), "
      + "(comment:Message {kind: 'Comment'})-[:REPLY_OF]->(post), (comment)-[:HAS_CREATOR]->(person1:Person), (person2)-[:KNOWS]-(person1)";

  private static final String Q8_LABELS     = "MATCH (tag1:Tag)<-[:HAS_TAG]-(message:Message)<-[:REPLY_OF]-(comment:Comment)-[:HAS_TAG]->(tag2:Tag)";
  private static final String Q8_PROPERTIES = "MATCH (tag1:Tag)<-[:HAS_TAG]-(message:Message)<-[:REPLY_OF]-(comment:Message {kind: 'Comment'})-[:HAS_TAG]->(tag2:Tag)";
  private static final String Q8_NOT        = " WHERE NOT (comment)-[:HAS_TAG]->(tag1) AND tag1 <> tag2";
  private static final String Q8_OPTIONAL   = " OPTIONAL MATCH (comment)-[h:HAS_TAG]->(tag1) WITH tag1, tag2, h WHERE tag1 <> tag2 AND h IS NULL";

  private static final String Q9 = "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag)";
  private static final String Q9_WHERE = " WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3";

  @Override
  protected void beginTest() {
    for (final String ddl : new String[] { "CREATE VERTEX TYPE Person", "CREATE VERTEX TYPE Tag", "CREATE VERTEX TYPE Message",
        "CREATE VERTEX TYPE Post EXTENDS Message", "CREATE VERTEX TYPE Comment EXTENDS Message", "CREATE EDGE TYPE KNOWS",
        "CREATE EDGE TYPE HAS_CREATOR", "CREATE EDGE TYPE REPLY_OF", "CREATE EDGE TYPE HAS_TAG", "CREATE EDGE TYPE HAS_INTEREST" })
      database.command("sql", ddl);
  }

  @Test
  void theReportedQ2PropertyFormIsAPairJoin() {
    populate(1);
    assertQ2();
  }

  @Test
  void theReportedQ8PropertyFormsAreAnAntiJoin() {
    populate(2);
    assertQ8();
  }

  @Test
  void aViewAnswersThePropertyFormsAlike() {
    populate(3);
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("lsqb").build();
    try {
      assertQ2();
      assertQ8();
      assertQ9();
    } finally {
      view.drop();
    }
  }

  /** The same predicates written in WHERE rather than on the node take the same operators. */
  @Test
  void wherePredicatesOnOneNodeAreAppliedLikeInlineMaps() {
    populate(4);
    final String q2 = "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
        + "(person1)<-[:HAS_CREATOR]-(comment:Message)-[:REPLY_OF]->(post:Message)-[:HAS_CREATOR]->(person2) "
        + "WHERE comment.kind = 'Comment' AND post.kind = 'Post' AND person1.flag = true";
    assertThat(pushDown(q2 + " RETURN count(*) AS n")).contains("COUNT PAIR JOIN");
    assertThat(count(q2 + " RETURN count(*) AS n")).isEqualTo(count(q2 + " RETURN sum(1) AS n")).isPositive();

    final String q8 = "MATCH (tag1:Tag)<-[:HAS_TAG]-(message:Message)<-[:REPLY_OF]-(comment:Message)-[:HAS_TAG]->(tag2:Tag) "
        + "WHERE comment.kind = 'Comment' AND NOT (comment)-[:HAS_TAG]->(tag1) AND tag1 <> tag2 AND message.lang = 'en'";
    assertThat(pushDown(q8 + " RETURN count(*) AS n")).contains("COUNT ANTI-JOIN CHAIN");
    assertThat(count(q8 + " RETURN count(*) AS n")).isEqualTo(count(q8 + " RETURN sum(1) AS n")).isPositive();
  }

  /** A predicate on every position of each shape, including a parameter, read through the record path and the CSR one. */
  @Test
  void aPredicateOnEveryPositionKeepsTheCount() {
    populate(5);
    assertQ9();
    final String q2 = "MATCH (person1:Person {flag: true})-[:KNOWS]-(person2:Person {flag: false}), "
        + "(person1)<-[:HAS_CREATOR]-(comment:Message {kind: $kind, lang: 'en'})-[:REPLY_OF]->(post:Message)-[:HAS_CREATOR]->(person2)";
    final Map<String, Object> parameters = Map.of("kind", "Comment");
    assertThat(pushDown(q2 + " RETURN count(*) AS n", parameters)).contains("COUNT PAIR JOIN");
    assertThat(count(q2 + " RETURN count(*) AS n", parameters)).isEqualTo(count(q2 + " RETURN sum(1) AS n", parameters));

    final String q8 = "MATCH (tag1:Tag {group: 1})<-[:HAS_TAG]-(message:Message {lang: 'en'})<-[:REPLY_OF]-"
        + "(comment:Message {kind: 'Comment'})-[:HAS_TAG]->(tag2:Tag {group: 0})";
    for (final String tail : new String[] { Q8_NOT, Q8_OPTIONAL }) {
      assertThat(pushDown(q8 + tail + " RETURN count(*) AS n")).contains("COUNT ANTI-JOIN CHAIN");
      assertThat(count(q8 + tail + " RETURN count(*) AS n")).as(tail).isEqualTo(count(q8 + Q8_NOT + " RETURN sum(1) AS n"));
    }
  }

  /**
   * A variable written at two positions keeps the properties of both: the pattern graph used to keep the labelled position
   * alone, which dropped a predicate written at the other one from every operator reading the joined pattern.
   */
  @Test
  void propertiesWrittenAtAnotherPositionOfTheVariableAreKept() {
    populate(6);
    for (final String match : new String[] {
        // a chain cut into parts, the predicate on the unlabelled position of comment
        "MATCH (c:Comment)-[:REPLY_OF]->(m:Message), (c {lang: 'en'})-[:HAS_TAG]->(t:Tag)",
        "MATCH (c {lang: 'en'})-[:REPLY_OF]->(m:Message), (c:Comment)-[:HAS_TAG]->(t:Tag)",
        // two different keys on the two positions
        "MATCH (c:Comment {kind: 'Comment'})-[:REPLY_OF]->(m:Message), (c {lang: 'en'})-[:HAS_TAG]->(t:Tag)",
        // the same key, two different values: nothing matches
        "MATCH (c:Comment {lang: 'it'})-[:REPLY_OF]->(m:Message), (c {lang: 'en'})-[:HAS_TAG]->(t:Tag)",
        // the Q2 cycle with the predicates on the probe's ends
        "MATCH (person1 {flag: true})-[:KNOWS]-(person2), "
            + "(person1:Person)<-[:HAS_CREATOR]-(comment:Message {kind: 'Comment'})-[:REPLY_OF]->(post:Message {kind: 'Post'})-[:HAS_CREATOR]->(person2:Person)",
        "MATCH (post:Message)-[:HAS_CREATOR]->(person2:Person), (comment:Message {kind: 'Comment'})-[:REPLY_OF]->(post {kind: 'Post'}), "
            + "(comment {lang: 'en'})-[:HAS_CREATOR]->(person1:Person), (person2 {flag: false})-[:KNOWS]-(person1)" })
      assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(count(match + " RETURN sum(1) AS n"));
  }

  /**
   * Two inline values prove two nodes different vertices only when no stored value can match both: a schema type on the
   * property, or a case-insensitive index answering the node seek, must not make the proof wrong.
   */
  @Test
  void typedAndCaseInsensitivelyIndexedPropertiesKeepTheCount() {
    database.command("sql", "CREATE PROPERTY Message.code STRING");
    database.command("sql", "CREATE PROPERTY Message.score INTEGER");
    database.command("sql", "CREATE INDEX ON Message (code COLLATE ci) NOTUNIQUE");
    populate(9);
    final Random random = new Random(9);
    final String[] codes = { "A", "a", "b" };
    database.transaction(() -> database.iterateType("Message", true).forEachRemaining(record -> record.asVertex().modify()
        .set("code", codes[random.nextInt(codes.length)]).set("score", random.nextInt(3)).save()));

    for (final String match : new String[] {
        "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
            + "(person1)<-[:HAS_CREATOR]-(comment:Message {code: 'A'})-[:REPLY_OF]->(post:Message {code: 'a'})-[:HAS_CREATOR]->(person2)",
        "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
            + "(person1)<-[:HAS_CREATOR]-(comment:Message {code: 'A'})-[:REPLY_OF]->(post:Message {code: 'b'})-[:HAS_CREATOR]->(person2)",
        "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
            + "(person1)<-[:HAS_CREATOR]-(comment:Message {score: 1})-[:REPLY_OF]->(post:Message {score: 2})-[:HAS_CREATOR]->(person2)",
        "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
            + "(person1)<-[:HAS_CREATOR]-(comment:Message {score: '1'})-[:REPLY_OF]->(post:Message {score: 1})-[:HAS_CREATOR]->(person2)",
        // a disjunction is no equality: it proves nothing about the two nodes
        "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
            + "(person1)<-[:HAS_CREATOR]-(comment:Message)-[:REPLY_OF]->(post:Message)-[:HAS_CREATOR]->(person2) "
            + "WHERE (comment.kind = 'Comment' OR comment.kind = 'Post') AND post.kind = 'Post'" })
      assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(count(match + " RETURN sum(1) AS n"));
  }

  /** Predicates no per-vertex filter can express leave the shapes to the row pipeline, with the same count. */
  @Test
  void predicatesThatAreNotPerVertexStayExact() {
    populate(7);
    for (final String query : new String[] {
        // two variables in one conjunct
        Q2_PROPERTIES + " WHERE comment.lang = post.lang",
        // a random function
        Q2_PROPERTIES + " WHERE rand() < 2",
        Q8_PROPERTIES + " WHERE NOT (comment)-[:HAS_TAG]->(tag1) AND tag1 <> tag2 AND comment.lang = message.lang",
        // an inline value reading another node
        "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
            + "(person1)<-[:HAS_CREATOR]-(comment:Message {kind: 'Comment'})-[:REPLY_OF]->(post:Message {lang: comment.lang})-[:HAS_CREATOR]->(person2)" })
      assertThat(count(query + " RETURN count(*) AS n")).as(query).isEqualTo(count(query + " RETURN sum(1) AS n"));
  }

  /**
   * The pair join counts the build chain between the probe's two ends: a chain written past one of them, or naming a node
   * twice, is not that shape, and the hops it would leave out are kept by the row pipeline.
   */
  @Test
  void aBuildChainRunningPastTheProbeIsNotAPairJoin() {
    populate(8);
    for (final String match : new String[] {
        "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
            + "(person1)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_CREATOR]->(person2)-[:HAS_INTEREST]->(t:Tag)",
        "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
            + "(t:Tag)<-[:HAS_INTEREST]-(person1)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_CREATOR]->(person2)",
        "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
            + "(person1)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(comment)-[:HAS_CREATOR]->(person2)" })
      assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(count(match + " RETURN sum(1) AS n"));
  }

  private void assertQ2() {
    final long labels = count(Q2_LABELS + " RETURN count(*) AS n");
    assertThat(labels).isPositive();
    for (final String match : new String[] { Q2_PROPERTIES, Q2_REORDERED }) {
      assertThat(pushDown(match + " RETURN count(*) AS n")).as(match).contains("COUNT PAIR JOIN").contains("kind: Comment");
      assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(labels);
      assertThat(count(match + " RETURN sum(1) AS n")).as(match).isEqualTo(labels);
    }
  }

  private void assertQ8() {
    final long labels = count(Q8_LABELS + Q8_NOT + " RETURN count(*) AS n");
    assertThat(labels).isPositive();
    assertThat(count(Q8_PROPERTIES + Q8_NOT + " RETURN sum(1) AS n")).isEqualTo(labels);
    for (final String tail : new String[] { Q8_NOT, Q8_OPTIONAL }) {
      final String query = Q8_PROPERTIES + tail + " RETURN count(*) AS n";
      assertThat(pushDown(query)).as(tail).contains("COUNT ANTI-JOIN CHAIN").contains("kind: Comment");
      assertThat(count(query)).as(tail).isEqualTo(labels);
    }
  }

  /** LSQB Q9 with a predicate at each position: the two-hop algebraic count, the merge-scan and the recursive walk. */
  private void assertQ9() {
    for (final String query : new String[] {
        "MATCH (p1:Person {flag: true})-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag)" + Q9_WHERE,
        "MATCH (p1:Person)-[:KNOWS]-(p2:Person {flag: false})-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag)" + Q9_WHERE,
        "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person {flag: true})-[:HAS_INTEREST]->(t:Tag)" + Q9_WHERE,
        "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag {group: 1})" + Q9_WHERE,
        // an unlabelled position takes the recursive walk
        "MATCH (p1:Person {flag: true})-[:KNOWS]-(p2 {flag: false})-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t {group: 0})" + Q9_WHERE,
        Q9 + Q9_WHERE + " AND p2.flag = true AND t.group = 2" }) {
      assertThat(pushDown(query + " RETURN count(*) AS n")).as(query).contains("COUNT ANTI-JOIN CHAIN");
      assertThat(count(query + " RETURN count(*) AS n")).as(query).isEqualTo(count(query + " RETURN sum(1) AS n"));
    }
  }

  /**
   * People who know each other and have interests, posts and comments each with a creator, comments replying to posts or
   * comments, tags on the messages. Every message carries its kind, as in the LSQB load of the issue, and a language.
   */
  private void populate(final long seed) {
    final Random random = new Random(seed);
    database.transaction(() -> {
      final List<MutableVertex> persons = new ArrayList<>();
      final List<MutableVertex> tags = new ArrayList<>();
      final List<MutableVertex> messages = new ArrayList<>();
      for (int i = 0; i < 30; i++)
        persons.add(database.newVertex("Person").set("flag", random.nextBoolean()).save());
      for (int i = 0; i < 8; i++)
        tags.add(database.newVertex("Tag").set("group", i % 3).save());
      for (final MutableVertex person : persons) {
        for (int k = random.nextInt(4); k > 0; k--)
          person.newEdge("KNOWS", persons.get(random.nextInt(persons.size())));
        for (int k = random.nextInt(3); k > 0; k--)
          person.newEdge("HAS_INTEREST", tags.get(random.nextInt(tags.size())));
      }
      for (int i = 0; i < 300; i++) {
        final boolean post = i < 80;
        final MutableVertex message = database.newVertex(post ? "Post" : "Comment").set("kind", post ? "Post" : "Comment")
            .set("lang", random.nextBoolean() ? "en" : "it").save();
        message.newEdge("HAS_CREATOR", persons.get(random.nextInt(persons.size())));
        for (int t = random.nextInt(3); t > 0; t--)
          message.newEdge("HAS_TAG", tags.get(random.nextInt(tags.size())));
        if (!post)
          message.newEdge("REPLY_OF", messages.get(random.nextInt(messages.size())));
        messages.add(message);
      }
    });
  }

  private String pushDown(final String query) {
    return pushDown(query, Map.of());
  }

  /** The count push-down part of the plan, which has to be there. */
  private String pushDown(final String query, final Map<String, Object> parameters) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query, parameters)) {
      final String plan = rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
      assertThat(plan).as("plan of %s", query).contains("Using Count Push-Down");
      final int start = plan.indexOf("+ COUNT");
      final int end = plan.indexOf("\nUsing ", start);
      return (end > 0 ? plan.substring(start, end) : plan.substring(start)).trim();
    }
  }

  private long count(final String query) {
    return count(query, Map.of());
  }

  private long count(final String query, final Map<String, Object> parameters) {
    try (final ResultSet rs = database.query("opencypher", query, parameters)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
