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
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9599: the same MATCH written in another order took another plan. LSQB Q2 starts from the KNOWS pair and the count
 * push-down answered it with a pair join; written from the post, as four one-hop patterns, no count push-down recognized it,
 * and it ran 17x slower. The detectors now read the MATCH as the pattern graph it describes: a cycle is split into a probe
 * hop and the chain closing it by the statistics, a chain cut into parts is joined again, and a variable's labels hold at
 * every position it is written at. So every spelling gets the same operator.
 * <p>
 * Every spelling is checked against the row pipeline, reached through a {@code sum(1)} that no push-down takes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9599PatternOrderCountPushDownTest extends TestHelper {
  /** The four relationships of LSQB Q2: from, type, to, undirected. */
  private static final String[][] Q2_PARTS = {
      { "person1", "KNOWS", "person2", "-" },
      { "comment", "HAS_CREATOR", "person1", ">" },
      { "comment", "REPLY_OF", "post", ">" },
      { "post", "HAS_CREATOR", "person2", ">" } };
  private static final String[][] Q2_LABELS = { { "person1", "Person" }, { "person2", "Person" }, { "comment", "Comment" },
      { "post", "Post" } };

  /** The three relationships of LSQB Q8's chain. */
  private static final String[][] Q8_PARTS = {
      { "message", "HAS_TAG", "tag1", ">" },
      { "comment", "REPLY_OF", "message", ">" },
      { "comment", "HAS_TAG", "tag2", ">" } };
  private static final String[][] Q8_LABELS = { { "tag1", "Tag" }, { "tag2", "Tag" }, { "comment", "Comment" },
      { "message", "Message" } };

  private static final String Q2 = "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
      + "(person1)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_CREATOR]->(person2)";
  private static final String Q2_REORDERED = "MATCH (post:Post)-[:HAS_CREATOR]->(person2:Person), (comment:Comment)-[:REPLY_OF]->(post), "
      + "(comment)-[:HAS_CREATOR]->(person1:Person), (person2)-[:KNOWS]-(person1)";

  @Override
  protected void beginTest() {
    for (final String ddl : new String[] { "CREATE VERTEX TYPE Person", "CREATE VERTEX TYPE Tag", "CREATE VERTEX TYPE Message",
        "CREATE VERTEX TYPE Post EXTENDS Message", "CREATE VERTEX TYPE Comment EXTENDS Message", "CREATE EDGE TYPE KNOWS",
        "CREATE EDGE TYPE HAS_CREATOR", "CREATE EDGE TYPE REPLY_OF", "CREATE EDGE TYPE HAS_TAG" })
      database.command("sql", ddl);
  }

  @Test
  void theReportedReorderingTakesTheSamePairJoin() {
    populate(1);
    final String plan = pushDown(Q2 + " RETURN count(*) AS n");
    assertThat(plan).contains("COUNT PAIR JOIN").contains("probe: KNOWS");
    assertThat(pushDown(Q2_REORDERED + " RETURN count(*) AS n")).isEqualTo(plan);
    assertThat(count(Q2_REORDERED + " RETURN count(*) AS n")).isEqualTo(count(Q2 + " RETURN sum(1) AS n"));
  }

  /** Every order of the four one-hop parts, each part read from either end and the labels at any of their positions. */
  @Test
  void everySpellingOfTheCycleGetsTheSameOperatorAndCount() {
    populate(2);
    final long expected = count(Q2 + " RETURN sum(1) AS n");
    final String plan = pushDown(Q2 + " RETURN count(*) AS n");
    final Random random = new Random(2);
    for (final List<String[]> order : permutations(Q2_PARTS)) {
      final String match = spell(order, Q2_LABELS, random);
      assertThat(pushDown(match + " RETURN count(*) AS n")).as(match).isEqualTo(plan);
      assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(expected);
    }
  }

  @Test
  void aViewAnswersEverySpellingAlike() {
    populate(3);
    final long expected = count(Q2 + " RETURN sum(1) AS n");
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("q2").build();
    try {
      final String plan = pushDown(Q2 + " RETURN count(*) AS n");
      final Random random = new Random(3);
      for (final List<String[]> order : permutations(Q2_PARTS)) {
        final String match = spell(order, Q2_LABELS, random);
        assertThat(pushDown(match + " RETURN count(*) AS n")).as(match).isEqualTo(plan);
        assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(expected);
      }
    } finally {
      view.drop();
    }
  }

  /** A chain cut into parts is the chain: Q8 written as three patterns, in any order, takes the anti-join. */
  @Test
  void aChainCutIntoPartsIsJoinedAgain() {
    populate(4);
    final String where = " WHERE NOT (comment)-[:HAS_TAG]->(tag1) AND tag1 <> tag2";
    final String q8 = "MATCH (tag1:Tag)<-[:HAS_TAG]-(message:Message)<-[:REPLY_OF]-(comment:Comment)-[:HAS_TAG]->(tag2:Tag)";
    final long expected = count(q8 + where + " RETURN sum(1) AS n");
    final String plan = pushDown(q8 + where + " RETURN count(*) AS n");
    assertThat(plan).contains("COUNT ANTI-JOIN CHAIN");

    final Random random = new Random(4);
    for (final List<String[]> order : permutations(Q8_PARTS)) {
      final String match = spell(order, Q8_LABELS, random);
      assertThat(pushDown(match + where + " RETURN count(*) AS n")).as(match).isEqualTo(plan);
      assertThat(count(match + where + " RETURN count(*) AS n")).as(match).isEqualTo(expected);
      // without the anti-join it is a plain chain
      assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(count(match + " RETURN sum(1) AS n"));
    }
  }

  /**
   * Over a unidirectional type only the outgoing side is stored: a chain written as outgoing parts is joined, but never read
   * from its other end, which would walk the incoming side and count nothing.
   */
  @Test
  void aChainOverAUnidirectionalTypeIsReadOnlyAlongItsStoredSide() {
    database.command("sql", "CREATE EDGE TYPE FOLLOWS UNIDIRECTIONAL");
    final Random random = new Random(6);
    database.transaction(() -> {
      final List<MutableVertex> persons = new ArrayList<>();
      for (int i = 0; i < 40; i++)
        persons.add(database.newVertex("Person").save());
      for (final MutableVertex person : persons)
        for (int k = random.nextInt(4); k > 0; k--)
          person.newEdge("FOLLOWS", persons.get(random.nextInt(persons.size())));
    });
    for (final String match : new String[] {
        "MATCH (a:Person)-[:FOLLOWS]->(b:Person), (b)-[:FOLLOWS]->(c:Person)",
        "MATCH (b)-[:FOLLOWS]->(c:Person), (a:Person)-[:FOLLOWS]->(b:Person)",
        "MATCH (b:Person)-[:FOLLOWS]->(c:Person), (c)-[:FOLLOWS]->(d:Person), (a:Person)-[:FOLLOWS]->(b)" }) {
      final long expected = count(match + " RETURN sum(1) AS n");
      assertThat(expected).isPositive();
      assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(expected);
    }
  }

  /**
   * Shapes the graph reading does not apply to, or applies to and must still decline, keep the pipeline's count: a variable
   * written with two different labels, a cycle whose two hops can bind the same relationship, a cycle with no named node to
   * probe between.
   */
  @Test
  void shapesThatAreNotOneCycleOrOneChainStayExact() {
    populate(5);
    for (final String match : new String[] {
        // comment is a Comment in one part and a Message in another
        "MATCH (person1:Person)-[:KNOWS]-(person2:Person), (comment:Comment)-[:HAS_CREATOR]->(person1), "
            + "(comment:Message)-[:REPLY_OF]->(post:Post), (post)-[:HAS_CREATOR]->(person2)",
        // a KNOWS triangle written as three parts: two hops can bind the same KNOWS edge
        "MATCH (a:Person)-[:KNOWS]-(b:Person), (b)-[:KNOWS]-(c:Person), (c)-[:KNOWS]-(a)",
        // a square of KNOWS, the pair join's shape, where relationship uniqueness is what the count is about
        "MATCH (a:Person)-[:KNOWS]->(b:Person), (b)-[:KNOWS]->(c:Person), (c)-[:KNOWS]->(d:Person), (d)-[:KNOWS]->(a)",
        // a cycle and a hop hanging off it
        "MATCH (person1:Person)-[:KNOWS]-(person2:Person), (person1)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post), "
            + "(post)-[:HAS_CREATOR]->(person2), (post)-[:HAS_TAG]->(t:Tag)" })
      assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(count(match + " RETURN sum(1) AS n"));
  }

  /**
   * People who know each other, posts and comments each with a creator, comments replying to posts or comments, tags on the
   * messages. KNOWS is the smallest relation, as in LSQB, so it is the cheapest probe.
   */
  private void populate(final long seed) {
    final Random random = new Random(seed);
    database.transaction(() -> {
      final List<MutableVertex> persons = new ArrayList<>();
      final List<MutableVertex> tags = new ArrayList<>();
      final List<MutableVertex> messages = new ArrayList<>();
      for (int i = 0; i < 30; i++)
        persons.add(database.newVertex("Person").save());
      for (int i = 0; i < 8; i++)
        tags.add(database.newVertex("Tag").save());
      for (final MutableVertex person : persons)
        for (int k = random.nextInt(3); k > 0; k--)
          person.newEdge("KNOWS", persons.get(random.nextInt(persons.size())));
      for (int i = 0; i < 300; i++) {
        final boolean post = i < 80;
        final MutableVertex message = database.newVertex(post ? "Post" : "Comment").save();
        message.newEdge("HAS_CREATOR", persons.get(random.nextInt(persons.size())));
        for (int t = random.nextInt(3); t > 0; t--)
          message.newEdge("HAS_TAG", tags.get(random.nextInt(tags.size())));
        if (!post)
          message.newEdge("REPLY_OF", messages.get(random.nextInt(messages.size())));
        messages.add(message);
      }
    });
  }

  /**
   * The parts as one MATCH, each read from a random end, every variable labelled at one random position of the ones it is
   * written at.
   */
  private static String spell(final List<String[]> parts, final String[][] labels, final Random random) {
    final List<String[]> oriented = new ArrayList<>(parts.size());
    for (final String[] part : parts)
      oriented.add(random.nextBoolean() ? part : new String[] { part[2], part[1], part[0], part[3].equals(">") ? "<" : part[3] });

    final List<int[]> positions = new ArrayList<>();
    final String[][] nodeLabels = new String[oriented.size()][2];
    for (final String[] variable : labels) {
      positions.clear();
      for (int p = 0; p < oriented.size(); p++)
        for (int side = 0; side < 2; side++)
          if (oriented.get(p)[side * 2].equals(variable[0]))
            positions.add(new int[] { p, side });
      final int[] at = positions.get(random.nextInt(positions.size()));
      nodeLabels[at[0]][at[1]] = variable[1];
    }

    final StringBuilder match = new StringBuilder("MATCH ");
    for (int p = 0; p < oriented.size(); p++) {
      final String[] part = oriented.get(p);
      if (p > 0)
        match.append(", ");
      match.append('(').append(part[0]).append(nodeLabels[p][0] != null ? ":" + nodeLabels[p][0] : "").append(')');
      match.append(switch (part[3]) {
        case ">" -> "-[:" + part[1] + "]->";
        case "<" -> "<-[:" + part[1] + "]-";
        default -> "-[:" + part[1] + "]-";
      });
      match.append('(').append(part[2]).append(nodeLabels[p][1] != null ? ":" + nodeLabels[p][1] : "").append(')');
    }
    return match.toString();
  }

  private static List<List<String[]>> permutations(final String[][] parts) {
    final List<List<String[]>> result = new ArrayList<>();
    permute(new ArrayList<>(List.of(parts)), 0, result);
    return result;
  }

  private static void permute(final List<String[]> parts, final int from, final List<List<String[]>> result) {
    if (from == parts.size()) {
      result.add(new ArrayList<>(parts));
      return;
    }
    for (int i = from; i < parts.size(); i++) {
      Collections.swap(parts, from, i);
      permute(parts, from + 1, result);
      Collections.swap(parts, from, i);
    }
  }

  /** The count push-down part of the plan, which has to be there. */
  private String pushDown(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      final String plan = rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
      assertThat(plan).as("plan of %s", query).contains("Using Count Push-Down");
      final int start = plan.indexOf("+ COUNT");
      final int end = plan.indexOf("\nUsing ", start);
      return (end > 0 ? plan.substring(start, end) : plan.substring(start)).trim();
    }
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
