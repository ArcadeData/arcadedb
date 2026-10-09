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
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9597: a {@code WITH} that only passes variables on - no aggregation, {@code DISTINCT}, {@code ORDER BY},
 * {@code SKIP}, {@code LIMIT} or {@code WHERE} - leaves the number of rows alone, but it took a count-only statement off
 * every count push-down: LSQB Q7 with {@code WITH message, creator} went from 0.008 s to 0.875 s. Such a {@code WITH} is
 * now transparent to the push-downs, aliases included.
 * <p>
 * Every count is checked against the row pipeline, reached through {@code RETURN sum(1)} which no count push-down
 * answers.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9597PassThroughWithCountTest extends TestHelper {
  private static final String PUSH_DOWN = "Using Count Push-Down";

  private static final String Q7_HEAD = "MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person) ";
  private static final String Q7_TAIL =
      "OPTIONAL MATCH (message)<-[:LIKES]-(liker:Person) OPTIONAL MATCH (message)<-[:REPLY_OF]-(comment:Message)";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Tag");
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE VERTEX TYPE Message");
    database.command("sql", "CREATE VERTEX TYPE Comment EXTENDS Message");
    database.command("sql", "CREATE EDGE TYPE HAS_TAG");
    database.command("sql", "CREATE EDGE TYPE HAS_CREATOR");
    database.command("sql", "CREATE EDGE TYPE LIKES");
    database.command("sql", "CREATE EDGE TYPE REPLY_OF");
    final Random random = new Random(9597);
    database.transaction(() -> {
      final List<Vertex> tags = new ArrayList<>();
      final List<Vertex> persons = new ArrayList<>();
      for (int i = 0; i < 20; i++)
        tags.add(database.newVertex("Tag").save());
      for (int i = 0; i < 30; i++)
        persons.add(database.newVertex("Person").set("id", i).save());
      final List<Vertex> messages = new ArrayList<>();
      for (int i = 0; i < 200; i++) {
        final MutableVertex m = database.newVertex(i % 3 == 0 ? "Comment" : "Message").set("id", i).save();
        messages.add(m);
        for (int k = random.nextInt(3); k > 0; k--)
          m.newEdge("HAS_TAG", tags.get(random.nextInt(tags.size())));
        if (random.nextInt(6) > 0)
          m.newEdge("HAS_CREATOR", persons.get(random.nextInt(persons.size())));
        for (int k = random.nextInt(4); k > 0; k--)
          persons.get(random.nextInt(persons.size())).modify().newEdge("LIKES", m);
        if (i > 0 && random.nextBoolean())
          m.newEdge("REPLY_OF", messages.get(random.nextInt(i)));
      }
    });
  }

  @Test
  void aPassThroughWithKeepsTheStarCount() {
    assertPushedDownAndExact(Q7_HEAD + "WITH message, creator " + Q7_TAIL);
    assertPushedDownAndExact(Q7_HEAD + "WITH * " + Q7_TAIL);
    assertPushedDownAndExact(Q7_HEAD + "WITH message " + Q7_TAIL);
    assertPushedDownAndExact(Q7_HEAD + "WITH message AS m, creator "
        + "OPTIONAL MATCH (m)<-[:LIKES]-(liker:Person) OPTIONAL MATCH (m)<-[:REPLY_OF]-(comment:Comment)");
    assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(message:Message) WITH message AS m WITH m AS x "
        + "MATCH (x)-[:HAS_CREATOR]->(:Person)");
  }

  @Test
  void aPassThroughWithKeepsTheChainCount() {
    assertPushedDownAndExact("MATCH (t:Tag)<-[:HAS_TAG]-(m:Message) WITH m MATCH (m)-[:HAS_CREATOR]->(p:Person)");
    assertPushedDownAndExact("MATCH (t:Tag)<-[:HAS_TAG]-(m:Message) WITH t, m RETURN count(*) AS n", false);
  }

  @Test
  void aWithThatChangesTheRowsIsNotTransparent() {
    for (final String with : new String[] { "WITH DISTINCT message, creator ", "WITH message, creator LIMIT 5 ",
        "WITH message, creator WHERE creator.id > 10 ", "WITH message, creator ORDER BY creator.id SKIP 3 ",
        "WITH message, count(*) AS c ", "WITH message, creator, 1 AS one ", "WITH message, creator.id AS cid " }) {
      final String match = Q7_HEAD + with + Q7_TAIL;
      assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(count(match + " RETURN sum(1) AS n"));
    }
  }

  @Test
  void aNameTheWithDropsIsAFreshVariableAfterIt() {
    // after WITH message, creator the name 'creator2' below is new, and so is a name the WITH dropped
    final String reused = "MATCH (p:Person)<-[:HAS_CREATOR]-(message:Message)-[:HAS_TAG]->(t:Tag) WITH message "
        + "MATCH (message)<-[:LIKES]-(p:Person)";
    assertThat(count(reused + " RETURN count(*) AS n")).isEqualTo(count(reused + " RETURN sum(1) AS n"));
    // concatenated, the two 'a' would be one variable and the star count would answer the likes alone
    final String fresh = "MATCH (a:Person)-[:LIKES]->(m:Message) WITH m MATCH (a:Person)";
    assertThat(count(fresh + " RETURN count(*) AS n")).isEqualTo(count(fresh + " RETURN sum(1) AS n"))
        .isEqualTo(count("MATCH (:Person)-[:LIKES]->(:Message) RETURN count(*) AS n") * 30);

    // an alias onto a name the WITH dropped binds that name to the projected value only
    final String swapped = "MATCH (message:Message)-[:HAS_CREATOR]->(creator:Person) WITH message AS creator "
        + "MATCH (creator)<-[:LIKES]-(liker:Person)";
    assertThat(count(swapped + " RETURN count(*) AS n")).isEqualTo(count(swapped + " RETURN sum(1) AS n"));
  }

  @Test
  void theSameAnswersOverAGraphAnalyticalView() throws InterruptedException {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW social VERTEX TYPES (Tag, Person, Message, Comment) EDGE TYPES "
        + "(HAS_TAG, HAS_CREATOR, LIKES, REPLY_OF) UPDATE MODE OFF");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "social");
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!view.isReady() && System.currentTimeMillis() < deadline)
      Thread.sleep(20);
    assertThat(view.isReady()).isTrue();
    try {
      aPassThroughWithKeepsTheStarCount();
      aPassThroughWithKeepsTheChainCount();
      aWithThatChangesTheRowsIsNotTransparent();
      aNameTheWithDropsIsAFreshVariableAfterIt();
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW social");
    }
  }

  private void assertPushedDownAndExact(final String match) {
    assertPushedDownAndExact(match, true);
  }

  private void assertPushedDownAndExact(final String statement, final boolean appendReturn) {
    final String query = appendReturn ? statement + " RETURN count(*) AS n" : statement;
    final String pipeline = appendReturn ? statement + " RETURN sum(1) AS n" : statement.replace("count(*)", "sum(1)");
    assertThat(plan(query)).as("plan of %s", query).contains(PUSH_DOWN);
    assertThat(count(query)).as(query).isEqualTo(count(pipeline));
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
    }
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      final Object value = rs.next().getProperty("n");
      return value == null ? 0L : ((Number) value).longValue();
    }
  }
}
