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
import com.arcadedb.database.RID;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9298: the COUNT push-downs (PairHashJoinOp for LSQB Q2, PartitionedTriangleOp for Q3) counted a parallel or a
 * reciprocal KNOWS edge once, where the row pipeline matches one row per relationship. The oracle is the same pattern
 * behind a WITH (the row pipeline), which is the Cypher answer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9298CountPushDownMultiplicityTest extends TestHelper {
  private static final String Q2 = "MATCH (person1:Person)-[:KNOWS]-(person2:Person), (person1)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_CREATOR]->(person2)";
  private static final String Q3 = "MATCH (country:Country) MATCH (person1:Person)-[:IS_LOCATED_IN]->(city1:City)-[:IS_PART_OF]->(country) "
      + "MATCH (person2:Person)-[:IS_LOCATED_IN]->(city2:City)-[:IS_PART_OF]->(country) MATCH (person3:Person)-[:IS_LOCATED_IN]->(city3:City)-[:IS_PART_OF]->(country) "
      + "MATCH (person1)-[:KNOWS]-(person2)-[:KNOWS]-(person3)-[:KNOWS]-(person1)";

  private RID[] p;

  @Override
  protected void beginTest() {
    for (final String t : new String[] { "Person", "Comment", "Post", "Country", "City" })
      database.command("sql", "CREATE VERTEX TYPE " + t);
    for (final String e : new String[] { "KNOWS", "HAS_CREATOR", "REPLY_OF", "IS_LOCATED_IN", "IS_PART_OF" })
      database.command("sql", "CREATE EDGE TYPE " + e);
    p = new RID[3];
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        p[i] = database.newVertex("Person").set("id", i).save().getIdentity();
      final RID country = database.newVertex("Country").set("id", 1).save().getIdentity();
      final RID city = database.newVertex("City").set("id", 1).save().getIdentity();
      city.asVertex().newEdge("IS_PART_OF", country);
      for (int i = 0; i < 3; i++)
        p[i].asVertex().newEdge("IS_LOCATED_IN", city);
      final RID comment = database.newVertex("Comment").set("id", 10).save().getIdentity();
      final RID post = database.newVertex("Post").set("id", 20).save().getIdentity();
      comment.asVertex().newEdge("HAS_CREATOR", p[0]);
      comment.asVertex().newEdge("REPLY_OF", post);
      post.asVertex().newEdge("HAS_CREATOR", p[1]);
    });
  }

  private void knows(final int from, final int to) {
    database.transaction(() -> p[from].asVertex().newEdge("KNOWS", p[to]));
  }

  @Test
  void q2ParallelAndReciprocalKnowsCountPerRelationship() throws InterruptedException {
    knows(0, 1);
    assertAllAgree(Q2, "person1, person2, comment, post", 1L);
    knows(0, 1);
    assertAllAgree(Q2, "person1, person2, comment, post", 2L);
    knows(1, 0);
    assertAllAgree(Q2, "person1, person2, comment, post", 3L);
  }

  @Test
  void q3ParallelKnowsCountPerRelationship() throws InterruptedException {
    knows(0, 1);
    knows(1, 2);
    knows(2, 0);
    assertAllAgree(Q3, "country, person1, person2, person3, city1, city2, city3", 6L);
    knows(0, 1);
    assertAllAgree(Q3, "country, person1, person2, person3, city1, city2, city3", 12L);
  }

  @Test
  void q3ReciprocalKnowsCountPerRelationship() throws InterruptedException {
    knows(0, 1);
    knows(1, 0);
    knows(1, 2);
    knows(2, 0);
    assertAllAgree(Q3, "country, person1, person2, person3, city1, city2, city3", 12L);
  }

  private void assertAllAgree(final String match, final String withVars, final long expected) throws InterruptedException {
    assertThat(count(match + " WITH " + withVars + " RETURN count(*) AS n")).as("row pipeline").isEqualTo(expected);
    final String written = match + " RETURN count(*) AS n";
    assertThat(count(written)).as("no view").isEqualTo(expected);

    createView("narrow", "VERTEX TYPES (Person) EDGE TYPES (KNOWS) PROPERTIES (id) UPDATE MODE OFF");
    assertThat(count(written)).as("narrow view").isEqualTo(expected);
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW narrow");

    createView("wide", "VERTEX TYPES (Person, Comment, Post, Country, City) EDGE TYPES (KNOWS, HAS_CREATOR, REPLY_OF, IS_LOCATED_IN, IS_PART_OF) PROPERTIES (id) UPDATE MODE OFF");
    assertThat(count(written)).as("wide view").isEqualTo(expected);
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW wide");
  }

  private void createView(final String name, final String definition) throws InterruptedException {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW " + name + " " + definition);
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, name);
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!view.isReady() && System.currentTimeMillis() < deadline)
      Thread.sleep(20);
    assertThat(view.isReady()).isTrue();
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
