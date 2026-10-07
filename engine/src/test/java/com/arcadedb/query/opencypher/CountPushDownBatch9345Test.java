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
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the wrong answers of the openCypher count push-downs and of the Graph Analytical View fast paths:
 * #9341 (algo.* count-only), #9345 (anti-join inequality order), #9349 (ExpandInto on the same vertex),
 * #9350 (partitioned triangles), #9377 (view edge coverage of pattern predicates).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CountPushDownBatch9345Test extends TestHelper {

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().<Number>getProperty("n").longValue();
    }
  }

  private GraphAnalyticalView createView(final String name, final String vertexTypes, final String edgeTypes) throws Exception {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW " + name + " VERTEX TYPES (" + vertexTypes + ") EDGE TYPES (" + edgeTypes
        + ") UPDATE MODE OFF");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, name);
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    return view;
  }

  // #9341
  @Test
  void algoAggregatesOverAYieldedColumnUnderAView() throws Exception {
    database.getSchema().createVertexType("P");
    database.getSchema().createEdgeType("K");
    database.transaction(() -> {
      final MutableVertex[] v = new MutableVertex[5];
      for (int i = 0; i < 5; i++)
        v[i] = database.newVertex("P").set("id", i).save();
      v[0].newEdge("K", v[1]).save();
      v[1].newEdge("K", v[2]).save();
      v[3].newEdge("K", v[4]).save();
    });
    final String[] queries = {
        "CALL algo.wcc() YIELD node, componentId RETURN count(DISTINCT componentId) AS n",
        "CALL algo.wcc() YIELD node, componentId RETURN count(node) AS n",
        "CALL algo.wcc() YIELD node, componentId RETURN count(*) AS n",
        "CALL algo.labelpropagation() YIELD node, communityId RETURN count(DISTINCT communityId) AS n" };
    final long[] expected = new long[queries.length];
    for (int i = 0; i < queries.length; i++)
      expected[i] = count(queries[i]);
    assertThat(expected[0]).isEqualTo(2);
    assertThat(expected[1]).isEqualTo(5);

    createView("g9341", "P", "K");
    try {
      for (int i = 0; i < queries.length; i++)
        assertThat(count(queries[i])).as(queries[i]).isEqualTo(expected[i]);
      assertThat(count("CALL algo.wcc() YIELD node, componentId RETURN max(componentId) AS n")).isGreaterThanOrEqualTo(1);
      try (final ResultSet rs = database.query("opencypher", "CALL algo.pagerank() YIELD node, score RETURN sum(score) AS n")) {
        assertThat(rs.next().<Number>getProperty("n").doubleValue()).isCloseTo(1.0, org.assertj.core.data.Offset.offset(1e-6));
      }
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW g9341");
    }
  }

  // #9345
  @Test
  void antiJoinChainUnlabelledFirstNodeAcceptsEitherInequalityOrder() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE VERTEX TYPE Tag");
    database.command("sql", "CREATE EDGE TYPE KNOWS");
    database.command("sql", "CREATE EDGE TYPE HAS_INTEREST");
    database.transaction(() -> {
      final RID a = database.newVertex("Person").save().getIdentity();
      final RID b = database.newVertex("Person").save().getIdentity();
      final RID t = database.newVertex("Tag").save().getIdentity();
      b.asVertex().newEdge("KNOWS", a).save();
      a.asVertex().newEdge("HAS_INTEREST", t).save();
      b.asVertex().newEdge("HAS_INTEREST", t).save();
    });
    final String first = "MATCH (p0)-[:KNOWS]-(p1:Person)-[:KNOWS]-(p2:Person)-[:HAS_INTEREST]->(t:Tag) ";
    for (final String where : new String[] { "WHERE NOT (p0)-[:KNOWS]-(p2) AND p2 <> p0 ", "WHERE NOT (p0)-[:KNOWS]-(p2) AND p0 <> p2 ",
        "WHERE NOT (p0)-[:KNOWS]-(p2) AND id(p2) <> id(p0) ", "WHERE NOT (p0)-[:KNOWS]->(p2) AND p2 <> p0 " }) {
      assertThat(count(first + where + "RETURN count(*) AS n")).as(where).isEqualTo(0);
      assertThat(count(first + "WITH p0, p1, p2, t " + where + "RETURN count(*) AS n")).as("rows " + where).isEqualTo(0);
    }
  }

  // #9349
  @Test
  void undirectedExpandIntoOnTheSameVertexMatchesOnlySelfLoops() throws Exception {
    for (final String t : new String[] { "Person", "Comment", "Post" })
      database.getSchema().createVertexType(t);
    for (final String t : new String[] { "KNOWS", "HAS_CREATOR", "REPLY_OF" })
      database.getSchema().createEdgeType(t);
    final int persons = 40, degree = 9, density = 7;
    database.transaction(() -> {
      final MutableVertex[] p = new MutableVertex[persons];
      for (int i = 0; i < persons; i++)
        p[i] = database.newVertex("Person").set("id", i).save();
      for (int i = 1; i <= degree; i++)
        p[0].newEdge("KNOWS", p[i]).save();
      for (int i = degree + 1; i < persons; i++)
        for (int d = 1; d <= density && i + d < persons; d++)
          p[i].newEdge("KNOWS", p[i + d]).save();
      final MutableVertex comment = database.newVertex("Comment").set("id", 1).save(), post = database.newVertex("Post").set("id", 1).save();
      comment.newEdge("HAS_CREATOR", p[0]).save();
      comment.newEdge("REPLY_OF", post).save();
      post.newEdge("HAS_CREATOR", p[0]).save();
      final MutableVertex comment2 = database.newVertex("Comment").set("id", 2).save(), post2 = database.newVertex("Post").set("id", 2).save();
      comment2.newEdge("HAS_CREATOR", p[0]).save();
      comment2.newEdge("REPLY_OF", post2).save();
      post2.newEdge("HAS_CREATOR", p[1]).save();
    });
    final String pattern = "MATCH (person1:Person)-[:KNOWS]-(person2:Person), (person1)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_CREATOR]->(person2)";
    final String rows = pattern + " WITH person1, person2, comment, post RETURN count(*) AS n";
    assertThat(count(rows)).as("no view").isEqualTo(1);
    createView("g9349", "Person, Comment, Post", "KNOWS, HAS_CREATOR, REPLY_OF");
    try {
      assertThat(count(rows)).as("view over every type").isEqualTo(1);
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW g9349");
    }
  }

  // #9350
  @Test
  void partitionedTrianglesCountEveryPathOfThePartitionChain() throws Exception {
    final String q3 = "MATCH (country:Country) MATCH (person1:Person)-[:IS_LOCATED_IN]->(city1:City)-[:IS_PART_OF]->(country) MATCH (person2:Person)-[:IS_LOCATED_IN]->(city2:City)-[:IS_PART_OF]->(country) "
        + "MATCH (person3:Person)-[:IS_LOCATED_IN]->(city3:City)-[:IS_PART_OF]->(country) MATCH (person1)-[:KNOWS]-(person2)-[:KNOWS]-(person3)-[:KNOWS]-(person1)";
    final String vars = "country, person1, person2, person3, city1, city2, city3";
    final long[] expected = { 12, 6, 12 };
    final String[] cases = { "A", "B", "C" };
    for (int c = 0; c < cases.length; c++) {
      for (final String t : new String[] { "Person", "Country", "City" })
        database.getSchema().createVertexType(t);
      for (final String t : new String[] { "KNOWS", "IS_LOCATED_IN", "IS_PART_OF" })
        database.getSchema().createEdgeType(t);
      final String kase = cases[c];
      database.transaction(() -> {
        final MutableVertex x = database.newVertex("Country").set("id", 1).save(), y = database.newVertex("Country").set("id", 2).save();
        final MutableVertex cityX1 = database.newVertex("City").set("id", 1).save(), cityX2 = database.newVertex("City").set("id", 2).save(),
            cityY = database.newVertex("City").set("id", 3).save();
        final MutableVertex[] p = new MutableVertex[3];
        for (int i = 0; i < 3; i++)
          p[i] = database.newVertex("Person").set("id", i).save();
        switch (kase) {
        case "A":
          cityX1.newEdge("IS_PART_OF", x).save();
          cityX2.newEdge("IS_PART_OF", x).save();
          p[0].newEdge("IS_LOCATED_IN", cityX1).save();
          p[0].newEdge("IS_LOCATED_IN", cityX2).save();
          break;
        case "B":
          cityY.newEdge("IS_PART_OF", y).save();
          cityX1.newEdge("IS_PART_OF", x).save();
          p[0].newEdge("IS_LOCATED_IN", cityY).save();
          p[0].newEdge("IS_LOCATED_IN", cityX1).save();
          break;
        default:
          cityX1.newEdge("IS_PART_OF", x).save();
          cityX1.newEdge("IS_PART_OF", y).save();
          p[0].newEdge("IS_LOCATED_IN", cityX1).save();
        }
        p[1].newEdge("IS_LOCATED_IN", cityX1).save();
        p[2].newEdge("IS_LOCATED_IN", cityX1).save();
        p[0].newEdge("KNOWS", p[1]).save();
        p[1].newEdge("KNOWS", p[2]).save();
        p[2].newEdge("KNOWS", p[0]).save();
      });
      final String label = "case " + kase;
      assertThat(count(q3 + " WITH " + vars + " RETURN count(*) AS n")).as(label + " row pipeline").isEqualTo(expected[c]);
      assertThat(count(q3 + " RETURN count(*) AS n")).as(label + " no view").isEqualTo(expected[c]);
      createView("narrow9350", "Person", "KNOWS");
      try {
        assertThat(count(q3 + " RETURN count(*) AS n")).as(label + " view over Person and KNOWS").isEqualTo(expected[c]);
      } finally {
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW narrow9350");
      }
      createView("wide9350", "Person, Country, City", "KNOWS, IS_LOCATED_IN, IS_PART_OF");
      try {
        assertThat(count(q3 + " RETURN count(*) AS n")).as(label + " view over every type").isEqualTo(expected[c]);
      } finally {
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW wide9350");
      }
      database.transaction(() -> {
        for (final String t : new String[] { "Person", "Country", "City" })
          database.command("sql", "DELETE FROM " + t);
        for (final String t : new String[] { "KNOWS", "IS_LOCATED_IN", "IS_PART_OF" })
          database.command("sql", "DELETE FROM " + t);
      });
      for (final String t : new String[] { "KNOWS", "IS_LOCATED_IN", "IS_PART_OF", "Person", "Country", "City" })
        database.getSchema().dropType(t);
    }
  }

  // #9377
  @Test
  void patternPredicateOverAnEdgeTypeTheViewDoesNotList() throws Exception {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE EDGE TYPE E");
    database.command("sql", "CREATE EDGE TYPE F");
    database.transaction(() -> {
      final RID x = database.newVertex("V").save().getIdentity(), y = database.newVertex("V").save().getIdentity(),
          z = database.newVertex("V").save().getIdentity();
      x.asVertex().newEdge("E", y).save();
      x.asVertex().newEdge("F", y).save();
      y.asVertex().newEdge("E", z).save();
    });
    final String[][] qs = {
        { "MATCH (a:V)-[:E]->(b:V) WHERE NOT (a)-[:F]->(b) RETURN count(*) AS n", "1" },
        { "MATCH (a:V)-[:E]->(b:V) WHERE (a)-[:F]->(b) RETURN count(*) AS n", "1" },
        { "MATCH (a:V)-[:E]->(b:V)-[:E]->(c:V) WHERE NOT (a)-[:F]->(b) RETURN count(*) AS n", "0" },
        { "MATCH (a:V)-[:E]->(b:V) WITH a, b WHERE NOT (a)-[:F]->(b) RETURN count(*) AS n", "1" } };
    for (final String edgeTypes : new String[] { "none", "E", "E, F" }) {
      if (!"none".equals(edgeTypes))
        createView("g9377", "V", edgeTypes);
      try {
        for (final String[] q : qs)
          assertThat(count(q[0])).as("view over (" + edgeTypes + ") " + q[0]).isEqualTo(Long.parseLong(q[1]));
      } finally {
        if (!"none".equals(edgeTypes))
          database.command("sql", "DROP GRAPH ANALYTICAL VIEW g9377");
      }
    }
    assertThat(GraphTraversalProviderRegistry.getProviders(database)).isEmpty();
  }

  // #9378
  @Test
  void lightEdgesOfAnUndeclaredTypeAreRefusedAndDeclaredOnesCountRight() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE EDGE TYPE E");
    database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT");
    try (final GraphBatch batch = database.batch().withLightEdges(true).build()) {
      final RID[] v = batch.createVertices("V", 3);
      assertThatThrownBy(() -> batch.newEdge(v[0], "E", v[1])).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("LIGHTWEIGHT");
      batch.newEdge(v[0], "L", v[1]);
      batch.newEdge(v[1], "L", v[2]);
    }
    assertThat(count("MATCH (a:V)-[:L]->(b:V) RETURN count(*) AS n")).isEqualTo(2);
    assertThat(count("MATCH (a:V)-[:E]->(b:V) RETURN count(*) AS n")).isEqualTo(0);

    try (final GraphBatch batch = database.batch().withLightEdges(false).build()) {
      final RID[] v = batch.createVertices("V", 2);
      batch.newEdge(v[0], "E", v[1]);
    }
    assertThat(count("MATCH (a:V)-[:E]->(b:V) RETURN count(*) AS n")).isEqualTo(1);
  }

  // #9350: an unambiguous chain takes the fast path, an ambiguous one the weighted path, and both count a triangle the same way
  @Test
  void weightedAndFastPartitionPathsAgreeOnAnUnambiguousGraph() throws Exception {
    final String q3 = "MATCH (country:Country) MATCH (person1:Person)-[:IS_LOCATED_IN]->(city1:City)-[:IS_PART_OF]->(country) MATCH (person2:Person)-[:IS_LOCATED_IN]->(city2:City)-[:IS_PART_OF]->(country) "
        + "MATCH (person3:Person)-[:IS_LOCATED_IN]->(city3:City)-[:IS_PART_OF]->(country) MATCH (person1)-[:KNOWS]-(person2)-[:KNOWS]-(person3)-[:KNOWS]-(person1) RETURN count(*) AS n";
    for (final String t : new String[] { "Person", "Country", "City" })
      database.getSchema().createVertexType(t);
    for (final String t : new String[] { "KNOWS", "IS_LOCATED_IN", "IS_PART_OF" })
      database.getSchema().createEdgeType(t);
    database.transaction(() -> {
      final MutableVertex x = database.newVertex("Country").set("id", 1).save();
      final MutableVertex city = database.newVertex("City").set("id", 1).save();
      city.newEdge("IS_PART_OF", x).save();
      final MutableVertex[] p = new MutableVertex[3];
      for (int i = 0; i < 3; i++) {
        p[i] = database.newVertex("Person").set("id", i).save();
        p[i].newEdge("IS_LOCATED_IN", city).save();
      }
      p[0].newEdge("KNOWS", p[1]).save();
      p[1].newEdge("KNOWS", p[2]).save();
      p[2].newEdge("KNOWS", p[0]).save();
    });
    assertThat(count(q3)).as("no view").isEqualTo(6);
    createView("wide9350u", "Person, Country, City", "KNOWS, IS_LOCATED_IN, IS_PART_OF");
    try {
      assertThat(count(q3)).as("fast path under a view").isEqualTo(6);
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW wide9350u");
    }
    // a second city in the same country for one person makes the chain ambiguous: that person has two paths
    database.transaction(() -> {
      final MutableVertex city2 = database.newVertex("City").set("id", 2).save();
      city2.newEdge("IS_PART_OF", database.query("sql", "SELECT FROM Country").next().getVertex().get()).save();
      database.query("sql", "SELECT FROM Person WHERE id = 0").next().getVertex().get().newEdge("IS_LOCATED_IN", city2).save();
    });
    assertThat(count(q3)).as("weighted path, no view").isEqualTo(12);
    createView("wide9350v", "Person, Country, City", "KNOWS, IS_LOCATED_IN, IS_PART_OF");
    try {
      assertThat(count(q3)).as("weighted path under a view").isEqualTo(12);
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW wide9350v");
    }
  }
}
