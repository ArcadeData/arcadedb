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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9485: openCypher requires the relationships of one {@code MATCH} clause, comma separated parts included, to be
 * distinct. The pair-join count push-down counted the adjacency paths instead, so a self loop let two hops land on one
 * relationship and the count came out too high. Separate {@code MATCH} clauses have no such rule and keep counting the
 * repeated relationship.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9485CommaPatternRelationshipUniquenessTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE E");
  }

  @Test
  void graphWithoutSelfLoop() {
    build(false);
    assertThat(count("MATCH (a:P)-[:E]->(b:P)-[:E]->(c:P), (a)-[:E]->(c) RETURN count(*) AS n")).isEqualTo(1L);
    assertThat(count("MATCH (a:P)-[:E]->(b:P)-[:E]->(c:P), (a)-[:E]->(c) RETURN count(a) AS n")).isEqualTo(1L);
    assertThat(count("MATCH (a:P)-[:E]->(b:P) MATCH (b)-[:E]->(c:P) MATCH (a)-[:E]->(c) RETURN count(*) AS n")).isEqualTo(1L);
    assertThat(count("MATCH (a:P)-[:E]->(b:P), (a)-[:E]->(b) RETURN count(*) AS n")).isEqualTo(0L);
    assertThat(count("MATCH (a:P)-[:E]->(b:P) MATCH (a)-[:E]->(b) RETURN count(*) AS n")).isEqualTo(3L);
    assertThat(count("MATCH (a:P)-[:E]->(b:P)<-[:E]-(a) RETURN count(*) AS n")).isEqualTo(0L);
    assertThat(count("MATCH (a:P)-[:E]-(b:P), (a)-[:E]-(b) RETURN count(*) AS n")).isEqualTo(0L);
    assertThat(count("MATCH (a:P)-[:E]-(b:P) MATCH (a)-[:E]-(b) RETURN count(*) AS n")).isEqualTo(6L);
  }

  @Test
  void graphWithSelfLoop() {
    build(true);
    // the triple a, b, c is the only one with three distinct relationships: a>b, b>c, a>c
    assertThat(count("MATCH (a:P)-[:E]->(b:P)-[:E]->(c:P), (a)-[:E]->(c) RETURN count(*) AS n")).isEqualTo(1L);
    assertThat(count("MATCH (a:P)-[:E]->(b:P)-[:E]->(c:P), (a)-[:E]->(c) RETURN count(a) AS n")).isEqualTo(1L);
    assertThat(count("MATCH (a:P)-[r1:E]->(b:P)-[r2:E]->(c:P), (a)-[r3:E]->(c) WITH a, b, c, r1, r2, r3 RETURN count(*) AS n"))
        .isEqualTo(1L);
    // separate MATCH clauses have no uniqueness rule across them
    assertThat(count("MATCH (a:P)-[:E]->(b:P) MATCH (b)-[:E]->(c:P) MATCH (a)-[:E]->(c) RETURN count(*) AS n")).isEqualTo(4L);
    assertThat(count("MATCH (a:P)-[:E]->(b:P), (a)-[:E]->(b) RETURN count(*) AS n")).isEqualTo(0L);
    assertThat(count("MATCH (a:P)-[:E]->(b:P) MATCH (a)-[:E]->(b) RETURN count(*) AS n")).isEqualTo(4L);
    assertThat(count("MATCH (a:P)-[:E]->(b:P)<-[:E]-(a) RETURN count(*) AS n")).isEqualTo(0L);
    assertThat(count("MATCH (a:P)-[:E]-(b:P), (a)-[:E]-(b) RETURN count(*) AS n")).isEqualTo(0L);
    assertThat(count("MATCH (a:P)-[r:E]-(b:P), (a)-[s:E]-(b) WITH a, b, r, s RETURN count(*) AS n")).isEqualTo(0L);
    assertThat(count("MATCH (a:P)-[:E]-(b:P) MATCH (a)-[:E]-(b) RETURN count(*) AS n")).isEqualTo(7L);
  }

  @Test
  void commaStarOverOneEdgeTypeNeverUsesAnEdgeTwice() {
    // a star whose two arms are the same type: with the arms free to coincide, a vertex with 2 out edges counts 2 * 2 = 4
    // rows instead of the 2 ordered pairs of distinct relationships
    database.transaction(() -> {
      final MutableVertex s = database.newVertex("P").set("id", 1).save();
      s.newEdge("E", database.newVertex("P").set("id", 2).save());
      s.newEdge("E", database.newVertex("P").set("id", 3).save());
    });
    assertThat(count("MATCH (s:P)-[:E]->(t:P), (s)-[:E]->(u:P) RETURN count(*) AS n")).isEqualTo(2L);
    assertThat(count("MATCH (s:P)-[:E]->(t:P), (s)-[:E]->(u:P) RETURN count(s) AS n")).isEqualTo(2L);
    assertThat(count("MATCH (s:P)-[:E]->(t:P) MATCH (s)-[:E]->(u:P) RETURN count(*) AS n")).isEqualTo(4L);
  }

  @Test
  void disjointEdgeTypesKeepThePushDown() {
    database.command("sql", "CREATE VERTEX TYPE Comment");
    database.command("sql", "CREATE VERTEX TYPE Post");
    database.command("sql", "CREATE EDGE TYPE HAS_CREATOR");
    database.command("sql", "CREATE EDGE TYPE REPLY_OF");
    database.command("sql", "CREATE EDGE TYPE KNOWS");
    database.transaction(() -> {
      final MutableVertex p1 = database.newVertex("P").save();
      final MutableVertex p2 = database.newVertex("P").save();
      final MutableVertex post = database.newVertex("Post").save();
      final MutableVertex comment = database.newVertex("Comment").save();
      p1.newEdge("KNOWS", p2);
      comment.newEdge("HAS_CREATOR", p1);
      comment.newEdge("REPLY_OF", post);
      post.newEdge("HAS_CREATOR", p2);
    });
    final String query = "MATCH (p1:P)-[:KNOWS]-(p2:P), (p1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(po:Post)-[:HAS_CREATOR]->(p2) "
        + "RETURN count(*) AS n";
    assertThat(count(query)).isEqualTo(1L);
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("COUNT PAIR JOIN");
    }
  }

  private void build(final boolean selfLoop) {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").set("id", 1).save();
      final MutableVertex b = database.newVertex("P").set("id", 2).save();
      final MutableVertex c = database.newVertex("P").set("id", 3).save();
      a.newEdge("E", b);
      b.newEdge("E", c);
      a.newEdge("E", c);
      if (selfLoop)
        b.newEdge("E", b);
    });
    reopenDatabase();
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
