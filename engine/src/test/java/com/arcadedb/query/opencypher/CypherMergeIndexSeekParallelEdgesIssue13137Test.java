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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #13137: a relationship MERGE with one end bound and the other an unbound node pattern found through an
 * index answered ONE relationship between the two nodes, where the same pattern without the index, and Neo4j 5.26, answer one row per
 * relationship: a RETURN had too few rows and an ON MATCH SET or a SET after the MERGE reached only the first of them. Type {@code E}
 * has an index on {@code k}, type {@code F} has none and is the control: both must answer alike.
 */
class CypherMergeIndexSeekParallelEdgesIssue13137Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE E");
      database.command("sql", "CREATE PROPERTY E.k STRING");
      database.command("sql", "CREATE INDEX ON E (k) NOTUNIQUE");
      database.command("sql", "CREATE VERTEX TYPE F");
      for (final String t : new String[] { "E", "F" }) {
        database.command("opencypher", "CREATE (:" + t + " {k: 'ka'}), (:" + t + " {k: 'kb'}), (:" + t + " {k: 'kc'})");
        // a -> b twice, b -> a once, a self-loop pair on c
        database.command("opencypher", "MATCH (a:" + t + " {k: 'ka'}), (b:" + t + " {k: 'kb'}) CREATE (a)-[:R" + t + " {i: 1}]->(b), (a)-[:R" + t
            + " {i: 2}]->(b), (b)-[:R" + t + " {i: 3}]->(a)");
        database.command("opencypher", "MATCH (c:" + t + " {k: 'kc'}) CREATE (c)-[:R" + t + " {i: 4}]->(c), (c)-[:R" + t + " {i: 5}]->(c)");
      }
    });
  }

  @Test
  void aBoundLeftEndAnswersEveryParallelRelationship() {
    for (final String t : new String[] { "F", "E" })
      assertThat(ints("MATCH (a:" + t + " {k: 'ka'}) MERGE (a)-[r:R" + t + "]->(b:" + t + " {k: 'kb'}) RETURN r.i AS v")).as(t).containsExactlyInAnyOrder(1L, 2L);
  }

  @Test
  void aBoundRightEndAnswersEveryParallelRelationship() {
    for (final String t : new String[] { "F", "E" }) {
      assertThat(ints("MATCH (b:" + t + " {k: 'kb'}) MERGE (a:" + t + " {k: 'ka'})-[r:R" + t + "]->(b) RETURN r.i AS v")).as(t).containsExactlyInAnyOrder(1L, 2L);
      assertThat(ints("MATCH (b:" + t + " {k: 'kb'}) MERGE (a:" + t + " {k: 'ka'})<-[r:R" + t + "]-(b) RETURN r.i AS v")).as(t).containsExactlyInAnyOrder(3L);
    }
  }

  @Test
  void anUndirectedPatternAnswersTheRelationshipsOfBothDirections() {
    for (final String t : new String[] { "F", "E" })
      assertThat(ints("MATCH (a:" + t + " {k: 'ka'}) MERGE (a)-[r:R" + t + "]-(b:" + t + " {k: 'kb'}) RETURN r.i AS v")).as(t).containsExactlyInAnyOrder(1L, 2L, 3L);
  }

  @Test
  void aSelfLoopIsOneMatchWhateverTheDirection() {
    for (final String t : new String[] { "F", "E" })
      assertThat(ints("MATCH (c:" + t + " {k: 'kc'}) MERGE (c)-[r:R" + t + "]->(d:" + t + " {k: 'kc'}) RETURN r.i AS v")).as(t).containsExactlyInAnyOrder(4L, 5L);
    // the undirected pattern meets a self-loop from both sides: one match each. Only the indexed end is asserted: the walk of an
    // unindexed end answers a self-loop twice until #13122 (issue #13117) is merged
    assertThat(ints("MATCH (c:E {k: 'kc'}) MERGE (c)-[r:RE]-(d:E {k: 'kc'}) RETURN r.i AS v")).containsExactlyInAnyOrder(4L, 5L);
  }

  @Test
  void aRelationshipPropertyKeepsTheMatchingOnesOnly() {
    for (final String t : new String[] { "F", "E" }) {
      assertThat(ints("MATCH (a:" + t + " {k: 'ka'}) MERGE (a)-[r:R" + t + " {i: 2}]->(b:" + t + " {k: 'kb'}) RETURN r.i AS v")).as(t).containsExactly(2L);
      assertThat(count("MATCH ()-[r:R" + t + "]->() RETURN count(r) AS v")).as(t).isEqualTo(5L);
    }
  }

  @Test
  void onMatchSetAndSetReachEveryParallelRelationship() {
    for (final String t : new String[] { "F", "E" }) {
      final List<Long> rows = ints("MATCH (a:" + t + " {k: 'ka'}) MERGE (a)-[r:R" + t + "]->(b:" + t + " {k: 'kb'}) ON MATCH SET r.m = 1 RETURN r.i AS v");
      assertThat(rows).as(t).containsExactlyInAnyOrder(1L, 2L);
      assertThat(ints("MATCH ()-[r:R" + t + "]->() WHERE r.m = 1 RETURN r.i AS v")).as(t).containsExactlyInAnyOrder(1L, 2L);

      ints("MATCH (a:" + t + " {k: 'ka'}) MERGE (a)-[r:R" + t + "]->(b:" + t + " {k: 'kb'}) SET r.s = 2 RETURN r.i AS v");
      assertThat(ints("MATCH ()-[r:R" + t + "]->() WHERE r.s = 2 RETURN r.i AS v")).as(t).containsExactlyInAnyOrder(1L, 2L);
    }
  }

  @Test
  void anEndSharedByTheParallelMatchesSeesTheWriteOfThePreviousOne() {
    database.transaction(() -> {
      database.command("opencypher", "MATCH (b:E {k: 'kb'}) SET b.hits = 0");
      database.command("opencypher", "MATCH (b:F {k: 'kb'}) SET b.hits = 0");
    });
    for (final String t : new String[] { "F", "E" }) {
      ints("MATCH (a:" + t + " {k: 'ka'}) MERGE (a)-[r:R" + t + "]->(b:" + t + " {k: 'kb'}) ON MATCH SET b.hits = b.hits + 1 RETURN r.i AS v");
      assertThat(ints("MATCH (b:" + t + " {k: 'kb'}) RETURN b.hits AS v")).as(t).containsExactly(2L);
    }
  }

  @Test
  void aPathVariableIsBoundOncePerRelationship() {
    for (final String t : new String[] { "F", "E" })
      assertThat(ints("MATCH (a:" + t + " {k: 'ka'}) MERGE p = (a)-[r:R" + t + "]->(b:" + t + " {k: 'kb'}) RETURN length(p) AS v")).as(t).containsExactly(1L, 1L);
  }

  @Test
  void aMissingRelationshipIsCreatedOnceAndFoundAgain() {
    for (final String t : new String[] { "F", "E" }) {
      for (int i = 0; i < 2; i++)
        assertThat(ints("MATCH (a:" + t + " {k: 'ka'}) MERGE (a)-[r:R" + t + " {i: 9}]->(b:" + t + " {k: 'kb'}) RETURN r.i AS v")).as(t).containsExactly(9L);
      assertThat(count("MATCH ()-[r:R" + t + "]->() RETURN count(r) AS v")).as(t).isEqualTo(6L);
    }
  }

  private long count(final String query) {
    return ints(query).getFirst();
  }

  private List<Long> ints(final String query) {
    final List<Long> values = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          values.add(((Number) row.getProperty("v")).longValue());
        }
      }
    });
    return values;
  }
}
