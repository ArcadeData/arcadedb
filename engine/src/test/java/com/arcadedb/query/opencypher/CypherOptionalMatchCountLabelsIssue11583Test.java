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
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #11583: {@code OPTIONAL MATCH (bound)-[:T]-(counted) WITH keys, count(counted)} and its chained
 * form over two OPTIONAL MATCH clauses were answered by counting the edges of the given types, whatever was at their
 * other end: a label on the counted or intermediate node, a label written as {@code WHERE c:Chunk}, a label re-stated on
 * the bound node and an inline relationship {@code WHERE} were all dropped. The counted node's label (and the chained
 * form's intermediate label) is now applied by the count; the other filters keep the statement on the general plan. Each
 * statement is checked with and without a Graph Analytical View, which the count reads when it maps the vertex.
 */
class CypherOptionalMatchCountLabelsIssue11583Test extends TestHelper {

  /** Statement, expected count of the only Entity / Record row, whether the count rewrite still answers it. */
  private static final Object[][] CASES = {
      { "MATCH (e:Entity) OPTIONAL MATCH (e)<-[:MENTIONS]-(c:Chunk) WITH e, count(c) AS occ RETURN occ", 1L, true },
      { "MATCH (e:Entity) OPTIONAL MATCH (c:Chunk)-[:MENTIONS]->(e) WITH e, count(c) AS occ RETURN occ", 1L, true },
      { "MATCH (e:Entity) OPTIONAL MATCH (e)-[:MENTIONS]-(c:Chunk) WITH e, count(c) AS occ RETURN occ", 1L, true },
      { "MATCH (e:Entity) OPTIONAL MATCH (e)<-[:MENTIONS]-(c) WHERE c:Chunk WITH e, count(c) AS occ RETURN occ", 1L, true },
      { "MATCH (e:Entity) OPTIONAL MATCH (e)<-[:MENTIONS]-(c:Nope) WITH e, count(c) AS occ RETURN occ", 0L, true },
      { "MATCH (e:Entity) OPTIONAL MATCH (e)<-[:MENTIONS]-(c) WITH e, count(c) AS occ RETURN occ", 2L, true },
      { "MATCH (e:Entity) OPTIONAL MATCH (e)<-[m:MENTIONS WHERE m.w = 1]-(c) WITH e, count(c) AS occ RETURN occ", 1L, false },
      { "MATCH (e:Entity) OPTIONAL MATCH (e:Archived)<-[:MENTIONS]-(c) WITH e, count(c) AS occ RETURN occ", 0L, false },
      { "MATCH (e:Entity) OPTIONAL MATCH (e)<-[:MENTIONS]-(c:Chunk|Note) WITH e, count(c) AS occ RETURN occ", 2L, false },
      { "MATCH (e:Entity) OPTIONAL MATCH (e)<-[:MENTIONS]-(c WHERE c.id = 3) WITH e, count(c) AS occ RETURN occ", 1L, false },
      // chained: the intermediate label of the first hop and the target label of the second
      { "MATCH (r:Record) OPTIONAL MATCH (r)<-[:OF_RECORD]-(c:Chunk) OPTIONAL MATCH (c)-[:MENTIONS]->(e:Entity) "
          + "WITH r, count(e) AS occ RETURN occ", 1L, true },
      { "MATCH (r:Record) OPTIONAL MATCH (r)<-[:OF_RECORD]-(c) OPTIONAL MATCH (c)-[:MENTIONS]->(e:Entity) "
          + "WITH r, count(e) AS occ RETURN occ", 2L, true },
      { "MATCH (r:Record) OPTIONAL MATCH (r)<-[:OF_RECORD]-(c:Chunk) OPTIONAL MATCH (c)-[:MENTIONS]->(e) "
          + "WITH r, count(e) AS occ RETURN occ", 2L, true },
      { "MATCH (r:Record) OPTIONAL MATCH (r)<-[:OF_RECORD]-(c) OPTIONAL MATCH (c)-[:MENTIONS]->(e) "
          + "WITH r, count(e) AS occ RETURN occ", 3L, true },
      { "MATCH (r:Record) OPTIONAL MATCH (r)<-[:OF_RECORD]-(c) OPTIONAL MATCH (c:Chunk)-[:MENTIONS]->(e) "
          + "WITH r, count(e) AS occ RETURN occ", 2L, false },
      { "MATCH (r:Record) OPTIONAL MATCH (r)<-[:OF_RECORD]-(c) OPTIONAL MATCH (c)-[m:MENTIONS WHERE m.w = 2]->(e) "
          + "WITH r, count(e) AS occ RETURN occ", 1L, false } };

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher", """
        CREATE (e:Entity {id: 1}), (c:Chunk {id: 2}), (n:Note {id: 3}), (r:Record {id: 4}),
               (c)-[:MENTIONS {w: 1}]->(e), (n)-[:MENTIONS {w: 2}]->(e),
               (c)-[:OF_RECORD]->(r), (n)-[:OF_RECORD]->(r), (c)-[:MENTIONS]->(:Topic {id: 5})"""));
  }

  @Test
  void countsApplyTheLabelsAndPredicatesOfThePattern() {
    for (final Object[] testCase : CASES) {
      final String query = (String) testCase[0];
      assertThat(count(query)).as(query).isEqualTo(testCase[1]);
      assertThat(plan(query).contains("COUNT EDGES OPTIMIZATION") || plan(query).contains("COUNT CHAINED EDGES OPTIMIZATION"))
          .as("rewritten: %s", query).isEqualTo(testCase[2]);
    }
  }

  @Test
  void countsApplyTheLabelsThroughAGraphAnalyticalView() {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav11583");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav11583");
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!view.isReady() && System.currentTimeMillis() < deadline)
      Thread.onSpinWait();
    assertThat(view.isReady()).isTrue();

    for (final Object[] testCase : CASES) {
      final String query = (String) testCase[0];
      assertThat(count(query)).as("with a view: %s", query).isEqualTo(testCase[1]);
    }
  }

  @Test
  void sameCountsAsTheAggregationInReturn() {
    for (final Object[] testCase : CASES) {
      final String query = ((String) testCase[0]).replace(" WITH e, count(c) AS occ RETURN occ", " RETURN count(c) AS occ")
          .replace(" WITH r, count(e) AS occ RETURN occ", " RETURN count(e) AS occ");
      assertThat(query).doesNotContain("WITH");
      assertThat(count(query)).as(query).isEqualTo(testCase[1]);
    }
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().<Number>getProperty("occ").longValue();
    }
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }
}
