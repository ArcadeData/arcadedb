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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #8941: the count push-down read a chain whose two ends are the SAME node variable as if they
 * were two variables, so {@code MATCH (x)-[:K]->(x) RETURN count(*)} counted every K edge instead of the self-loops.
 * Each count is checked against the same question asked through {@code WITH}, which the push-down does not claim.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCountPushDownRepeatedVariableIssue8941Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("K");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V").set("name", "a").save();
      final MutableVertex b = database.newVertex("V").set("name", "b").save();
      a.newEdge("K", b);
      b.newEdge("K", a);
      a.newEdge("K", a); // the only self-loop
    });
  }

  private long count(final String query) {
    return ((Number) database.query("opencypher", query).next().getProperty("n")).longValue();
  }

  @Test
  void directedSelfLoopCountsOnlySelfLoops() {
    assertThat(count("MATCH (x:V)-[:K]->(x) RETURN count(*) AS n")).isEqualTo(1);
    assertThat(count("MATCH (x:V)-[:K]->(x) WITH x RETURN count(*) AS n")).isEqualTo(1);
  }

  @Test
  void undirectedSelfLoopMatchesOnce() {
    assertThat(count("MATCH (x:V)-[:K]-(x) RETURN count(*) AS n")).isEqualTo(1);
    assertThat(count("MATCH (x:V)-[:K]-(x) WITH x RETURN count(*) AS n")).isEqualTo(1);
  }

  @Test
  void repeatedVariableInALongerChain() {
    // x=a,y=b and x=b,y=a; x=a,y=a would need the self-loop twice, which relationship uniqueness forbids
    assertThat(count("MATCH (x:V)-[:K]->(y:V)-[:K]->(x) RETURN count(*) AS n")).isEqualTo(
        count("MATCH (x:V)-[:K]->(y:V)-[:K]->(x) WITH x RETURN count(*) AS n"));
    assertThat(count("MATCH (x:V)-[:K]->(y:V)-[:K]->(x) RETURN count(*) AS n")).isEqualTo(2);
  }

  @Test
  void everyShapeAgreesWithTheRecordPath() {
    final String[] patterns = { "(x:V)-[:K]->(y:V)-[:K]->(y)", "(x:V)-[:K]->(x)-[:K]->(y:V)", "(x)-[:K]->(y)<-[:K]-(x)", "(x)-[:K]->(x)<-[:K]-(y)",
        "(x:V)-[:K]->(y:V)-[:K]->(z:V)-[:K]->(x)", "(x:V)<-[:K]-(x)", "(x)-[:K]-(y)-[:K]-(x)", "(x:V)-[:K]->(x)-[:K]->(x)" };
    for (final String pattern : patterns)
      assertThat(count("MATCH " + pattern + " RETURN count(*) AS n")).as(pattern)
          .isEqualTo(count("MATCH " + pattern + " WITH 1 AS one RETURN count(*) AS n"));
  }

  @Test
  void starDetectorDeclinesARepeatedVariable() {
    // the labelled self-loop is what the star (degree product) detector claimed before the fix
    final String[] patterns = { "(c:V)-[:K]->(c)", "(c:V)-[:K]->(y)<-[:K]-(c)", "(c:V)-[:K]->(y)-[:K]->(y)", "(c:V)-[:K]->(y), (c)-[:K]->(y)" };
    for (final String pattern : patterns) {
      assertThat(database.query("opencypher", "EXPLAIN MATCH " + pattern + " RETURN count(*) AS n").getExecutionPlan().get().prettyPrint(0, 2))
          .as(pattern).doesNotContain("COUNT STAR JOIN");
      assertThat(count("MATCH " + pattern + " RETURN count(*) AS n")).as(pattern)
          .isEqualTo(count("MATCH " + pattern + " WITH 1 AS one RETURN count(*) AS n"));
    }
  }

  @Test
  void distinctVariablesStillUseThePushDown() {
    assertThat(count("MATCH (x:V)-[:K]->(y:V) RETURN count(*) AS n")).isEqualTo(3);
    assertThat(database.query("opencypher", "EXPLAIN MATCH (x:V)-[:K]->(y:V) RETURN count(*) AS n").getExecutionPlan().get().prettyPrint(0, 2))
        .contains("COUNT CHAIN PATHS");
  }
}
