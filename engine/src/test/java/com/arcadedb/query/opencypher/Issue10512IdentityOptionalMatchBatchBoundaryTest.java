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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #10512, #10513, #10515: a no-match {@code OPTIONAL MATCH ... WHERE false} followed by {@code WITH *} is the identity on the
 * rows that reach it, yet it changed what a write placed after it did to a result of more than 100 rows. The pipeline pulls
 * its rows in batches of 100, and the query with the identity OPTIONAL MATCH and the one without it disagreed from the 101st
 * row on, so each pair is pinned to the same answer, and to the right one:
 * <ul>
 *   <li>#10512: a correlated {@code CALL} subquery that deletes the node a variable-length path starts from;</li>
 *   <li>#10513: a path endpoint and the variable bound to the same node, read after a {@code REMOVE} of that node's property;</li>
 *   <li>#10515: an {@code OPTIONAL MATCH} that follows a {@code MERGE ... ON CREATE SET}, and must find the relationship the
 *       MERGE created for the current row.</li>
 * </ul>
 * The three have one cause in common, a clause that reads what an earlier clause of the same statement wrote or is about to
 * write while the pipeline hands the rows over a hundred at a time. The tests after the pinned queries cover the neighbours
 * of each fix: a body that only deletes and where it does not need to wait for the rows, a CREATE in place of the MERGE, a
 * path returned after the write.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10512IdentityOptionalMatchBatchBoundaryTest extends TestHelper {

  private static final String IDENTITY_OPTIONAL = "OPTIONAL MATCH (:NoMatch)\nWHERE false\nWITH *\n";

  private List<Result> run(final String cypher) {
    final List<Result> rows = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", cypher)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
    });
    return rows;
  }

  private long count(final String cypher) {
    final List<Result> rows = run(cypher);
    return ((Number) rows.get(0).getProperty("c")).longValue();
  }

  // ---- #10512 ----

  private static final String TETRAHEDRON = "CREATE (a:N)-[:R]->(b:N), (a)-[:R]->(c:N), (b)-[:R]->(c), (b)-[:R]->(d:N), "
      + "(c)-[:R]->(d), (c)-[:R]->(a), (d)-[:R]->(a), (d)-[:R]->(b)";

  private static String correlatedDelete(final String identity) {
    return "MATCH p=(n0:N)-[*1..5]->()\n" + identity + "CALL (n0, p) {\n  DETACH DELETE n0\n  UNWIND [1] AS x\n  CREATE (:X)\n"
        + "  RETURN collect(x) AS rows\n}\nUNWIND rows AS r\nRETURN count(*) AS resultRows";
  }

  private void assertCorrelatedDelete(final String identity) {
    run(TETRAHEDRON);
    final long paths = count("MATCH p=(n0:N)-[*1..5]->() RETURN count(*) AS c");
    assertThat(paths).isGreaterThan(100L);

    final List<Result> rows = run(correlatedDelete(identity));

    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).getProperty("resultRows")).longValue()).isEqualTo(paths);
    assertThat(count("MATCH (x:X) RETURN count(x) AS c")).isEqualTo(paths);
    assertThat(count("MATCH (n:N) RETURN count(n) AS c")).isZero();
    assertThat(count("MATCH ()-[r]->() RETURN count(r) AS c")).isZero();
  }

  @Test
  void correlatedDeleteOverVariableLengthPathsSeesEveryPath() {
    assertCorrelatedDelete("");
  }

  @Test
  void correlatedDeleteOverVariableLengthPathsWithIdentityOptionalMatchSeesEveryPath() {
    assertCorrelatedDelete(IDENTITY_OPTIONAL);
  }

  @Test
  void correlatedDeleteWithNothingElseInTheBodySeesEveryPath() {
    run(TETRAHEDRON);
    final long paths = count("MATCH p=(n0:N)-[*1..5]->() RETURN count(*) AS c");

    final long rows = count("MATCH p=(n0:N)-[*1..5]->() CALL (n0) { DETACH DELETE n0 RETURN 1 AS x } RETURN count(*) AS c");

    assertThat(rows).isEqualTo(paths);
    assertThat(count("MATCH (n:N) RETURN count(n) AS c")).isZero();
  }

  @Test
  void correlatedCreateInTheBodyDoesNotFeedTheMatchBehindIt() {
    run("UNWIND range(1, 5) AS i CREATE (:N {i: i})");

    // The MATCH is enumerated again for every row of the UNWIND and must not meet what the body created for the one before.
    // Pins the openCypher answer; it holds without a barrier too, the chained scan being a snapshot (issue #6602)
    final long rows = count("UNWIND range(1, 3) AS u MATCH (n:N) CALL (n) { CREATE (:N) RETURN 1 AS x } RETURN count(*) AS c");

    assertThat(rows).isEqualTo(15L);
    assertThat(count("MATCH (n:N) RETURN count(n) AS c")).isEqualTo(20L);
  }

  private String explain(final String cypher) {
    try (final ResultSet plan = database.command("opencypher", "EXPLAIN " + cypher)) {
      return plan.next().getProperty("executionPlanAsString");
    }
  }

  @Test
  void aBodyThatDeletesIsReadAheadOnlyBehindAnEnumerationItCanBreak() {
    final String body = " CALL (n) { DETACH DELETE n RETURN 1 AS x } RETURN count(*) AS c";

    // a variable-length walk keeps edge cursors open across the rows it still produces: the rows are drained first
    assertThat(explain("MATCH p=(n:N)-[*1..3]->()" + body)).contains("EAGER");
    // a plain scan is not broken by a delete of the row it just handed over: it keeps streaming
    assertThat(explain("MATCH (n:N)" + body)).doesNotContain("EAGER");
    // a body that does not delete never asks for it
    assertThat(explain("MATCH p=(n:N)-[*1..3]->() CALL (n) { CREATE (:X) RETURN 1 AS x } RETURN count(*) AS c")).doesNotContain("EAGER");
  }

  // ---- #10513 ----

  private static String removeThenCompare(final String identity) {
    return "UNWIND range(1, 101) AS row\nMATCH p=(a)<-[:R]-(b)\n" + identity + "REMOVE b.k10\n"
        + "RETURN count(*) AS rows,\n       count(CASE WHEN properties(b) <> properties(last(nodes(p))) THEN 1 END) AS inconsistentAliasRows";
  }

  private void assertAliasAgrees(final String identity) {
    run("CREATE (a {k10: false})<-[:R]-(b {k10: true})");

    final List<Result> rows = run(removeThenCompare(identity));

    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).getProperty("rows")).longValue()).isEqualTo(101L);
    assertThat(((Number) rows.get(0).getProperty("inconsistentAliasRows")).longValue()).isZero();
  }

  @Test
  void pathEndpointAgreesWithTheBoundNodeAfterRemove() {
    assertAliasAgrees("");
  }

  @Test
  void pathEndpointAgreesWithTheBoundNodeAfterRemoveWithIdentityOptionalMatch() {
    assertAliasAgrees(IDENTITY_OPTIONAL);
  }

  // ---- #10515 ----

  private static String mergeThenOptionalMatch(final String identity) {
    return "UNWIND range(1, 101) AS i\nMERGE (:A)-[r:rt8 {k11: 'before'}]->(n:N)\nON CREATE SET r.k11 = 'after'\n" + identity
        + "OPTIONAL MATCH ()-[r3:rt8]->(n)\nRETURN i, properties(r3) AS relationshipProperties ORDER BY i";
  }

  private void assertMergedRelationshipIsFound(final String identity) {
    final List<Result> rows = run(mergeThenOptionalMatch(identity));

    assertThat(rows).hasSize(101);
    for (final Result row : rows) {
      final Object properties = row.getProperty("relationshipProperties");
      assertThat(properties).as("row %s", row.<Object>getProperty("i")).isEqualTo(Map.of("k11", "after"));
    }
  }

  @Test
  void optionalMatchFindsTheRelationshipMergeCreatedForTheRow() {
    assertMergedRelationshipIsFound("");
  }

  @Test
  void optionalMatchFindsTheRelationshipMergeCreatedForTheRowWithIdentityOptionalMatch() {
    assertMergedRelationshipIsFound(IDENTITY_OPTIONAL);
  }

  @Test
  void optionalMatchFindsTheRelationshipCreateMadeForTheRow() {
    final List<Result> rows = run("UNWIND range(1, 101) AS i\nCREATE (:A)-[r:rt8 {k11: 'before'}]->(n:N)\nSET r.k11 = 'after'\n"
        + "OPTIONAL MATCH ()-[r3:rt8]->(n)\nRETURN i, properties(r3) AS relationshipProperties ORDER BY i");

    assertThat(rows).hasSize(101);
    for (final Result row : rows)
      assertThat(row.<Object>getProperty("relationshipProperties")).as("row %s", row.<Object>getProperty("i")).isEqualTo(Map.of("k11", "after"));
  }

  @Test
  void matchFindsTheRelationshipMergeCreatedForTheRow() {
    final List<Result> rows = run("UNWIND range(1, 101) AS i\nMERGE (:A)-[r:rt8 {k11: 'before'}]->(n:N)\nON CREATE SET r.k11 = 'after'\n"
        + "MATCH ()-[r3:rt8]->(n)\nRETURN i, r3");

    assertThat(rows).hasSize(101);
  }

  // ---- a path returned after the write ----

  @Test
  void pathReturnedAfterSetCarriesTheWrittenNode() {
    run("CREATE (:P {n: 1})-[:R]->(:P {n: 2})");

    final List<Result> rows = run("MATCH p = (a:P {n: 1})-[:R]->(b) SET b.v = 1 RETURN properties(last(nodes(p))) AS end");

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).<Object>getProperty("end")).isEqualTo(Map.of("n", 2, "v", 1L));
  }

  @Test
  void pathReturnedAfterSetOfARelationshipCarriesTheWrittenRelationship() {
    run("CREATE (:P {n: 1})-[:R]->(:P {n: 2})");

    final List<Result> rows = run("MATCH p = (a:P {n: 1})-[r:R]->(b) SET r.w = 5 RETURN properties(head(relationships(p))) AS rel");

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).<Object>getProperty("rel")).isEqualTo(Map.of("w", 5L));
  }

  @Test
  void pathReturnedAfterSetOfTheStartNodeCarriesItAndLeavesTheOtherNodeAlone() {
    run("CREATE (:P {n: 1})-[:R]->(:P {n: 2})");

    final List<Result> rows = run("MATCH p = (a:P {n: 1})-[:R]->(b) SET a.v = 1 RETURN properties(last(nodes(p))) AS end, properties(head(nodes(p))) AS start");

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).<Object>getProperty("end")).isEqualTo(Map.of("n", 2));
    assertThat(rows.get(0).<Object>getProperty("start")).isEqualTo(Map.of("n", 1, "v", 1L));
  }
}
