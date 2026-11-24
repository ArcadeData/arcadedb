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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #12726: the COUNT STAR JOIN push-down took a label written on the central variable
 * only inside an OPTIONAL MATCH as the type of central node to enumerate, so it counted the B nodes instead of
 * every row the mandatory MATCH produces. Each count is checked against the row pipeline for the same pattern.
 */
class CypherOptionalLabelCountStarIssue12726Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createEdgeType("R");
    database.command("opencypher", "CREATE (:C {id: 0}), (:C {id: 1}), (:B {id: 2})");
  }

  @Test
  void anOptionalOnlyLabelDoesNotFilterTheCentralNode() {
    assertCountMatchesRows("MATCH (n0) OPTIONAL MATCH (n0:B)-[:R]->(n1)", 3);
    assertThat(single("MATCH (n0) OPTIONAL MATCH (n0:B)-[:R]->(n1) RETURN count(n0) AS c")).isEqualTo(3L);
  }

  @Test
  void anOptionalWhereLabelDoesNotFilterTheCentralNode() {
    assertCountMatchesRows("MATCH (n0) OPTIONAL MATCH (n0)-[:R]->(n1) WHERE n0:B", 3);
  }

  @Test
  void aLabelRepeatedFromTheMandatoryClauseKeepsThePushDown() {
    final String query = "MATCH (n0:B) OPTIONAL MATCH (n0:B)-[:R]->(n1)";
    assertCountMatchesRows(query, 1);
    assertThat(explainOf(query + " RETURN count(*) AS c")).contains("COUNT STAR JOIN");
  }

  @Test
  void aLabelConflictingWithTheMandatoryClause() {
    assertCountMatchesRows("MATCH (n0:C) OPTIONAL MATCH (n0:B)-[:R]->(n1)", 2);
  }

  @Test
  void withEdges() {
    database.transaction(() -> database.command("opencypher",
        "MATCH (b:B), (c:C) CREATE (b)-[:R]->(c)"));
    assertCountMatchesRows("MATCH (n0) OPTIONAL MATCH (n0:B)-[:R]->(n1)", 4);
    assertCountMatchesRows("MATCH (n0) OPTIONAL MATCH (n0)-[:R]->(n1) WHERE n0:B", 4);
    assertCountMatchesRows("MATCH (n0:B) OPTIONAL MATCH (n0:B)-[:R]->(n1)", 2);
    assertCountMatchesRows("MATCH (n0:C) OPTIONAL MATCH (n0:B)-[:R]->(n1)", 2);
  }

  /**
   * The degree product scores an OPTIONAL arm as max(1, degree) per central vertex, which only stands for the rows when
   * a mandatory clause bound the central node BEFORE the OPTIONAL clause. A leading OPTIONAL clause binds it itself, and
   * the mandatory clause after it keeps only the rows it reached, wherever the label is written.
   */
  @Test
  void aLeadingOptionalClauseIsNotAnOptionalArm() {
    database.getSchema().createEdgeType("S");
    database.getSchema().createEdgeType("T");
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:A)-[:S]->(b1:B), (b2:B), (b1)-[:T]->(:C), (b2)-[:T]->(:C)"));

    assertCountMatchesRows("OPTIONAL MATCH (x:A)-[:S]->(y:B) MATCH (y)-[:T]->(z)", "x, y, z", 1);
    assertCountMatchesRows("OPTIONAL MATCH (x:A)-[:S]->(y) MATCH (y:B)-[:T]->(z)", "x, y, z", 1);
    assertCountMatchesRows("OPTIONAL MATCH (x)-[:S]->(y:B) MATCH (y:B)-[:T]->(z)", "x, y, z", 1);
    // no S edge leaves a C node: the leading OPTIONAL clause yields one row with y null, which the MATCH then drops
    assertCountMatchesRows("OPTIONAL MATCH (x:C)-[:S]->(y:B) MATCH (y:B)-[:T]->(z)", "x, y, z", 0);
  }

  @Test
  void aLeadingOptionalLabelMissingFromTheSchemaCountsNothing() {
    database.getSchema().createEdgeType("S");
    database.transaction(() -> database.command("opencypher", "CREATE (:A)-[:S]->(:B)"));

    assertCountMatchesRows("OPTIONAL MATCH (x:Nope) MATCH (x)-[:S]->(b:B)", "x, b", 0);
  }

  /** An inline WHERE on the central node of an OPTIONAL clause decides whether that arm matched, never whether the row exists. */
  @Test
  void anInlineWhereOnTheOptionalCentralNodeIsAConditionOfTheArm() {
    database.transaction(() -> database.command("opencypher", "MATCH (b:B), (c:C) CREATE (b)-[:R]->(c)"));

    assertCountMatchesRows("MATCH (n0:B) OPTIONAL MATCH (n0:B WHERE n0.id = 99)-[:R]->(n1)", 1);
    assertCountMatchesRows("MATCH (n0:B) OPTIONAL MATCH (n0:B WHERE n0.id = 2)-[:R]->(n1)", 2);
  }

  private void assertCountMatchesRows(final String pattern, final long expected) {
    assertCountMatchesRows(pattern, "n0, n1", expected);
  }

  private void assertCountMatchesRows(final String pattern, final String projection, final long expected) {
    assertThat(single(pattern + " RETURN count(*) AS c")).as(pattern).isEqualTo(expected);
    long rows = 0;
    try (final ResultSet rs = database.query("opencypher", pattern + " RETURN " + projection)) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    assertThat(rows).as(pattern).isEqualTo(expected);
  }

  private long single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private String explainOf(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
