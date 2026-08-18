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
 * Regression tests for issue #6337: {@code tryDetectStarCountStar} reads only the central variable's label and
 * carries nothing for an arm's far endpoint - {@code DegreeProductOp.Arm} has no room for one - so
 * {@code (p)<-[:WROTE]-(:Author)} and {@code (p)<-[:WROTE]-()} produced the same operator and the same,
 * over-counted, answer. The fix declines the push-down whenever a non-central node of a star-join pattern
 * carries a label, exactly as the sibling detectors already do for the central variable's own label
 * (issue #6322).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherStarCountArmLabelIssue6337Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE (:Author {k:'a1'})");
    database.command("opencypher", "CREATE (:Bot {k:'b1'})");
    database.command("opencypher", "CREATE (:Post {k:'p1'})");
    database.command("opencypher", "CREATE (:Topic {k:'t1'})");

    database.command("opencypher", "MATCH (a:Author {k:'a1'}), (p:Post {k:'p1'}) CREATE (a)-[:WROTE]->(p)");
    database.command("opencypher", "MATCH (b:Bot {k:'b1'}), (p:Post {k:'p1'}) CREATE (b)-[:WROTE]->(p)");
    database.command("opencypher", "MATCH (p:Post {k:'p1'}), (t:Topic {k:'t1'}) CREATE (p)-[:TAGGED]->(t)");
  }

  /**
   * Only {@code a1} is an {@code Author}; {@code b1} is a {@code Bot}. The degree product used to count the
   * in-degree of {@code WROTE} on {@code p1} - which is 2, one Author and one Bot - and multiply it by the
   * {@code TAGGED} out-degree, giving 2 regardless of the {@code :Author} label on the arm. The materialized
   * pipeline is ground truth: only the Author-authored, Topic-tagged post exists once.
   */
  @Test
  void aLabelledArmEndpointDeclinesTheStarCountPushDown() {
    final String labelled = "MATCH (p:Post)<-[:WROTE]-(:Author), (p)-[:TAGGED]->(:Topic) RETURN count(*) AS c";
    assertThat(explainOf(labelled)).doesNotContain("COUNT STAR JOIN");
    assertThat(scalarOf(labelled)).isEqualTo(1);
    assertThat(scalarOf(labelled)).isEqualTo(rowCountOf(
        "MATCH (p:Post)<-[:WROTE]-(a:Author), (p)-[:TAGGED]->(t:Topic) RETURN p"));
  }

  /**
   * When no arm carries a label at all, the push-down still applies and still counts both {@code WROTE} arms -
   * the fix declines on a label the operator cannot enforce, not on the presence of an arm.
   */
  @Test
  void starCountPushDownStillAppliesWhenNoArmCarriesALabel() {
    final String unlabelled = "MATCH (p:Post)<-[:WROTE]-(), (p)-[:TAGGED]->() RETURN count(*) AS c";
    assertThat(explainOf(unlabelled)).contains("COUNT STAR JOIN");
    assertThat(scalarOf(unlabelled)).isEqualTo(2);
  }

  /** A label on an interior node of a multi-hop arm is just as unenforceable as one on the far endpoint. */
  @Test
  void aLabelledInteriorArmNodeDeclinesTheStarCountPushDown() {
    database.command("opencypher", "CREATE (:Bad {k:'x1'})");
    database.command("opencypher", "MATCH (b:Bad {k:'x1'}), (p:Post {k:'p1'}) CREATE (b)-[:VIA]->(p)");
    database.command("opencypher", "MATCH (a:Author {k:'a1'}), (b:Bad {k:'x1'}) CREATE (a)-[:LINK]->(b)");

    final String query = "MATCH (p:Post)<-[:VIA]-(:Bad)<-[:LINK]-(:Author), (p)-[:TAGGED]->(:Topic) RETURN count(*) AS c";
    assertThat(explainOf(query)).doesNotContain("COUNT STAR JOIN");
    assertThat(scalarOf(query)).isEqualTo(rowCountOf(
        "MATCH (p:Post)<-[:VIA]-(x:Bad)<-[:LINK]-(a:Author), (p)-[:TAGGED]->(t:Topic) RETURN p"));
  }

  // ===================================================================================================
  // helpers
  // ===================================================================================================

  private long scalarOf(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private int rowCountOf(final String query) {
    int count = 0;
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
    }
    return count;
  }

  private String explainOf(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
