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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #6431: {@code tryDetectStarCountStar}'s two node-label decline checks
 * - the central-variable one (issue #6322) and the arm-endpoint one (issue #6337) - gated only on
 * {@code node.hasLabels()}. Neither checked {@code node.hasProperties()} or {@code node.hasDynamicLabels()},
 * so an inline property filter (e.g. {@code {status:'active'}}) or a dynamic label on the central variable
 * or an arm node was silently dropped, exactly the same over-count class as #6337 fixed for plain labels -
 * {@code DegreeProductOp.Arm} has no way to check either.
 * <p>
 * In practice the top-level {@code hasInlineNodePropertyOrDynamicLabel()} guard in
 * {@code tryOptimizeCountStar} (added for issue #5071, which predates #6337) already declines the whole
 * push-down before any detector - including the star one - runs, whenever any node in the statement carries
 * an inline property or a dynamic label. So the silent over-count this issue describes is not reachable on
 * current {@code main}: these tests pass before and after the {@code tryDetectStarCountStar} change. The
 * change adds the same {@code hasProperties()}/{@code hasDynamicLabels()} decline directly to
 * {@code tryDetectStarCountStar} anyway (matching the pair-join and chain-hop detectors, which check both
 * at their own level rather than relying solely on the outer guard), so the detector stays correct on its
 * own if the outer guard's scope or placement ever changes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherStarCountPropertyIssue6431Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE (:Author {k:'a1', status:'active'})");
    database.command("opencypher", "CREATE (:Author {k:'a2', status:'inactive'})");
    database.command("opencypher", "CREATE (:Post {k:'p1', status:'published'})");
    database.command("opencypher", "CREATE (:Post {k:'p2', status:'draft'})");
    database.command("opencypher", "CREATE (:Topic {k:'t1'})");

    // p1 is written by the INACTIVE author; p2 is written by the ACTIVE author.
    database.command("opencypher", "MATCH (a:Author {k:'a2'}), (p:Post {k:'p1'}) CREATE (a)-[:WROTE]->(p)");
    database.command("opencypher", "MATCH (a:Author {k:'a1'}), (p:Post {k:'p2'}) CREATE (a)-[:WROTE]->(p)");
    database.command("opencypher", "MATCH (p:Post {k:'p1'}), (t:Topic {k:'t1'}) CREATE (p)-[:TAGGED]->(t)");
    database.command("opencypher", "MATCH (p:Post {k:'p2'}), (t:Topic {k:'t1'}) CREATE (p)-[:TAGGED]->(t)");
  }

  /**
   * Only {@code a1} is an active Author; {@code a2}, who wrote the tagged post {@code p1}, is inactive. The
   * degree product would count the in-degree of {@code WROTE} on each tagged Post - 1 for both p1 and p2 -
   * with no way to check the {@code status} property on the arm endpoint, so it would over-count p1 too.
   */
  @Test
  void aPropertyFilteredArmEndpointDeclinesTheStarCountPushDown() {
    final String query = "MATCH (p:Post)<-[:WROTE]-(a:Author {status:'active'}), (p)-[:TAGGED]->(:Topic) RETURN count(*) AS c";
    assertThat(explainOf(query)).doesNotContain("COUNT STAR JOIN");
    assertThat(scalarOf(query)).isEqualTo(1);
    assertThat(scalarOf(query)).isEqualTo(rowCountOf(
        "MATCH (p:Post)<-[:WROTE]-(a:Author {status:'active'}), (p)-[:TAGGED]->(t:Topic) RETURN p"));
  }

  /**
   * The same gap on the central variable itself: only {@code p1} is published, but the degree product
   * enumerates every {@code Post} that is the central variable's label, with no way to check the
   * {@code status} property filter written directly on it.
   */
  @Test
  void aPropertyFilteredCentralVariableDeclinesTheStarCountPushDown() {
    final String query = "MATCH (p:Post {status:'published'})<-[:WROTE]-(:Author), (p)-[:TAGGED]->(:Topic) RETURN count(*) AS c";
    assertThat(explainOf(query)).doesNotContain("COUNT STAR JOIN");
    assertThat(scalarOf(query)).isEqualTo(1);
    assertThat(scalarOf(query)).isEqualTo(rowCountOf(
        "MATCH (p:Post {status:'published'})<-[:WROTE]-(a:Author), (p)-[:TAGGED]->(t:Topic) RETURN p"));
  }

  /** A dynamic label on an arm endpoint is just as unenforceable by the degree product as a static one. */
  @Test
  void aDynamicallyLabelledArmEndpointDeclinesTheStarCountPushDown() {
    final String query = "MATCH (p:Post)<-[:WROTE]-(:$($armLabel)), (p)-[:TAGGED]->(:Topic) RETURN count(*) AS c";
    final Map<String, Object> params = Map.of("armLabel", "Author");
    assertThat(explainOf(query, params)).doesNotContain("COUNT STAR JOIN");
    assertThat(scalarOf(query, params)).isEqualTo(2);
    assertThat(scalarOf(query, params)).isEqualTo(rowCountOf(
        "MATCH (p:Post)<-[:WROTE]-(a:Author), (p)-[:TAGGED]->(t:Topic) RETURN p"));
  }

  /** A dynamic label on the central variable is just as unenforceable by the degree product as a static one. */
  @Test
  void aDynamicallyLabelledCentralVariableDeclinesTheStarCountPushDown() {
    final String query = "MATCH (p:$($centralLabel))<-[:WROTE]-(:Author), (p)-[:TAGGED]->(:Topic) RETURN count(*) AS c";
    final Map<String, Object> params = Map.of("centralLabel", "Post");
    assertThat(explainOf(query, params)).doesNotContain("COUNT STAR JOIN");
    assertThat(scalarOf(query, params)).isEqualTo(2);
    assertThat(scalarOf(query, params)).isEqualTo(rowCountOf(
        "MATCH (p:Post)<-[:WROTE]-(a:Author), (p)-[:TAGGED]->(t:Topic) RETURN p"));
  }

  // ===================================================================================================
  // helpers
  // ===================================================================================================

  private long scalarOf(final String query) {
    return scalarOf(query, Map.of());
  }

  private long scalarOf(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", query, params)) {
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
    return explainOf(query, Map.of());
  }

  private String explainOf(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query, params)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
