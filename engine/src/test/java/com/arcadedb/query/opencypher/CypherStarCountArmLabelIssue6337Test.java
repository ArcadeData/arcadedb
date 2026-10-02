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
 * carried nothing for an arm's far endpoint, so {@code (p)<-[:WROTE]-(:Author)} and {@code (p)<-[:WROTE]-()}
 * produced the same operator and the same, over-counted, answer. The first fix declined the push-down whenever a
 * non-central node carried a label, which was correct but sent LSQB Q4/Q7 (every endpoint labelled) to full pattern
 * matching (0.01s to 5-12s on SF1). {@code DegreeProductOp.Arm} now carries a label per hop and the operator enforces
 * it, on the CSR paths (a label the edge type already implies costs nothing, one that filters gets a filtered degree)
 * and on the OLTP paths. These tests keep the original over-count scenario as the ground truth: the push-down is used
 * AND the answer is the materialized pipeline's.
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
  void aLabelledArmEndpointIsEnforcedByTheStarCountPushDown() {
    final String labelled = "MATCH (p:Post)<-[:WROTE]-(:Author), (p)-[:TAGGED]->(:Topic) RETURN count(*) AS c";
    assertThat(explainOf(labelled)).contains("COUNT STAR JOIN");
    assertThat(scalarOf(labelled)).isEqualTo(1);
    assertThat(scalarOf(labelled)).isEqualTo(rowCountOf(
        "MATCH (p:Post)<-[:WROTE]-(a:Author), (p)-[:TAGGED]->(t:Topic) RETURN p"));
  }

  /**
   * When no arm carries a label at all, the push-down still applies and still counts both {@code WROTE} arms.
   */
  @Test
  void starCountPushDownStillAppliesWhenNoArmCarriesALabel() {
    final String unlabelled = "MATCH (p:Post)<-[:WROTE]-(), (p)-[:TAGGED]->() RETURN count(*) AS c";
    assertThat(explainOf(unlabelled)).contains("COUNT STAR JOIN");
    assertThat(scalarOf(unlabelled)).isEqualTo(2);
  }

  /** A label on an interior node of a multi-hop arm is enforced hop by hop. */
  @Test
  void aLabelledInteriorArmNodeIsEnforcedByTheStarCountPushDown() {
    database.command("opencypher", "CREATE (:Bad {k:'x1'})");
    database.command("opencypher", "MATCH (b:Bad {k:'x1'}), (p:Post {k:'p1'}) CREATE (b)-[:VIA]->(p)");
    database.command("opencypher", "MATCH (a:Author {k:'a1'}), (b:Bad {k:'x1'}) CREATE (a)-[:LINK]->(b)");

    final String query = "MATCH (p:Post)<-[:VIA]-(:Bad)<-[:LINK]-(:Author), (p)-[:TAGGED]->(:Topic) RETURN count(*) AS c";
    assertThat(explainOf(query)).contains("COUNT STAR JOIN");
    assertThat(scalarOf(query)).isEqualTo(rowCountOf(
        "MATCH (p:Post)<-[:VIA]-(x:Bad)<-[:LINK]-(a:Author), (p)-[:TAGGED]->(t:Topic) RETURN p"));
  }

  /**
   * The other three tests all put the central variable at position 0 or the last position of its path
   * pattern, which builds a single {@code Arm} via {@code buildArmForward}/{@code buildArmBackward}. When the
   * central variable sits in the <em>interior</em> of a pattern instead, {@code tryDetectStarCountStar} splits
   * it into a {@code leftArm} and a {@code rightArm} from the same pattern - a third construction path the
   * label-decline loop has to cover too, since it runs once per pattern before that split, not once per arm.
   */
  @Test
  void aLabelledEndpointIsEnforcedWhenTheCentralNodeIsInterior() {
    database.command("opencypher", "CREATE (:Extra {k:'e1'})");
    database.command("opencypher", "MATCH (p:Post {k:'p1'}), (e:Extra {k:'e1'}) CREATE (p)-[:VIA]->(e)");

    // p sits between (:Author) and (:Topic) in the first pattern, so this one PathPattern alone yields both
    // a leftArm (back to :Author) and a rightArm (forward to :Topic); the second pattern only supplies p's
    // second occurrence so it counts as the central variable at all.
    final String query = "MATCH (:Author)-[:WROTE]->(p:Post)-[:TAGGED]->(:Topic), (p)-[:VIA]->() RETURN count(*) AS c";
    assertThat(explainOf(query)).contains("COUNT STAR JOIN");
    assertThat(scalarOf(query)).isEqualTo(rowCountOf(
        "MATCH (a:Author)-[:WROTE]->(p:Post)-[:TAGGED]->(t:Topic), (p)-[:VIA]->(e) RETURN p"));
  }

  /**
   * The same scenarios on a Graph Analytical View, the path LSQB takes. The {@code WROTE} label really filters (a Bot
   * writes the post too), so the arm gets a filtered degree; the {@code TAGGED} label is implied (only Posts tag Topics
   * here) and keeps the plain degree. Both have to give the materialized pipeline's answer.
   */
  @Test
  void labelledArmsAreEnforcedOnTheCsrPathToo() {
    createView();
    final String filtering = "MATCH (p:Post)<-[:WROTE]-(:Author), (p)-[:TAGGED]->(:Topic) RETURN count(*) AS c";
    assertThat(explainOf(filtering)).contains("COUNT STAR JOIN");
    assertThat(scalarOf(filtering)).isEqualTo(1);

    final String implied = "MATCH (p:Post)-[:TAGGED]->(:Topic), (p)<-[:WROTE]-() RETURN count(*) AS c";
    assertThat(explainOf(implied)).contains("COUNT STAR JOIN");
    assertThat(scalarOf(implied)).isEqualTo(rowCountOf("MATCH (p:Post)-[:TAGGED]->(t:Topic), (p)<-[:WROTE]-(w) RETURN p"));

    final String optional = "MATCH (p:Post)-[:TAGGED]->(:Topic) OPTIONAL MATCH (p)<-[:WROTE]-(:Author) RETURN count(*) AS c";
    assertThat(scalarOf(optional)).isEqualTo(rowCountOf("MATCH (p:Post)-[:TAGGED]->(t:Topic) OPTIONAL MATCH (p)<-[:WROTE]-(a:Author) RETURN p"));

    final String bots = "MATCH (p:Post)-[:TAGGED]->(:Topic), (p)<-[:WROTE]-(:Bot) RETURN count(*) AS c";
    assertThat(scalarOf(bots)).isEqualTo(1);

    final String none = "MATCH (p:Post)-[:TAGGED]->(:Topic), (p)<-[:WROTE]-(:Topic) RETURN count(*) AS c";
    assertThat(scalarOf(none)).isZero();
  }

  /** A label the schema does not know matches nothing (a mandatory arm) or leaves the optional row alone. */
  @Test
  void anUnknownEndpointLabelMatchesNothing() {
    createView();
    assertThat(scalarOf("MATCH (p:Post)-[:TAGGED]->(:Topic), (p)<-[:WROTE]-(:Ghost) RETURN count(*) AS c")).isZero();
    assertThat(scalarOf("MATCH (p:Post)-[:TAGGED]->(:Topic) OPTIONAL MATCH (p)<-[:WROTE]-(:Ghost) RETURN count(*) AS c")).isEqualTo(1);
  }

  private void createView() {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav6337 VERTEX TYPES (Author, Bot, Post, Topic) "
        + "EDGE TYPES (WROTE, TAGGED)");
    final var view = com.arcadedb.graph.olap.GraphAnalyticalViewRegistry.get(database, "gav6337");
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!view.isReady() && System.currentTimeMillis() < deadline)
      Thread.onSpinWait();
    assertThat(view.isReady()).isTrue();
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
