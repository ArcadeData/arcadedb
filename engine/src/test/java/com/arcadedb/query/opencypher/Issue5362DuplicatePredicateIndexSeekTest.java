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
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5362: a redundant equality predicate composed with AND
 * ({@code WHERE (n.id = 1) AND (n.id = 1)}) must not downgrade the plan from
 * {@code NodeIndexSeek} to a full {@code NodeByLabelScan}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue5362DuplicatePredicateIndexSeekTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final var type = database.getSchema().createVertexType("Bench");
      type.createProperty("id", Integer.class);
      type.createProperty("name", String.class);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    });

    database.transaction(() -> {
      for (int i = 0; i < 2000; i++)
        database.command("opencypher", "CREATE (n:Bench {id: " + i + ", name: 'n" + (i % 10) + "'})");
    });
  }

  @Test
  void singleEqualityUsesIndexSeek() {
    final String plan = profilePlan("MATCH (n:Bench) WHERE n.id = 1 RETURN count(n) AS c");
    assertThat(plan).as("baseline\n%s", plan).contains("NodeIndexSeek");
  }

  @Test
  void duplicateEqualityStillUsesIndexSeek() {
    final String plan = profilePlan("MATCH (n:Bench) WHERE (n.id = 1) AND (n.id = 1) RETURN count(n) AS c");
    assertThat(plan).as("redundant AND must not disable the index seek\n%s", plan)
        .contains("NodeIndexSeek")
        .doesNotContain("NodeByLabelScan");
  }

  @Test
  void duplicateEqualityReturnsSameResult() {
    assertThat(count("MATCH (n:Bench) WHERE (n.id = 1) AND (n.id = 1) RETURN count(n) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (n:Bench) WHERE n.id = 1 RETURN count(n) AS c")).isEqualTo(1L);
  }

  /**
   * Two different equality predicates on the same indexed property are contradictory: the result must
   * be empty, and picking one of them for the index seek must not lose the other filter.
   */
  @Test
  void contradictoryEqualitiesReturnNothing() {
    assertThat(count("MATCH (n:Bench) WHERE (n.id = 1) AND (n.id = 2) RETURN count(n) AS c")).isEqualTo(0L);
  }

  /**
   * A redundant equality ANDed with a further restricting predicate must keep both filters applied.
   */
  @Test
  void duplicateEqualityWithExtraPredicate() {
    assertThat(count("MATCH (n:Bench) WHERE (n.id = 1) AND (n.id = 1) AND n.id > 5 RETURN count(n) AS c")).isEqualTo(0L);
    assertThat(count("MATCH (n:Bench) WHERE (n.id = 7) AND (n.id = 7) AND n.id > 5 RETURN count(n) AS c")).isEqualTo(1L);
  }

  /**
   * The same gap hit any conjunction, not just a duplicated one: an indexed equality ANDed with a
   * non-indexed predicate must still anchor on the index.
   */
  @Test
  void mixedConjunctionUsesIndexSeek() {
    final String plan = profilePlan("MATCH (n:Bench) WHERE n.id = 7 AND n.name = 'n7' RETURN count(n) AS c");
    assertThat(plan).as("indexed equality ANDed with a non-indexed one\n%s", plan).contains("NodeIndexSeek");
    assertThat(count("MATCH (n:Bench) WHERE n.id = 7 AND n.name = 'n7' RETURN count(n) AS c")).isEqualTo(1L);
    // Conjunction that no row satisfies: the seek must not swallow the second predicate.
    assertThat(count("MATCH (n:Bench) WHERE n.id = 7 AND n.name = 'n8' RETURN count(n) AS c")).isEqualTo(0L);
  }

  /**
   * A disjunction is not anchorable: an equality under OR does not hold for every row, so it must not
   * become an index seek (which would drop the other branch's rows).
   */
  @Test
  void disjunctionIsNotAnchoredOnTheIndex() {
    final String query = "MATCH (n:Bench) WHERE n.id = 1 OR n.id = 2 RETURN count(n) AS c";
    final String plan = profilePlan(query);
    assertThat(plan).as("OR must not be turned into a single-value seek\n%s", plan).doesNotContain("NodeIndexSeek");
    assertThat(count(query)).isEqualTo(2L);
  }

  /**
   * A negated equality must not seed the seek either.
   */
  @Test
  void negatedEqualityIsNotAnchoredOnTheIndex() {
    final String query = "MATCH (n:Bench) WHERE NOT (n.id = 1) RETURN count(n) AS c";
    final String plan = profilePlan(query);
    assertThat(plan).as("NOT must not be turned into a seek\n%s", plan).doesNotContain("NodeIndexSeek");
    assertThat(count(query)).isEqualTo(1999L);
  }

  /**
   * An equality nested in an OR, ANDed at the top level with an indexed equality: only the top-level
   * conjunct is anchorable and the OR branch must survive as a filter.
   */
  @Test
  void conjunctionOfIndexedEqualityAndDisjunction() {
    final String query = "MATCH (n:Bench) WHERE n.id = 7 AND (n.name = 'n7' OR n.name = 'n8') RETURN count(n) AS c";
    assertThat(profilePlan(query)).contains("NodeIndexSeek");
    assertThat(count(query)).isEqualTo(1L);
    assertThat(count("MATCH (n:Bench) WHERE n.id = 7 AND (n.name = 'n1' OR n.name = 'n8') RETURN count(n) AS c")).isEqualTo(0L);
  }

  private long count(final String cypher) {
    final long[] c = new long[1];
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher", cypher);
      c[0] = ((Number) rs.next().getProperty("c")).longValue();
      rs.close();
    });
    return c[0];
  }

  private String profilePlan(final String cypher) {
    final StringBuilder plan = new StringBuilder();
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher", "PROFILE " + cypher);
      while (rs.hasNext())
        rs.next();
      plan.append(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2));
      rs.close();
    });
    return plan.toString();
  }
}
