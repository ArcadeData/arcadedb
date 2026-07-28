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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #5480.
 * <p>
 * A node inline {@code WHERE} predicate - the {@code WHERE n.v = 2} in {@code (n:A WHERE n.v = 2)} -
 * must filter candidate nodes in plain {@code MATCH}, in {@code EXISTS {}} / {@code COUNT {}}
 * subqueries and in pattern comprehensions. Before the fix the predicate was parsed but silently
 * discarded by the pattern-comprehension parser path, so every candidate node matched.
 * <p>
 * {@code shortestPath} is deliberately not covered here: that evaluator ignores inline predicates
 * altogether and is tracked by issue #5481.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5480NodeInlineWhereTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-5480-node-inline-where").create();
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:A {v:1}), (b:A {v:2}), (c:A {v:3})");
      database.command("opencypher", "MATCH (a:A {v:1}), (b:A {v:2}), (c:A {v:3}) "
          + "CREATE (a)-[:E]->(b), (a)-[:E]->(c)");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private List<Integer> queryInts(final String query, final String field) {
    final List<Integer> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final Number n = row.getProperty(field);
        values.add(n == null ? null : n.intValue());
      }
    }
    return values;
  }

  private int queryInt(final String query, final String field) {
    final List<Integer> values = queryInts(query, field);
    assertThat(values).hasSize(1);
    return values.get(0);
  }

  // ---------------------------------------------------------------- plain MATCH

  @Test
  void plainMatchAppliesNodeInlineWhere() {
    assertThat(queryInts("MATCH (n:A WHERE n.v = 2) RETURN n.v AS v ORDER BY v", "v")).containsExactly(2);
  }

  @Test
  void plainMatchAppliesNodeInlineWhereOnEveryNodeOfThePath() {
    assertThat(queryInts("MATCH (a:A WHERE a.v = 1)-[:E]->(b:A WHERE b.v = 3) RETURN b.v AS v", "v"))
        .containsExactly(3);
  }

  @Test
  void plainMatchCombinesNodeInlineWhereWithClauseWhere() {
    assertThat(queryInts("MATCH (n:A WHERE n.v > 1) WHERE n.v < 3 RETURN n.v AS v", "v")).containsExactly(2);
  }

  // ---------------------------------------------------------------- EXISTS {}

  @Test
  void existsSubqueryAppliesNodeInlineWhere() {
    assertThat(queryInts("MATCH (a:A {v:1}) WHERE EXISTS { (a)-[:E]->(x:A WHERE x.v = 99) } RETURN a.v AS v", "v"))
        .isEmpty();
    assertThat(queryInts("MATCH (a:A {v:1}) WHERE EXISTS { (a)-[:E]->(x:A WHERE x.v = 2) } RETURN a.v AS v", "v"))
        .containsExactly(1);
  }

  @Test
  void existsSubqueryWithExplicitMatchAppliesNodeInlineWhere() {
    assertThat(queryInts(
        "MATCH (a:A {v:1}) WHERE EXISTS { MATCH (a)-[:E]->(x:A WHERE x.v = 99) } RETURN a.v AS v", "v")).isEmpty();
    assertThat(queryInts(
        "MATCH (a:A {v:1}) WHERE EXISTS { MATCH (a)-[:E]->(x:A WHERE x.v = 3) } RETURN a.v AS v", "v"))
        .containsExactly(1);
  }

  @Test
  void countSubqueryAppliesNodeInlineWhere() {
    assertThat(queryInt("MATCH (a:A {v:1}) RETURN COUNT { (a)-[:E]->(x:A WHERE x.v = 2) } AS c", "c")).isEqualTo(1);
    assertThat(queryInt("MATCH (a:A {v:1}) RETURN COUNT { (a)-[:E]->(x:A) } AS c", "c")).isEqualTo(2);
  }

  // ---------------------------------------------------------------- pattern comprehension

  @Test
  void patternComprehensionAppliesNodeInlineWhere() {
    assertThat(queryInt("MATCH (a:A {v:1}) RETURN size([(a)-[:E]->(x:A WHERE x.v = 2) | x]) AS c", "c")).isEqualTo(1);
    assertThat(queryInt("MATCH (a:A {v:1}) RETURN size([(a)-[:E]->(x:A) | x]) AS c", "c")).isEqualTo(2);
  }

  @Test
  void patternComprehensionAppliesNodeInlineWhereOnTheAnchorNode() {
    assertThat(queryInt("MATCH (a:A {v:1}) RETURN size([(a WHERE a.v = 99)-[:E]->(x:A) | x]) AS c", "c")).isEqualTo(0);
    assertThat(queryInt("MATCH (a:A {v:1}) RETURN size([(a WHERE a.v = 1)-[:E]->(x:A) | x]) AS c", "c")).isEqualTo(2);
  }

  @Test
  void patternComprehensionAppliesNodeInlineWhereOnAVariableLengthHop() {
    // The evaluation row is reused across candidates of one expansion, so a predicate that matches
    // only some of them must still reject the others.
    assertThat(queryInt("MATCH (a:A {v:1}) RETURN size([(a)-[:E*1..2]->(x:A WHERE x.v = 3) | x]) AS c", "c"))
        .isEqualTo(1);
    assertThat(queryInt("MATCH (a:A {v:1}) RETURN size([(a)-[:E*1..2]->(x:A) | x]) AS c", "c")).isEqualTo(2);
  }

  @Test
  void patternComprehensionCombinesNodeInlineWhereWithTrailingWhere() {
    assertThat(queryInt(
        "MATCH (a:A {v:1}) RETURN size([(a)-[:E]->(x:A WHERE x.v > 1) WHERE x.v < 3 | x]) AS c", "c")).isEqualTo(1);
  }
}
