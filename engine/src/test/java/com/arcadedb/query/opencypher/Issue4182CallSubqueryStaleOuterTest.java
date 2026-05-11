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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #4182.
 * <p>
 * When a {@code CALL} subquery mutates a node imported via {@code WITH n},
 * the outer variable {@code n} must reflect the updated state after the subquery
 * returns, matching Neo4j semantics. Previously, the outer binding was a snapshot
 * captured before the inner mutation, so {@code RETURN n.score} kept returning the
 * pre-update value.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4182CallSubqueryStaleOuterTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4182-call-stale-outer").create();
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Minimised repro from the issue. The inner {@code SET n.score = 2} must be
   * visible through the outer variable {@code n} after the subquery returns.
   */
  @Test
  void outerVariableReflectsMutationFromCallSubqueryWithSeparateReturn() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Node {score: 1})"));

    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "MATCH (n:Node) CALL { WITH n SET n.score = 2 RETURN 1 AS ok } RETURN n.score AS score");

      final List<Result> rows = drain(rs);
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("score")).longValue()).isEqualTo(2L);
    });
  }

  /**
   * Same as above but returning the whole node, to make sure {@code n} as a value
   * in the outer scope is refreshed too.
   */
  @Test
  void outerNodeBindingReflectsMutationFromCallSubquery() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Node {score: 1})"));

    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "MATCH (n:Node) CALL { WITH n SET n.score = 2 RETURN 1 AS ok } RETURN n");

      final List<Result> rows = drain(rs);
      assertThat(rows).hasSize(1);
      final Vertex n = (Vertex) rows.get(0).getElement().get();
      assertThat(((Number) n.get("score")).longValue()).isEqualTo(2L);
    });
  }

  /**
   * Issue variant with a list-valued property and {@code IN TRANSACTIONS}.
   * The outer variable {@code n} must reflect the inner {@code SET n.vector = ...}.
   * The inner subquery has no {@code RETURN} (unit subquery).
   */
  @Test
  void outerVariableReflectsMutationInTransactionsUnitSubquery() {
    database.transaction(() -> database.command("opencypher", "CREATE (:TestNode {id: 1, vector: [1.0, 2.0, 3.0]})"));

    final ResultSet rs = database.command("opencypher",
        "MATCH (n:TestNode) CALL { WITH n SET n.vector = [4.0, 5.0, 6.0] } IN TRANSACTIONS OF 1000 ROWS RETURN n");

    final List<Result> rows = drain(rs);
    assertThat(rows).hasSize(1);
    final Vertex n = (Vertex) rows.get(0).getElement().get();
    final Object vector = n.get("vector");
    assertThat(vector).isNotNull();
    final List<?> values = (List<?>) vector;
    assertThat(values).hasSize(3);
    assertThat(((Number) values.get(0)).doubleValue()).isEqualTo(4.0d);
    assertThat(((Number) values.get(1)).doubleValue()).isEqualTo(5.0d);
    assertThat(((Number) values.get(2)).doubleValue()).isEqualTo(6.0d);
  }

  /**
   * Control case from the issue: when the updated node is returned explicitly from
   * the subquery as an alias, it already works with the fresh value. The assertion
   * guards against accidentally regressing that path.
   */
  @Test
  void aliasedReturnFromSubqueryAlreadyReflectsMutation() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Node {score: 1})"));

    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "MATCH (n:Node) CALL { WITH n SET n.score = 2 RETURN n AS updated } RETURN updated.score AS score");

      final List<Result> rows = drain(rs);
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("score")).longValue()).isEqualTo(2L);
    });
  }

  /**
   * A scalar outer value (not a node) carried alongside the mutated node must
   * remain untouched by the refresh logic.
   */
  @Test
  void scalarOuterVariableIsPreservedAcrossCallSubquery() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Node {score: 1})"));

    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "MATCH (n:Node) WITH n, 42 AS k CALL { WITH n SET n.score = 7 RETURN 1 AS ok } "
              + "RETURN k, n.score AS score");

      final List<Result> rows = drain(rs);
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("k")).longValue()).isEqualTo(42L);
      assertThat(((Number) rows.get(0).getProperty("score")).longValue()).isEqualTo(7L);
    });
  }

  /**
   * Multiple outer nodes, each mutated through the imported variable. Every output
   * row must reflect its own mutation.
   */
  @Test
  void multipleOuterRowsEachReflectOwnMutation() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (:Node {id: 1, score: 10}), (:Node {id: 2, score: 20}), (:Node {id: 3, score: 30})"));

    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "MATCH (n:Node) CALL { WITH n SET n.score = n.score + 100 RETURN 1 AS ok } "
              + "RETURN n.id AS id, n.score AS score ORDER BY id");

      final List<Result> rows = drain(rs);
      assertThat(rows).hasSize(3);
      assertThat(((Number) rows.get(0).getProperty("score")).longValue()).isEqualTo(110L);
      assertThat(((Number) rows.get(1).getProperty("score")).longValue()).isEqualTo(120L);
      assertThat(((Number) rows.get(2).getProperty("score")).longValue()).isEqualTo(130L);
    });
  }

  private static List<Result> drain(final ResultSet rs) {
    final List<Result> rows = new ArrayList<>();
    while (rs.hasNext())
      rows.add(rs.next());
    return rows;
  }
}
