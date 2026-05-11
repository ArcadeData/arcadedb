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
 * Regression test for GitHub issue #4191.
 * <p>
 * A write-only {@code CALL} subquery (a body with no {@code RETURN}) must not
 * emit any output row when it sits at the top level of the query. Neo4j returns
 * zero rows for {@code CALL { CREATE (:N) }}; ArcadeDB used to return a single
 * empty row because {@link com.arcadedb.query.opencypher.executor.steps.SubqueryStep}
 * synthesised one empty inner row per outer row, and the top-level case spawns
 * one synthetic outer row when no previous step exists.
 * <p>
 * When the {@code CALL} is preceded by a clause that produces rows (e.g.
 * {@code MATCH (n)}) and the outer query has a {@code RETURN}, the unit subquery
 * must still preserve the outer cardinality, so a separate test guards that
 * existing behaviour (see also {@link Issue4182CallSubqueryStaleOuterTest}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4191WriteOnlyCallSubqueryTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4191-write-only-call").create();
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Minimal repro from the issue: a top-level {@code CALL} body containing a single
   * {@code CREATE} with no {@code RETURN} must produce zero rows, matching Neo4j.
   * Side effect (vertex creation) must still happen.
   */
  @Test
  void topLevelWriteOnlyCallReturnsZeroRows() {
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher", "CALL { CREATE (:N) }");
      assertThat(drain(rs)).isEmpty();

      final ResultSet count = database.query("opencypher", "MATCH (n:N) RETURN count(n) AS c");
      assertThat(count.hasNext()).isTrue();
      assertThat(((Number) count.next().getProperty("c")).longValue()).isEqualTo(1L);
    });
  }

  /**
   * Same expectation for the richer variant from the issue, which exercises the
   * {@code WITH} / {@code SKIP 0} / second {@code CREATE} path inside the body.
   */
  @Test
  void topLevelWriteOnlyCallWithChainedCreatesReturnsZeroRows() {
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "CALL { CREATE (a) WITH a SKIP 0 CREATE (a)-[:KNOWS]->(b) }");
      assertThat(drain(rs)).isEmpty();

      final ResultSet edges = database.query("opencypher", "MATCH ()-[r:KNOWS]->() RETURN count(r) AS c");
      assertThat(edges.hasNext()).isTrue();
      assertThat(((Number) edges.next().getProperty("c")).longValue()).isEqualTo(1L);
    });
  }

  /**
   * Top-level {@code CREATE} (without {@code CALL}) already returned zero rows. The
   * assertion documents the control case so a regression on the standalone write
   * path would also be caught here.
   */
  @Test
  void topLevelCreateReturnsZeroRows() {
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher", "CREATE (:N)");
      assertThat(drain(rs)).isEmpty();
    });
  }

  /**
   * Control from the issue: a {@code CALL} body with an explicit {@code RETURN}
   * must keep returning its row, both with and without an outer {@code RETURN}.
   */
  @Test
  void callSubqueryWithExplicitReturnEmitsRows() {
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "CALL { CREATE (a) RETURN 1 AS x } RETURN x");

      final List<Result> rows = drain(rs);
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.get(0).getProperty("x")).longValue()).isEqualTo(1L);
    });
  }

  /**
   * Outer {@code MATCH} followed by a write-only {@code CALL} and a {@code RETURN}
   * must keep emitting one row per matched outer row: the unit subquery preserves
   * the outer cardinality even though it has no {@code RETURN} of its own.
   */
  @Test
  void writeOnlyCallPreservesOuterCardinalityWhenOuterReturnExists() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (:Node {id: 1}), (:Node {id: 2}), (:Node {id: 3})"));

    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "MATCH (n:Node) CALL { CREATE (:Side) } RETURN n.id AS id ORDER BY id");

      final List<Result> rows = drain(rs);
      assertThat(rows).hasSize(3);
      assertThat(((Number) rows.get(0).getProperty("id")).longValue()).isEqualTo(1L);
      assertThat(((Number) rows.get(1).getProperty("id")).longValue()).isEqualTo(2L);
      assertThat(((Number) rows.get(2).getProperty("id")).longValue()).isEqualTo(3L);

      final ResultSet sideCount = database.query("opencypher", "MATCH (s:Side) RETURN count(s) AS c");
      assertThat(sideCount.hasNext()).isTrue();
      assertThat(((Number) sideCount.next().getProperty("c")).longValue()).isEqualTo(3L);
    });
  }

  /**
   * Outer {@code MATCH} followed by a write-only {@code CALL} with NO outer
   * {@code RETURN}: Neo4j returns zero rows because the query as a whole has no
   * projection. The side effects of the inner {@code CREATE} must still happen.
   */
  @Test
  void writeOnlyCallWithNoOuterReturnReturnsZeroRowsButPerformsWrites() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (:Node {id: 1}), (:Node {id: 2})"));

    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "MATCH (n:Node) CALL { CREATE (:Side) }");
      assertThat(drain(rs)).isEmpty();

      final ResultSet sideCount = database.query("opencypher", "MATCH (s:Side) RETURN count(s) AS c");
      assertThat(sideCount.hasNext()).isTrue();
      assertThat(((Number) sideCount.next().getProperty("c")).longValue()).isEqualTo(2L);
    });
  }

  /**
   * IN TRANSACTIONS variant: same Neo4j semantics apply, the inner side effect
   * must commit and zero rows must be returned.
   */
  @Test
  void topLevelWriteOnlyCallInTransactionsReturnsZeroRows() {
    final ResultSet rs = database.command("opencypher",
        "CALL { CREATE (:N) } IN TRANSACTIONS OF 1000 ROWS");
    assertThat(drain(rs)).isEmpty();

    final ResultSet count = database.query("opencypher", "MATCH (n:N) RETURN count(n) AS c");
    assertThat(count.hasNext()).isTrue();
    assertThat(((Number) count.next().getProperty("c")).longValue()).isEqualTo(1L);
  }

  private static List<Result> drain(final ResultSet rs) {
    final List<Result> rows = new ArrayList<>();
    while (rs.hasNext())
      rows.add(rs.next());
    return rows;
  }
}
