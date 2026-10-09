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
 * Regression test for issues #9579 and #9580: an expensive subquery expression ({@code COUNT { }}) was evaluated although
 * a cheap sibling operand had already decided the result.
 * <ul>
 *   <li>#9580: {@code coalesce(null, 0, COUNT { ... })} evaluated the COUNT after the non-null constant {@code 0}.</li>
 *   <li>#9579: {@code COUNT { ... } > 0 AND cheap} ran the COUNT for rows the cheap operand rejects.</li>
 * </ul>
 * The "work" the COUNT would do is observed through a probe that fails when it runs: the body divides by zero for every
 * neighbour it visits, so a COUNT that was evaluated over a node with neighbours raises an error.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9579And9580SubqueryEvaluationOrderTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue9579-9580").create();
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Anchor");
      database.command("sql", "CREATE PROPERTY Anchor.uid LONG");
      database.command("sql", "CREATE PROPERTY Anchor.keep BOOLEAN");
      database.command("sql", "CREATE VERTEX TYPE Leaf");
      database.command("sql", "CREATE PROPERTY Leaf.uid LONG");
      database.command("sql", "CREATE PROPERTY Leaf.mark BOOLEAN");
      database.command("sql", "CREATE EDGE TYPE EXPAND");
    });
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Anchor {uid: 1, keep: false})");
      database.command("opencypher", "CREATE (:Anchor {uid: 2, keep: true})-[:EXPAND]->(:Leaf {uid: 3, mark: true})");
      database.command("opencypher",
          "MATCH (u:Anchor {uid: 1}) UNWIND range(1, 50) AS i CREATE (u)-[:EXPAND]->(:Leaf {uid: 100 + i, mark: false})");
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  // #9580
  @Test
  void coalesceStopsAtTheFirstNonNullArgument() {
    // Every anchor has at least one neighbour, so evaluating the COUNT would hit the division by zero.
    final List<Result> rows = collect("""
        MATCH (u:Anchor {uid: 1})
        RETURN coalesce(null, 0, COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) WHERE l.uid / 0 > 0 }) AS c0""");

    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).getProperty("c0")).longValue()).isZero();
  }

  @Test
  void coalesceStillEvaluatesLaterArgumentsWhenEarlierOnesAreNull() {
    final List<Result> rows = collect("""
        MATCH (u:Anchor {uid: 1})
        RETURN coalesce(null, u.missing, COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) }) AS c0""");

    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).getProperty("c0")).longValue()).isEqualTo(50L);
  }

  @Test
  void coalesceInWhereStopsAtTheFirstNonNullArgument() {
    final List<Result> rows = collect("""
        MATCH (u:Anchor {uid: 1})
        WHERE coalesce(u.uid, COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) WHERE l.uid / 0 > 0 }) = 1
        RETURN u.uid AS c0""");

    assertThat(rows).hasSize(1);
  }

  // #9579
  @Test
  void cheapOperandOnTheRightOfAndIsEvaluatedBeforeTheCount() {
    final List<Result> rows = collect("""
        MATCH (u:Anchor)
        WHERE (COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) WHERE (l.mark = false AND l.uid / 0 > 0) OR l.mark = true } > 0)
          AND (coalesce(u.keep, false) = true) AND u.uid = 2
        RETURN u.uid AS c0""");

    // uid 2 is the only anchor passing the cheap operands, and its only leaf has mark = true.
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).getProperty("c0")).longValue()).isEqualTo(2L);
  }

  @Test
  void bothOperandOrdersGiveTheSameRowsAsTheIssue() {
    final String count = "COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) WHERE coalesce(l.mark, false) = true } > 0";
    final String cheap = "coalesce(u.keep, false) = true";

    final List<Result> q1 = collect("MATCH (u:Anchor) WHERE (" + cheap + ") AND (" + count + ") RETURN u.uid AS c0");
    final List<Result> q2 = collect("MATCH (u:Anchor) WHERE (" + count + ") AND (" + cheap + ") RETURN u.uid AS c0");

    assertThat(q1).hasSize(1);
    assertThat(q2).hasSize(1);
    assertThat(((Number) q1.get(0).getProperty("c0")).longValue()).isEqualTo(2L);
    assertThat(((Number) q2.get(0).getProperty("c0")).longValue()).isEqualTo(2L);
  }

  @Test
  void cheapTrueOperandOnTheRightOfOrSkipsTheCount() {
    final List<Result> rows = collect("""
        MATCH (u:Anchor {uid: 1})
        WHERE (COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) WHERE l.uid / 0 > 0 } > 0) OR u.uid = 1
        RETURN u.uid AS c0""");

    assertThat(rows).hasSize(1);
  }

  @Test
  void returnedBooleanExpressionKeepsThreeValuedSemantics() {
    // keep is false: the cheap operand decides, regardless of what the COUNT would do.
    final List<Result> rows = collect("""
        MATCH (u:Anchor {uid: 1})
        RETURN (COUNT { MATCH (u)-[:EXPAND]->(l:Leaf) WHERE l.uid / 0 > 0 } > 0) AND (u.keep = true) AS c0""");

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).<Boolean>getProperty("c0")).isFalse();
  }

  private List<Result> collect(final String query) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }
}
