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
 * Regression test for issue #5165: a correlated {@code count { ... }} subquery whose inner
 * {@code WHERE} predicate combines {@code IS NULL} with another predicate using {@code OR}
 * (e.g. {@code WHERE f.active IS NULL OR f.active = true}) returned wrong counts.
 * <p>
 * The injected correlation condition ({@code id(u) = $__count_u AND ...}) was placed before the
 * OR-containing predicate without parentheses, so operator precedence turned it into
 * {@code (id(u) = ... AND f.active IS NULL) OR f.active = true}, decoupling the OR branch from the
 * correlated outer row and counting matches globally.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5165CountSubqueryOrPredicateTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue5165count").create();
    database.transaction(() -> {
      database.command("opencypher",
          """
          CREATE
            (u1:User {id:1, active:true}),
            (u2:User {id:2, active:true}),
            (u3:User {id:3, active:true}),
            (u4:User {id:4, active:false}),
            (u5:User {id:5, active:false}),
            (u6:User {id:6, active:false}),
            (u1)-[:FRIEND]->(u4),
            (u1)-[:FRIEND]->(u5),
            (u2)-[:FRIEND]->(u4),
            (u2)-[:FRIEND]->(u1),
            (u3)-[:FRIEND]->(u5),
            (u3)-[:FRIEND]->(u6)""");
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  /**
   * Failing query from the issue: only u2 has a friend with active=true; none has active IS NULL.
   * Expected: u1=0, u2=1, u3=0 (matches Neo4j 2026.05.0).
   */
  @Test
  void countSubqueryWithIsNullOrPredicate() {
    final List<Result> rows = collect(database.query("opencypher",
        """
        MATCH (u:User {active:true})
        RETURN u.id AS id,
          count {
            MATCH (u)-[:FRIEND]->(f:User)
            WHERE f.active IS NULL OR f.active = true
          } AS cnt
        ORDER BY u.id"""));

    assertThat(rows).hasSize(3);
    assertThat(((Number) rows.get(0).getProperty("cnt")).longValue()).isEqualTo(0L);
    assertThat(((Number) rows.get(1).getProperty("cnt")).longValue()).isEqualTo(1L);
    assertThat(((Number) rows.get(2).getProperty("cnt")).longValue()).isEqualTo(0L);
  }

  /**
   * Control: same expression without OR must keep working.
   */
  @Test
  void countSubqueryWithSimplePredicate() {
    final List<Result> rows = collect(database.query("opencypher",
        """
        MATCH (u:User {active:true})
        RETURN u.id AS id,
          count {
            MATCH (u)-[:FRIEND]->(f:User)
            WHERE f.active = true
          } AS cnt
        ORDER BY u.id"""));

    assertThat(rows).hasSize(3);
    assertThat(((Number) rows.get(0).getProperty("cnt")).longValue()).isEqualTo(0L);
    assertThat(((Number) rows.get(1).getProperty("cnt")).longValue()).isEqualTo(1L);
    assertThat(((Number) rows.get(2).getProperty("cnt")).longValue()).isEqualTo(0L);
  }

  /**
   * Control: IS NULL alone counts zero for every outer row.
   */
  @Test
  void countSubqueryWithIsNullOnly() {
    final List<Result> rows = collect(database.query("opencypher",
        """
        MATCH (u:User {active:true})
        RETURN u.id AS id,
          count {
            MATCH (u)-[:FRIEND]->(f:User)
            WHERE f.active IS NULL
          } AS cnt
        ORDER BY u.id"""));

    assertThat(rows).hasSize(3);
    assertThat(((Number) rows.get(0).getProperty("cnt")).longValue()).isEqualTo(0L);
    assertThat(((Number) rows.get(1).getProperty("cnt")).longValue()).isEqualTo(0L);
    assertThat(((Number) rows.get(2).getProperty("cnt")).longValue()).isEqualTo(0L);
  }

  /**
   * Control: basic correlated count without WHERE counts all friends.
   */
  @Test
  void countSubqueryWithoutWhere() {
    final List<Result> rows = collect(database.query("opencypher",
        """
        MATCH (u:User {active:true})
        RETURN u.id AS id,
          count {
            MATCH (u)-[:FRIEND]->(:User)
          } AS cnt
        ORDER BY u.id"""));

    assertThat(rows).hasSize(3);
    assertThat(((Number) rows.get(0).getProperty("cnt")).longValue()).isEqualTo(2L);
    assertThat(((Number) rows.get(1).getProperty("cnt")).longValue()).isEqualTo(2L);
    assertThat(((Number) rows.get(2).getProperty("cnt")).longValue()).isEqualTo(2L);
  }

  private static List<Result> collect(final ResultSet rs) {
    final List<Result> list = new ArrayList<>();
    while (rs.hasNext())
      list.add(rs.next());
    return list;
  }
}
