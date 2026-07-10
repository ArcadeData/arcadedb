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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for issue #5005: CALL subquery with ORDER BY ... LIMIT must return the
 * correct (minimum) value, matching Neo4j semantics. The variable-length path endpoint bound
 * via the importing WITH must remain constrained even when ORDER BY + LIMIT are present.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5005SubqueryOrderByLimitTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-issue5005").create();
    database.command("opencypher",
        """
        CREATE (alice:Person {name: 'Alice'}), (bob:Person {name: 'Bob'}), \
        (carol:Person {name: 'Carol'}), (dave:Person {name: 'Dave'}), \
        (eve:Person {name: 'Eve'}), \
        (alice)-[:FOLLOWS]->(bob), \
        (bob)-[:FOLLOWS]->(carol), \
        (alice)-[:FOLLOWS]->(dave), \
        (dave)-[:FOLLOWS]->(carol), \
        (carol)-[:FOLLOWS]->(eve)""");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private long single(final String query, final String column) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      return ((Number) row.getProperty(column)).longValue();
    }
  }

  @Test
  void directOrderByLimit() {
    final long len = single(
        """
        MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) \
        MATCH p = (a)-[:FOLLOWS*1..4]->(c) \
        RETURN length(p) AS len ORDER BY len LIMIT 1""", "len");
    assertThat(len).isEqualTo(2);
  }

  @Test
  void subqueryMinAggregation() {
    final long distance = single(
        """
        MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) \
        CALL { WITH a, c MATCH p = (a)-[:FOLLOWS*1..4]->(c) RETURN p, length(p) AS len } \
        RETURN min(len) AS distance""", "distance");
    assertThat(distance).isEqualTo(2);
  }

  @Test
  void subqueryOrderByLimit() {
    final long distance = single(
        """
        MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) \
        CALL { WITH a, c MATCH p = (a)-[:FOLLOWS*1..4]->(c) RETURN length(p) AS len ORDER BY len LIMIT 1 } \
        RETURN len AS distance""", "distance");
    assertThat(distance).isEqualTo(2);
  }
}
