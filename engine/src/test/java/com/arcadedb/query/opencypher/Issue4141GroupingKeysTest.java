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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #4141 (ISO GQL / Cypher 25 cleanup): grouping keys in aggregating RETURN must be explicit.
 * <p>
 * GQL forbids ambiguous (implicit) grouping: an aggregating projection that also references a
 * non-grouping-key variable/property is an error. This verifies that explicit grouping works as
 * expected and that ambiguous grouping is rejected with {@code AmbiguousAggregationExpression}.
 */
class Issue4141GroupingKeysTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/testIssue4141GroupingKeys");
    if (factory.exists())
      factory.open().drop(); // defend against a leftover db from a previously interrupted run
    database = factory.create();
    database.getSchema().createVertexType("Person");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (p:Person {name: 'Alice', dept: 'eng', team: 'core', age: 30})");
      database.command("opencypher", "CREATE (p:Person {name: 'Bob',   dept: 'eng', team: 'core', age: 40})");
      database.command("opencypher", "CREATE (p:Person {name: 'Carol', dept: 'sales', team: 'west', age: 50})");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  // ---- Explicit grouping keys work ------------------------------------------------------------

  @Test
  void singleGroupingKey() {
    final Map<String, Long> counts = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person) RETURN p.dept AS dept, count(*) AS c")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        counts.put(r.getProperty("dept"), ((Number) r.getProperty("c")).longValue());
      }
    }
    assertThat(counts).containsEntry("eng", 2L).containsEntry("sales", 1L);
  }

  @Test
  void multipleGroupingKeys() {
    int rows = 0;
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person) RETURN p.dept AS dept, p.team AS team, count(*) AS c")) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    // (eng,core) and (sales,west) -> 2 distinct groups
    assertThat(rows).isEqualTo(2);
  }

  @Test
  void aggregationMixedWithAGroupingKeyIsAllowed() {
    // p.age is a grouping key here (first item), so 'count(*) + p.age' is unambiguous.
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person) RETURN p.age AS age, count(*) + p.age AS expr ORDER BY age")) {
      assertThat(rs.hasNext()).isTrue();
      final Result first = rs.next();
      // age 30 appears once -> 1 + 30 = 31
      assertThat(((Number) first.getProperty("age")).longValue()).isEqualTo(30L);
      assertThat(((Number) first.getProperty("expr")).longValue()).isEqualTo(31L);
    }
  }

  // ---- Ambiguous (implicit) grouping is rejected ----------------------------------------------

  @Test
  void aggregationMixedWithNonGroupingKeyIsRejected() {
    // p.age is NOT a grouping key (no non-aggregating item references it), so mixing it into an
    // aggregating projection is an ambiguous implicit grouping.
    assertThatThrownBy(() ->
        database.query("opencypher", "MATCH (p:Person) RETURN count(*) + p.age AS expr").close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("AmbiguousAggregationExpression");
  }

  @Test
  void aggregationInWhereIsRejected() {
    assertThatThrownBy(() ->
        database.query("opencypher", "MATCH (p:Person) WHERE count(*) > 1 RETURN p").close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("InvalidAggregation: Aggregation functions are not allowed in WHERE");
  }
}
