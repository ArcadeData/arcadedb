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
 * Regression test for issue #4015: correlated COUNT { ... } subqueries reading an outer
 * variable's property (e.g. {@code WHERE p2.age > p.age}) must evaluate per outer row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCountSubqueryCorrelatedTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cyphercountcorrelated").create();
    database.transaction(() -> {
      database.getSchema().createVertexType("Person");
      database.newVertex("Person").set("name", "Alice").set("age", 30).save();
      database.newVertex("Person").set("name", "Bob").set("age", 25).save();
      database.newVertex("Person").set("name", "Charlie").set("age", 35).save();
      database.newVertex("Person").set("name", "Diana").set("age", 40).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  /**
   * Issue #4015: correlated COUNT subquery reading outer p.age should count properly per row.
   * Neo4j returns Alice=2, Charlie=1, Diana=0.
   */
  @Test
  void correlatedCountOuterPropertyInWhere() {
    final ResultSet results = database.query("opencypher",
        """
        MATCH (p:Person)
        WHERE p.age > 25
        RETURN p.name AS name,
               COUNT {
                 MATCH (p2:Person)
                 WHERE p2.age > p.age
                 RETURN p2
               } AS older_count
        ORDER BY name""");

    final List<Result> rows = collect(results);

    assertThat(rows).hasSize(3);
    assertThat((String) rows.get(0).getProperty("name")).isEqualTo("Alice");
    assertThat(((Number) rows.get(0).getProperty("older_count")).longValue()).isEqualTo(2L);
    assertThat((String) rows.get(1).getProperty("name")).isEqualTo("Charlie");
    assertThat(((Number) rows.get(1).getProperty("older_count")).longValue()).isEqualTo(1L);
    assertThat((String) rows.get(2).getProperty("name")).isEqualTo("Diana");
    assertThat(((Number) rows.get(2).getProperty("older_count")).longValue()).isEqualTo(0L);
  }

  /**
   * Control: the equivalent OPTIONAL MATCH + count() form must also produce the same result.
   */
  @Test
  void correlatedCountControlOptionalMatch() {
    final ResultSet results = database.query("opencypher",
        """
        MATCH (p:Person)
        WHERE p.age > 25
        OPTIONAL MATCH (p2:Person)
        WHERE p2.age > p.age
        RETURN p.name AS name, count(p2) AS older_count
        ORDER BY name""");

    final List<Result> rows = collect(results);

    assertThat(rows).hasSize(3);
    assertThat(((Number) rows.get(0).getProperty("older_count")).longValue()).isEqualTo(2L);
    assertThat(((Number) rows.get(1).getProperty("older_count")).longValue()).isEqualTo(1L);
    assertThat(((Number) rows.get(2).getProperty("older_count")).longValue()).isEqualTo(0L);
  }

  /**
   * Control: an uncorrelated COUNT subquery with a constant comparison should also work.
   */
  @Test
  void uncorrelatedCount() {
    final ResultSet results = database.query("opencypher",
        """
        MATCH (p:Person)
        WHERE p.age > 25
        RETURN p.name AS name,
               COUNT {
                 MATCH (p2:Person)
                 WHERE p2.age > 30
                 RETURN p2
               } AS older_count
        ORDER BY name""");

    final List<Result> rows = collect(results);

    assertThat(rows).hasSize(3);
    assertThat(((Number) rows.get(0).getProperty("older_count")).longValue()).isEqualTo(2L);
    assertThat(((Number) rows.get(1).getProperty("older_count")).longValue()).isEqualTo(2L);
    assertThat(((Number) rows.get(2).getProperty("older_count")).longValue()).isEqualTo(2L);
  }

  private static List<Result> collect(final ResultSet rs) {
    final List<Result> list = new ArrayList<>();
    while (rs.hasNext())
      list.add(rs.next());
    return list;
  }
}
