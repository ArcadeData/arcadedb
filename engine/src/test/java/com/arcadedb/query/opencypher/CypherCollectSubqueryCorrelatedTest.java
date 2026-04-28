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
 * Regression test for issue #4014: correlated COLLECT { ... } subqueries reading an outer
 * variable's property (e.g. {@code WHERE p2.age > p.age}) must evaluate per outer row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCollectSubqueryCorrelatedTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cyphercollectcorrelated").create();
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
   * Issue #4014: correlated COLLECT subquery reading outer p.age should collect per row.
   * Neo4j returns Alice=[Charlie,Diana], Charlie=[Diana], Diana=[].
   */
  @Test
  void correlatedCollectOuterPropertyInWhere() {
    final ResultSet results = database.query("opencypher",
        """
        MATCH (p:Person)
        WHERE p.age > 25
        RETURN p.name AS name,
               COLLECT {
                 MATCH (p2:Person)
                 WHERE p2.age > p.age
                 RETURN p2.name
               } AS xs
        ORDER BY name""");

    final List<Result> rows = collect(results);

    assertThat(rows).hasSize(3);
    assertThat((String) rows.get(0).getProperty("name")).isEqualTo("Alice");
    assertThat((List<Object>) rows.get(0).getProperty("xs")).containsExactlyInAnyOrder("Charlie", "Diana");
    assertThat((String) rows.get(1).getProperty("name")).isEqualTo("Charlie");
    assertThat((List<Object>) rows.get(1).getProperty("xs")).containsExactly("Diana");
    assertThat((String) rows.get(2).getProperty("name")).isEqualTo("Diana");
    assertThat((List<Object>) rows.get(2).getProperty("xs")).isEmpty();
  }

  /**
   * Issue #4014 alias variant: aliasing p.age via WITH should also work.
   */
  @Test
  void correlatedCollectAliasedOuterValue() {
    final ResultSet results = database.query("opencypher",
        """
        MATCH (p:Person)
        WHERE p.age > 25
        WITH p, p.age AS age
        RETURN p.name AS name,
               COLLECT {
                 MATCH (p2:Person)
                 WHERE p2.age > age
                 RETURN p2.name
               } AS xs
        ORDER BY name""");

    final List<Result> rows = collect(results);

    assertThat(rows).hasSize(3);
    assertThat((List<Object>) rows.get(0).getProperty("xs")).containsExactlyInAnyOrder("Charlie", "Diana");
    assertThat((List<Object>) rows.get(1).getProperty("xs")).containsExactly("Diana");
    assertThat((List<Object>) rows.get(2).getProperty("xs")).isEmpty();
  }

  /**
   * Control: uncorrelated COLLECT must still work.
   */
  @Test
  void uncorrelatedCollect() {
    final ResultSet results = database.query("opencypher",
        """
        MATCH (p:Person)
        WHERE p.age > 25
        RETURN p.name AS name,
               COLLECT {
                 MATCH (p2:Person)
                 WHERE p2.age > 30
                 RETURN p2.name
               } AS xs
        ORDER BY name""");

    final List<Result> rows = collect(results);

    assertThat(rows).hasSize(3);
    for (final Result row : rows) {
      assertThat((List<Object>) row.getProperty("xs")).containsExactlyInAnyOrder("Charlie", "Diana");
    }
  }

  private static List<Result> collect(final ResultSet rs) {
    final List<Result> list = new ArrayList<>();
    while (rs.hasNext())
      list.add(rs.next());
    return list;
  }
}
