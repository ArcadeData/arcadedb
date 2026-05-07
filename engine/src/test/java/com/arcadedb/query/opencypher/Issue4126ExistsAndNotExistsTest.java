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
 * Regression test for GitHub issue #4126.
 * <p>
 * A WHERE clause combining {@code <bool> = true OR EXISTS { ... }} with a grouped
 * {@code AND NOT (EXISTS { ... } OR EXISTS { ... })} returned 0 rows when both
 * outer entities were bound by separate MATCH statements (cross-product), even
 * when the boolean branch alone matched and there were no excluded edges.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4126ExistsAndNotExistsTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4126-exists-and-not-exists").create();
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Owner");
      database.command("sql", "CREATE PROPERTY Owner.id STRING");
      database.command("sql", "CREATE VERTEX TYPE Item");
      database.command("sql", "CREATE PROPERTY Item.id STRING");
      database.command("sql", "CREATE PROPERTY Item.flag BOOLEAN");
      database.command("sql", "CREATE EDGE TYPE rel");
      database.command("sql", "CREATE EDGE TYPE excl");

      database.command("opencypher", "CREATE (:Owner {id: 'u'})");
      database.command("opencypher", "CREATE (:Item  {id: 'c', flag: true})");
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Probe B1 (control): two-MATCH + boolean only must return the Item row.
   */
  @Test
  void twoMatchBooleanOnly() {
    final List<Result> rows = collect(database.query("opencypher",
        "MATCH (u:Owner {id: 'u'}) MATCH (c:Item) WHERE c.flag = true RETURN c.id AS id"));
    assertThat(rows).hasSize(1);
    assertThat((String) rows.get(0).getProperty("id")).isEqualTo("c");
  }

  /**
   * Probe B2 (control): two-MATCH + {@code bool OR EXISTS { ... }} must return the Item row.
   */
  @Test
  void twoMatchBooleanOrExists() {
    final List<Result> rows = collect(database.query("opencypher",
        "MATCH (u:Owner {id: 'u'}) MATCH (c:Item) " +
            "WHERE c.flag = true OR EXISTS { MATCH (u)-[:rel]->(c) } " +
            "RETURN c.id AS id"));
    assertThat(rows).hasSize(1);
    assertThat((String) rows.get(0).getProperty("id")).isEqualTo("c");
  }

  /**
   * Probe B3: two-MATCH + (bool OR EXISTS) AND NOT EXISTS must return the Item row.
   */
  @Test
  void twoMatchBooleanOrExistsAndNotExists() {
    final List<Result> rows = collect(database.query("opencypher",
        "MATCH (u:Owner {id: 'u'}) MATCH (c:Item) " +
            "WHERE (c.flag = true OR EXISTS { MATCH (u)-[:rel]->(c) }) " +
            "  AND NOT EXISTS { MATCH (u)-[:excl]->(c) } " +
            "RETURN c.id AS id"));
    assertThat(rows).hasSize(1);
    assertThat((String) rows.get(0).getProperty("id")).isEqualTo("c");
  }

  /**
   * Probe B4: two-MATCH + grouped exclusion (NOT EXISTS OR NOT EXISTS) must return the Item row.
   */
  @Test
  void twoMatchBooleanOrExistsAndNotGroupedExclusion() {
    final List<Result> rows = collect(database.query("opencypher",
        "MATCH (u:Owner {id: 'u'}) MATCH (c:Item) " +
            "WHERE (c.flag = true OR EXISTS { MATCH (u)-[:rel]->(c) }) " +
            "  AND NOT (EXISTS { MATCH (u)-[:excl]->(c) } " +
            "        OR EXISTS { MATCH (u)-[:excl*1..3]->(c) }) " +
            "RETURN c.id AS id"));
    assertThat(rows).hasSize(1);
    assertThat((String) rows.get(0).getProperty("id")).isEqualTo("c");
  }

  /**
   * Probe B5: same WHERE shape wrapped under a single outer MATCH must return the Item row.
   */
  @Test
  void singleMatchWithExistsWrappingMatchAndWhere() {
    final List<Result> rows = collect(database.query("opencypher",
        "MATCH (c:Item) " +
            "WHERE EXISTS { " +
            "  MATCH (u:Owner {id: 'u'}) " +
            "  WHERE (c.flag = true OR EXISTS { MATCH (u)-[:rel]->(c) }) " +
            "    AND NOT (EXISTS { MATCH (u)-[:excl]->(c) } " +
            "          OR EXISTS { MATCH (u)-[:excl*1..3]->(c) }) " +
            "} " +
            "RETURN c.id AS id"));
    assertThat(rows).hasSize(1);
    assertThat((String) rows.get(0).getProperty("id")).isEqualTo("c");
  }

  private static List<Result> collect(final ResultSet rs) {
    final List<Result> out = new ArrayList<>();
    while (rs.hasNext())
      out.add(rs.next());
    return out;
  }
}
